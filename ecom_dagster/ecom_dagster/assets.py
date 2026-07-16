import os
from pathlib import Path

from dagster import asset, AssetExecutionContext, MetadataValue
from dagster_aws.s3 import S3Resource

from ecom_dagster.tasks import (
    discover_csv_files,
    upload_csvs_to_s3,
    build_ingestion_summary,
    post_slack_message,
)
import subprocess
import sys
from ecom_dagster.tasks import query_snowflake_schemas
from ecom_dagster.tasks import query_snowflake_tables, build_transform_summary

LOCAL_RAW_DIR = Path("/opt/dagster/data/raw")

REQUIRED_ENV_VARS = [
    "DAGSTER_POSTGRES_USER", "DAGSTER_POSTGRES_PASSWORD", "DAGSTER_POSTGRES_DB",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_S3_BUCKET",
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
    "SLACK_WEBHOOK_URL", "SLACK_CHANNEL", "DBT_TARGET",
]

SPARK_JOB_PATH = "/opt/dagster/app/spark_jobs/load_to_snowflake.py"

SPARK_PACKAGES = ",".join([
    "net.snowflake:snowflake-jdbc:3.13.30",
    "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


DBT_PROJECT_DIR = "/opt/dagster/app/ecom_dbt"

REQUIRED_DBT_ENV_VARS = ["SNOWFLAKE_SCHEMA1", "DBT_TARGET"]

EXPECTED_DIM_FACT_TABLES = [
    "DIM_CUSTOMERS",
    "DIM_PRODUCT_CATEGORY_NAME_TRANSLATION",
    "DIM_SELLER",
    "FACT_ORDER_REVIEWS",
    "FACT_ORDER_ITEMS",
    "FACT_ORDER_PAYMENTS",
    "FACT_ORDERS",
    "FACT_PRODUCTS",
]


@asset(group_name="environment", compute_kind="python")
def environment_check(context: AssetExecutionContext) -> None:
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v, "").strip()]
    present = [v for v in REQUIRED_ENV_VARS if v not in missing]

    context.add_output_metadata(
        {
            "required_count": len(REQUIRED_ENV_VARS),
            "present_count": len(present),
            "missing_count": len(missing),
            "missing_vars": MetadataValue.json(missing),
        }
    )

    if missing:
        raise EnvironmentError(
            f"Missing {len(missing)} required environment variable(s): {', '.join(missing)}. "
            f"Fill these in .env before running the pipeline."
        )
    context.log.info(f"All {len(REQUIRED_ENV_VARS)} required environment variables are set.")


@asset(group_name="environment", compute_kind="python", deps=[environment_check])
def raw_data_directory_check(context: AssetExecutionContext) -> None:
    if not LOCAL_RAW_DIR.exists():
        raise FileNotFoundError(f"Directory {LOCAL_RAW_DIR} does not exist.")

    csv_files = sorted(LOCAL_RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"{LOCAL_RAW_DIR} is empty - drop the 8 source CSVs there before running ingestion."
        )

    context.log.info(f"Found {len(csv_files)} CSV file(s) in {LOCAL_RAW_DIR}")
    context.add_output_metadata(
        {
            "file_count": len(csv_files),
            "files": MetadataValue.json([f.name for f in csv_files]),
        }
    )


@asset(group_name="ingestion", compute_kind="python", deps=[raw_data_directory_check])
def raw_csvs_ingested_to_s3(context: AssetExecutionContext, s3: S3Resource) -> None:
    bucket = os.getenv("AWS_S3_BUCKET")

    csv_files = discover_csv_files(LOCAL_RAW_DIR)
    imported_files = upload_csvs_to_s3(s3.get_client(), bucket, csv_files, context.log)

    summary_text = build_ingestion_summary(imported_files, bucket)
    post_slack_message(summary_text, context.log)

    context.add_output_metadata(
        {
            "num_files_imported": len(imported_files),
            "total_bytes": sum(f["size_bytes"] for f in imported_files),
            "s3_bucket": bucket,
            "imported_files": MetadataValue.json(imported_files),
            "preview": MetadataValue.md(
                "| File | Table | S3 key |\n|---|---|---|\n"
                + "\n".join(
                    f"| {f['file']} | {f['table']} | `{f['s3_key']}` |" for f in imported_files
                )
            ),
        }
    )

@asset(group_name="warehouse_load", compute_kind="pyspark", deps=[raw_csvs_ingested_to_s3])
def snowflake_raw_loaded(context: AssetExecutionContext) -> None:
    cmd = [
        "spark-submit",
        "--master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "--packages", SPARK_PACKAGES,
        SPARK_JOB_PATH,
    ]
    context.log.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, env=os.environ.copy())

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"spark-submit failed with exit code {result.returncode}")

    context.add_output_metadata({"spark_submit_log_tail": MetadataValue.text(result.stdout[-3000:])})



@asset(group_name="warehouse_transform", compute_kind="dbt", deps=[snowflake_raw_loaded])
def dbt_dim_fact_transformed(context: AssetExecutionContext) -> None:
    """Runs dbt build on dim + fact models (raw is ephemeral, inlined automatically).
    `build` runs models AND their schema tests together, in dependency order."""
    cmd = [
        "dbt", "build",
        "--select", "dim", "--select", "fact",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ]
    context.log.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, env=os.environ.copy())
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"dbt build --select dim fact failed with exit code {result.returncode}")

    context.add_output_metadata({"dbt_build_log_tail": MetadataValue.text(result.stdout[-3000:])})



@asset(group_name="warehouse_transform", compute_kind="python", deps=[dbt_dim_fact_transformed])
def dbt_transform_verified_and_notified(context: AssetExecutionContext) -> None:
    """Confirms every expected dim/fact table genuinely exists in dev_michael,
    then posts a Slack summary listing what was created."""
    database = os.environ["SNOWFLAKE_DATABASE"]
    schema = os.environ["SNOWFLAKE_SCHEMA1"]

    actual_tables = [t.upper() for t in query_snowflake_tables(database, schema)]
    missing = [t for t in EXPECTED_DIM_FACT_TABLES if t not in actual_tables]
    if missing:
        raise Exception(f"Expected table(s) missing from '{schema}' after dbt build: {missing}")

    context.log.info(f"Confirmed all {len(EXPECTED_DIM_FACT_TABLES)} dim/fact tables exist in '{schema}'.")

    summary_text = build_transform_summary(EXPECTED_DIM_FACT_TABLES, schema)
    post_slack_message(summary_text, context.log)

    context.add_output_metadata(
        {
            "schema": schema,
            "tables_confirmed": MetadataValue.json(EXPECTED_DIM_FACT_TABLES),
        }
    )


