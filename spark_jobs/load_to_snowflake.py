import os
import sys
import boto3
from pyspark.sql import SparkSession


def list_raw_csv_keys(bucket: str, region: str) -> list[dict]:
    """Lists every .csv under s3://<bucket>/raw/ and derives a Snowflake
    table name from each filename - same discovery pattern as tasks.py's
    discover_csv_files, just against S3 instead of local disk."""
    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix="raw/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                filename = key.rsplit("/", 1)[-1]
                table_name = filename.replace(".csv", "").upper()
                keys.append({"key": key, "filename": filename, "table": table_name})
    if not keys:
        raise FileNotFoundError(f"No CSV files found under s3://{bucket}/raw/")
    return keys


def build_spark_session(bucket_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("s3_to_snowflake_raw_load")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{bucket_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def snowflake_options() -> dict:
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfRole": os.environ["SNOWFLAKE_ROLE"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    }


def main():
    bucket = os.environ["AWS_S3_BUCKET"]
    region = os.environ["AWS_REGION"]

    csv_objects = list_raw_csv_keys(bucket, region)
    print(f"[load_to_snowflake] Found {len(csv_objects)} CSV file(s) in s3://{bucket}/raw/")

    spark = build_spark_session(region)
    sf_options = snowflake_options()

    loaded = []
    for obj in csv_objects:
        s3_path = f"s3a://{bucket}/{obj['key']}"
        print(f"[load_to_snowflake] Reading {s3_path} -> table {obj['table']}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(s3_path)
        )
        
        row_count = df.count()

        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", obj["table"])
            .mode("overwrite")
            .save()
        )
        loaded.append({"table": obj["table"], "rows": row_count})
        print(f"[load_to_snowflake] Loaded {row_count:,} rows into {obj['table']}")

    print("[load_to_snowflake] SUMMARY:")
    for entry in loaded:
        print(f"  - {entry['table']}: {entry['rows']:,} rows")

    spark.stop()


if __name__ == "__main__":
    main()