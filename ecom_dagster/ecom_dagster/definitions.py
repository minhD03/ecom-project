import os

from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    load_assets_from_modules,
    run_status_sensor,
    DagsterRunStatus,
    RunStatusSensorContext,
)
from dagster_aws.s3 import S3Resource

from ecom_dagster import assets
from ecom_dagster.tasks import post_slack_message


def get_s3_resource() -> S3Resource:
    return S3Resource(
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


all_assets = load_assets_from_modules([assets])

ingestion_job = define_asset_job(
    name="ecom_pipeline",
    selection=AssetSelection.all(),
    description="This is my ecom-pipeline that controls the tasks from Ingesting data to Amazon S3 Servers (Data Lake), transfer to Snowflake (Data Warehouse) and then transform in there.",
)


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def slack_on_pipeline_success(context: RunStatusSensorContext):
    post_slack_message(f":white_check_mark: *{context.dagster_run.job_name}* succeeded.", context.log)


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def slack_on_pipeline_failure(context: RunStatusSensorContext):
    error = context.failure_event.message if context.failure_event else "unknown error"
    post_slack_message(
        f":x: *{context.dagster_run.job_name}* FAILED.\n```{error[:500]}```", context.log
    )


defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job],
    resources={"s3": get_s3_resource()},
    sensors=[slack_on_pipeline_success, slack_on_pipeline_failure],
)