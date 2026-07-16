import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import snowflake.connector


def discover_csv_files(local_dir: Path) -> list[Path]:
    csv_files = sorted(local_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {local_dir}. Drop the 8 source CSVs into data/raw/ first."
        )
    return csv_files


def upload_csvs_to_s3(s3_client: Any, bucket: str, csv_files: list[Path], log) -> list[dict]:
    """Uploads each CSV directly under s3://<bucket>/raw/<filename>.csv - flat,
    no date/table subfolders. Returns the imported file list (the 'remembered' record)."""
    imported_files = []
    for csv_path in csv_files:
        s3_key = f"raw/{csv_path.name}"
        file_size = csv_path.stat().st_size
        s3_client.upload_file(str(csv_path), bucket, s3_key)
        imported_files.append(
            {
                "file": csv_path.name,
                "table": csv_path.stem,
                "s3_key": s3_key,
                "size_bytes": file_size,
            }
        )
        log.info(f"Uploaded {csv_path.name} -> s3://{bucket}/{s3_key} ({file_size:,} bytes)")
    return imported_files


def build_ingestion_summary(imported_files: list[dict], bucket: str) -> str:
    total_bytes = sum(f["size_bytes"] for f in imported_files)
    file_list_text = "\n".join(f"- `{f['file']}` -> `{f['s3_key']}`" for f in imported_files)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f":inbox_tray: [{ts}] Ingested {len(imported_files)} file(s) "
        f"({total_bytes:,} bytes total) to s3://{bucket}/raw/\n{file_list_text}"
    )


def post_slack_message(text: str, log) -> bool:
    """Returns True if sent. Never raises - a Slack failure shouldn't fail the pipeline."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL not set - skipping Slack notification.")
        return False
    try:
        requests.post(webhook_url, json={"text": text}, timeout=10)
        return True
    except Exception as e:
        log.warning(f"Slack notification failed (non-fatal): {e}")
        return False



def query_snowflake_schemas(database: str) -> list[str]:
    """Returns every schema name that exists in the given Snowflake database."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
    )
    try:
        cur = conn.cursor()
        cur.execute("SHOW SCHEMAS")
        rows = cur.fetchall()
        return [row[1] for row in rows]  
    finally:
        conn.close()

def query_snowflake_tables(database: str, schema: str) -> list[str]:
    """Returns every table name that exists in the given Snowflake schema."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN SCHEMA {schema}")
        rows = cur.fetchall()
        return [row[1] for row in rows]  
    finally:
        conn.close()


def build_transform_summary(tables: list[str], schema: str) -> str:
    table_list_text = "\n".join(f"- `{t}`" for t in sorted(tables))
    return (
        f":sparkles: Data successfully transformed into `{schema}`.\n"
        f"{len(tables)} table(s) created:\n{table_list_text}"
    )