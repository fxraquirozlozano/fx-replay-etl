"""
appcues_to_bigquery.py

Daily DAG that ingests Appcues survey responses (NPS + forms + future surveys)
into `appcues.responses` in BigQuery using the "long" schema (one row per
event x answered question).

All the heavy lifting lives in `appcues_lib.py`. This file only handles:
  - Reading configuration from Airflow Variables (with env-var overrides).
  - Bookmark persistence between runs.
  - Window resolution for incremental loads.
  - Wiring the two tasks together in a DAG.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from typing import Any

from airflow import DAG
try:
    from airflow.sdk import Variable, get_current_context, task
except ImportError:
    from airflow.decorators import task
    from airflow.models import Variable
    try:
        from airflow.operators.python import get_current_context
    except ImportError:
        from airflow.decorators import get_current_context
from google.cloud import bigquery

from appcues_lib import (
    AppcuesConfig,
    download_export,
    extract_rows_from_export,
    get_bq_client,
    load_rows,
    merge_raw_into_history,
    resolve_download_url,
    submit_export_job,
    wait_for_export_job,
)


# --------------------------------------------------------------------------- #
# Airflow-side configuration
# --------------------------------------------------------------------------- #

def airflow_var(name: str, default: str) -> str:
    """Env var first (dev-friendly), then Airflow Variable, then default."""
    env_value = os.getenv(name)
    if env_value not in (None, ""):
        return env_value

    try:
        return Variable.get(name)
    except Exception:
        return default


PROJECT_ID = airflow_var("APPCUES_GCP_PROJECT_ID", "fxr-analytics")
SCHEDULE = airflow_var("APPCUES_DAG_SCHEDULE", "0 7 * * *")
RAW_BQ_DATASET = airflow_var("APPCUES_BQ_DATASET", "appcues_raw")
FINAL_BQ_DATASET = airflow_var("APPCUES_FINAL_BQ_DATASET", "appcues")
BQ_TABLE = airflow_var("APPCUES_BQ_TABLE", "responses")
DEFAULT_LOOKBACK_DAYS = int(airflow_var("APPCUES_DEFAULT_LOOKBACK_DAYS", "2"))
STATE_VAR_NAME = airflow_var("APPCUES_BOOKMARKS_VAR_NAME", "appcues_bookmarks")

STREAM_NAME = "responses"


def _build_config() -> AppcuesConfig:
    return AppcuesConfig(
        project_id=PROJECT_ID,
        account_id_secret_id=airflow_var(
            "APPCUES_ACCOUNT_ID_SECRET_ID", "APPCUES_ACCOUNT_ID"
        ),
        api_key_secret_id=airflow_var(
            "APPCUES_API_KEY_SECRET_ID", "APPCUES_API_KEY"
        ),
        api_secret_secret_id=airflow_var(
            "APPCUES_API_SECRET_SECRET_ID", "APPCUES_API_SECRET"
        ),
        request_timeout_seconds=int(
            airflow_var("APPCUES_REQUEST_TIMEOUT_SECONDS", "120")
        ),
        export_poll_seconds=int(
            airflow_var("APPCUES_EXPORT_POLL_SECONDS", "10")
        ),
        export_timeout_seconds=int(
            airflow_var("APPCUES_EXPORT_TIMEOUT_SECONDS", "1800")
        ),
    )


# --------------------------------------------------------------------------- #
# Bookmarks
# --------------------------------------------------------------------------- #

def get_bookmarks() -> dict[str, dict[str, str]]:
    try:
        raw = Variable.get(STATE_VAR_NAME)
    except Exception:
        raw = "{}"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def set_bookmark(stream_name: str, bookmark_field: str, value: str) -> None:
    bookmarks = get_bookmarks()
    bookmarks.setdefault(stream_name, {})
    bookmarks[stream_name][bookmark_field] = value
    Variable.set(STATE_VAR_NAME, json.dumps(bookmarks))


def parse_iso_date(value: str) -> date:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


# --------------------------------------------------------------------------- #
# Window resolution
# --------------------------------------------------------------------------- #

def resolve_window() -> tuple[date, date, bool]:
    context = get_current_context()
    ds = datetime.fromisoformat(context["ds"]).date()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    full_refresh = bool(conf.get("full_refresh", False))

    if conf.get("start_date"):
        start_date = datetime.fromisoformat(conf["start_date"]).date()
    else:
        start_date = ds - timedelta(
            days=int(conf.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
        )

    end_date = (
        datetime.fromisoformat(conf["end_date"]).date()
        if conf.get("end_date")
        else ds
    )

    if not full_refresh and not conf.get("start_date"):
        bookmark = get_bookmarks().get(STREAM_NAME, {}).get("event_date")
        if bookmark:
            start_date = max(start_date, parse_iso_date(bookmark))

    return start_date, end_date, full_refresh


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #

def load_appcues_raw() -> dict[str, Any]:
    config = _build_config()
    client = get_bq_client(PROJECT_ID)
    start_date, end_date, _ = resolve_window()

    raw_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{BQ_TABLE}"
    final_table_ref = f"{PROJECT_ID}.{FINAL_BQ_DATASET}.{BQ_TABLE}"

    if start_date > end_date:
        load_rows(
            client,
            raw_table_ref,
            [],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        return {
            "raw_table": raw_table_ref,
            "final_table": final_table_ref,
            "rows": 0,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

    job_url = submit_export_job(config, start_date, end_date)
    job_payload = wait_for_export_job(config, job_url)
    download_url = resolve_download_url(job_payload)
    export_file_path = download_export(config, download_url)

    ingested_at = datetime.now(UTC).isoformat()
    try:
        rows = extract_rows_from_export(export_file_path, ingested_at)
    finally:
        if os.path.exists(export_file_path):
            os.remove(export_file_path)

    load_rows(
        client,
        raw_table_ref,
        rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    return {
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "rows": len(rows),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def upsert_appcues_history(load_result: dict[str, Any]) -> dict[str, Any]:
    client = get_bq_client(PROJECT_ID)
    raw_table_ref = str(load_result["raw_table"])
    final_table_ref = str(load_result["final_table"])
    row_count = int(load_result.get("rows", 0))

    if row_count:
        merge_raw_into_history(client, raw_table_ref, final_table_ref)

    end_date = str(load_result["end_date"])
    set_bookmark(STREAM_NAME, "event_date", end_date)

    return {
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "rows": row_count,
        "start_date": str(load_result["start_date"]),
        "end_date": end_date,
    }


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #

with DAG(
    dag_id="appcues_to_bigquery",
    description=(
        "Extracts Appcues survey responses (NPS + forms + future surveys) "
        "from the export API and loads them into BigQuery using a long schema."
    ),
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["appcues", "surveys", "nps", "bigquery"],
) as dag:
    load_raw_task = task(task_id="load_appcues_raw")(load_appcues_raw)()
    upsert_history_task = task(task_id="upsert_appcues_history")(upsert_appcues_history)(
        load_raw_task
    )

    load_raw_task >> upsert_history_task
