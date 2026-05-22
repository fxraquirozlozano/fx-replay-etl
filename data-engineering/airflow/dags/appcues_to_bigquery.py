from __future__ import annotations

import gzip
import json
import os
import tempfile
import time
import zipfile
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests
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
from google.cloud import secretmanager
from requests.auth import HTTPBasicAuth


APPCUES_API_BASE_URL = "https://api.appcues.com/v2"
APPCUES_EVENT_NAME = "appcues:v2:step_interaction"
STREAM_NAME = "nps_events"


def airflow_var(name: str, default: str) -> str:
    env_value = os.getenv(name)
    if env_value not in (None, ""):
        return env_value

    try:
        return Variable.get(name)
    except Exception:
        return default


PROJECT_ID = airflow_var("APPCUES_GCP_PROJECT_ID", "fxr-analytics")
SCHEDULE = airflow_var("APPCUES_DAG_SCHEDULE", "0 * * * *")
RAW_BQ_DATASET = airflow_var("APPCUES_BQ_DATASET", "appcues_raw")
FINAL_BQ_DATASET = airflow_var("APPCUES_FINAL_BQ_DATASET", "appcues")
BQ_TABLE = airflow_var("APPCUES_BQ_TABLE", "nps_events")
DEFAULT_LOOKBACK_DAYS = int(airflow_var("APPCUES_DEFAULT_LOOKBACK_DAYS", "2"))
EXPORT_JOB_TIMEOUT_SECONDS = int(
    airflow_var("APPCUES_EXPORT_TIMEOUT_SECONDS", "1800")
)
EXPORT_JOB_POLL_SECONDS = int(airflow_var("APPCUES_EXPORT_POLL_SECONDS", "10"))
REQUEST_TIMEOUT_SECONDS = int(airflow_var("APPCUES_REQUEST_TIMEOUT_SECONDS", "120"))
STATE_VAR_NAME = airflow_var("APPCUES_BOOKMARKS_VAR_NAME", "appcues_bookmarks")
ACCOUNT_ID_SECRET_ID = airflow_var(
    "APPCUES_ACCOUNT_ID_SECRET_ID",
    "APPCUES_ACCOUNT_ID",
)
API_KEY_SECRET_ID = airflow_var("APPCUES_API_KEY_SECRET_ID", "APPCUES_API_KEY")
API_SECRET_SECRET_ID = airflow_var(
    "APPCUES_API_SECRET_SECRET_ID",
    "APPCUES_API_SECRET",
)

TABLE_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("experience_name", "STRING"),
    bigquery.SchemaField("experience_type", "STRING"),
    bigquery.SchemaField("nps_score", "INT64"),
    bigquery.SchemaField("attributes", "STRING"),
    bigquery.SchemaField("identity", "STRING"),
    bigquery.SchemaField("raw_payload", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_window_start", "DATE"),
    bigquery.SchemaField("_window_end", "DATE"),
]
TABLE_COLUMNS = [field.name for field in TABLE_SCHEMA]


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def get_secret_client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()


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


def get_secret_value(secret_id: str) -> str:
    env_value = os.getenv(secret_id)
    if env_value not in (None, ""):
        return env_value

    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        response = get_secret_client().access_secret_version(request={"name": secret_path})
    except Exception as exc:
        raise ValueError(
            f"GCP Secret Manager secret '{secret_id}' is required."
        ) from exc

    value = response.payload.data.decode("utf-8")
    if not value:
        raise ValueError(f"GCP Secret Manager secret '{secret_id}' is empty.")
    return value


def get_basic_auth() -> HTTPBasicAuth:
    return HTTPBasicAuth(
        get_secret_value(API_KEY_SECRET_ID),
        get_secret_value(API_SECRET_SECRET_ID),
    )


def get_headers() -> dict[str, str]:
    return {"Content-Type": "application/json"}


def get_export_url() -> str:
    account_id = get_secret_value(ACCOUNT_ID_SECRET_ID)
    return f"{APPCUES_API_BASE_URL}/accounts/{account_id}/export/events"


def submit_export_job(start_date: date, end_date: date) -> str:
    payload = {
        "format": "json",
        "start_time": start_date.isoformat(),
        "end_time": (end_date + timedelta(days=1)).isoformat(),
        "conditions": [["name", "==", APPCUES_EVENT_NAME]],
    }
    response = requests.post(
        get_export_url(),
        auth=get_basic_auth(),
        headers=get_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code != 202:
        raise RuntimeError(
            f"Appcues export request failed ({response.status_code}): {response.text}"
        )

    job_url = response.json().get("job_url")
    if not job_url:
        raise RuntimeError("Appcues export response did not include job_url.")
    return job_url


def wait_for_export_job(job_url: str) -> dict[str, Any]:
    started_at = time.monotonic()

    while True:
        response = requests.get(
            job_url,
            auth=get_basic_auth(),
            headers=get_headers(),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        status = str(payload.get("status", "UNKNOWN")).upper()

        if status in {"COMPLETED", "FINISHED", "SUCCESS", "DONE"}:
            return payload

        if status in {"FAILED", "ERROR", "TIMEOUT", "CANCELED"}:
            raise RuntimeError(f"Appcues export job failed: {payload}")

        if time.monotonic() - started_at > EXPORT_JOB_TIMEOUT_SECONDS:
            raise TimeoutError(
                "Appcues export job did not complete before timeout. "
                f"Last payload: {payload}"
            )

        time.sleep(EXPORT_JOB_POLL_SECONDS)


def resolve_download_url(job_payload: dict[str, Any]) -> str:
    download_url = job_payload.get("download_url") or job_payload.get("file_url")
    if download_url:
        return download_url

    events = job_payload.get("events", [])
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            attributes = event.get("attributes", {})
            if isinstance(attributes, dict) and attributes.get("signed_s3_url"):
                return attributes["signed_s3_url"]

    raise RuntimeError(f"Appcues export job completed without a download URL: {job_payload}")


def download_export(download_url: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp_file:
        temp_path = tmp_file.name

    with requests.get(
        download_url,
        stream=True,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

    return temp_path


def iter_export_lines(file_path: str):
    with open(file_path, "rb") as source_file:
        magic_bytes = source_file.read(2)

    if magic_bytes == b"\x1f\x8b":
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as gzip_file:
            for line in gzip_file:
                yield line
        return

    if magic_bytes == b"PK":
        with zipfile.ZipFile(file_path) as zip_file:
            member_name = next(
                (name for name in zip_file.namelist() if not name.endswith("/")),
                None,
            )
            if not member_name:
                return
            with zip_file.open(member_name) as member:
                for raw_line in member:
                    yield raw_line.decode("utf-8", errors="replace")
        return

    with open(file_path, "rt", encoding="utf-8", errors="replace") as plain_file:
        for line in plain_file:
            yield line


def parse_json_field(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def serialize_json_field(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def is_nps_event(attributes: Any) -> bool:
    if not isinstance(attributes, dict):
        return False

    experience_type = str(attributes.get("experienceType", "")).lower()
    experience_name = str(attributes.get("experienceName", "")).upper()
    return experience_type == "nps" or "NPS" in experience_name


def extract_nps_score(attributes: Any) -> int | None:
    if not isinstance(attributes, dict):
        return None

    interaction_data = attributes.get("interactionData", {})
    if not isinstance(interaction_data, dict):
        return None

    survey_responses = interaction_data.get("surveyResponses", [])
    if not isinstance(survey_responses, list):
        return None

    for response in survey_responses:
        if not isinstance(response, dict) or response.get("type") != "nps_score":
            continue

        value = response.get("value")
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(float(value.strip()))
            except ValueError:
                return None

    return None


def build_row(
    record: dict[str, Any],
    window_start: date,
    window_end: date,
    load_date: date,
    ingested_at: str,
) -> dict[str, Any] | None:
    if record.get("name") != APPCUES_EVENT_NAME:
        return None

    attributes = parse_json_field(record.get("attributes"))
    if not is_nps_event(attributes):
        return None

    nps_score = extract_nps_score(attributes)
    if nps_score is None:
        return None

    event_timestamp = record.get("timestamp")
    if not event_timestamp:
        return None

    identity = parse_json_field(record.get("identity"))

    return {
        "event_id": str(record.get("id", "")) or None,
        "event_name": str(record.get("name", "")) or None,
        "event_timestamp": event_timestamp,
        "load_date": load_date.isoformat(),
        "user_id": str(record.get("user_id", "")) or None,
        "account_id": str(record.get("account_id", "")) or None,
        "experience_name": str(attributes.get("experienceName", "")) or None,
        "experience_type": str(attributes.get("experienceType", "")) or None,
        "nps_score": nps_score,
        "attributes": serialize_json_field(attributes),
        "identity": serialize_json_field(identity),
        "raw_payload": json.dumps(record, separators=(",", ":")),
        "_ingested_at": ingested_at,
        "_window_start": window_start.isoformat(),
        "_window_end": window_end.isoformat(),
    }


def extract_rows_from_export(
    file_path: str,
    window_start: date,
    window_end: date,
    load_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ingested_at = datetime.now(UTC).isoformat()

    for line in iter_export_lines(file_path):
        if not line.strip():
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not isinstance(record, dict):
            continue

        row = build_row(record, window_start, window_end, load_date, ingested_at)
        if row:
            rows.append(row)

    return rows


def ensure_table(client: bigquery.Client, table_ref: str) -> None:
    table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
    client.create_table(table, exists_ok=True)

    query = f"""
    ALTER TABLE `{table_ref}`
    ADD COLUMN IF NOT EXISTS load_date DATE
    """
    client.query(query).result()


def truncate_table(client: bigquery.Client, table_ref: str) -> None:
    ensure_table(client, table_ref)
    client.query(f"TRUNCATE TABLE `{table_ref}`").result()


def load_rows(
    client: bigquery.Client,
    table_ref: str,
    rows: list[dict[str, Any]],
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    ensure_table(client, table_ref)
    if not rows:
        return

    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_json(rows, table_ref, job_config=job_config).result()


def merge_raw_into_history(
    client: bigquery.Client,
    raw_table_ref: str,
    final_table_ref: str,
) -> None:
    ensure_table(client, final_table_ref)

    insert_columns = ", ".join(TABLE_COLUMNS)
    select_columns = ", ".join(TABLE_COLUMNS)

    query = f"""
    DELETE FROM `{final_table_ref}`
    WHERE event_id IN (
        SELECT event_id
        FROM `{raw_table_ref}`
        WHERE event_id IS NOT NULL
    );

    INSERT INTO `{final_table_ref}` ({insert_columns})
    SELECT {select_columns}
    FROM `{raw_table_ref}`
    WHERE event_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY event_timestamp DESC, _ingested_at DESC
    ) = 1
    """
    client.query(query).result()


def load_appcues_raw() -> dict[str, Any]:
    context = get_current_context()
    load_date = datetime.fromisoformat(context["ds"]).date()
    client = get_bq_client()
    start_date, end_date, _ = resolve_window()
    if start_date > end_date:
        raw_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{BQ_TABLE}"
        final_table_ref = f"{PROJECT_ID}.{FINAL_BQ_DATASET}.{BQ_TABLE}"
        truncate_table(client, raw_table_ref)
        return {
            "raw_table": raw_table_ref,
            "final_table": final_table_ref,
            "rows": 0,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

    job_url = submit_export_job(start_date, end_date)
    job_payload = wait_for_export_job(job_url)
    download_url = resolve_download_url(job_payload)
    export_file_path = download_export(download_url)

    try:
        rows = extract_rows_from_export(
            export_file_path,
            start_date,
            end_date,
            load_date,
        )
    finally:
        if os.path.exists(export_file_path):
            os.remove(export_file_path)

    raw_table_ref = f"{PROJECT_ID}.{RAW_BQ_DATASET}.{BQ_TABLE}"
    final_table_ref = f"{PROJECT_ID}.{FINAL_BQ_DATASET}.{BQ_TABLE}"
    truncate_table(client, raw_table_ref)
    load_rows(client, raw_table_ref, rows)

    return {
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "rows": len(rows),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def upsert_appcues_history(load_result: dict[str, Any]) -> dict[str, Any]:
    client = get_bq_client()
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


with DAG(
    dag_id="appcues_to_bigquery",
    description="Extrae eventos NPS desde Appcues API y los carga en BigQuery.",
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
    tags=["appcues", "nps", "bigquery", "stitch-replacement"],
) as dag:
    load_raw_task = task(task_id="load_appcues_raw")(load_appcues_raw)()
    upsert_history_task = task(task_id="upsert_appcues_history")(upsert_appcues_history)(
        load_raw_task
    )

    load_raw_task >> upsert_history_task
