from __future__ import annotations

import argparse
import gzip
import json
import os
import tempfile
import time
import zipfile
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests
from google.api_core.exceptions import BadRequest, NotFound, PermissionDenied
from google.cloud import bigquery
from google.cloud import secretmanager
from requests.auth import HTTPBasicAuth


APPCUES_API_BASE_URL = "https://api.appcues.com/v2"
APPCUES_EVENT_NAME = "appcues:v2:step_interaction"

TABLE_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("_run_date", "DATE"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("experience_name", "STRING"),
    bigquery.SchemaField("experience_type", "STRING"),
    bigquery.SchemaField("nps_score", "INT64"),
    bigquery.SchemaField("attributes", "STRING"),
    bigquery.SchemaField("identity", "STRING"),
    bigquery.SchemaField("raw_payload", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]
TABLE_COLUMNS = [field.name for field in TABLE_SCHEMA]


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def parse_date(value: str) -> date:
    return datetime.fromisoformat(value).date()


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def get_secret_value(project_id: str, secret_id: str) -> str:
    env_value = os.getenv(secret_id)
    if env_value not in (None, ""):
        return env_value

    secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = secretmanager.SecretManagerServiceClient().access_secret_version(
            request={"name": secret_path}
        )
    except PermissionDenied as exc:
        raise ValueError(
            "Missing Secret Manager access. Grant roles/secretmanager.secretAccessor "
            f"for secret '{secret_id}'."
        ) from exc
    except NotFound as exc:
        raise ValueError(
            f"Secret Manager secret '{secret_id}' does not exist or has no active version."
        ) from exc

    value = response.payload.data.decode("utf-8")
    if not value:
        raise ValueError(f"Secret Manager secret '{secret_id}' is empty.")
    return value


def get_basic_auth(project_id: str, api_key_secret_id: str, api_secret_secret_id: str) -> HTTPBasicAuth:
    return HTTPBasicAuth(
        get_secret_value(project_id, api_key_secret_id),
        get_secret_value(project_id, api_secret_secret_id),
    )


def submit_export_job(
    project_id: str,
    account_id_secret_id: str,
    api_key_secret_id: str,
    api_secret_secret_id: str,
    request_timeout_seconds: int,
    start_date: date,
    end_date: date,
) -> str:
    account_id = get_secret_value(project_id, account_id_secret_id)
    payload = {
        "format": "json",
        "start_time": start_date.isoformat(),
        "end_time": (end_date + timedelta(days=1)).isoformat(),
        "conditions": [["name", "==", APPCUES_EVENT_NAME]],
    }
    response = requests.post(
        f"{APPCUES_API_BASE_URL}/accounts/{account_id}/export/events",
        auth=get_basic_auth(project_id, api_key_secret_id, api_secret_secret_id),
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=request_timeout_seconds,
    )
    if response.status_code != 202:
        raise RuntimeError(
            f"Appcues export request failed ({response.status_code}): {response.text}"
        )

    job_url = response.json().get("job_url")
    if not job_url:
        raise RuntimeError("Appcues export response did not include job_url.")
    return job_url


def wait_for_export_job(
    project_id: str,
    api_key_secret_id: str,
    api_secret_secret_id: str,
    request_timeout_seconds: int,
    export_job_timeout_seconds: int,
    export_job_poll_seconds: int,
    job_url: str,
) -> dict[str, Any]:
    started_at = time.monotonic()

    while True:
        response = requests.get(
            job_url,
            auth=get_basic_auth(project_id, api_key_secret_id, api_secret_secret_id),
            headers={"Content-Type": "application/json"},
            timeout=request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        status = str(payload.get("status", "UNKNOWN")).upper()

        if status in {"COMPLETED", "FINISHED", "SUCCESS", "DONE"}:
            return payload

        if status in {"FAILED", "ERROR", "TIMEOUT", "CANCELED"}:
            raise RuntimeError(f"Appcues export job failed: {payload}")

        if time.monotonic() - started_at > export_job_timeout_seconds:
            raise TimeoutError(
                "Appcues export job did not complete before timeout. "
                f"Last payload: {payload}"
            )

        time.sleep(export_job_poll_seconds)


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


def download_export(download_url: str, request_timeout_seconds: int) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp_file:
        temp_path = tmp_file.name

    with requests.get(
        download_url,
        stream=True,
        timeout=request_timeout_seconds,
    ) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

    return temp_path


def iter_export_lines(file_path: str) -> Iterator[str]:
    with open(file_path, "rb") as source_file:
        magic_bytes = source_file.read(2)

    if magic_bytes == b"\x1f\x8b":
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as gzip_file:
            yield from gzip_file
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
        yield from plain_file


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


def build_row(record: dict[str, Any], run_date: date, ingested_at: str) -> dict[str, Any] | None:
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
        "_run_date": run_date.isoformat(),
        "user_id": str(record.get("user_id", "")) or None,
        "account_id": str(record.get("account_id", "")) or None,
        "experience_name": str(attributes.get("experienceName", "")) or None,
        "experience_type": str(attributes.get("experienceType", "")) or None,
        "nps_score": nps_score,
        "attributes": serialize_json_field(attributes),
        "identity": serialize_json_field(identity),
        "raw_payload": json.dumps(record, separators=(",", ":")),
        "_ingested_at": ingested_at,
    }


def extract_rows_from_export(file_path: str, run_date: date) -> list[dict[str, Any]]:
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

        row = build_row(record, run_date, ingested_at)
        if row:
            rows.append(row)

    return rows


def ensure_table(client: bigquery.Client, table_ref: str) -> None:
    table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
    client.create_table(table, exists_ok=True)

    existing_table = client.get_table(table_ref)
    existing_fields = {field.name for field in existing_table.schema}
    missing_fields = [
        field for field in TABLE_SCHEMA if field.name not in existing_fields
    ]
    if not missing_fields:
        return

    existing_table.schema = [*existing_table.schema, *missing_fields]
    client.update_table(existing_table, ["schema"])


def load_rows(
    client: bigquery.Client,
    table_ref: str,
    rows: list[dict[str, Any]],
    write_disposition: str,
) -> None:
    if write_disposition != bigquery.WriteDisposition.WRITE_TRUNCATE:
        ensure_table(client, table_ref)
    if not rows:
        if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
            empty_table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
            client.delete_table(table_ref, not_found_ok=True)
            client.create_table(empty_table)
        return

    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    try:
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()
    except BadRequest as exc:
        details = getattr(exc, "errors", None)
        raise RuntimeError(
            f"BigQuery load to '{table_ref}' failed. rows={len(rows)} details={details or str(exc)}"
        ) from exc


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


def process_day(args: argparse.Namespace, process_date: date) -> dict[str, Any]:
    client = bigquery.Client(project=args.project_id)
    raw_table_ref = f"{args.project_id}.{args.raw_dataset}.{args.raw_table}"
    final_table_ref = f"{args.project_id}.{args.final_dataset}.{args.final_table}"

    job_url = submit_export_job(
        args.project_id,
        args.account_id_secret_id,
        args.api_key_secret_id,
        args.api_secret_secret_id,
        args.request_timeout_seconds,
        process_date,
        process_date,
    )
    job_payload = wait_for_export_job(
        args.project_id,
        args.api_key_secret_id,
        args.api_secret_secret_id,
        args.request_timeout_seconds,
        args.export_job_timeout_seconds,
        args.export_job_poll_seconds,
        job_url,
    )
    download_url = resolve_download_url(job_payload)
    export_file_path = download_export(download_url, args.request_timeout_seconds)

    try:
        rows = extract_rows_from_export(export_file_path, process_date)
    finally:
        if os.path.exists(export_file_path):
            os.remove(export_file_path)

    load_rows(
        client,
        raw_table_ref,
        rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if rows:
        merge_raw_into_history(client, raw_table_ref, final_table_ref)

    return {
        "date": process_date.isoformat(),
        "raw_table": raw_table_ref,
        "final_table": final_table_ref,
        "rows": len(rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Appcues NPS daily backfills on demand."
    )
    parser.add_argument("--start-date", required=True, type=parse_date)
    parser.add_argument("--end-date", required=True, type=parse_date)
    parser.add_argument("--project-id", default=env("APPCUES_GCP_PROJECT_ID", "fxr-analytics"))
    parser.add_argument("--raw-dataset", default=env("APPCUES_BQ_DATASET", "appcues_raw"))
    parser.add_argument("--final-dataset", default=env("APPCUES_FINAL_BQ_DATASET", "appcues"))
    parser.add_argument("--raw-table", default=env("APPCUES_BACKFILL_BQ_TABLE", "nps_events_backfill"))
    parser.add_argument("--final-table", default=env("APPCUES_BQ_TABLE", "nps_events"))
    parser.add_argument("--account-id-secret-id", default=env("APPCUES_ACCOUNT_ID_SECRET_ID", "APPCUES_ACCOUNT_ID"))
    parser.add_argument("--api-key-secret-id", default=env("APPCUES_API_KEY_SECRET_ID", "APPCUES_API_KEY"))
    parser.add_argument("--api-secret-secret-id", default=env("APPCUES_API_SECRET_SECRET_ID", "APPCUES_API_SECRET"))
    parser.add_argument("--request-timeout-seconds", type=int, default=int(env("APPCUES_REQUEST_TIMEOUT_SECONDS", "120")))
    parser.add_argument("--export-job-timeout-seconds", type=int, default=int(env("APPCUES_EXPORT_TIMEOUT_SECONDS", "1800")))
    parser.add_argument("--export-job-poll-seconds", type=int, default=int(env("APPCUES_EXPORT_POLL_SECONDS", "10")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise ValueError("--start-date must be before or equal to --end-date")

    total_rows = 0
    for process_date in iter_days(args.start_date, args.end_date):
        result = process_day(args, process_date)
        total_rows += int(result["rows"])
        print(json.dumps(result, sort_keys=True))

    print(
        json.dumps(
            {
                "start_date": args.start_date.isoformat(),
                "end_date": args.end_date.isoformat(),
                "total_rows": total_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
