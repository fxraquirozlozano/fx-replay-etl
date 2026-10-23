"""
appcues_lib.py

Pure building blocks for the Appcues -> BigQuery pipeline.

No Airflow, no CLI, no globals for tenant-specific config: this module is
imported by both the daily DAG (appcues_to_bigquery.py) and the historical
backfill (backfill_appcues_responses.py). Both callers build an
`AppcuesConfig` and pass it into the functions here, so the two entry points
produce byte-identical rows for the same window.

What lives here:
  - Constants (API base URL, event names, BigQuery table schema).
  - `AppcuesConfig` dataclass grouping the tenant / auth / timeout knobs.
  - Secret Manager + auth helpers.
  - Export API flow (submit, poll, resolve URL, download).
  - Parsing + shape-based row building.
  - BigQuery load + MERGE.
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterator

import requests
from google.api_core.exceptions import BadRequest, NotFound, PermissionDenied
from google.cloud import bigquery, secretmanager
from requests.auth import HTTPBasicAuth


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

APPCUES_API_BASE_URL = "https://api.appcues.com/v2"

# Events we ingest: any survey submit (classic forms + v2 / NPS 2.0 + legacy
# NPS Survey). Applied API-side. If Appcues honors "in" (it ignores "not in"),
# the download drops from GB to MB. The builder still applies a shape-based
# safety net in case the API returns extra events.
APPCUES_SURVEY_EVENT_NAMES = [
    "appcues:form_submitted",
    "appcues:v2:step_interaction",
    "appcues:nps_score",
]

# Legacy event that is an exact duplicate of appcues:form_submitted (same
# submit emitted under two names). Discarded explicitly in the builder to
# avoid double counting.
APPCUES_LEGACY_DUPLICATE_EVENT = "appcues:step_interacted"


# "Long" schema: one row per (event x answered question).
# An NPS submit with score + feedback produces 2 rows sharing event_id but
# differing on field_id. A multi-question form produces N rows per submission.
TABLE_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("user_email", "STRING"),
    bigquery.SchemaField("plan_tier", "STRING"),
    bigquery.SchemaField("survey_id", "STRING"),
    bigquery.SchemaField("survey_name", "STRING"),
    bigquery.SchemaField("survey_kind", "STRING"),
    bigquery.SchemaField("submission_id", "STRING"),
    bigquery.SchemaField("field_id", "STRING"),
    bigquery.SchemaField("field_label", "STRING"),
    bigquery.SchemaField("field_type", "STRING"),
    bigquery.SchemaField("field_index", "INT64"),
    bigquery.SchemaField("value_string", "STRING"),
    bigquery.SchemaField("value_number", "FLOAT64"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]
TABLE_COLUMNS = [field_.name for field_ in TABLE_SCHEMA]


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class AppcuesConfig:
    """
    Tenant-specific configuration passed into the functions that touch
    Appcues or Secret Manager. Keeping this explicit (instead of module
    globals) lets the DAG and the backfill each build their own instance
    with their own overrides.
    """

    project_id: str
    account_id_secret_id: str = "APPCUES_ACCOUNT_ID"
    api_key_secret_id: str = "APPCUES_API_KEY"
    api_secret_secret_id: str = "APPCUES_API_SECRET"
    request_timeout_seconds: int = 120
    export_poll_seconds: int = 10
    export_timeout_seconds: int = 1800


# --------------------------------------------------------------------------- #
# Clients
# --------------------------------------------------------------------------- #

def get_bq_client(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id)


def _get_secret_client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()


# --------------------------------------------------------------------------- #
# Secret Manager + auth
# --------------------------------------------------------------------------- #

def get_secret_value(secret_id: str, project_id: str) -> str:
    """
    Resolve a secret by ID. Checks env vars first (developer-friendly), then
    falls back to GCP Secret Manager.
    """
    env_value = os.getenv(secret_id)
    if env_value not in (None, ""):
        return env_value

    secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = _get_secret_client().access_secret_version(request={"name": secret_path})
    except PermissionDenied as exc:
        raise ValueError(
            "Composer does not have permission to read secret "
            f"'{secret_id}'. Grant 'roles/secretmanager.secretAccessor' "
            "to the environment's service account."
        ) from exc
    except NotFound as exc:
        raise ValueError(
            f"GCP Secret Manager secret '{secret_id}' does not exist or has no active version."
        ) from exc

    value = response.payload.data.decode("utf-8")
    if not value:
        raise ValueError(f"GCP Secret Manager secret '{secret_id}' is empty.")
    return value


def get_basic_auth(config: AppcuesConfig) -> HTTPBasicAuth:
    return HTTPBasicAuth(
        get_secret_value(config.api_key_secret_id, config.project_id),
        get_secret_value(config.api_secret_secret_id, config.project_id),
    )


def get_export_url(config: AppcuesConfig) -> str:
    account_id = get_secret_value(config.account_id_secret_id, config.project_id)
    return f"{APPCUES_API_BASE_URL}/accounts/{account_id}/export/events"


def _get_headers() -> dict[str, str]:
    return {"Content-Type": "application/json"}


# --------------------------------------------------------------------------- #
# Export API flow
# --------------------------------------------------------------------------- #

def submit_export_job(
    config: AppcuesConfig,
    start_date: date,
    end_date: date,
) -> str:
    # API-side name filter. If the API honors "in", the download shrinks
    # drastically. The builder still applies the shape filter as a safety
    # net, so if the API ignores this nothing breaks -- we just download
    # more than necessary.
    payload = {
        "format": "json",
        "start_time": start_date.isoformat(),
        "end_time": (end_date + timedelta(days=1)).isoformat(),
        "conditions": [["name", "in", APPCUES_SURVEY_EVENT_NAMES]],
    }
    response = requests.post(
        get_export_url(config),
        auth=get_basic_auth(config),
        headers=_get_headers(),
        json=payload,
        timeout=config.request_timeout_seconds,
    )
    if response.status_code != 202:
        raise RuntimeError(
            f"Appcues export request failed ({response.status_code}): {response.text}"
        )

    job_url = response.json().get("job_url")
    if not job_url:
        raise RuntimeError("Appcues export response did not include job_url.")
    return job_url


def wait_for_export_job(config: AppcuesConfig, job_url: str) -> dict[str, Any]:
    started_at = time.monotonic()

    while True:
        response = requests.get(
            job_url,
            auth=get_basic_auth(config),
            headers=_get_headers(),
            timeout=config.request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        status = str(payload.get("status", "UNKNOWN")).upper()

        if status in {"COMPLETED", "FINISHED", "SUCCESS", "DONE"}:
            return payload

        if status in {"FAILED", "ERROR", "TIMEOUT", "CANCELED"}:
            raise RuntimeError(f"Appcues export job failed: {payload}")

        if time.monotonic() - started_at > config.export_timeout_seconds:
            raise TimeoutError(
                "Appcues export job did not complete before timeout. "
                f"Last payload: {payload}"
            )

        time.sleep(config.export_poll_seconds)


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

    raise RuntimeError(
        f"Appcues export job completed without a download URL: {job_payload}"
    )


def download_export(config: AppcuesConfig, download_url: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp_file:
        temp_path = tmp_file.name

    with requests.get(
        download_url,
        stream=True,
        timeout=config.request_timeout_seconds,
    ) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

    return temp_path


def iter_export_lines(file_path: str) -> Iterator[str]:
    """Yields decoded lines from an export file (gzip, zip, or plain)."""
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


# --------------------------------------------------------------------------- #
# Parsing + shape-based row building
# --------------------------------------------------------------------------- #

def parse_json_field(value: Any) -> Any:
    """attributes / identity arrive as JSON strings in the export."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def extract_survey_responses(attributes: Any) -> list[dict[str, Any]] | None:
    """
    SHAPE-based filter (independent of the event name).

    Returns the list of responses if the event has a survey submission shape,
    or None otherwise. Covers the two known shapes:

      Shape 1 (classic forms):  attributes.interaction.response[]
      Shape 2 (v2 / NPS 2.0):   attributes.interactionData.surveyResponses[]

    Any future event that reuses one of these shapes is picked up
    automatically by the ingestor.
    """
    if not isinstance(attributes, dict):
        return None

    interaction = attributes.get("interaction")
    if isinstance(interaction, dict):
        response = interaction.get("response")
        if isinstance(response, list) and response:
            return [r for r in response if isinstance(r, dict)]

    interaction_data = attributes.get("interactionData")
    if isinstance(interaction_data, dict):
        survey_responses = interaction_data.get("surveyResponses")
        if isinstance(survey_responses, list) and survey_responses:
            return [r for r in survey_responses if isinstance(r, dict)]

    return None


def coerce_value(val: Any) -> tuple[str | None, float | None]:
    """
    Returns (value_string, value_number).
    Numbers -> value_number. Text / booleans / arrays / dicts -> value_string.
    Booleans are serialized as "true"/"false" to preserve semantics.
    """
    if val is None:
        return (None, None)
    if isinstance(val, bool):
        return (str(val).lower(), None)
    if isinstance(val, (int, float)):
        return (None, float(val))
    if isinstance(val, str):
        return (val, None)
    return (json.dumps(val, ensure_ascii=False), None)


def _coerce_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except (ValueError, AttributeError):
            return None
    return None


def build_response_rows(
    record: dict[str, Any],
    ingested_at: str,
) -> list[dict[str, Any]]:
    """
    Transforms an export event into "long" rows for BigQuery.
    An event with N responses produces N rows.

    Rules:
      - Discards appcues:step_interacted (legacy duplicate of form_submitted).
      - Applies a shape-based safety net: an event with an expected name that
        does not carry responses is silently skipped.
      - For form_submitted: prefers customReportingLabel over label when set.
      - For v2:step_interaction: filters by interactionType == "Survey submitted".
      - For appcues:nps_score (legacy NPS Survey, deprecated ~2026-04):
        emits a single nps_score row from attributes.score. This shape does
        NOT carry an interaction / interactionData array, so it is handled
        before the shape safety net.
    """
    name = str(record.get("name") or "")
    if not name or name == APPCUES_LEGACY_DUPLICATE_EVENT:
        return []

    event_timestamp = record.get("timestamp")
    if not event_timestamp:
        return []

    attributes = parse_json_field(record.get("attributes"))
    if not isinstance(attributes, dict):
        return []

    identity = parse_json_field(record.get("identity"))
    if not isinstance(identity, dict):
        identity = {}

    base = {
        "event_id": str(record.get("id", "")) or None,
        "event_name": name,
        "event_timestamp": event_timestamp,
        "account_id": str(record.get("account_id", "")) or None,
        "user_id": str(record.get("user_id", "")) or None,
        "user_email": identity.get("email") or None,
        "plan_tier": identity.get("planTier") or None,
        "_ingested_at": ingested_at,
    }

    rows: list[dict[str, Any]] = []

    # Legacy shape: attributes.score is a bare number at the root of attributes.
    # No responses array, no feedback field, no submissionId. Deprecated
    # ~2026-04 in favor of appcues:v2:step_interaction with NPS 2.0, but the
    # historical events are still needed for the backfill.
    if name == "appcues:nps_score":
        score = attributes.get("score")
        if score is None:
            return []
        _, v_num = coerce_value(score)
        if v_num is None:
            return []  # unparseable score value

        survey_id = attributes.get("flowId")
        survey_name = attributes.get("flowName")

        rows.append({
            **base,
            "survey_id": str(survey_id) if survey_id else None,
            "survey_name": survey_name,
            "survey_kind": "nps",
            # This shape has no submissionId; use event_id as logical grouper.
            "submission_id": base["event_id"],
            # Stable synthetic field_id so all legacy score rows share it and
            # dbt can pivot / filter them consistently.
            "field_id": "legacy_nps_score",
            # Canonical NPS question text (matches the v2 label so downstream
            # models can treat both together when convenient).
            "field_label": "On a scale of 0-10, how likely are you to recommend this product to a friend or colleague?",
            "field_type": "nps_score",
            "field_index": 0,
            "value_string": None,
            "value_number": v_num,
        })
        return rows

    # Shape-based safety net for the two array-based shapes (in case the API
    # returns other event names we did not filter).
    responses = extract_survey_responses(attributes)
    if responses is None:
        return []

    if name == "appcues:form_submitted":
        interaction = attributes.get("interaction") or {}
        submission_id = interaction.get("submissionId") if isinstance(interaction, dict) else None
        survey_id = attributes.get("flowId")
        survey_name = attributes.get("flowName")

        for r in responses:
            v_str, v_num = coerce_value(r.get("value"))
            rows.append({
                **base,
                "survey_id": str(survey_id) if survey_id else None,
                "survey_name": survey_name,
                "survey_kind": "form",
                "submission_id": str(submission_id) if submission_id else None,
                "field_id": str(r.get("fieldId")) if r.get("fieldId") else None,
                "field_label": r.get("customReportingLabel") or r.get("label"),
                "field_type": r.get("fieldType"),
                "field_index": _coerce_int(r.get("formFieldIndex")),
                "value_string": v_str,
                "value_number": v_num,
            })
        return rows

    if name == "appcues:v2:step_interaction":
        # v2 emits several sub-types (Survey seen, Button Clicked, snoozes, etc).
        # We only care about the actual submit.
        if attributes.get("interactionType") != "Survey submitted":
            return []

        survey_id = attributes.get("experienceId")
        survey_name = attributes.get("experienceName")
        # experienceType carries the real kind ("nps", "survey", etc); fallback to "v2".
        survey_kind = attributes.get("experienceType") or "v2"
        # v2 has no submissionId of its own; use event_id as a logical grouper.
        submission_id = base["event_id"]

        for r in responses:
            v_str, v_num = coerce_value(r.get("value"))
            rows.append({
                **base,
                "survey_id": str(survey_id) if survey_id else None,
                "survey_name": survey_name,
                "survey_kind": survey_kind,
                "submission_id": submission_id,
                "field_id": str(r.get("fieldId")) if r.get("fieldId") else None,
                "field_label": r.get("label"),
                "field_type": r.get("type"),
                "field_index": _coerce_int(r.get("order")),
                "value_string": v_str,
                "value_number": v_num,
            })
        return rows

    # Unknown event name with a valid shape: we do not emit rows to avoid
    # surprises. When a new event name appears, add it explicitly above.
    return []


def extract_rows_from_export(file_path: str, ingested_at: str) -> list[dict[str, Any]]:
    """
    Reads an export file and returns the flat "long" rows ready for BigQuery.
    `ingested_at` is a caller-provided ISO timestamp so every row in one run
    shares the same load time.
    """
    rows: list[dict[str, Any]] = []

    for line in iter_export_lines(file_path):
        if not line.strip():
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not isinstance(record, dict):
            continue

        rows.extend(build_response_rows(record, ingested_at))

    return rows


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #

def ensure_table(client: bigquery.Client, table_ref: str) -> None:
    table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
    client.create_table(table, exists_ok=True)

    existing_table = client.get_table(table_ref)
    existing_fields = {field_.name for field_ in existing_table.schema}
    missing_fields = [
        field_ for field_ in TABLE_SCHEMA if field_.name not in existing_fields
    ]
    if not missing_fields:
        return

    existing_table.schema = [*existing_table.schema, *missing_fields]
    client.update_table(existing_table, ["schema"])


def load_rows(
    client: bigquery.Client,
    table_ref: str,
    rows: list[dict[str, Any]],
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    if write_disposition != bigquery.WriteDisposition.WRITE_TRUNCATE:
        ensure_table(client, table_ref)
    if not rows:
        if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
            empty_table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
            client.delete_table(table_ref, not_found_ok=True)
            client.create_table(empty_table)
        return

    job_config_kwargs: dict[str, Any] = {
        "schema": TABLE_SCHEMA,
        "write_disposition": write_disposition,
        "create_disposition": bigquery.CreateDisposition.CREATE_IF_NEEDED,
    }
    if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
        job_config_kwargs["schema_update_options"] = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ]

    job_config = bigquery.LoadJobConfig(**job_config_kwargs)
    try:
        job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()
    except BadRequest as exc:
        details = getattr(exc, "errors", None)
        raise RuntimeError(
            f"BigQuery load to '{table_ref}' failed. "
            f"rows={len(rows)} details={details or str(exc)}"
        ) from exc


def merge_raw_into_history(
    client: bigquery.Client,
    raw_table_ref: str,
    final_table_ref: str,
) -> None:
    """
    Dedup by (event_id, field_id): under the long schema an event produces
    N rows (one per question) that share event_id but differ on field_id.
    The DELETE removes every row for the event and reinserts from raw.
    """
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
        PARTITION BY event_id, field_id
        ORDER BY event_timestamp DESC, _ingested_at DESC
    ) = 1
    """
    client.query(query).result()


# --------------------------------------------------------------------------- #
# Backfill helpers
#
# Building blocks shared by the backfill DAG (`appcues_to_bigquery_backfill.py`)
# and the standalone CLI script (`backfill_appcues_responses.py`). Kept here
# so both entry points produce identical rows and neither has to depend on
# the other.
# --------------------------------------------------------------------------- #

# Staging table name reserved for the backfill flow, so the daily DAG's
# productive raw table is never clobbered by a concurrent backfill run.
DEFAULT_STAGING_TABLE = "responses_backfill"


def month_chunks(start: date, end: date) -> Iterator[tuple[date, date]]:
    """
    Yield (chunk_start, chunk_end) tuples covering [start, end] inclusive,
    one calendar month at a time. Both bounds are inclusive dates.
    """
    if start > end:
        return

    cursor = start
    while cursor <= end:
        if cursor.month == 12:
            next_month_first = date(cursor.year + 1, 1, 1)
        else:
            next_month_first = date(cursor.year, cursor.month + 1, 1)
        chunk_end = min(next_month_first - timedelta(days=1), end)
        yield (cursor, chunk_end)
        cursor = chunk_end + timedelta(days=1)


def run_chunk(
    config: AppcuesConfig,
    client: bigquery.Client,
    chunk_start: date,
    chunk_end: date,
    staging_table_ref: str,
    final_table_ref: str,
) -> int:
    """
    Loads one backfill chunk into the final table via staging + MERGE.
    Returns the number of rows written to staging (pre-dedup).

    Progress is printed to stdout so it shows up in Airflow task logs when
    called from the backfill DAG, and in the terminal when called from the
    CLI script.
    """
    started = time.monotonic()

    job_url = submit_export_job(config, chunk_start, chunk_end)
    print(f"    submitted export job: {job_url}")

    job_payload = wait_for_export_job(config, job_url)
    print(f"    export job completed in {int(time.monotonic() - started)}s")

    download_url = resolve_download_url(job_payload)
    export_file_path = download_export(config, download_url)
    size_mb = os.path.getsize(export_file_path) / (1024 * 1024)
    print(f"    downloaded {size_mb:,.1f} MB")

    ingested_at = datetime.now(UTC).isoformat()
    try:
        rows = extract_rows_from_export(export_file_path, ingested_at)
    finally:
        if os.path.exists(export_file_path):
            os.remove(export_file_path)

    print(f"    parsed {len(rows):,} response rows")

    # Load into staging (TRUNCATE so each chunk starts clean), then MERGE.
    load_rows(
        client,
        staging_table_ref,
        rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    if rows:
        merge_raw_into_history(client, staging_table_ref, final_table_ref)
        print(f"    merged into {final_table_ref}")

    elapsed = int(time.monotonic() - started)
    print(f"    chunk done in {elapsed}s")
    return len(rows)


def drop_table(client: bigquery.Client, table_ref: str) -> None:
    client.delete_table(table_ref, not_found_ok=True)
