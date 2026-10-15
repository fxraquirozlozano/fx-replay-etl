import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo
from zipfile import ZipFile

import requests
from google.cloud import bigquery


PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
BQ_DATASET = os.getenv("BQ_DATASET", "product_analytics")
BQ_TABLE = os.getenv("BQ_TABLE", "weekly_user_activity_with_commitment")
BQ_LOCATION = os.getenv("BQ_LOCATION", "").strip() or None
BQ_SOURCE_SQL = os.getenv("BQ_SOURCE_SQL", "").strip()
COUNTRY_BQ_DATASET = os.getenv("COUNTRY_BQ_DATASET", "dbt_cloud")
COUNTRY_BQ_TABLE = os.getenv("COUNTRY_BQ_TABLE", "dim_user")
BQ_WEEK_START_FIELD = os.getenv("BQ_WEEK_START_FIELD", "week_start").strip()
BQ_WEEK_END_FIELD = os.getenv("BQ_WEEK_END_FIELD", "week_end").strip()
USER_ID_FIELD = os.getenv("USER_ID_FIELD", "user_id").strip()
EVENT_NAME_FIELD = os.getenv("EVENT_NAME_FIELD", "event_name").strip()
EVENT_TIMESTAMP_FIELD = os.getenv("EVENT_TIMESTAMP_FIELD", "event_timestamp").strip()
APPCUES_BASE_URL = os.getenv("APPCUES_BASE_URL", "https://api.appcues.com").rstrip("/")
APPCUES_ACCOUNT_ID = os.getenv("APPCUES_ACCOUNT_ID", "").strip()
APPCUES_API_KEY = os.getenv("APPCUES_API_KEY", "").strip()
APPCUES_API_SECRET = os.getenv("APPCUES_API_SECRET", "").strip()
APPCUES_EVENT_NAME = os.getenv("APPCUES_EVENT_NAME", "weekly_product_activity").strip()
WINDOW_TIME_ZONE = os.getenv("WINDOW_TIME_ZONE", "America/Chicago").strip()
BACKFILL_START_WEEK = os.getenv("BACKFILL_START_WEEK", "").strip()
BACKFILL_END_WEEK = os.getenv("BACKFILL_END_WEEK", "").strip()
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_RETRY_DELAY_SECONDS = float(os.getenv("BASE_RETRY_DELAY_SECONDS", "2"))
APPCUES_JOB_POLL_SECONDS = float(os.getenv("APPCUES_JOB_POLL_SECONDS", "5"))
APPCUES_JOB_TIMEOUT_SECONDS = int(os.getenv("APPCUES_JOB_TIMEOUT_SECONDS", "1800"))
APPCUES_IMPORT_CHUNK_BYTES = int(os.getenv("APPCUES_IMPORT_CHUNK_BYTES", "18000000"))
SKIP_EXISTING_EVENTS = (
    os.getenv("SKIP_EXISTING_EVENTS", "true").strip().lower() == "true"
)
SYNC_COUNTRY_PROFILES = (
    os.getenv("SYNC_COUNTRY_PROFILES", "true").strip().lower() == "true"
)
COUNTRY_PROFILE_BACKFILL_ONLY = (
    os.getenv("COUNTRY_PROFILE_BACKFILL_ONLY", "false").strip().lower() == "true"
)
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() == "true"
FAIL_ON_PARTIAL_ERRORS = (
    os.getenv("FAIL_ON_PARTIAL_ERRORS", "true").strip().lower() == "true"
)

REQUIRED_WPA_FIELDS = (
    "wpa_tier_week",
    "hit_wpa_week",
    "wpa_tier_week_0",
)
RESERVED_FIELDS = {
    USER_ID_FIELD,
    EVENT_NAME_FIELD,
    EVENT_TIMESTAMP_FIELD,
    "country_code",
    "country_name",
}


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must use YYYY-MM-DD format. Received: {value}"
        ) from exc


def resolve_week_window() -> tuple[datetime, datetime]:
    if not BACKFILL_START_WEEK and not BACKFILL_END_WEEK:
        return weekly_window()

    if not BACKFILL_START_WEEK or not BACKFILL_END_WEEK:
        raise ValueError(
            "BACKFILL_START_WEEK and BACKFILL_END_WEEK must both be provided."
        )

    start_week = parse_iso_date(BACKFILL_START_WEEK, "BACKFILL_START_WEEK")
    end_week = parse_iso_date(BACKFILL_END_WEEK, "BACKFILL_END_WEEK")
    if end_week < start_week:
        raise ValueError("BACKFILL_END_WEEK must be on or after BACKFILL_START_WEEK.")

    start_local = datetime.combine(
        start_week,
        datetime.min.time(),
        tzinfo=ZoneInfo(WINDOW_TIME_ZONE),
    )
    end_local = datetime.combine(
        end_week + timedelta(days=7),
        datetime.min.time(),
        tzinfo=ZoneInfo(WINDOW_TIME_ZONE),
    )
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def weekly_window() -> tuple[datetime, datetime]:
    local_now = datetime.now(ZoneInfo(WINDOW_TIME_ZONE))
    week_end_local = local_now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ) - timedelta(days=local_now.weekday())
    week_start_local = week_end_local - timedelta(days=7)
    return (
        week_start_local.astimezone(timezone.utc),
        week_end_local.astimezone(timezone.utc),
    )


def default_query() -> str:
    return f"""
    WITH weekly_source AS (
      SELECT
        user_id,
        week_start,
        trades_count,
        time_spent_seconds,
        historical_seconds,
        is_wpa_10,
        is_wpa_25,
        is_wpa_50,
        is_wpa_90,
        weeks_since_first_commit,
        CASE
          WHEN is_wpa_90 THEN 'WPA90'
          WHEN is_wpa_50 THEN 'WPA50'
          WHEN is_wpa_25 THEN 'WPA25'
          WHEN is_wpa_10 THEN 'WPA10'
          ELSE NULL
        END AS wpa_tier
      FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    ),
    week_zero AS (
      SELECT
        user_id,
        ARRAY_AGG(
          wpa_tier IGNORE NULLS
          ORDER BY CASE wpa_tier
            WHEN 'WPA90' THEN 90
            WHEN 'WPA50' THEN 50
            WHEN 'WPA25' THEN 25
            WHEN 'WPA10' THEN 10
          END DESC
          LIMIT 1
        )[SAFE_OFFSET(0)]
          AS wpa_tier_week_0
      FROM weekly_source
      WHERE weeks_since_first_commit = 0
      GROUP BY user_id
    )
    SELECT
      src.user_id,
      'weekly_product_activity' AS event_name,
      TIMESTAMP(DATE_ADD(src.week_start, INTERVAL 7 DAY), 'UTC') AS event_timestamp,
      src.week_start,
      DATE_ADD(src.week_start, INTERVAL 7 DAY) AS week_end,
      src.trades_count,
      src.time_spent_seconds,
      src.historical_seconds,
      src.wpa_tier AS wpa_tier_week,
      COALESCE(src.is_wpa_10, FALSE) AS hit_wpa_week,
      week_zero.wpa_tier_week_0
    FROM weekly_source AS src
    LEFT JOIN week_zero
      USING (user_id)
    WHERE src.week_start >= @range_start
      AND src.week_start < @range_end
    """


def country_profile_query() -> str:
    return f"""
    SELECT
      user_id,
      NULLIF(TRIM(country_code), '') AS country_code,
      NULLIF(TRIM(country_name), '') AS country_name
    FROM `{PROJECT_ID}.{COUNTRY_BQ_DATASET}.{COUNTRY_BQ_TABLE}`
    WHERE user_id IS NOT NULL
      AND created_at >= @week_start_ts
      AND created_at < @week_end_ts
      AND (NULLIF(TRIM(country_code), '') IS NOT NULL
        OR NULLIF(TRIM(country_name), '') IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY created_at DESC, country_name DESC, country_code DESC
    ) = 1
    """


def country_backfill_query() -> str:
    return f"""
    WITH appcues_managed_users AS (
      SELECT DISTINCT user_id
      FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
      WHERE user_id IS NOT NULL
    ),
    latest_country AS (
      SELECT
        user_id,
        NULLIF(TRIM(country_code), '') AS country_code,
        NULLIF(TRIM(country_name), '') AS country_name
      FROM `{PROJECT_ID}.{COUNTRY_BQ_DATASET}.{COUNTRY_BQ_TABLE}`
      WHERE user_id IS NOT NULL
        AND (NULLIF(TRIM(country_code), '') IS NOT NULL
          OR NULLIF(TRIM(country_name), '') IS NOT NULL)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY created_at DESC, country_name DESC, country_code DESC
      ) = 1
    )
    SELECT
      latest_country.user_id,
      latest_country.country_code,
      latest_country.country_name
    FROM latest_country
    INNER JOIN appcues_managed_users
      USING (user_id)
    """


def build_query_job_config(
    week_start_utc: datetime,
    week_end_utc: datetime,
) -> bigquery.QueryJobConfig:
    week_start_date = week_start_utc.date()
    week_end_date = week_end_utc.date()
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("week_start", "DATE", week_start_date),
            bigquery.ScalarQueryParameter("week_end", "DATE", week_end_date),
            bigquery.ScalarQueryParameter("range_start", "DATE", week_start_date),
            bigquery.ScalarQueryParameter("range_end", "DATE", week_end_date),
            bigquery.ScalarQueryParameter("week_start_ts", "TIMESTAMP", week_start_utc),
            bigquery.ScalarQueryParameter("week_end_ts", "TIMESTAMP", week_end_utc),
        ]
    )


def run_query(week_start_utc: datetime, week_end_utc: datetime) -> list[dict[str, Any]]:
    query = BQ_SOURCE_SQL or default_query()
    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    job = client.query(
        query,
        job_config=build_query_job_config(week_start_utc, week_end_utc),
    )
    return [dict(row.items()) for row in job.result()]


def run_country_backfill_query() -> list[dict[str, Any]]:
    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    job = client.query(country_backfill_query())
    return [dict(row.items()) for row in job.result()]


def run_country_profile_query(
    week_start_utc: datetime,
    week_end_utc: datetime,
) -> list[dict[str, Any]]:
    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    job = client.query(
        country_profile_query(),
        job_config=build_query_job_config(week_start_utc, week_end_utc),
    )
    return [dict(row.items()) for row in job.result()]


def to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value.isoformat()
    return value


def normalize_row(row: dict[str, Any], default_timestamp: str) -> dict[str, Any]:
    if USER_ID_FIELD not in row or row[USER_ID_FIELD] in (None, ""):
        raise ValueError(f"Result row is missing required field '{USER_ID_FIELD}'.")

    missing_wpa = [field for field in REQUIRED_WPA_FIELDS if field not in row]
    if missing_wpa:
        raise ValueError(
            "Result row is missing required WPA fields: "
            + ", ".join(sorted(missing_wpa))
        )

    user_id = str(row[USER_ID_FIELD])
    event_name = str(row.get(EVENT_NAME_FIELD) or APPCUES_EVENT_NAME)
    raw_timestamp = row.get(EVENT_TIMESTAMP_FIELD)
    event_timestamp = (
        to_jsonable(raw_timestamp)
        if raw_timestamp not in (None, "")
        else default_timestamp
    )

    attributes = {
        key: to_jsonable(value)
        for key, value in row.items()
        if key not in RESERVED_FIELDS and value is not None
    }

    return {
        "user_id": user_id,
        "event_name": event_name,
        "event_timestamp": event_timestamp,
        "attributes": attributes,
    }


def validate_credentials() -> None:
    if not APPCUES_ACCOUNT_ID:
        raise ValueError("Missing required APPCUES_ACCOUNT_ID.")
    if not APPCUES_API_KEY:
        raise ValueError("Missing required APPCUES_API_KEY.")
    if not APPCUES_API_SECRET:
        raise ValueError("Missing required APPCUES_API_SECRET.")


def should_retry(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def retry_delay_seconds(attempt: int, response: requests.Response | None = None) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
    return BASE_RETRY_DELAY_SECONDS * (2 ** max(attempt - 1, 0))


def appcues_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        response = None
        try:
            response = requests.request(
                method,
                url,
                auth=(APPCUES_API_KEY, APPCUES_API_SECRET),
                timeout=REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
            if 200 <= response.status_code < 300:
                return response

            if not should_retry(response.status_code):
                response.raise_for_status()

            last_error = RuntimeError(
                f"Appcues request failed with status {response.status_code}: {response.text}"
            )
        except requests.RequestException as exc:
            last_error = exc
            if response is not None and not should_retry(response.status_code):
                raise

        if attempt == MAX_RETRIES:
            break

        time.sleep(retry_delay_seconds(attempt, response))

    raise RuntimeError(
        f"Appcues request failed after {MAX_RETRIES} attempts: {method} {url}"
    ) from last_error


def wait_for_appcues_job(job_url: str) -> dict[str, Any]:
    deadline = time.monotonic() + APPCUES_JOB_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        job = appcues_request("GET", job_url).json()
        status = str(job.get("status", "")).upper()
        if status == "COMPLETED":
            return job
        if status == "FAILED":
            raise RuntimeError(f"Appcues offline job failed: {job.get('id')}")
        time.sleep(APPCUES_JOB_POLL_SECONDS)
    raise TimeoutError(f"Appcues offline job did not finish in time: {job_url}")


def completion_attributes(job: dict[str, Any]) -> dict[str, Any]:
    for event in reversed(job.get("events", [])):
        if event.get("name") == "Completed":
            return event.get("attributes") or {}
    return {}


def ndjson_chunks(documents: list[dict[str, Any]]) -> list[bytes]:
    chunks: list[bytes] = []
    current_lines: list[bytes] = []
    current_size = 0

    for document in documents:
        line = json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        line_size = len(line) + (1 if current_lines else 0)
        if len(line) > APPCUES_IMPORT_CHUNK_BYTES:
            raise ValueError("A single Appcues import row exceeds the chunk size limit.")
        if current_lines and current_size + line_size > APPCUES_IMPORT_CHUNK_BYTES:
            chunks.append(b"\n".join(current_lines))
            current_lines = []
            current_size = 0
            line_size = len(line)
        current_lines.append(line)
        current_size += line_size

    if current_lines:
        chunks.append(b"\n".join(current_lines))
    return chunks


def submit_bulk_import(
    import_type: str,
    documents: list[dict[str, Any]],
) -> dict[str, Any]:
    chunks = ndjson_chunks(documents)
    completed_count = 0
    failed_count = 0
    job_ids: list[str] = []
    payload_bytes = 0

    for chunk in chunks:
        payload_bytes += len(chunk)
        response = appcues_request(
            "POST",
            f"{APPCUES_BASE_URL}/v2/accounts/{APPCUES_ACCOUNT_ID}/import/{import_type}",
            data=chunk,
            headers={"Content-Type": "application/ndjson"},
        ).json()
        job = wait_for_appcues_job(response["job_url"])
        completed = completion_attributes(job)
        chunk_rows = chunk.count(b"\n") + 1
        completed_count += int(completed.get("ingestions_completed", chunk_rows))
        failed_count += int(completed.get("ingestions_failed", 0))
        failed_count += int(completed.get("requests_failed", 0))
        if job.get("id"):
            job_ids.append(str(job["id"]))

    return {
        "submitted": len(documents),
        "sent": completed_count,
        "failed": failed_count,
        "appcues_job_ids": job_ids,
        "chunks": len(chunks),
        "payload_bytes": payload_bytes,
    }


def canonical_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace(" ", "T").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        row["user_id"],
        row["event_name"],
        canonical_timestamp(row["event_timestamp"]),
    )


def existing_event_keys(rows: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    if not SKIP_EXISTING_EVENTS or not rows:
        return set()

    timestamps = [
        datetime.fromisoformat(row["event_timestamp"].replace("Z", "+00:00"))
        for row in rows
    ]
    event_names = sorted({row["event_name"] for row in rows})
    name_condition = (
        ["name", "==", event_names[0]]
        if len(event_names) == 1
        else ["name", "in", event_names]
    )
    export_response = appcues_request(
        "POST",
        f"{APPCUES_BASE_URL}/v2/accounts/{APPCUES_ACCOUNT_ID}/export/events",
        json={
            "format": "json",
            "conditions": [name_condition],
            "start_time": min(timestamps).isoformat(),
            "end_time": (max(timestamps) + timedelta(seconds=1)).isoformat(),
            "time_zone": "+00:00",
        },
    ).json()
    export_job = wait_for_appcues_job(export_response["job_url"])
    signed_url = completion_attributes(export_job).get("signed_s3_url")
    if not signed_url:
        raise RuntimeError("Appcues export completed without a download URL.")

    export_file = requests.get(signed_url, timeout=REQUEST_TIMEOUT_SECONDS)
    export_file.raise_for_status()
    keys: set[tuple[str, str, str]] = set()
    with ZipFile(io.BytesIO(export_file.content)) as archive:
        for entry_name in archive.namelist():
            with archive.open(entry_name) as entry:
                for raw_line in entry:
                    if not raw_line.strip():
                        continue
                    event = json.loads(raw_line)
                    keys.add(
                        (
                            str(event["user_id"]),
                            str(event["name"]),
                            canonical_timestamp(str(event["timestamp"])),
                        )
                    )
    return keys


def send_events(rows: list[dict[str, Any]], default_timestamp: str) -> dict[str, Any]:
    normalized_rows = [normalize_row(row, default_timestamp) for row in rows]
    unique_rows = {event_key(row): row for row in normalized_rows}

    if DRY_RUN:
        return {
            "total_rows": len(rows),
            "source_duplicates": len(rows) - len(unique_rows),
            "would_submit": len(unique_rows),
            "sent": 0,
            "failed": 0,
        }

    validate_credentials()
    existing_keys = existing_event_keys(list(unique_rows.values()))
    pending_rows = [
        row for key, row in unique_rows.items() if key not in existing_keys
    ]
    if not pending_rows:
        return {
            "total_rows": len(rows),
            "source_duplicates": len(rows) - len(unique_rows),
            "already_present": len(unique_rows),
            "submitted": 0,
            "sent": 0,
            "failed": 0,
        }

    import_summary = submit_bulk_import(
        "events",
        [
            {
                "user_id": row["user_id"],
                "name": row["event_name"],
                "timestamp": row["event_timestamp"],
                "attributes": row["attributes"],
            }
            for row in pending_rows
        ],
    )
    result = {
        "total_rows": len(rows),
        "source_duplicates": len(rows) - len(unique_rows),
        "already_present": len(unique_rows) - len(pending_rows),
        **import_summary,
    }
    if result["failed"] and FAIL_ON_PARTIAL_ERRORS:
        raise RuntimeError(json.dumps(result, ensure_ascii=True))
    return result


def normalize_country_profile(row: dict[str, Any]) -> dict[str, str] | None:
    user_id = str(row.get(USER_ID_FIELD) or "").strip()
    country_code = str(row.get("country_code") or "").strip().upper()
    country_name = str(row.get("country_name") or "").strip()
    if not user_id or (not country_code and not country_name):
        return None

    profile = {
        "user_id": user_id,
        "country": country_name or country_code,
    }
    if country_code:
        profile["country_code"] = country_code
    if country_name:
        profile["country_name"] = country_name
    return profile


def send_country_profiles(rows: list[dict[str, Any]]) -> dict[str, Any]:
    profiles = [normalize_country_profile(row) for row in rows]
    profiles_by_user = {
        profile["user_id"]: profile
        for profile in profiles
        if profile is not None
    }
    skipped_without_country = len(rows) - len([p for p in profiles if p is not None])

    if DRY_RUN:
        return {
            "total_rows": len(rows),
            "unique_profiles": len(profiles_by_user),
            "skipped_without_country": skipped_without_country,
            "would_submit": len(profiles_by_user),
            "sent": 0,
            "failed": 0,
        }

    validate_credentials()
    if not profiles_by_user:
        return {
            "total_rows": len(rows),
            "unique_profiles": 0,
            "skipped_without_country": skipped_without_country,
            "submitted": 0,
            "sent": 0,
            "failed": 0,
            "appcues_job_ids": [],
            "chunks": 0,
            "payload_bytes": 0,
        }

    result = {
        "total_rows": len(rows),
        "unique_profiles": len(profiles_by_user),
        "skipped_without_country": skipped_without_country,
        **submit_bulk_import("profiles", list(profiles_by_user.values())),
    }
    if result["failed"] and FAIL_ON_PARTIAL_ERRORS:
        raise RuntimeError(json.dumps(result, ensure_ascii=True))
    return result


def main() -> None:
    if COUNTRY_PROFILE_BACKFILL_ONLY:
        rows = run_country_backfill_query()
        summary = send_country_profiles(rows)
        print(
            json.dumps(
                {
                    "message": "Appcues country profile backfill completed.",
                    "project": PROJECT_ID,
                    "dataset": COUNTRY_BQ_DATASET,
                    "table": COUNTRY_BQ_TABLE,
                    "dry_run": DRY_RUN,
                    **summary,
                },
                ensure_ascii=True,
            )
        )
        return

    week_start_utc, week_end_utc = resolve_week_window()
    event_timestamp = week_end_utc.isoformat().replace("+00:00", "Z")
    rows = run_query(week_start_utc, week_end_utc)
    summary = send_events(rows, event_timestamp)
    if SYNC_COUNTRY_PROFILES:
        country_rows = run_country_profile_query(week_start_utc, week_end_utc)
        country_profile_summary = send_country_profiles(country_rows)
    else:
        country_profile_summary = {"enabled": False}
    print(
        json.dumps(
            {
                "message": "Weekly Appcues activity sync completed.",
                "project": PROJECT_ID,
                "dataset": BQ_DATASET,
                "table": BQ_TABLE,
                "country_dataset": COUNTRY_BQ_DATASET,
                "country_table": COUNTRY_BQ_TABLE,
                "using_custom_sql": bool(BQ_SOURCE_SQL),
                "range_start": week_start_utc.date().isoformat(),
                "range_end_exclusive": week_end_utc.date().isoformat(),
                "backfill_start_week": BACKFILL_START_WEEK or None,
                "backfill_end_week": BACKFILL_END_WEEK or None,
                "window_time_zone": WINDOW_TIME_ZONE,
                "appcues_account_id_configured": bool(APPCUES_ACCOUNT_ID),
                "dry_run": DRY_RUN,
                "country_profiles": country_profile_summary,
                **summary,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
