import json
import os
import time
from datetime import date, datetime
from typing import Any

import requests
from google.cloud import bigquery


GCP_PROJECT = os.getenv("GCP_PROJECT", "fxr-analytics").strip()
BQ_LOCATION = os.getenv("BQ_LOCATION", "US").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "engagement_prod").strip()
BQ_TABLE = os.getenv("BQ_TABLE", "onboarding_questionnaire_response").strip()
APPCUES_BASE_URL = os.getenv("APPCUES_BASE_URL", "https://api.appcues.com").rstrip("/")
APPCUES_ACCOUNT_ID = os.getenv("APPCUES_ACCOUNT_ID", "").strip()
APPCUES_API_KEY = os.getenv("APPCUES_API_KEY", "").strip()
APPCUES_API_SECRET = os.getenv("APPCUES_API_SECRET", "").strip()
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_RETRY_DELAY_SECONDS = float(os.getenv("BASE_RETRY_DELAY_SECONDS", "2"))
APPCUES_JOB_POLL_SECONDS = float(os.getenv("APPCUES_JOB_POLL_SECONDS", "5"))
APPCUES_JOB_TIMEOUT_SECONDS = int(os.getenv("APPCUES_JOB_TIMEOUT_SECONDS", "1800"))
APPCUES_IMPORT_CHUNK_BYTES = int(os.getenv("APPCUES_IMPORT_CHUNK_BYTES", "18000000"))
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"
FAIL_ON_PARTIAL_ERRORS = (
    os.getenv("FAIL_ON_PARTIAL_ERRORS", "true").strip().lower() == "true"
)

KNOWN_MARKETS = ("crypto", "forex", "futures", "indices", "stocks")


def source_query() -> str:
    """Return one deterministic Appcues profile row per user."""
    return f"""
    WITH latest_response_version AS (
      SELECT * EXCEPT (response_version_rank)
      FROM (
        SELECT
          source.*,
          ROW_NUMBER() OVER (
            PARTITION BY source._id
            ORDER BY source._sdc_sequence DESC, source._sdc_extracted_at DESC
          ) AS response_version_rank
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` AS source
      )
      WHERE response_version_rank = 1
    ),

    latest_response_per_user AS (
      SELECT * EXCEPT (user_response_rank)
      FROM (
        SELECT
          source.*,
          ROW_NUMBER() OVER (
            PARTITION BY source.userid
            ORDER BY
              source._sdc_extracted_at DESC,
              source._sdc_sequence DESC,
              source._id DESC
          ) AS user_response_rank
        FROM latest_response_version AS source
        WHERE NULLIF(TRIM(source.userid), '') IS NOT NULL
      )
      WHERE user_response_rank = 1
    ),

    answers AS (
      SELECT
        source.userid AS user_id,
        source.version AS questionnaire_version,
        LOWER(TRIM(answer.value.questionkey)) AS question_key,
        answer.value.stepnumber AS step_number,
        answer.value.answeredat AS answered_at,
        COALESCE(answer.value.skipped, FALSE) AS is_skipped,
        NULLIF(TRIM(answer.value.othervalue), '') AS other_value,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT NULLIF(TRIM(option.value), '')
            FROM UNNEST(answer.value.values) AS option
            WHERE NULLIF(TRIM(option.value), '') IS NOT NULL
            ORDER BY option.value
          ),
          '|'
        ) AS selected_values
      FROM latest_response_per_user AS source
      LEFT JOIN UNNEST(source.answers) AS answer ON TRUE
    ),

    profiles AS (
      SELECT
        user_id,
        TRUE AS onboarding_questionnaire_response_present,
        MAX(questionnaire_version) AS onboarding_questionnaire_version,
        MAX(IF(question_key = 'experience', selected_values, NULL)) AS onboarding_experience,
        MAX(IF(question_key = 'markets', selected_values, NULL)) AS onboarding_markets,
        MAX(IF(question_key = 'markets', other_value, NULL)) AS onboarding_markets_other,
        MAX(IF(question_key = 'style', selected_values, NULL)) AS onboarding_style,
        MAX(IF(question_key = 'life_fit', selected_values, NULL)) AS onboarding_life_fit,
        MAX(IF(question_key = 'goal', selected_values, NULL)) AS onboarding_goal,
        MAX(IF(question_key = 'discovery_channel', selected_values, NULL)) AS onboarding_discovery_channel,
        MAX(IF(question_key = 'discovery_channel', other_value, NULL)) AS onboarding_discovery_channel_other,
        MAX(answered_at) AS onboarding_answered_at,
        COUNT(DISTINCT IF(NOT is_skipped AND question_key IN (
          'experience', 'markets', 'style', 'life_fit', 'goal'
        ), question_key, NULL)) = 5 AS onboarding_completed
      FROM answers
      GROUP BY user_id
    )

    SELECT
      profiles.*,
      STRPOS(CONCAT('|', onboarding_markets, '|'), '|crypto|') > 0 AS onboarding_market_crypto,
      STRPOS(CONCAT('|', onboarding_markets, '|'), '|forex|') > 0 AS onboarding_market_forex,
      STRPOS(CONCAT('|', onboarding_markets, '|'), '|futures|') > 0 AS onboarding_market_futures,
      STRPOS(CONCAT('|', onboarding_markets, '|'), '|indices|') > 0 AS onboarding_market_indices,
      STRPOS(CONCAT('|', onboarding_markets, '|'), '|stocks|') > 0 AS onboarding_market_stocks
    FROM profiles
    """


def to_jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def normalize_profile(row: dict[str, Any]) -> dict[str, Any]:
    user_id = str(row.get("user_id") or "").strip()
    if not user_id:
        raise ValueError("Onboarding profile is missing user_id.")

    profile: dict[str, Any] = {"user_id": user_id}
    for key, value in row.items():
        if key == "user_id" or value is None:
            continue
        if isinstance(value, (list, tuple, dict)):
            raise TypeError(f"Appcues profile property {key} must be scalar.")
        profile[key] = to_jsonable(value)
    return profile


def fetch_profiles() -> list[dict[str, Any]]:
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
    rows = client.query(source_query()).result()
    return [normalize_profile(dict(row.items())) for row in rows]


def validate_credentials() -> None:
    missing = [
        name
        for name, value in (
            ("APPCUES_ACCOUNT_ID", APPCUES_ACCOUNT_ID),
            ("APPCUES_API_KEY", APPCUES_API_KEY),
            ("APPCUES_API_SECRET", APPCUES_API_SECRET),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")


def should_retry(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def retry_delay_seconds(attempt: int, response: requests.Response | None) -> float:
    if response is not None and response.headers.get("Retry-After"):
        try:
            return max(float(response.headers["Retry-After"]), 0.0)
        except ValueError:
            pass
    return BASE_RETRY_DELAY_SECONDS * (2 ** max(attempt - 1, 0))


def appcues_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        response: requests.Response | None = None
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
                f"Appcues returned HTTP {response.status_code}: {response.text}"
            )
        except requests.RequestException as exc:
            last_error = exc
            if response is not None and not should_retry(response.status_code):
                raise

        if attempt < MAX_RETRIES:
            time.sleep(retry_delay_seconds(attempt, response))

    raise RuntimeError(
        f"Appcues request failed after {MAX_RETRIES} attempts: {method} {url}"
    ) from last_error


def completion_attributes(job: dict[str, Any]) -> dict[str, Any]:
    for event in reversed(job.get("events", [])):
        if event.get("name") == "Completed":
            return event.get("attributes") or {}
    return {}


def wait_for_appcues_job(job_url: str) -> dict[str, Any]:
    deadline = time.monotonic() + APPCUES_JOB_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        job = appcues_request("GET", job_url).json()
        status = str(job.get("status", "")).upper()
        if status == "COMPLETED":
            return job
        if status == "FAILED":
            raise RuntimeError(f"Appcues import job failed: {job.get('id')}")
        time.sleep(APPCUES_JOB_POLL_SECONDS)
    raise TimeoutError(f"Appcues import job timed out: {job_url}")


def ndjson_chunks(profiles: list[dict[str, Any]]) -> list[bytes]:
    chunks: list[bytes] = []
    current_lines: list[bytes] = []
    current_size = 0

    for profile in profiles:
        line = json.dumps(profile, ensure_ascii=True, separators=(",", ":")).encode()
        line_size = len(line) + (1 if current_lines else 0)
        if len(line) > APPCUES_IMPORT_CHUNK_BYTES:
            raise ValueError("A single profile exceeds the Appcues chunk limit.")
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


def import_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    validate_credentials()
    chunks = ndjson_chunks(profiles)
    completed_count = 0
    failed_count = 0
    job_ids: list[str] = []
    payload_bytes = 0

    for chunk in chunks:
        payload_bytes += len(chunk)
        response = appcues_request(
            "POST",
            f"{APPCUES_BASE_URL}/v2/accounts/{APPCUES_ACCOUNT_ID}/import/profiles",
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
        "submitted": len(profiles),
        "sent": completed_count,
        "failed": failed_count,
        "appcues_job_ids": job_ids,
        "chunks": len(chunks),
        "payload_bytes": payload_bytes,
    }


def main() -> None:
    profiles = fetch_profiles()
    if DRY_RUN:
        summary = {
            "would_submit": len(profiles),
            "chunks": len(ndjson_chunks(profiles)),
            "payload_bytes": sum(len(chunk) for chunk in ndjson_chunks(profiles)),
            "completed_profiles": sum(
                profile.get("onboarding_completed") is True for profile in profiles
            ),
        }
    else:
        summary = import_profiles(profiles)
        if summary["failed"] and FAIL_ON_PARTIAL_ERRORS:
            raise RuntimeError(json.dumps(summary, ensure_ascii=True))

    print(
        json.dumps(
            {
                "message": "Appcues onboarding profile sync completed.",
                "project": GCP_PROJECT,
                "dataset": BQ_DATASET,
                "table": BQ_TABLE,
                "endpoint": "/v2/accounts/{account_id}/import/profiles",
                "dry_run": DRY_RUN,
                **summary,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
