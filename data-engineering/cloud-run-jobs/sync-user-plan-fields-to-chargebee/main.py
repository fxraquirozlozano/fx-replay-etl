from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable
from urllib.parse import quote

import requests
from google.cloud import bigquery


LOGGER = logging.getLogger("sync_user_plan_fields_to_chargebee")

PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
SOURCE_DATASET = os.getenv("SOURCE_DATASET", "analytics_marts")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "users_view")
CHARGEBEE_DATASET = os.getenv("CHARGEBEE_DATASET", "chargebee")
CHARGEBEE_CUSTOMERS_TABLE = os.getenv("CHARGEBEE_CUSTOMERS_TABLE", "customers")
STATE_DATASET = os.getenv("STATE_DATASET", "sandbox")
SOURCE_SNAPSHOT_TABLE = os.getenv(
    "SOURCE_SNAPSHOT_TABLE", "chargebee_customer_plan_fields_source_snapshot"
)
STATE_TABLE = os.getenv(
    "STATE_TABLE", "chargebee_customer_plan_fields_sync_state"
)
FAILURE_TABLE = os.getenv(
    "FAILURE_TABLE", "chargebee_customer_plan_fields_sync_failures"
)

CHARGEBEE_SITE = os.getenv("CHARGEBEE_SITE", "").strip()
CHARGEBEE_API_KEY = os.getenv("CHARGEBEE_API_KEY", "").strip()
BEGINNER_CATEGORY_FIELD = os.getenv(
    "CHARGEBEE_BEGINNER_CATEGORY_FIELD", "cf_Beginner_Category"
).strip()
PLAN_TYPE_FIELD = os.getenv(
    "CHARGEBEE_PLAN_TYPE_FIELD", "cf_Plan_Type"
).strip()
NULL_PLAN_TYPE_VALUE = os.getenv("NULL_PLAN_TYPE_VALUE", "No Plan").strip()
SYNC_VERSION = os.getenv("SYNC_VERSION", "v1").strip()

MAX_CUSTOMERS_PER_RUN = int(os.getenv("MAX_CUSTOMERS_PER_RUN", "200000"))
REQUESTS_PER_MINUTE = int(os.getenv("CHARGEBEE_REQUESTS_PER_MINUTE", "800"))
HTTP_CONCURRENCY = int(os.getenv("HTTP_CONCURRENCY", "12"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_RETRY_DELAY_SECONDS = float(os.getenv("BASE_RETRY_DELAY_SECONDS", "1"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
FAIL_ON_PARTIAL_ERRORS = (
    os.getenv("FAIL_ON_PARTIAL_ERRORS", "true").lower() == "true"
)


STATE_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STATE_DATASET}.{STATE_TABLE}` (
  customer_id STRING NOT NULL,
  beginner_category STRING NOT NULL,
  plan_type STRING NOT NULL,
  source_hash STRING NOT NULL,
  sync_version STRING NOT NULL,
  synced_at TIMESTAMP NOT NULL,
  run_id STRING NOT NULL
)
CLUSTER BY customer_id
"""

FAILURE_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STATE_DATASET}.{FAILURE_TABLE}` (
  run_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  beginner_category STRING NOT NULL,
  plan_type STRING NOT NULL,
  source_hash STRING NOT NULL,
  attempted_at TIMESTAMP NOT NULL,
  status_code INT64,
  error STRING
)
PARTITION BY DATE(attempted_at)
CLUSTER BY customer_id, run_id
"""

SOURCE_SNAPSHOT_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}` (
  customer_id STRING NOT NULL,
  beginner_category STRING NOT NULL,
  plan_type STRING NOT NULL,
  snapshot_date DATE NOT NULL,
  snapshotted_at TIMESTAMP NOT NULL
)
CLUSTER BY customer_id
"""


def validate_config() -> None:
    if not DRY_RUN and not CHARGEBEE_SITE:
        raise ValueError("CHARGEBEE_SITE is required unless DRY_RUN=true")
    if not DRY_RUN and not CHARGEBEE_API_KEY:
        raise ValueError("CHARGEBEE_API_KEY is required unless DRY_RUN=true")
    if not BEGINNER_CATEGORY_FIELD.startswith("cf_"):
        raise ValueError("CHARGEBEE_BEGINNER_CATEGORY_FIELD must start with cf_")
    if not PLAN_TYPE_FIELD.startswith("cf_"):
        raise ValueError("CHARGEBEE_PLAN_TYPE_FIELD must start with cf_")
    for name, value in (
        ("MAX_CUSTOMERS_PER_RUN", MAX_CUSTOMERS_PER_RUN),
        ("CHARGEBEE_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE),
        ("HTTP_CONCURRENCY", HTTP_CONCURRENCY),
        ("REQUEST_TIMEOUT_SECONDS", REQUEST_TIMEOUT_SECONDS),
    ):
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")
    if MAX_RETRIES < 0:
        raise ValueError("MAX_RETRIES cannot be negative")


@dataclass(frozen=True)
class CustomerUpdate:
    customer_id: str
    beginner_category: str
    plan_type: str

    @property
    def source_hash(self) -> str:
        payload = json.dumps(
            {
                "beginner_category": self.beginner_category,
                "plan_type": self.plan_type,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SyncResult:
    update: CustomerUpdate
    succeeded: bool
    status_code: int | None = None
    error: str | None = None


class RateLimiter:
    """Thread-safe limiter shared by initial requests and retries."""

    def __init__(self, requests_per_minute: int) -> None:
        self._interval = 60.0 / requests_per_minute
        self._next_request_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_request_at - now)
            self._next_request_at = max(now, self._next_request_at) + self._interval
        if delay:
            time.sleep(delay)


class ChargebeeClient:
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self) -> None:
        self._limiter = RateLimiter(REQUESTS_PER_MINUTE)
        self._thread_local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session

    def update_customer(self, update: CustomerUpdate, run_id: str) -> SyncResult:
        customer_id = quote(update.customer_id, safe="")
        url = (
            f"https://{CHARGEBEE_SITE}.chargebee.com/api/v2/"
            f"customers/{customer_id}"
        )
        payload = {
            BEGINNER_CATEGORY_FIELD: update.beginner_category,
            PLAN_TYPE_FIELD: update.plan_type,
        }
        idempotency_key = hashlib.sha256(
            f"{run_id}:{update.customer_id}:{update.source_hash}".encode("utf-8")
        ).hexdigest()

        for attempt in range(MAX_RETRIES + 1):
            self._limiter.wait()
            try:
                response = self._session().post(
                    url,
                    auth=(CHARGEBEE_API_KEY, ""),
                    data=payload,
                    headers={"chargebee-idempotency-key": idempotency_key},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    return SyncResult(update, False, error=str(exc)[:2000])
                self._retry_sleep(attempt, None)
                continue

            if 200 <= response.status_code < 300:
                return SyncResult(update, True, status_code=response.status_code)

            if (
                response.status_code not in self.RETRYABLE_STATUS_CODES
                or attempt == MAX_RETRIES
            ):
                return SyncResult(
                    update,
                    False,
                    status_code=response.status_code,
                    error=response.text[:2000],
                )

            self._retry_sleep(attempt, response.headers.get("Retry-After"))

        return SyncResult(update, False, error="Retry loop exhausted")

    @staticmethod
    def _retry_sleep(attempt: int, retry_after: str | None) -> None:
        try:
            delay = float(retry_after) if retry_after else None
        except ValueError:
            delay = None
        if delay is None:
            delay = BASE_RETRY_DELAY_SECONDS * (2**attempt) + random.uniform(0, 0.5)
        time.sleep(max(0.0, delay))


class BigQueryStore:
    def __init__(self, client: bigquery.Client) -> None:
        self.client = client

    def ensure_tables(self) -> None:
        self.client.query(SOURCE_SNAPSHOT_SCHEMA_SQL).result()
        self.client.query(STATE_SCHEMA_SQL).result()
        self.client.query(FAILURE_SCHEMA_SQL).result()

    def refresh_source_snapshot(self) -> int:
        freshness_query = f"""
        SELECT COUNTIF(snapshot_date = CURRENT_DATE('UTC')) > 0 AS is_current
        FROM `{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}`
        """
        freshness_rows = list(self.client.query(freshness_query).result())
        if freshness_rows and freshness_rows[0]["is_current"]:
            return self.client.get_table(
                f"{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}"
            ).num_rows

        refresh_query = f"""
        CREATE OR REPLACE TABLE
          `{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}`
        CLUSTER BY customer_id AS
        WITH current_chargebee_customers AS (
          SELECT id, deleted
          FROM `{PROJECT_ID}.{CHARGEBEE_DATASET}.{CHARGEBEE_CUSTOMERS_TABLE}`
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _sdc_sequence DESC, _sdc_received_at DESC
          ) = 1
        )
        SELECT
          source.customer_id,
          source.beginner_history_segment AS beginner_category,
          COALESCE(source.last_subscription_plan_type, @null_plan_type) AS plan_type,
          CURRENT_DATE('UTC') AS snapshot_date,
          CURRENT_TIMESTAMP() AS snapshotted_at
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}` source
        INNER JOIN current_chargebee_customers chargebee
          ON chargebee.id = source.customer_id
         AND NOT COALESCE(chargebee.deleted, FALSE)
        WHERE source.customer_id IS NOT NULL
          AND TRIM(source.customer_id) != ''
          AND source.beginner_history_segment IS NOT NULL
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "null_plan_type", "STRING", NULL_PLAN_TYPE_VALUE
                )
            ]
        )
        self.client.query(refresh_query, job_config=config).result()
        return self.client.get_table(
            f"{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}"
        ).num_rows

    def pending_updates(self) -> Iterable[CustomerUpdate]:
        query = f"""
        SELECT
          desired.customer_id,
          desired.beginner_category,
          desired.plan_type
        FROM `{PROJECT_ID}.{STATE_DATASET}.{SOURCE_SNAPSHOT_TABLE}` desired
        LEFT JOIN `{PROJECT_ID}.{STATE_DATASET}.{STATE_TABLE}` state
          USING (customer_id)
        WHERE state.customer_id IS NULL
           OR state.beginner_category IS DISTINCT FROM desired.beginner_category
           OR state.plan_type IS DISTINCT FROM desired.plan_type
           OR state.sync_version IS DISTINCT FROM @sync_version
        ORDER BY customer_id
        LIMIT @max_customers
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sync_version", "STRING", SYNC_VERSION),
                bigquery.ScalarQueryParameter(
                    "max_customers", "INT64", MAX_CUSTOMERS_PER_RUN
                ),
            ]
        )
        for row in self.client.query(query, job_config=config).result():
            yield CustomerUpdate(
                customer_id=row["customer_id"],
                beginner_category=row["beginner_category"],
                plan_type=row["plan_type"],
            )

    def persist_results(self, results: list[SyncResult], run_id: str) -> None:
        attempted_at = datetime.now(UTC).isoformat()
        successes = [
            {
                "customer_id": result.update.customer_id,
                "beginner_category": result.update.beginner_category,
                "plan_type": result.update.plan_type,
                "source_hash": result.update.source_hash,
                "sync_version": SYNC_VERSION,
                "synced_at": attempted_at,
                "run_id": run_id,
            }
            for result in results
            if result.succeeded
        ]
        failures = [
            {
                "run_id": run_id,
                "customer_id": result.update.customer_id,
                "beginner_category": result.update.beginner_category,
                "plan_type": result.update.plan_type,
                "source_hash": result.update.source_hash,
                "attempted_at": attempted_at,
                "status_code": result.status_code,
                "error": result.error,
            }
            for result in results
            if not result.succeeded
        ]
        if successes:
            self._merge_successes(successes, run_id)
        if failures:
            failure_table = f"{PROJECT_ID}.{STATE_DATASET}.{FAILURE_TABLE}"
            for start in range(0, len(failures), 500):
                errors = self.client.insert_rows_json(
                    failure_table, failures[start : start + 500]
                )
                if errors:
                    raise RuntimeError(
                        "Failed to persist Chargebee failures: "
                        + json.dumps(errors)[:4000]
                    )

    def _merge_successes(self, rows: list[dict[str, Any]], run_id: str) -> None:
        suffix = "".join(character for character in run_id if character.isalnum())[-32:]
        stage_table = (
            f"{PROJECT_ID}.{STATE_DATASET}."
            f"{STATE_TABLE}_stage_{suffix or uuid.uuid4().hex}"
        )
        schema = [
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("beginner_category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("plan_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sync_version", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("synced_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        ]
        load_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        self.client.load_table_from_json(
            rows, stage_table, job_config=load_config
        ).result()
        merge_sql = f"""
        MERGE `{PROJECT_ID}.{STATE_DATASET}.{STATE_TABLE}` target
        USING `{stage_table}` source
          ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
          beginner_category = source.beginner_category,
          plan_type = source.plan_type,
          source_hash = source.source_hash,
          sync_version = source.sync_version,
          synced_at = source.synced_at,
          run_id = source.run_id
        WHEN NOT MATCHED THEN INSERT (
          customer_id, beginner_category, plan_type, source_hash,
          sync_version, synced_at, run_id
        ) VALUES (
          source.customer_id, source.beginner_category, source.plan_type,
          source.source_hash, source.sync_version, source.synced_at, source.run_id
        )
        """
        try:
            self.client.query(merge_sql).result()
        finally:
            self.client.delete_table(stage_table, not_found_ok=True)


def sync_updates(
    updates: list[CustomerUpdate], client: ChargebeeClient, run_id: str
) -> list[SyncResult]:
    results: list[SyncResult] = []
    with ThreadPoolExecutor(max_workers=HTTP_CONCURRENCY) as executor:
        # Bound queued futures so a 200k-customer backfill stays memory-safe.
        window_size = max(HTTP_CONCURRENCY * 4, 100)
        for start in range(0, len(updates), window_size):
            window = updates[start : start + window_size]
            futures = {
                executor.submit(client.update_customer, update, run_id): update
                for update in window
            }
            for future in as_completed(futures):
                update = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:  # Preserve unexpected per-customer failures.
                    LOGGER.exception("Unexpected customer sync failure")
                    results.append(
                        SyncResult(update, False, error=str(exc)[:2000])
                    )
    return results


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    validate_config()
    run_id = os.getenv("CLOUD_RUN_EXECUTION", "").strip() or uuid.uuid4().hex
    store = BigQueryStore(bigquery.Client(project=PROJECT_ID))
    store.ensure_tables()
    snapshot_customers = store.refresh_source_snapshot()
    updates = list(store.pending_updates())

    if DRY_RUN:
        print(
            "Chargebee customer fields dry run completed",
            {
                "run_id": run_id,
                "selected_customers": len(updates),
                "snapshot_customers": snapshot_customers,
                "max_customers_per_run": MAX_CUSTOMERS_PER_RUN,
                "beginner_category_field": BEGINNER_CATEGORY_FIELD,
                "plan_type_field": PLAN_TYPE_FIELD,
            },
        )
        return

    results = sync_updates(updates, ChargebeeClient(), run_id)
    store.persist_results(results, run_id)
    succeeded = sum(result.succeeded for result in results)
    failed = len(results) - succeeded
    print(
        "Chargebee customer fields sync completed",
        {
            "run_id": run_id,
            "selected_customers": len(updates),
            "snapshot_customers": snapshot_customers,
            "succeeded": succeeded,
            "failed": failed,
            "requests_per_minute": REQUESTS_PER_MINUTE,
        },
    )
    if failed and FAIL_ON_PARTIAL_ERRORS:
        raise RuntimeError(f"Chargebee sync completed with {failed} failed customers")


if __name__ == "__main__":
    main()
