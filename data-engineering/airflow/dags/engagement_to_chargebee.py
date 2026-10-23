from __future__ import annotations

import json
import logging
import os
import random
import re
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import lru_cache
from typing import Any

import requests
from google.cloud import bigquery, secretmanager, storage

try:
    from airflow.sdk import Variable
except ImportError:  # pragma: no cover
    from airflow.models import Variable


logger = logging.getLogger(__name__)


def airflow_var(name: str, default: str) -> str:
    try:
        return Variable.get(name)
    except Exception:
        return default


SCHEDULE = airflow_var("CHARGEBEE_DAG_SCHEDULE", "0 */8 5-11 * *")
MAX_RUNTIME_MINUTES = int(airflow_var("CHARGEBEE_MAX_RUNTIME_MINUTES", "300"))


def normalize_segment(value: str) -> str:
    return re.sub(r"[_\-\s]+", " ", value.strip().lower())


@dataclass(frozen=True)
class Config:
    project_id: str
    api_base: str
    api_key_secret_id: str
    bq_dataset: str
    failures_table_fqn: str
    customers_table_fqn: str
    gcs_bucket: str
    gcs_prefix: str
    requests_per_minute: int
    max_runtime_minutes: int
    max_api_calls_per_run: int
    max_workers: int
    request_timeout_seconds: int
    max_retries_per_request: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    excluded_segments: tuple[str, ...]
    min_segment_coverage: float
    extract_row_limit: int
    dry_run: bool
    progress_log_every: int
    progress_log_seconds: float


@lru_cache(maxsize=1)
def config() -> Config:
    project_id = airflow_var("CHARGEBEE_GCP_PROJECT_ID", "fxr-analytics")
    site = airflow_var("CHARGEBEE_SITE", "fxreplay")
    dataset = airflow_var("CHARGEBEE_BQ_DATASET", "chargebee")
    failures_table = airflow_var("CHARGEBEE_FAILURES_TABLE", "engagement_sync_failures")
    excluded_raw = airflow_var("CHARGEBEE_EXCLUDED_SEGMENTS", "Inactive_Subscription")

    return Config(
        project_id=project_id,
        api_base=f"https://{site}.chargebee.com/api/v2",
        api_key_secret_id=airflow_var("CHARGEBEE_API_KEY_SECRET_ID", "CHARGEBEE_API_KEY"),
        bq_dataset=dataset,
        failures_table_fqn=f"{project_id}.{dataset}.{failures_table}",
        customers_table_fqn=f"{project_id}.{dataset}.customers",
        gcs_bucket=airflow_var("CHARGEBEE_GCS_STAGING_BUCKET", "fx-replay-etl"),
        gcs_prefix=airflow_var("CHARGEBEE_GCS_STAGING_PREFIX", "tmp/engagement_to_chargebee"),
        requests_per_minute=int(airflow_var("CHARGEBEE_REQUESTS_PER_MINUTE", "800")),
        max_runtime_minutes=MAX_RUNTIME_MINUTES,
        max_api_calls_per_run=int(airflow_var("CHARGEBEE_MAX_API_CALLS_PER_RUN", "200000")),
        max_workers=int(airflow_var("CHARGEBEE_MAX_WORKERS", "12")),
        request_timeout_seconds=int(airflow_var("CHARGEBEE_REQUEST_TIMEOUT_SECONDS", "30")),
        max_retries_per_request=int(airflow_var("CHARGEBEE_MAX_RETRIES_PER_REQUEST", "3")),
        backoff_base_seconds=float(airflow_var("CHARGEBEE_BACKOFF_BASE_SECONDS", "1.0")),
        backoff_max_seconds=float(airflow_var("CHARGEBEE_BACKOFF_MAX_SECONDS", "30.0")),
        excluded_segments=tuple(
            normalize_segment(s) for s in excluded_raw.split(",") if s.strip()
        ),
        min_segment_coverage=float(airflow_var("CHARGEBEE_MIN_SEGMENT_COVERAGE", "0.9")),
        extract_row_limit=int(airflow_var("CHARGEBEE_EXTRACT_ROW_LIMIT", "0")),
        dry_run=airflow_var("CHARGEBEE_SYNC_DRY_RUN", "false").lower() == "true",
        progress_log_every=int(airflow_var("CHARGEBEE_PROGRESS_LOG_EVERY", "1000")),
        progress_log_seconds=float(airflow_var("CHARGEBEE_PROGRESS_LOG_SECONDS", "60")),
    )


FIELD_MAP: tuple[tuple[str, str], ...] = (
    ("days_since_last_activity", "cf_days_since_last_activity"),
    ("sessions_30d",             "cf_Sessions_30d"),
    ("active_days_30d",          "cf_Active_Days_30d"),
    ("replay_sessions_30d",      "cf_Replay_Sessions_30d"),
    ("engagement_segment",       "cf_Engagement_Segment"),
)


class ExtractSkipped(Exception):
    pass


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=config().project_id)


def get_gcs_client() -> storage.Client:
    return storage.Client(project=config().project_id)


def get_secret_value(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{config().project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": secret_path})
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            f"Could not read secret '{secret_id}'. Grant "
            f"'roles/secretmanager.secretAccessor' to the worker service account."
        ) from exc
    return response.payload.data.decode("utf-8").strip()


def get_chargebee_api_key() -> str:
    return get_secret_value(config().api_key_secret_id)


def _blob_name(task_tag: str, run_id: str) -> str:
    safe_run = run_id.replace(":", "").replace("+", "").replace(".", "")
    return f"{config().gcs_prefix}/{task_tag}/{safe_run}.jsonl"


def _upload_file_to_gcs(local_path: str, blob_name: str) -> str:
    bucket_name = config().gcs_bucket
    blob = get_gcs_client().bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(local_path, content_type="application/x-ndjson")
    return f"gs://{bucket_name}/{blob_name}"


def _download_gcs_to_local(gcs_uri: str) -> str:
    assert gcs_uri.startswith("gs://"), f"invalid URI: {gcs_uri}"
    bucket_name, _, blob_name = gcs_uri[len("gs://"):].partition("/")

    blob = get_gcs_client().bucket(bucket_name).blob(blob_name)
    tmp = tempfile.NamedTemporaryFile(mode="wb", suffix=".jsonl", delete=False)
    tmp.close()
    blob.download_to_filename(tmp.name)
    return tmp.name


def ensure_failures_table(client: bigquery.Client | None = None) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{config().failures_table_fqn}` (
        run_id STRING NOT NULL,
        target_month DATE NOT NULL,
        customer_id STRING NOT NULL,
        http_status STRING,
        error_message STRING,
        attempted_at TIMESTAMP NOT NULL,
        retried BOOL NOT NULL
    )
    PARTITION BY target_month
    CLUSTER BY customer_id, run_id
    """
    (client or get_bq_client()).query(ddl).result()


def resolve_target_month(explicit: str | date | None) -> date:
    if isinstance(explicit, date):
        return explicit.replace(day=1)
    if isinstance(explicit, str) and explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date().replace(day=1)
    first_of_current = datetime.now(UTC).date().replace(day=1)
    if first_of_current.month == 1:
        return date(first_of_current.year - 1, 12, 1)
    return date(first_of_current.year, first_of_current.month - 1, 1)


MONTH_ROWS_CTE = """
month_rows AS (
    SELECT
        b.customer_id,
        b.days_since_last_activity,
        IFNULL(b.created_sessions_qty,  0) AS sessions_30d,
        IFNULL(b.usage_days,            0) AS active_days_30d,
        IFNULL(b.activated_replays_qty, 0) AS replay_sessions_30d,
        c.user_profile          AS engagement_segment,
        IFNULL(LOWER(REGEXP_REPLACE(TRIM(c.user_profile), r'[_\\-\\s]+', ' ')), '')
            AS segment_norm
    FROM `fxr-analytics.engagement_marts.fct_user_behaviour_monthly` b
    LEFT JOIN `fxr-analytics.ds_products.user_clusters_monthly` c
        ON b.user_id = c.user_id
       AND b.data_month = c.data_month
    WHERE b.data_month = @target_month
      AND b.customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.customer_id
        ORDER BY b.usage_days DESC, b.created_sessions_qty DESC, b.user_id
    ) = 1
)
"""

ELIGIBLE_CTE = """
eligible AS (
    SELECT
        customer_id,
        IF(is_excluded, NULL, days_since_last_activity) AS days_since_last_activity,
        IF(is_excluded, 0,    sessions_30d)             AS sessions_30d,
        IF(is_excluded, 0,    active_days_30d)          AS active_days_30d,
        IF(is_excluded, 0,    replay_sessions_30d)      AS replay_sessions_30d,
        engagement_segment,
        is_excluded
    FROM (
        SELECT *, segment_norm IN UNNEST(@excluded_segments) AS is_excluded
        FROM month_rows
    )
)
"""

DIFF_PREDICATE = """
       IFNULL(e.days_since_last_activity, 0) != IFNULL(cb.cb_days_since_last_activity, 0)
    OR IFNULL(e.sessions_30d, 0)             != IFNULL(cb.cb_sessions_30d, 0)
    OR IFNULL(e.active_days_30d, 0)          != IFNULL(cb.cb_active_days_30d, 0)
    OR IFNULL(e.replay_sessions_30d, 0)      != IFNULL(cb.cb_replay_sessions_30d, 0)
    OR IFNULL(e.engagement_segment, '')      != IFNULL(cb.cb_engagement_segment, '')
"""

SELECTION_PREDICATE = f"""
    (NOT e.is_excluded OR cb.cb_engagement_segment IS NOT NULL)
    AND ({DIFF_PREDICATE})
"""


def _chargebee_state_cte(c: Config) -> str:
    return f"""
cb AS (
    SELECT
        id,
        SAFE_CAST(JSON_VALUE(custom_fields, '$.cf_days_since_last_activity') AS INT64)
            AS cb_days_since_last_activity,
        SAFE_CAST(JSON_VALUE(custom_fields, '$.cf_Sessions_30d') AS INT64)
            AS cb_sessions_30d,
        SAFE_CAST(JSON_VALUE(custom_fields, '$.cf_Active_Days_30d') AS INT64)
            AS cb_active_days_30d,
        SAFE_CAST(JSON_VALUE(custom_fields, '$.cf_Replay_Sessions_30d') AS INT64)
            AS cb_replay_sessions_30d,
        JSON_VALUE(custom_fields, '$.cf_Engagement_Segment')
            AS cb_engagement_segment
    FROM `{c.customers_table_fqn}`
    WHERE deleted IS NOT TRUE
)
"""


def precheck_sql(c: Config) -> str:
    return f"""
WITH {MONTH_ROWS_CTE},
{_chargebee_state_cte(c)},
{ELIGIBLE_CTE}
SELECT
    (SELECT COUNT(*) FROM month_rows) AS total_customers,
    (SELECT COUNTIF(engagement_segment IS NOT NULL) FROM month_rows) AS with_segment,
    (SELECT COUNTIF(segment_norm IN UNNEST(@excluded_segments)) FROM month_rows)
        AS excluded_by_segment,
    COUNTIF(cb.id IS NULL) AS not_in_chargebee,
    COUNTIF(cb.id IS NOT NULL AND NOT ({SELECTION_PREDICATE})) AS already_correct,
    COUNTIF(cb.id IS NOT NULL AND ({SELECTION_PREDICATE})) AS pending,
    COUNTIF(cb.id IS NOT NULL AND e.is_excluded AND ({SELECTION_PREDICATE}))
        AS pending_excluded
FROM eligible e
LEFT JOIN cb ON e.customer_id = cb.id
"""


def extract_sql(c: Config) -> str:
    return f"""
WITH {MONTH_ROWS_CTE},
{_chargebee_state_cte(c)},
{ELIGIBLE_CTE}
SELECT
    e.customer_id,
    e.days_since_last_activity,
    e.sessions_30d,
    e.active_days_30d,
    e.replay_sessions_30d,
    e.engagement_segment
FROM eligible e
JOIN cb ON e.customer_id = cb.id
WHERE {SELECTION_PREDICATE}
ORDER BY e.customer_id
"""


def _query_params(c: Config, target_month: date) -> list[Any]:
    return [
        bigquery.ScalarQueryParameter("target_month", "DATE", target_month.isoformat()),
        bigquery.ArrayQueryParameter("excluded_segments", "STRING", list(c.excluded_segments)),
    ]


def extract_users_to_gcs(target_month: date, run_id: str) -> dict[str, Any]:
    c = config()
    client = get_bq_client()
    ensure_failures_table(client)

    job_config = bigquery.QueryJobConfig(query_parameters=_query_params(c, target_month))
    stats = next(iter(client.query(precheck_sql(c), job_config=job_config).result()))

    if stats.total_customers == 0:
        raise ExtractSkipped(
            f"fct_user_behaviour_monthly has no rows for {target_month}: the month "
            f"has not closed yet, or the mart has not run."
        )

    if stats.with_segment == 0:
        raise ExtractSkipped(
            f"user_clusters_monthly has no data for {target_month}. Without segments "
            f"we cannot exclude {list(c.excluded_segments)} and every customer would "
            f"look different (~{stats.total_customers} POSTs). Waiting for the "
            f"clustering model."
        )

    coverage = stats.with_segment / stats.total_customers
    if coverage < c.min_segment_coverage:
        raise RuntimeError(
            f"[extract] user_clusters_monthly coverage for {target_month} is "
            f"{coverage:.1%}, below CHARGEBEE_MIN_SEGMENT_COVERAGE="
            f"{c.min_segment_coverage:.1%}. The month ran incomplete: investigate "
            f"before syncing."
        )

    logger.info(
        f"[extract] target_month={target_month}: {stats.total_customers} customers "
        f"with activity, segment_coverage={coverage:.1%}"
    )
    logger.info(
        f"[extract]   -{stats.excluded_by_segment} in excluded segment "
        f"{list(c.excluded_segments)}, of which {stats.pending_excluded} transitioned "
        f"and get a one-off reset payload"
    )
    logger.info(f"[extract]   -{stats.already_correct} already hold these values in Chargebee")
    logger.info(f"[extract]   -{stats.not_in_chargebee} do not exist in chargebee.customers")
    logger.info(
        f"[extract]   ={stats.pending} pending POSTs "
        f"({stats.pending - stats.pending_excluded} regular updates, "
        f"{stats.pending_excluded} transitions to excluded)"
    )

    sql = extract_sql(c) + (f"\nLIMIT {c.extract_row_limit}" if c.extract_row_limit > 0 else "")
    rows = client.query(sql, job_config=job_config).result()

    tmp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".jsonl", delete=False)
    tmp_path = tmp.name
    n = 0
    try:
        with tmp:
            for row in rows:
                tmp.write(json.dumps({
                    "customer_id": row.customer_id,
                    "days_since_last_activity": row.days_since_last_activity,
                    "sessions_30d": row.sessions_30d,
                    "active_days_30d": row.active_days_30d,
                    "replay_sessions_30d": row.replay_sessions_30d,
                    "engagement_segment": row.engagement_segment,
                }, separators=(",", ":"), default=str))
                tmp.write("\n")
                n += 1

        gcs_uri = _upload_file_to_gcs(tmp_path, _blob_name("extract", run_id))
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    saved_pct = (1 - n / stats.total_customers) * 100
    logger.info(
        f"[extract] staged {n} rows ({saved_pct:.1f}% fewer calls than syncing the "
        f"whole month) -> {gcs_uri}"
    )
    return {
        "gcs_uri": gcs_uri,
        "row_count": n,
        "target_month": target_month.isoformat(),
        "total_customers": stats.total_customers,
        "excluded_by_segment": stats.excluded_by_segment,
        "already_correct": stats.already_correct,
        "not_in_chargebee": stats.not_in_chargebee,
        "segment_coverage": round(coverage, 4),
    }


class RateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self.interval = 60.0 / max(requests_per_minute, 1)
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self._next_slot < now:
                self._next_slot = now
            wait_for = self._next_slot - now
            self._next_slot += self.interval
        if wait_for > 0:
            time.sleep(wait_for)

    def penalize(self, seconds: float) -> None:
        with self._lock:
            self._next_slot = max(self._next_slot, time.monotonic() + seconds)


def build_payload(customer: dict[str, Any]) -> dict[str, Any]:
    return {
        cf_name: "" if customer.get(bq_field) is None else customer[bq_field]
        for bq_field, cf_name in FIELD_MAP
    }


def _sleep_backoff(
    c: Config, limiter: RateLimiter, attempt: int, retry_after: str | None
) -> None:
    if retry_after:
        try:
            delay = min(float(retry_after), c.backoff_max_seconds)
            limiter.penalize(delay)
            time.sleep(delay)
            return
        except ValueError:
            pass
    base = min(c.backoff_base_seconds * (2 ** attempt), c.backoff_max_seconds)
    delay = base + random.uniform(0, base * 0.25)
    limiter.penalize(delay)
    time.sleep(delay)


def _post_customer(
    c: Config,
    session: requests.Session,
    limiter: RateLimiter,
    api_key: str,
    customer_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    url = f"{c.api_base}/customers/{customer_id}"
    last_status: Any = None
    last_error: str | None = None

    for attempt in range(c.max_retries_per_request):
        limiter.acquire()
        try:
            resp = session.post(
                url, auth=(api_key, ""), data=payload, timeout=c.request_timeout_seconds,
            )
        except requests.RequestException as e:
            last_status, last_error = "exception", str(e)[:500]
            _sleep_backoff(c, limiter, attempt, None)
            continue

        if resp.status_code == 200:
            return {"ok": True, "http_status": 200, "error_message": None}

        last_status = resp.status_code
        last_error = resp.text[:500] if resp.text else None

        if resp.status_code in (429, 500, 502, 503, 504):
            _sleep_backoff(c, limiter, attempt, resp.headers.get("Retry-After"))
            continue

        return {"ok": False, "http_status": last_status, "error_message": last_error}

    return {"ok": False, "http_status": last_status, "error_message": last_error}


def sync_to_chargebee(extract_result: dict[str, Any], run_id: str) -> dict[str, Any]:
    c = config()
    gcs_uri = extract_result["gcs_uri"]
    target_month = extract_result["target_month"]
    row_count = extract_result.get("row_count", 0)

    budget = c.max_api_calls_per_run if c.max_api_calls_per_run > 0 else row_count
    planned = min(row_count, budget)

    logger.info(
        f"[sync] {row_count} pending, {c.requests_per_minute} req/min, caps: "
        f"{c.max_runtime_minutes} min / {budget} calls, workers={c.max_workers}, "
        f"dry_run={c.dry_run}"
    )
    logger.info(
        f"[sync] ETA for {planned} POSTs: "
        f"{planned / c.requests_per_minute if c.requests_per_minute else 0:.0f} min"
    )

    if row_count == 0:
        logger.info("[sync] Nothing pending: 0 API calls")
        return {
            "ok": 0, "failed": 0, "not_attempted": 0, "api_calls": 0,
            "failures_uri": None, "target_month": target_month,
            "elapsed_seconds": 0.0, "complete": True, "dry_run": c.dry_run,
        }

    local_ndjson = _download_gcs_to_local(gcs_uri)
    api_key = "" if c.dry_run else get_chargebee_api_key()
    limiter = RateLimiter(c.requests_per_minute)

    started = time.monotonic()
    deadline = started + c.max_runtime_minutes * 60

    ok_count = 0
    fail_count = 0
    submitted = 0
    processed = 0
    last_log = started

    failures_local = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".jsonl", delete=False,
    )
    failures_path = failures_local.name

    def _log_progress() -> None:
        nonlocal last_log
        last_log = time.monotonic()
        elapsed = last_log - started
        rate = processed / elapsed * 60 if elapsed > 0 else 0
        remaining = row_count - processed
        logger.info(
            f"[sync] {processed}/{row_count}  ok={ok_count} fail={fail_count}  "
            f"{rate:.0f} req/min  {remaining} left "
            f"(ETA {remaining / rate if rate > 0 else 0:.0f} min)"
        )

    def _should_log() -> bool:
        return (
            processed % c.progress_log_every == 0
            or time.monotonic() - last_log >= c.progress_log_seconds
        )

    try:
        with open(local_ndjson, encoding="utf-8") as f, failures_local as out_f:
            if c.dry_run:
                for i, line in enumerate(f, 1):
                    if i > budget:
                        break
                    build_payload(json.loads(line))
                    ok_count = processed = i
                    if _should_log():
                        _log_progress()
            else:
                session = requests.Session()
                session.mount("https://", requests.adapters.HTTPAdapter(
                    pool_connections=c.max_workers, pool_maxsize=c.max_workers,
                    max_retries=0,
                ))

                def _worker(line: str) -> dict[str, Any]:
                    customer = json.loads(line)
                    result = _post_customer(
                        c, session, limiter, api_key,
                        customer["customer_id"], build_payload(customer),
                    )
                    result["customer_id"] = customer["customer_id"]
                    return result

                def _handle(result: dict[str, Any]) -> None:
                    nonlocal ok_count, fail_count, processed
                    processed += 1
                    if result["ok"]:
                        ok_count += 1
                    else:
                        fail_count += 1
                        out_f.write(json.dumps({
                            "run_id": run_id,
                            "target_month": target_month,
                            "customer_id": result["customer_id"],
                            "http_status": str(result["http_status"]),
                            "error_message": result["error_message"],
                            "attempted_at": datetime.now(UTC).isoformat(),
                            "retried": True,
                        }, separators=(",", ":")))
                        out_f.write("\n")
                    if _should_log():
                        _log_progress()

                in_flight: set = set()
                max_in_flight = c.max_workers * 2
                stop_reason = None

                with ThreadPoolExecutor(max_workers=c.max_workers) as executor:
                    for line in f:
                        if submitted >= budget:
                            stop_reason = f"budget of {budget} calls"
                            break
                        if time.monotonic() >= deadline:
                            stop_reason = f"window of {c.max_runtime_minutes} min"
                            break

                        while len(in_flight) >= max_in_flight:
                            done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                            for future in done:
                                _handle(future.result())

                        in_flight.add(executor.submit(_worker, line))
                        submitted += 1

                    for future in as_completed(in_flight):
                        _handle(future.result())

                if stop_reason:
                    logger.info(
                        f"[sync] Stopped by {stop_reason}. {row_count - submitted} "
                        f"were not attempted: the next run picks them up."
                    )

        failures_uri = None
        if fail_count > 0:
            failures_uri = _upload_file_to_gcs(failures_path, _blob_name("failures", run_id))
    finally:
        for path in (local_ndjson, failures_path):
            if os.path.exists(path):
                os.unlink(path)

    elapsed = time.monotonic() - started
    not_attempted = row_count - processed
    logger.info(
        f"[sync] Done. ok={ok_count} fail={fail_count} "
        f"not_attempted={not_attempted} in {elapsed / 60:.1f} min "
        f"({processed / elapsed * 60 if elapsed else 0:.0f} req/min effective)"
    )

    return {
        "ok": ok_count,
        "failed": fail_count,
        "not_attempted": not_attempted,
        "api_calls": processed,
        "failures_uri": failures_uri,
        "target_month": target_month,
        "elapsed_seconds": round(elapsed, 1),
        "complete": not_attempted == 0,
        "dry_run": c.dry_run,
    }


FAILURES_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_month", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("http_status", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("attempted_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("retried", "BOOL", mode="REQUIRED"),
]


def persist_failures(sync_result: dict[str, Any]) -> dict[str, Any]:
    failures_uri = sync_result.get("failures_uri")
    if not failures_uri:
        logger.info("[persist] No failures to persist")
        return {"persisted_rows": 0, "skipped": True}

    c = config()
    client = get_bq_client()
    ensure_failures_table(client)

    job_config = bigquery.LoadJobConfig(
        schema=FAILURES_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )
    logger.info(f"[persist] Loading {failures_uri} -> {c.failures_table_fqn}")
    client.load_table_from_uri(
        failures_uri, c.failures_table_fqn, job_config=job_config
    ).result()

    return {
        "persisted_rows": sync_result["failed"],
        "skipped": False,
        "target_month": sync_result.get("target_month"),
    }
