"""Daily Appcues profile sync with monthly historical reconciliation."""

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as day_time, timedelta, timezone
from email.utils import parsedate_to_datetime
from itertools import islice
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


SQL_QUERIES = {
    'source': """
WITH user_customers AS (
    SELECT DISTINCT user_id, customer_id
    FROM `fxr-analytics.dbt_cloud.dim_user`
    WHERE user_id IS NOT NULL AND customer_id IS NOT NULL
),
user_daily_flags AS (
    SELECT
        u.user_id,
        d.business_date,
        MAX(COALESCE(d.is_bod_paid, 0)) AS has_paid_bod,
        MAX(COALESCE(d.is_eod_paid, 0)) AS has_paid_eod,
        MAX(IF(d.bod_plan_score = 0
            AND d.bod_status IN ('active', 'non_renewing'), 1, 0)) AS has_free_bod,
        MAX(IF(d.eod_plan_score = 0
            AND d.eod_status IN ('active', 'non_renewing'), 1, 0)) AS has_free_eod,
        MAX(IF(d.bod_status IN ('active', 'non_renewing'), 1, 0)) AS has_active_bod,
        MAX(IF(d.bod_status = 'in_trial', 1, 0)) AS has_trial_bod,
        MAX(IF(d.bod_status = 'paused', 1, 0)) AS has_paused_bod,
        MAX(IF(d.bod_status = 'future', 1, 0)) AS has_future_bod,
        MAX(IF(d.bod_status = 'cancelled', 1, 0)) AS has_cancelled_bod,
        MAX(IF(d.eod_status IN ('active', 'non_renewing'), 1, 0)) AS has_active_eod,
        MAX(IF(d.eod_status = 'in_trial', 1, 0)) AS has_trial_eod,
        MAX(IF(d.eod_status = 'paused', 1, 0)) AS has_paused_eod,
        MAX(IF(d.eod_status = 'future', 1, 0)) AS has_future_eod,
        MAX(IF(d.eod_status = 'cancelled', 1, 0)) AS has_cancelled_eod
    FROM `fxr-analytics.finance_marts.fct_subscription_daily_detail` d
    INNER JOIN user_customers u ON d.customer_id = u.customer_id
    WHERE d.business_date >= @window_start
      AND d.business_date <= @as_of_date
    GROUP BY u.user_id, d.business_date
),
user_daily_states AS (
    SELECT
        user_id,
        business_date,
        CASE
            WHEN has_paid_bod = 1 THEN 'paid'
            WHEN has_free_bod = 1 THEN 'free'
            WHEN has_active_bod = 1 THEN 'unknown'
            WHEN has_trial_bod = 1 THEN 'trial'
            WHEN has_paused_bod = 1 THEN 'paused'
            WHEN has_future_bod = 1 THEN 'future'
            WHEN has_cancelled_bod = 1 THEN 'cancelled'
            ELSE 'unknown'
        END AS start_plan_state,
        CASE
            WHEN has_paid_eod = 1 THEN 'paid'
            WHEN has_free_eod = 1 THEN 'free'
            -- An active subscription with an unclassified plan is not cancelled.
            WHEN has_active_eod = 1 THEN 'unknown'
            WHEN has_trial_eod = 1 THEN 'trial'
            WHEN has_paused_eod = 1 THEN 'paused'
            WHEN has_future_eod = 1 THEN 'future'
            WHEN has_cancelled_eod = 1 THEN 'cancelled'
            ELSE 'unknown'
        END AS end_plan_state
    FROM user_daily_flags
)
SELECT * FROM user_daily_states
""",
    'profiles': """
WITH users AS (
    SELECT user_id FROM `fxr-analytics.appcues_sync.user_plan_state`
    UNION DISTINCT
    SELECT user_id FROM recomputed_days
),
ordered_days AS (
    SELECT *,
        LAG(end_plan_state) OVER (PARTITION BY user_id ORDER BY business_date) AS previous_end_state,
        LAG(business_date) OVER (PARTITION BY user_id ORDER BY business_date) AS previous_business_date
    FROM recomputed_days
),
observed_changes AS (
    SELECT d.*,
        CASE
            WHEN d.previous_business_date = DATE_SUB(d.business_date, INTERVAL 1 DAY)
                THEN d.previous_end_state
            WHEN NOT @full_history AND p.state_as_of_date = DATE_SUB(d.business_date, INTERVAL 1 DAY)
                THEN p.current_plan_state
            -- At the history boundary or after a gap, use a known BOD state.
            -- Do not invent a change date for a first observation with no prior state.
            ELSE NULLIF(d.start_plan_state, 'unknown')
        END AS preceding_state
    FROM ordered_days d
    LEFT JOIN `fxr-analytics.appcues_sync.user_plan_state` p USING (user_id)
),
window_dates AS (
    SELECT user_id,
        MAX(IF(preceding_state IS NOT NULL AND preceding_state != end_plan_state,
            business_date, NULL)) AS state_changed_date
    FROM observed_changes
    GROUP BY user_id
),
current_states AS (
    SELECT user_id, end_plan_state FROM recomputed_days
    WHERE business_date = @as_of_date
)
SELECT u.user_id, @as_of_date AS state_as_of_date,
    COALESCE(c.end_plan_state, 'unknown') AS current_plan_state,
    CASE
        -- No current observation: unknown is a data condition, not a dated plan event.
        WHEN c.user_id IS NULL THEN CAST(NULL AS DATE)
        WHEN @full_history THEN w.state_changed_date
        WHEN w.state_changed_date IS NOT NULL THEN w.state_changed_date
        WHEN c.end_plan_state = p.current_plan_state THEN p.state_changed_date
        ELSE CAST(NULL AS DATE)
    END AS state_changed_date
FROM users u
LEFT JOIN current_states c USING (user_id)
LEFT JOIN window_dates w USING (user_id)
LEFT JOIN `fxr-analytics.appcues_sync.user_plan_state` p USING (user_id)
""",
    'setup': """
CREATE SCHEMA IF NOT EXISTS `fxr-analytics.appcues_sync` OPTIONS(location = 'US');
CREATE TABLE IF NOT EXISTS `fxr-analytics.appcues_sync.user_plan_state` (
    user_id STRING NOT NULL, state_as_of_date DATE NOT NULL,
    current_plan_state STRING NOT NULL, state_changed_date DATE,
    computed_at TIMESTAMP NOT NULL, last_historical_refresh_date DATE,
    schema_version INT64
) CLUSTER BY user_id;
ALTER TABLE `fxr-analytics.appcues_sync.user_plan_state`
ADD COLUMN IF NOT EXISTS state_changed_date DATE;
ALTER TABLE `fxr-analytics.appcues_sync.user_plan_state`
ADD COLUMN IF NOT EXISTS schema_version INT64;
ALTER TABLE `fxr-analytics.appcues_sync.user_plan_state`
ADD COLUMN IF NOT EXISTS last_historical_refresh_date DATE;
CREATE TABLE IF NOT EXISTS `fxr-analytics.appcues_sync.user_plan_last_sent` (
    user_id STRING NOT NULL, state_as_of_date DATE NOT NULL,
    current_plan_state STRING NOT NULL, state_changed_date DATE,
    confirmed_at TIMESTAMP NOT NULL, schema_version INT64
) CLUSTER BY user_id;
ALTER TABLE `fxr-analytics.appcues_sync.user_plan_last_sent`
ADD COLUMN IF NOT EXISTS state_changed_date DATE;
ALTER TABLE `fxr-analytics.appcues_sync.user_plan_last_sent`
ADD COLUMN IF NOT EXISTS schema_version INT64;
""",
    'refresh': """
CREATE TEMP TABLE recomputed_days AS
{{DAILY_STATES}};
ASSERT (SELECT COUNT(DISTINCT business_date) FROM recomputed_days)
    = DATE_DIFF(@as_of_date, @window_start, DAY) + 1
    AS 'Missing dates in daily_detail; do not advance the checkpoint';
BEGIN TRANSACTION;
ASSERT (SELECT MAX(state_as_of_date) FROM `fxr-analytics.appcues_sync.user_plan_state`)
    IS NOT DISTINCT FROM @previous_date
    AS 'Calculation checkpoint changed; do not overlap job executions';
MERGE `fxr-analytics.appcues_sync.user_plan_state` target
USING (
{{PROFILES}}
) source ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    state_as_of_date = source.state_as_of_date,
    current_plan_state = source.current_plan_state,
    state_changed_date = source.state_changed_date,
    computed_at = CURRENT_TIMESTAMP(),
    last_historical_refresh_date = @historical_refresh_date,
    schema_version = 2
WHEN NOT MATCHED THEN INSERT (
    user_id, state_as_of_date, current_plan_state, state_changed_date,
    computed_at, last_historical_refresh_date, schema_version
) VALUES (
    source.user_id, source.state_as_of_date, source.current_plan_state,
    source.state_changed_date, CURRENT_TIMESTAMP(), @historical_refresh_date, 2
);
COMMIT TRANSACTION;
DROP TABLE recomputed_days;
""",
    'pending': """
SELECT s.user_id, s.state_as_of_date, s.current_plan_state, s.state_changed_date
FROM {{COMPUTED}} s
LEFT JOIN {{SENT}} p USING (user_id)
WHERE p.user_id IS NULL
   OR COALESCE(p.schema_version, 0) < 2
   OR s.current_plan_state IS DISTINCT FROM p.current_plan_state
   OR s.state_changed_date IS DISTINCT FROM p.state_changed_date
""",
    'confirm': """
MERGE `fxr-analytics.appcues_sync.user_plan_last_sent` target
USING (
    SELECT JSON_VALUE(r, '$.user_id') AS user_id,
        DATE(JSON_VALUE(r, '$.state_as_of_date')) AS state_as_of_date,
        JSON_VALUE(r, '$.current_plan_state') AS current_plan_state,
        DATE(JSON_VALUE(r, '$.state_changed_date')) AS state_changed_date
    FROM UNNEST(JSON_QUERY_ARRAY(@confirmed_rows)) r
) source ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    state_as_of_date = source.state_as_of_date,
    current_plan_state = source.current_plan_state,
    state_changed_date = source.state_changed_date,
    confirmed_at = CURRENT_TIMESTAMP(), schema_version = 2
WHEN NOT MATCHED THEN INSERT (
    user_id, state_as_of_date, current_plan_state, state_changed_date,
    confirmed_at, schema_version
) VALUES (
    source.user_id, source.state_as_of_date, source.current_plan_state,
    source.state_changed_date, CURRENT_TIMESTAMP(), 2
);
""",
}
BUSINESS_TZ = ZoneInfo("America/Chicago")
HISTORY_START = date(2024, 12, 1)
FIELDS = ("user_id", "state_as_of_date", "current_plan_state",
          "state_changed_date")
STATES = {"paid", "free", "unknown", "trial", "paused", "future", "cancelled"}
EMPTY_PROFILES = """(SELECT CAST(NULL AS STRING) user_id,
    CAST(NULL AS DATE) state_as_of_date, CAST(NULL AS STRING) current_plan_state,
    CAST(NULL AS DATE) state_changed_date, CAST(NULL AS INT64) schema_version
    FROM UNNEST([1]) WHERE FALSE)"""


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


@dataclass
class Config:
    project: str = "fxr-analytics"
    source_project: str = "fxr-analytics"
    dataset: str = "appcues_sync"
    location: str = "US"
    base_url: str = "https://api.appcues.com"
    account_id: str = ""
    api_key: str = ""
    api_secret: str = ""
    request_timeout: float = 30
    max_retries: int = 5
    retry_delay: float = 2
    poll_seconds: float = 5
    job_timeout: float = 1800
    chunk_bytes: int = 18_000_000
    chunk_rows: int = 20_000
    max_profiles: int = 0
    test_user_ids: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "Config":
        project = os.getenv("GCP_PROJECT", "fxr-analytics").strip()
        return cls(
            project=project,
            source_project=os.getenv("SOURCE_PROJECT", project).strip(),
            dataset=os.getenv("SYNC_DATASET", "appcues_sync").strip(),
            location=os.getenv("BQ_LOCATION", "US").strip(),
            base_url=os.getenv("APPCUES_BASE_URL", "https://api.appcues.com").rstrip("/"),
            account_id=os.getenv("APPCUES_ACCOUNT_ID", "").strip(),
            api_key=os.getenv("APPCUES_API_KEY", "").strip(),
            api_secret=os.getenv("APPCUES_API_SECRET", "").strip(),
            request_timeout=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "5")),
            retry_delay=float(os.getenv("BASE_RETRY_DELAY_SECONDS", "2")),
            poll_seconds=float(os.getenv("APPCUES_JOB_POLL_SECONDS", "5")),
            job_timeout=float(os.getenv("APPCUES_JOB_TIMEOUT_SECONDS", "1800")),
            chunk_bytes=int(os.getenv("APPCUES_IMPORT_CHUNK_BYTES", "18000000")),
            chunk_rows=int(os.getenv("APPCUES_IMPORT_CHUNK_ROWS", "20000")),
            max_profiles=int(os.getenv("APPCUES_MAX_PROFILES", "0")),
            test_user_ids=tuple(filter(None, os.getenv("APPCUES_TEST_USER_IDS", "").split("|"))),
        )

    def validate(self, send: bool = False) -> None:
        for value in (self.project, self.source_project):
            if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", value):
                raise ValueError("Invalid BigQuery project ID")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.dataset):
            raise ValueError("Invalid SYNC_DATASET")
        if self.location != "US":
            raise ValueError("The source datasets and sync dataset must be in US")
        if not 1 <= self.chunk_bytes <= 18_000_000 or not 1 <= self.chunk_rows <= 20_000:
            raise ValueError("Chunk limits: 1..18000000 bytes and 1..20000 rows")
        if self.max_profiles < 0:
            raise ValueError("APPCUES_MAX_PROFILES must be nonnegative; 0 means unlimited")
        if self.test_user_ids and (not self.max_profiles or len(self.test_user_ids) > self.max_profiles):
            raise ValueError("A test cohort requires a positive cap covering the cohort")
        if min(self.request_timeout, self.max_retries, self.poll_seconds, self.job_timeout) <= 0:
            raise ValueError("Timeouts, poll interval and retries must be positive")
        if self.retry_delay < 0:
            raise ValueError("Retry delay must be nonnegative")
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path not in {"", "/"}:
            raise ValueError("APPCUES_BASE_URL must be an HTTPS origin")
        if send and not all((self.account_id, self.api_key, self.api_secret)):
            raise ValueError("APPCUES_ACCOUNT_ID, APPCUES_API_KEY and APPCUES_API_SECRET are required")
        if self.account_id and not re.fullmatch(r"[A-Za-z0-9_-]+", self.account_id):
            raise ValueError("Invalid APPCUES_ACCOUNT_ID")


@dataclass(frozen=True)
class Plan:
    as_of_date: date
    previous_date: date | None
    window_start: date
    full_history: bool
    historical_refresh_date: date | None
    recalculate: bool

    def parameters(self) -> list:
        return [bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in (
            ("as_of_date", "DATE", self.as_of_date),
            ("previous_date", "DATE", self.previous_date),
            ("window_start", "DATE", self.window_start),
            ("full_history", "BOOL", self.full_history),
            ("historical_refresh_date", "DATE", self.historical_refresh_date),
        )]


def make_plan(mode: str, today: date, cutoff: date | None,
              previous: date | None, last_history: date | None) -> Plan:
    if mode not in {"auto", "daily", "historical"}:
        raise ValueError("Invalid run mode")
    if cutoff != today - timedelta(days=1):
        raise ValueError("daily_detail has not reached yesterday; wait for dbt")
    if previous is not None and previous > cutoff:
        raise ValueError("Source cutoff must not move backwards")
    full = previous is None or mode == "historical" or (
        mode == "auto" and (last_history is None or last_history < today.replace(day=1))
    )
    return Plan(cutoff, previous,
                HISTORY_START if full else previous + timedelta(days=1),
                full, today if full else last_history,
                full or previous < cutoff)


def log(message: str, **values: Any) -> None:
    print(json.dumps({"message": message, "severity": "INFO",
                      "execution": os.getenv("CLOUD_RUN_EXECUTION"), **values}, default=str), flush=True)


class Store:
    def __init__(self, config: Config, client: bigquery.Client):
        self.config, self.client = config, client

    def table(self, name: str) -> str:
        return f"{self.config.project}.{self.config.dataset}.{name}"

    def sql(self, name: str) -> str:
        value = SQL_QUERIES[name]
        return value.replace("fxr-analytics.appcues_sync", "__SYNC__").replace(
            "fxr-analytics", self.config.source_project
        ).replace("__SYNC__", f"{self.config.project}.{self.config.dataset}")

    def query(self, sql: str, parameters: list | None = None):
        started = time.monotonic()
        job = self.client.query(sql, location=self.config.location,
                                job_config=bigquery.QueryJobConfig(query_parameters=parameters or []))
        log("BigQuery query started", job_id=job.job_id)
        result = job.result(page_size=2000)
        log("BigQuery query completed", job_id=job.job_id,
            bytes_processed=job.total_bytes_processed,
            elapsed_seconds=round(time.monotonic() - started, 1))
        return result

    def exists(self, name: str) -> bool:
        try:
            self.client.get_table(self.table(name))
            return True
        except NotFound:
            return False

    def checkpoint(self) -> tuple[date | None, date | None]:
        if not self.exists("user_plan_state"):
            return None, None
        table = self.client.get_table(self.table("user_plan_state"))
        fields = {f.name for f in table.schema}
        history = ("IF(MIN(COALESCE(schema_version, 0)) >= 2, "
                   "MAX(last_historical_refresh_date), NULL)"
                   if {"last_historical_refresh_date", "schema_version", "state_changed_date"} <= fields
                   else "CAST(NULL AS DATE)")
        row = next(iter(self.query(f"SELECT MAX(state_as_of_date) cutoff, {history} historical "
                                  f"FROM `{self.table('user_plan_state')}`")))
        return row["cutoff"], row["historical"]

    def cutoff(self) -> date | None:
        row = next(iter(self.query(f"""SELECT MAX(SAFE.PARSE_DATE('%Y%m%d',partition_id)) cutoff
            FROM `{self.config.source_project}.finance_marts.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name='fct_subscription_daily_detail'
            AND SAFE.PARSE_DATE('%Y%m%d',partition_id)<CURRENT_DATE('America/Chicago')""")))
        return row["cutoff"]

    def setup(self) -> None:
        self.query(self.sql("setup"))

    def refresh(self, plan: Plan) -> None:
        if plan.recalculate:
            sql = self.sql("refresh").replace("{{DAILY_STATES}}", self.sql("source"))
            sql = sql.replace("{{PROFILES}}", self.sql("profiles"))
            self.query(sql, plan.parameters())

    def pending(self, plan: Plan, preview: bool = False):
        state = f"`{self.table('user_plan_state')}`"
        sent = f"`{self.table('user_plan_last_sent')}`"
        if preview:
            if not self.exists("user_plan_state"):
                state = EMPTY_PROFILES
            if not self.exists("user_plan_last_sent"):
                sent = EMPTY_PROFILES
        if preview:
            for name in ("user_plan_state", "user_plan_last_sent"):
                relation = state if name == "user_plan_state" else sent
                if relation != EMPTY_PROFILES:
                    fields = {f.name for f in self.client.get_table(self.table(name)).schema}
                    additions = []
                    if "state_changed_date" not in fields:
                        additions.append("CAST(NULL AS DATE) AS state_changed_date")
                    if "schema_version" not in fields:
                        additions.append("CAST(NULL AS INT64) AS schema_version")
                    if additions:
                        relation = f"(SELECT *, {', '.join(additions)} FROM {relation})"
                    if name == "user_plan_state":
                        state = relation
                    else:
                        sent = relation
        sql = self.sql("pending").replace("{{SENT}}", sent)
        if preview and plan.recalculate:
            profiles = self.sql("profiles").replace(f"`{self.table('user_plan_state')}`", state)
            sql = (f"WITH recomputed_days AS ({self.sql('source')}), "
                   f"computed AS ({profiles}) " + sql.replace("{{COMPUTED}}", "computed"))
        else:
            sql = sql.replace("{{COMPUTED}}", state)
        parameters = plan.parameters()
        if self.config.test_user_ids:
            sql = f"SELECT * FROM ({sql}) WHERE user_id IN UNNEST(@test_user_ids)"
            parameters.append(bigquery.ArrayQueryParameter("test_user_ids", "STRING", self.config.test_user_ids))
        if self.config.max_profiles:
            sql += f"\nORDER BY user_id LIMIT {self.config.max_profiles}"
        return self.query(sql, parameters)

    def confirm(self, rows: list[dict]) -> None:
        exact_values = [{key: row[key] for key in FIELDS} for row in rows]
        parameter = bigquery.ScalarQueryParameter(
            "confirmed_rows", "STRING", json.dumps(exact_values, default=str, separators=(",", ":")))
        # Never copy current state here: only the values of the completed batch.
        self.query(self.sql("confirm"), [parameter])


def appcues_date(value: date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = date.fromisoformat(value)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError("Expected a business DATE")
    # A date, not an exact event time; encode Chicago midnight for date targeting.
    return datetime.combine(value, day_time.min, BUSINESS_TZ).astimezone(
        timezone.utc).isoformat().replace("+00:00", "Z")


def profile(row: dict) -> dict:
    user_id = str(row.get("user_id") or "")
    if not user_id.strip() or len(user_id.encode()) > 1024:
        raise ValueError("Missing or excessively long user_id")
    if row.get("current_plan_state") not in STATES or not row.get("state_as_of_date"):
        raise ValueError("Invalid profile state or cutoff")
    result = {"user_id": user_id,
              "bq_plan_state": row["current_plan_state"],
              "bq_plan_state_as_of_date": appcues_date(row["state_as_of_date"])}
    result["bq_plan_state_changed_date"] = appcues_date(row["state_changed_date"])
    return result


def batches(rows: Iterable, config: Config) -> Iterator[tuple[bytes, list[dict]]]:
    lines, originals, size = [], [], 0
    for item in rows:
        row = dict(item.items()) if hasattr(item, "items") else dict(item)
        line = json.dumps(profile(row), ensure_ascii=True, separators=(",", ":")).encode()
        if len(line) > config.chunk_bytes:
            raise ValueError("One profile exceeds the configured chunk limit")
        if lines and (size + 1 + len(line) > config.chunk_bytes or len(lines) >= config.chunk_rows):
            yield b"\n".join(lines), originals
            lines, originals, size = [], [], 0
        size += len(line) + bool(lines)
        lines.append(line)
        originals.append(row)
    if lines:
        yield b"\n".join(lines), originals


class Appcues:
    def __init__(self, config: Config):
        self.config = config

    def retry_delay(self, attempt: int, response) -> float:
        value = response.headers.get("Retry-After") if response is not None else None
        if value:
            try:
                return max(float(value), 0)
            except ValueError:
                try:
                    return max((parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds(), 0)
                except (ValueError, TypeError):
                    pass
        return self.config.retry_delay * 2 ** (attempt - 1)

    def request(self, method: str, url: str, **kwargs):
        cfg = self.config
        origin = urlparse(url)
        expected = urlparse(cfg.base_url)
        if (origin.scheme, origin.netloc) != (expected.scheme, expected.netloc):
            raise ValueError("Refusing to send Appcues credentials to another origin")
        for attempt in range(1, cfg.max_retries + 1):
            response = None
            try:
                response = requests.request(method, url, auth=(cfg.api_key, cfg.api_secret),
                                            timeout=cfg.request_timeout, allow_redirects=False, **kwargs)
            except requests.RequestException:
                pass
            else:
                if 200 <= response.status_code < 300:
                    return response
                if response.status_code != 429 and not 500 <= response.status_code <= 599:
                    raise RuntimeError(f"Appcues HTTP {response.status_code}; request rejected")
            if attempt == cfg.max_retries:
                raise RuntimeError("Appcues request exhausted retries")
            delay = self.retry_delay(attempt, response)
            log("Appcues request retry", severity="WARNING", method=method,
                status_code=response.status_code if response is not None else None,
                attempt=attempt, delay_seconds=delay)
            time.sleep(delay)

    def import_batch(self, data: bytes, expected_rows: int) -> str:
        cfg = self.config
        response = self.request("POST", f"{cfg.base_url}/v2/accounts/{cfg.account_id}/import/profiles",
                                data=data, headers={"Content-Type": "application/ndjson"}).json()
        job_url = response["job_url"]
        log("Appcues import accepted", appcues_job_id=response.get("job_id"), profiles=expected_rows)
        deadline = time.monotonic() + cfg.job_timeout
        polls = 0
        while time.monotonic() < deadline:
            job = self.request("GET", job_url).json()
            status = str(job.get("status", "")).upper()
            polls += 1
            if polls == 1 or polls % 6 == 0:
                log("Appcues import status", appcues_job_id=response.get("job_id"),
                    status=status, polls=polls)
            if status in {"FAILED", "CANCELLED", "CANCELED"}:
                raise RuntimeError("Appcues import failed; batch remains unconfirmed")
            if status == "COMPLETED":
                events = job.get("events") or []
                # Current import-* jobs return COMPLETED without per-row events.
                # Match the returned job ID; do not invent ingestion counters.
                if (not events and job.get("name") == "Import" and job.get("id")
                        and response.get("job_id") == f"import-{job['id']}"):
                    log("Appcues import completed without per-row counters",
                        appcues_job_id=job["id"], profiles_submitted=expected_rows)
                    return str(job["id"])
                attrs = next((e.get("attributes", {}) for e in reversed(events)
                              if e.get("name") == "Completed"), {})
                try:
                    completed = int(attrs["ingestions_completed"])
                    failed = int(attrs["ingestions_failed"]) + int(attrs["requests_failed"])
                except (KeyError, ValueError, TypeError) as exc:
                    raise RuntimeError("Import completion lacks usable counts; not confirming") from exc
                if failed != 0 or completed != expected_rows:
                    raise RuntimeError(f"Incomplete import: {completed}/{expected_rows}, failures={failed}; "
                                       "entire batch remains pending")
                return str(job.get("id") or response.get("job_id") or "")
            time.sleep(cfg.poll_seconds)
        raise TimeoutError("Appcues import timed out; batch remains unconfirmed")


def deliver(rows: Iterable, store: Store, appcues: Appcues | None,
            config: Config, dry_run: bool) -> dict:
    total, count, payload_bytes = 0, 0, 0
    started = time.monotonic()
    expected = getattr(rows, "total_rows", None)
    if isinstance(expected, int) and config.max_profiles:
        expected = min(expected, config.max_profiles)
    log("Profile delivery starting", pending_profiles=expected, dry_run=dry_run)
    if config.max_profiles:
        # Enforce the execution-wide cap even if a caller bypasses Store.pending.
        rows = islice(rows, config.max_profiles)
    for data, originals in batches(rows, config):
        log("Appcues batch starting", batch=count + 1, profiles=len(originals),
            payload_bytes=len(data), profiles_completed=total, pending_profiles=expected)
        if not dry_run:
            try:
                job_id = appcues.import_batch(data, len(originals))
                store.confirm(originals)
            except Exception as exc:
                log("Appcues batch failed", severity="ERROR", batch=count + 1,
                    profiles_completed=total, error_type=type(exc).__name__)
                raise
            log("Appcues batch confirmed", appcues_job_id=job_id, profiles=len(originals))
        total += len(originals)
        count += 1
        payload_bytes += len(data)
        elapsed = time.monotonic() - started
        log("Profile delivery progress", batches_completed=count, profiles_completed=total,
            pending_profiles=expected,
            percent_complete=round(100 * total / expected, 2) if expected else None,
            elapsed_seconds=round(elapsed, 1),
            eta_seconds=round(elapsed * (expected - total) / total) if expected and total else None)
    return {"profiles": total, "chunks": count, "payload_bytes": payload_bytes}


def run(config: Config, store: Store, mode: str, dry_run: bool,
        today: date | None = None, appcues: Appcues | None = None) -> dict:
    config.validate(send=not dry_run)
    today = today or datetime.now(BUSINESS_TZ).date()
    previous, last_history = store.checkpoint()
    plan = make_plan(mode, today, store.cutoff(), previous, last_history)
    log("Plan sync starting", dry_run=dry_run, full_history=plan.full_history,
        max_profiles=config.max_profiles,
        recalculate=plan.recalculate, previous_cutoff=previous,
        window_start=plan.window_start if plan.recalculate else None, cutoff=plan.as_of_date)
    if not dry_run:
        store.setup()
        store.refresh(plan)
    rows = store.pending(plan, preview=dry_run)
    summary = deliver(rows, store, appcues or (Appcues(config) if not dry_run else None), config, dry_run)
    log("Plan sync completed", dry_run=dry_run, full_history=plan.full_history, **summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-mode", choices=("auto", "daily", "historical"),
                        default=os.getenv("RUN_MODE", "auto"))
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", dest="dry_run", action="store_true")
    group.add_argument("--execute", dest="dry_run", action="store_false")
    parser.set_defaults(dry_run=env_bool("DRY_RUN", True))
    args = parser.parse_args()
    config = Config.from_env()
    config.validate(send=not args.dry_run)
    client = bigquery.Client(project=config.project, location=config.location)
    run(config, Store(config, client), args.run_mode, args.dry_run)


if __name__ == "__main__":
    main()
