import os
import sys
import time
from datetime import datetime, timedelta, timezone

import google.auth
import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, secretmanager
from requests.auth import HTTPBasicAuth

BASE_URL = "https://api.getrewardful.com"
ENDPOINT = "/v1/referrals"
PROJECT_ID = "fxr-analytics"
PROJECT_NUMBER = "362612879927"
DATASET = os.getenv("REWARDFUL_DATASET")
TABLE = os.getenv("REFERRALS_TABLE")
DATASET_TABLE = f"{DATASET}.{TABLE}"
STAGING_DATASET = os.getenv("REWARDFUL_RAW_DATASET", "rewardful_raw")
SECRET_NAME = os.getenv("REWARDFUL_API_KEY_SECRET")

# Window: from the watermark (or BACKFILL_START) up to WINDOW_HOURS later, never past now.
WINDOW_HOURS = float(os.getenv("WINDOW_HOURS", "12"))
BACKFILL_START = os.getenv("BACKFILL_START")
BACKFILL_END = os.getenv("BACKFILL_END")

# Backfill mode: walk a fixed historical range one window per run, keeping progress in BigQuery so
# each run (manual or scheduled) continues where the last one stopped.
BACKFILL_RANGE_START = os.getenv("BACKFILL_RANGE_START")
BACKFILL_RANGE_END = os.getenv("BACKFILL_RANGE_END")
IS_BACKFILL_MODE = bool(BACKFILL_RANGE_START and BACKFILL_RANGE_END)

# Keep every run well inside the hourly schedule: the window shrinks until it fits the page budget,
# and the run aborts without merging if paging still takes longer than the runtime budget.
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "300"))
MAX_RUNTIME_MINUTES = float(os.getenv("MAX_RUNTIME_MINUTES", "45"))
RUN_STARTED_AT = time.monotonic()
MIN_WINDOW = timedelta(minutes=1)

PRIMARY_KEY = "id"
PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 60
# Minimum time between request starts; Rewardful allows ~45 requests per 30 s.
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "1"))
MAX_RETRIES = 5
BACKOFF_SECONDS = 5
RATE_LIMIT_FALLBACK_SECONDS = 30
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

REGION = "us-central1"
JOB_NAME = os.getenv("CLOUD_RUN_JOB")
EXECUTION_NAME = os.getenv("CLOUD_RUN_EXECUTION")

# One staging table per execution so overlapping runs never truncate each other's batch.
EXECUTION_ID = (EXECUTION_NAME or "local").replace("-", "_")
STAGING_DATASET_TABLE = f"{STAGING_DATASET}.{TABLE}__{EXECUTION_ID}"
BACKFILL_PROGRESS_TABLE = f"{STAGING_DATASET}.{TABLE}_backfill_progress"


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def get_secret(secret_name: str) -> str:
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{PROJECT_NUMBER}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")
    except Exception as exc:
        raise RuntimeError(f"Error retrieving secret '{secret_name}': {exc}") from exc


def is_backfill_execution(execution: dict) -> bool:
    for container in (execution.get("template") or {}).get("containers", []):
        if any(env.get("name") == "BACKFILL_END" for env in container.get("env", [])):
            return True
    return False


def find_earlier_running_execution() -> str | None:
    # Runs of the same job without BACKFILL_END read the same starting point (the watermark, or the
    # backfill progress), so only one may run at a time. The later one yields; one-off runs with
    # explicit ranges are ignored both ways. The backfill job lists only its own executions.
    if not JOB_NAME or not EXECUTION_NAME:
        return None

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    url = f"https://run.googleapis.com/v2/projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}/executions"
    try:
        response = AuthorizedSession(credentials).get(url, params={"pageSize": 50}, timeout=30)
        response.raise_for_status()
    except Exception as exc:
        print(f"Could not list executions, skipping the overlap check: {exc}")
        return None

    executions = response.json().get("executions", [])
    own = next((e for e in executions if e["name"].endswith(f"/{EXECUTION_NAME}")), None)
    own_created_at = own["createTime"] if own else datetime.now(timezone.utc).isoformat()

    for execution in executions:
        name = execution["name"].rsplit("/", 1)[-1]
        if name == EXECUTION_NAME or execution.get("completionTime") or is_backfill_execution(execution):
            continue
        if parse_timestamp(execution["createTime"]) < parse_timestamp(own_created_at):
            return name
    return None


def get_last_updated_at(client: bigquery.Client) -> datetime:
    query = f"SELECT MAX(updated_at) AS max_updated_at FROM `{PROJECT_ID}.{DATASET_TABLE}`"
    max_updated_at = next(client.query(query).result()).max_updated_at
    if not max_updated_at:
        raise RuntimeError(
            f"`{PROJECT_ID}.{DATASET_TABLE}` has no updated_at; set BACKFILL_START to load it."
        )

    if max_updated_at.tzinfo is None:
        max_updated_at = max_updated_at.replace(tzinfo=timezone.utc)
    return max_updated_at.astimezone(timezone.utc)


def get_backfill_window(client: bigquery.Client) -> tuple[datetime, datetime] | None:
    client.query(f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BACKFILL_PROGRESS_TABLE}` (
      window_start TIMESTAMP,
      window_end TIMESTAMP,
      referrals INT64,
      execution STRING,
      completed_at TIMESTAMP
    )
    """).result()

    range_start = parse_timestamp(BACKFILL_RANGE_START)
    range_end = parse_timestamp(BACKFILL_RANGE_END)
    query = f"""
    SELECT MAX(window_end) AS last_window_end
    FROM `{PROJECT_ID}.{BACKFILL_PROGRESS_TABLE}`
    WHERE window_end > @range_start AND window_end <= @range_end
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("range_start", "TIMESTAMP", range_start),
        bigquery.ScalarQueryParameter("range_end", "TIMESTAMP", range_end),
    ])
    last_window_end = next(client.query(query, job_config=job_config).result()).last_window_end

    window_start = range_start
    if last_window_end:
        if last_window_end.tzinfo is None:
            last_window_end = last_window_end.replace(tzinfo=timezone.utc)
        window_start = last_window_end.astimezone(timezone.utc)

    if window_start >= range_end:
        return None
    return window_start, min(window_start + timedelta(hours=WINDOW_HOURS), range_end)


def record_backfill_progress(
    client: bigquery.Client,
    window_start: datetime,
    window_end: datetime,
    referrals: int,
) -> None:
    query = f"""
    INSERT INTO `{PROJECT_ID}.{BACKFILL_PROGRESS_TABLE}` (window_start, window_end, referrals, execution, completed_at)
    VALUES (@window_start, @window_end, @referrals, @execution, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
        bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
        bigquery.ScalarQueryParameter("referrals", "INT64", referrals),
        bigquery.ScalarQueryParameter("execution", "STRING", EXECUTION_NAME or "local"),
    ])
    client.query(query, job_config=job_config).result()
    print(f"Backfill progress saved up to {format_timestamp(window_end)} (range ends {BACKFILL_RANGE_END}).")


def get_window(client: bigquery.Client) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)

    if BACKFILL_START:
        window_start = parse_timestamp(BACKFILL_START)
    else:
        window_start = get_last_updated_at(client)

    if BACKFILL_END:
        window_end = parse_timestamp(BACKFILL_END)
    else:
        window_end = window_start + timedelta(hours=WINDOW_HOURS)

    window_end = min(window_end, now)
    if window_end <= window_start:
        raise RuntimeError(
            f"Empty window: {format_timestamp(window_start)} -> {format_timestamp(window_end)}"
        )
    return window_start, window_end


def ensure_staging_dataset_exists(client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{STAGING_DATASET}")
    dataset_ref.location = "US"

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Created staging dataset {PROJECT_ID}.{STAGING_DATASET}")


def get_final_table(client: bigquery.Client) -> bigquery.Table:
    return client.get_table(f"{PROJECT_ID}.{DATASET_TABLE}")


def get_rate_limit_wait_seconds(response: requests.Response) -> float:
    headers = response.headers

    # Separate headers (RateLimit-Reset / X-RateLimit-Reset) or Retry-After, in seconds.
    for name in ("RateLimit-Reset", "X-RateLimit-Reset", "Retry-After"):
        value = headers.get(name)
        if value and value.strip().isdigit():
            seconds = float(value.strip())
            # Some APIs send the reset as an epoch timestamp instead of seconds left.
            if seconds > 10**9:
                seconds = max(seconds - time.time(), 0)
            return min(seconds, RATE_LIMIT_FALLBACK_SECONDS * 2) + 1

    # Combined header from newer drafts: RateLimit: limit=45, remaining=0, reset=12
    combined = headers.get("RateLimit")
    if combined:
        for part in combined.replace(";", ",").split(","):
            key, _, value = part.strip().partition("=")
            if key == "reset" and value.strip().isdigit():
                return float(value.strip()) + 1

    return RATE_LIMIT_FALLBACK_SECONDS


class ThrottledSession:
    def __init__(self, api_key: str):
        self.auth = HTTPBasicAuth(api_key, "")
        self.headers = {"Accept": "*/*"}
        self.last_request_started = 0.0
        self.logged_rate_limit_headers = False

    def _wait_for_slot(self) -> None:
        elapsed = time.monotonic() - self.last_request_started
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        self.last_request_started = time.monotonic()

    def get(self, url: str, params: dict) -> requests.Response:
        for attempt in range(1, MAX_RETRIES + 1):
            self._wait_for_slot()
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    auth=self.auth,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Error fetching Rewardful data after {MAX_RETRIES} attempts: {exc}") from exc

                sleep_seconds = BACKOFF_SECONDS * attempt
                print(f"Request exception on attempt {attempt}/{MAX_RETRIES}: {exc}. Retrying in {sleep_seconds}s.")
                time.sleep(sleep_seconds)
                continue

            if response.status_code == 200:
                return response

            if response.status_code not in RETRYABLE_STATUS_CODES:
                raise RuntimeError(
                    f"Non-retryable Rewardful API error {response.status_code}: {response.text[:500]}"
                )

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Retryable Rewardful API error persisted after {MAX_RETRIES} attempts: "
                    f"{response.status_code} - {response.text[:500]}"
                )

            if response.status_code == 429:
                if not self.logged_rate_limit_headers:
                    rate_headers = {
                        key: value
                        for key, value in response.headers.items()
                        if "ratelimit" in key.lower() or key.lower() == "retry-after"
                    }
                    print(f"Rate limit headers on first 429: {rate_headers}")
                    self.logged_rate_limit_headers = True
                sleep_seconds = get_rate_limit_wait_seconds(response)
            else:
                sleep_seconds = BACKOFF_SECONDS * attempt

            print(
                f"Retryable Rewardful API error {response.status_code} on attempt "
                f"{attempt}/{MAX_RETRIES}. Retrying in {sleep_seconds:.0f}s."
            )
            time.sleep(sleep_seconds)

        raise RuntimeError("Failed to fetch Rewardful data.")


def window_params(window_start: datetime, window_end: datetime, page: int) -> dict:
    return {
        "expand[]": "affiliate",
        "updated_since": format_timestamp(window_start),
        "updated_until": format_timestamp(window_end),
        "limit": PAGE_SIZE,
        "page": page,
    }


def runtime_minutes() -> float:
    return (time.monotonic() - RUN_STARTED_AT) / 60


def fit_window(
    session: ThrottledSession,
    window_start: datetime,
    window_end: datetime,
    is_explicit_range: bool,
) -> datetime:
    while True:
        response = session.get(f"{BASE_URL}{ENDPOINT}", window_params(window_start, window_end, 1))
        total_pages = (response.json().get("pagination") or {}).get("total_pages", 1)
        print(f"Window {format_timestamp(window_start)} -> {format_timestamp(window_end)} has {total_pages} pages.")

        if total_pages <= MAX_PAGES_PER_RUN:
            return window_end

        if is_explicit_range:
            raise RuntimeError(
                f"Backfill range has {total_pages} pages, over MAX_PAGES_PER_RUN={MAX_PAGES_PER_RUN}. "
                "Split it into smaller ranges."
            )

        half_window = (window_end - window_start) / 2
        if half_window < MIN_WINDOW:
            raise RuntimeError(
                f"A {half_window} window still has {total_pages} pages; raise MAX_PAGES_PER_RUN to continue."
            )
        window_end = window_start + half_window
        print(f"Over the {MAX_PAGES_PER_RUN}-page budget; shrinking the window to end at {format_timestamp(window_end)}.")


def fetch_pass(session: ThrottledSession, window_start: datetime, window_end: datetime) -> tuple[list[dict], int]:
    url = f"{BASE_URL}{ENDPOINT}"
    params = window_params(window_start, window_end, 1)
    all_data = []
    expected_count = 0

    while True:
        if runtime_minutes() > MAX_RUNTIME_MINUTES:
            raise RuntimeError(
                f"Runtime budget of {MAX_RUNTIME_MINUTES:.0f} min exceeded at page {params['page']}; "
                "window not merged. Lower MAX_PAGES_PER_RUN if this repeats."
            )

        response = session.get(url, params)
        data = response.json()
        all_data.extend(data.get("data", []))

        pagination = data.get("pagination") or {}
        current_page = params["page"]
        if current_page == 1:
            expected_count = pagination.get("total_count", 0)
        total_pages = pagination.get("total_pages", current_page)
        print(f"Processing page {current_page} of {total_pages}")

        if current_page >= total_pages:
            break

        params["page"] += 1

    return all_data, expected_count


def fetch_window(session: ThrottledSession, window_start: datetime, window_end: datetime) -> pd.DataFrame:
    first_pass_started_at = runtime_minutes()
    records, expected_count = fetch_pass(session, window_start, window_end)
    first_pass_minutes = runtime_minutes() - first_pass_started_at
    fetched_ids = {record.get(PRIMARY_KEY) for record in records}
    print(f"Fetched {len(fetched_ids)} unique referrals; API reported {expected_count}.")

    # The API sorts by creation date. A referral updated past the window end while paging drops out
    # of the results and shifts the rest up one slot, so a record can be skipped. A second pass over
    # the same (closed) window picks those up; the batch dedup keeps the latest version of each id.
    # It only runs if it fits in the runtime budget; otherwise the first pass is merged as is.
    if len(fetched_ids) < expected_count and runtime_minutes() + first_pass_minutes > MAX_RUNTIME_MINUTES:
        print(
            f"Missing {expected_count - len(fetched_ids)} referrals, but a second pass would exceed "
            f"the {MAX_RUNTIME_MINUTES:.0f}-min budget; merging the first pass only."
        )
    elif len(fetched_ids) < expected_count:
        print(f"Missing {expected_count - len(fetched_ids)} referrals; fetching the window again.")
        retry_records, retry_expected_count = fetch_pass(session, window_start, window_end)
        records.extend(retry_records)
        recovered_ids = {record.get(PRIMARY_KEY) for record in retry_records} - fetched_ids
        fetched_ids |= recovered_ids
        print(
            f"Second pass recovered {len(recovered_ids)} referrals; "
            f"{len(fetched_ids)} unique vs {retry_expected_count} reported now."
        )

    return pd.DataFrame(records)


def deduplicate_batch(df: pd.DataFrame) -> pd.DataFrame:
    if PRIMARY_KEY not in df.columns:
        return df

    working_df = df.copy()
    sort_columns = []

    if "updated_at" in working_df.columns:
        working_df["_sort_updated_at"] = pd.to_datetime(working_df["updated_at"], errors="coerce", utc=True)
        sort_columns.append("_sort_updated_at")

    if "created_at" in working_df.columns:
        working_df["_sort_created_at"] = pd.to_datetime(working_df["created_at"], errors="coerce", utc=True)
        sort_columns.append("_sort_created_at")

    if sort_columns:
        working_df = working_df.sort_values(sort_columns, kind="stable", na_position="first")

    initial_count = len(working_df)
    working_df = working_df.drop_duplicates(subset=[PRIMARY_KEY], keep="last")
    dropped_rows = initial_count - len(working_df)
    if dropped_rows:
        print(f"Dropped {dropped_rows} duplicate rows from the current batch using `{PRIMARY_KEY}`.")

    return working_df.drop(columns=sort_columns, errors="ignore")


def prepare_dataframe_for_load(df: pd.DataFrame, final_schema: list[bigquery.SchemaField]) -> pd.DataFrame:
    scalar_schema = {
        "id": "STRING",
        "stripe_account_id": "STRING",
        "stripe_customer_id": "STRING",
        "conversion_state": "STRING",
        "deactivated_at": "DATETIME",
        "expires_at": "TIMESTAMP",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
        "became_lead_at": "TIMESTAMP",
        "became_conversion_at": "TIMESTAMP",
        "visits": "INTEGER",
    }

    working_df = df.copy()

    for col, dtype in scalar_schema.items():
        if col not in working_df.columns:
            continue

        if dtype == "STRING":
            working_df[col] = working_df[col].where(working_df[col].notna(), None)
            working_df[col] = working_df[col].astype(str).replace("nan", None)
        elif dtype == "TIMESTAMP":
            working_df[col] = pd.to_datetime(working_df[col], errors="coerce", utc=True)
        elif dtype == "DATETIME":
            converted = pd.to_datetime(working_df[col], errors="coerce", utc=True)
            working_df[col] = converted.dt.tz_localize(None)
        elif dtype == "INTEGER":
            working_df[col] = pd.to_numeric(working_df[col], errors="coerce").fillna(0).astype(int)

    final_columns = [field.name for field in final_schema]
    dropped_columns = sorted(col for col in working_df.columns if col not in final_columns)
    if dropped_columns:
        print(
            "Dropping unexpected columns not present in BigQuery schema: "
            + ", ".join(dropped_columns)
        )

    for column in final_columns:
        if column not in working_df.columns:
            working_df[column] = None

    working_df = working_df[[column for column in final_columns]]
    working_df = deduplicate_batch(working_df)

    return working_df.reset_index(drop=True)


def load_batch_into_staging(
    client: bigquery.Client,
    df: pd.DataFrame,
    final_schema: list[bigquery.SchemaField],
) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=final_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{STAGING_DATASET_TABLE}",
        job_config=job_config,
    )
    job.result()
    print(f"Loaded {len(df)} rows into staging table {PROJECT_ID}.{STAGING_DATASET_TABLE}.")


def merge_staging_into_final(
    client: bigquery.Client,
    final_schema: list[bigquery.SchemaField],
) -> None:
    final_columns = [field.name for field in final_schema]
    update_columns = [column for column in final_columns if column != PRIMARY_KEY]
    update_clause = ",\n      ".join(
        f"target.`{column}` = source.`{column}`" for column in update_columns
    )
    insert_columns = ", ".join(f"`{column}`" for column in final_columns)
    insert_values = ", ".join(f"source.`{column}`" for column in final_columns)

    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_TABLE}` AS target
    USING (
      SELECT * EXCEPT (_row_num)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY `{PRIMARY_KEY}`
            ORDER BY `updated_at` DESC NULLS LAST, `created_at` DESC NULLS LAST
          ) AS _row_num
        FROM `{PROJECT_ID}.{STAGING_DATASET_TABLE}`
      )
      WHERE _row_num = 1
    ) AS source
    ON target.`{PRIMARY_KEY}` = source.`{PRIMARY_KEY}`
    WHEN MATCHED THEN
      UPDATE SET
      {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    client.query(query).result()
    print(f"Merged staging table {PROJECT_ID}.{STAGING_DATASET_TABLE} into {PROJECT_ID}.{DATASET_TABLE}.")


def main() -> None:
    if not DATASET or not TABLE or not SECRET_NAME:
        raise RuntimeError("Missing required environment variables for dataset, table, or secret name.")

    if not BACKFILL_END:
        running_execution = find_earlier_running_execution()
        if running_execution:
            print(f"Execution {running_execution} is still running the same window; skipping this run.")
            return

    client = bigquery.Client(project=PROJECT_ID)
    ensure_staging_dataset_exists(client)

    if IS_BACKFILL_MODE:
        backfill_window = get_backfill_window(client)
        if backfill_window is None:
            print(f"Backfill complete: progress already reaches {BACKFILL_RANGE_END}. Nothing to do.")
            return
        window_start, window_end = backfill_window
    else:
        window_start, window_end = get_window(client)

    session = ThrottledSession(get_secret(SECRET_NAME))
    # The backfill may shrink its window too: progress records where it actually stopped.
    window_end = fit_window(session, window_start, window_end, is_explicit_range=bool(BACKFILL_END))
    print(f"Window: updated_since={format_timestamp(window_start)} updated_until={format_timestamp(window_end)}")

    data = fetch_window(session, window_start, window_end)

    if data.empty and IS_BACKFILL_MODE:
        record_backfill_progress(client, window_start, window_end, 0)
        print("No referrals in this window.")
        return

    if data.empty:
        # Without new rows the watermark cannot move, so an empty window is only fine if it reaches now.
        if window_end < datetime.now(timezone.utc) - timedelta(minutes=5) and not BACKFILL_END:
            raise RuntimeError(
                "Window returned no referrals but ends before now; the watermark would not advance. "
                f"Run with BACKFILL_START={format_timestamp(window_end)} to skip it."
            )
        print("No new data to merge.")
        return

    final_table = get_final_table(client)
    prepared_df = prepare_dataframe_for_load(data, final_table.schema)
    try:
        load_batch_into_staging(client, prepared_df, final_table.schema)
        merge_staging_into_final(client, final_table.schema)
    finally:
        client.delete_table(f"{PROJECT_ID}.{STAGING_DATASET_TABLE}", not_found_ok=True)

    print(f"Window done: {len(prepared_df)} referrals merged.")
    if IS_BACKFILL_MODE:
        record_backfill_progress(client, window_start, window_end, len(prepared_df))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Job failed: {exc}", file=sys.stderr)
        raise
