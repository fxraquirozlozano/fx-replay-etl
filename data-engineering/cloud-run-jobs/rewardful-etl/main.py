"""Full extracts of Rewardful campaigns, affiliates, payouts and commissions into BigQuery.

The Rewardful API ignores `updated_since` on these endpoints, so every run downloads each one completely.
Campaigns, affiliates and payouts are replaced as a snapshot; commissions are merged by `id`.
Endpoints run one after the other and fail independently; the run exits with an error if any failed.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, secretmanager
from google.cloud.bigquery import SchemaField
from requests.auth import HTTPBasicAuth

BASE_URL = "https://api.getrewardful.com/v1"
PROJECT_ID = "fxr-analytics"
PROJECT_NUMBER = "362612879927"
REGION = "us-central1"
DATASET = os.getenv("REWARDFUL_DATASET", "rewardful")
STAGING_DATASET = os.getenv("REWARDFUL_RAW_DATASET", "rewardful_raw")
SECRET_NAME = os.getenv("REWARDFUL_API_KEY_SECRET", "rewardful-api-key")
# Lets a run write commissions somewhere else, e.g. a check table during the cutover.
COMMISSIONS_TABLE = os.getenv("COMMISSIONS_TABLE", f"{DATASET}.commissions")
# Comma-separated subset of endpoints to run; all by default.
ENDPOINTS_TO_RUN = [e.strip() for e in os.getenv("ENDPOINTS", "").split(",") if e.strip()]

PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 60
# Minimum time between request starts; Rewardful allows ~45 requests per 30 s.
MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "1"))
MAX_RETRIES = 6
BACKOFF_SECONDS = 10
RATE_LIMIT_FALLBACK_SECONDS = 30
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

JOB_NAME = os.getenv("CLOUD_RUN_JOB")
EXECUTION_NAME = os.getenv("CLOUD_RUN_EXECUTION")
EXECUTION_ID = (EXECUTION_NAME or "local").replace("-", "_")


def ts(name: str) -> SchemaField:
    return SchemaField(name, "TIMESTAMP")


def s(name: str) -> SchemaField:
    return SchemaField(name, "STRING")


def i(name: str) -> SchemaField:
    return SchemaField(name, "INTEGER")


def b(name: str) -> SchemaField:
    return SchemaField(name, "BOOLEAN")


CAMPAIGNS_SCHEMA = [
    s("id"), s("name"), s("url"), b("default"), b("private"), b("private_tokens"),
    s("reward_type"), SchemaField("commission_percent", "FLOAT"),
    i("commission_amount_cents"), s("commission_amount_currency"),
    i("max_commission_period_months"), i("max_commissions"),
    i("days_before_referrals_expire"), i("days_until_commissions_are_due"),
    i("minimum_payout_cents"), s("minimum_payout_currency"),
    s("parameter_type"), s("stripe_coupon_id"),
    b("customers_visible_to_affiliates"), b("sale_description_visible_to_affiliates"),
    s("affiliate_dashboard_text"), s("custom_reward_description"), s("welcome_text"),
    i("affiliates"), i("visitors"), i("leads"), i("conversions"),
    ts("created_at"), ts("updated_at"),
]

AFFILIATES_SCHEMA = [
    s("id"), s("state"), s("first_name"), s("last_name"), s("email"), s("unconfirmed_email"),
    ts("confirmed_at"), s("paypal_email"), ts("paypal_email_confirmed_at"),
    s("wise_email"), ts("wise_email_confirmed_at"),
    s("stripe_customer_id"), s("stripe_account_id"),
    b("receive_new_commission_notifications"), i("sign_in_count"),
    i("visitors"), i("leads"), i("conversions"),
    SchemaField("campaign", "RECORD", fields=[s("id"), s("name"), ts("created_at"), ts("updated_at")]),
    SchemaField("links", "RECORD", mode="REPEATED", fields=[
        s("id"), s("token"), s("url"), i("visitors"), i("leads"), i("conversions"),
    ]),
    ts("created_at"), ts("updated_at"),
]

PAYOUTS_SCHEMA = [
    s("id"), s("affiliate_id"), s("state"), i("amount"), s("currency"),
    ts("paid_at"), s("paid_by_id"),
    SchemaField("commissions", "RECORD", mode="REPEATED", fields=[
        s("id"), s("state"), i("amount"), s("currency"),
        ts("due_at"), ts("paid_at"), ts("voided_at"), ts("created_at"), ts("updated_at"),
    ]),
    ts("created_at"), ts("updated_at"),
]

COMMISSIONS_CAMPAIGN_FIELD = SchemaField(
    "campaign", "RECORD", fields=[s("id"), s("name"), s("created_at"), s("updated_at")]
)


def payout_row(payout: dict) -> dict:
    payout["affiliate_id"] = (payout.get("affiliate") or {}).get("id")
    return payout


# name, API path, expand params, mode, row transform
ENDPOINTS = [
    ("campaigns", "campaigns", [], "replace", None),
    ("affiliates", "affiliates", ["links", "campaign"], "replace", None),
    ("payouts", "payouts", ["commissions", "affiliate"], "replace", payout_row),
    ("commissions", "commissions", ["sale", "campaign"], "merge", None),
]
SCHEMAS = {"campaigns": CAMPAIGNS_SCHEMA, "affiliates": AFFILIATES_SCHEMA, "payouts": PAYOUTS_SCHEMA}


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_NUMBER}/secrets/{secret_name}/versions/latest"
    return client.access_secret_version(name=secret_path).payload.data.decode("UTF-8")


def find_earlier_running_execution() -> str | None:
    # Two runs at once would compete for the same API rate limit; the later one yields.
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
        if name == EXECUTION_NAME or execution.get("completionTime"):
            continue
        if execution["createTime"] < own_created_at:
            return name
    return None


def get_rate_limit_wait_seconds(response: requests.Response) -> float:
    for name in ("RateLimit-Reset", "X-RateLimit-Reset", "Retry-After"):
        value = response.headers.get(name)
        if value and value.strip().isdigit():
            seconds = float(value.strip())
            # Some APIs send the reset as an epoch timestamp instead of seconds left.
            if seconds > 10**9:
                seconds = max(seconds - time.time(), 0)
            return min(seconds, RATE_LIMIT_FALLBACK_SECONDS * 2) + 1
    return RATE_LIMIT_FALLBACK_SECONDS


class ThrottledSession:
    def __init__(self, api_key: str):
        self.auth = HTTPBasicAuth(api_key, "")
        self.last_request_started = 0.0

    def _wait_for_slot(self) -> None:
        elapsed = time.monotonic() - self.last_request_started
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        self.last_request_started = time.monotonic()

    def get(self, path: str, params: list) -> dict:
        for attempt in range(1, MAX_RETRIES + 1):
            self._wait_for_slot()
            try:
                response = requests.get(
                    f"{BASE_URL}/{path}", params=params, auth=self.auth,
                    headers={"Accept": "*/*"}, timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Request failed after {MAX_RETRIES} attempts: {exc}") from exc
                print(f"Request exception on attempt {attempt}/{MAX_RETRIES}: {exc}. Retrying.")
                time.sleep(BACKOFF_SECONDS * attempt)
                continue

            if response.status_code == 200:
                return response.json()
            if response.status_code not in RETRYABLE_STATUS_CODES:
                raise RuntimeError(f"Non-retryable Rewardful API error {response.status_code}: {response.text[:300]}")
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Rewardful API error {response.status_code} persisted after {MAX_RETRIES} attempts"
                )

            # 503s come in bursts that last a few minutes, so the backoff grows up to ~5 min in total.
            wait = get_rate_limit_wait_seconds(response) if response.status_code == 429 else BACKOFF_SECONDS * attempt
            print(f"Rewardful API error {response.status_code} on attempt {attempt}/{MAX_RETRIES}. Retrying in {wait:.0f}s.")
            time.sleep(wait)
        raise RuntimeError("unreachable")


def fetch_all(session: ThrottledSession, path: str, expand: list[str]) -> tuple[list[dict], int]:
    rows, page, reported_total = [], 1, None
    expand_params = [("expand[]", e) for e in expand]
    while True:
        body = session.get(path, [("limit", PAGE_SIZE), ("page", page)] + expand_params)
        rows.extend(body.get("data", []))
        pagination = body.get("pagination") or {}
        if reported_total is None:
            reported_total = pagination.get("total_count", 0)
        total_pages = pagination.get("total_pages") or page
        if page == 1 or page % 50 == 0 or page >= total_pages:
            print(f"{path}: page {page} of {total_pages}")
        if page >= total_pages:
            return rows, reported_total
        page += 1


def fetch_complete(session: ThrottledSession, path: str, expand: list[str]) -> tuple[dict, bool]:
    # Rows shift between pages while paging; a second pass covers ids that fell between pages.
    by_id: dict[str, dict] = {}
    for attempt in (1, 2):
        rows, reported_total = fetch_all(session, path, expand)
        for row in rows:
            by_id[row["id"]] = row
        print(f"{path}: {len(by_id)} unique rows; API reported {reported_total}.")
        if len(by_id) >= reported_total:
            return by_id, True
        if attempt == 1:
            print(f"{path}: missing {reported_total - len(by_id)} rows; fetching again.")
    return by_id, False


def conform(value, field: SchemaField):
    # Shape a JSON value to its BigQuery field so the load never fails on an unexpected type.
    if value is None:
        return [] if field.mode == "REPEATED" else None
    if field.mode == "REPEATED":
        items = value if isinstance(value, list) else [value]
        single = SchemaField(field.name, field.field_type, fields=field.fields)
        return [conform(item, single) for item in items if item is not None]
    if field.field_type in ("RECORD", "STRUCT"):
        if not isinstance(value, dict):
            return None
        return {sub.name: conform(value.get(sub.name), sub) for sub in field.fields}
    if field.field_type == "STRING":
        return json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    if field.field_type in ("INTEGER", "INT64"):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if field.field_type in ("FLOAT", "FLOAT64", "NUMERIC"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if field.field_type in ("BOOLEAN", "BOOL"):
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def conform_rows(rows, schema: list[SchemaField]) -> list[dict]:
    return [{field.name: conform(row.get(field.name), field) for field in schema} for row in rows]


def load_rows(client: bigquery.Client, rows: list[dict], table_id: str, schema, disposition: str) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()


def run_replace(client: bigquery.Client, name: str, rows: list[dict], complete: bool) -> None:
    if not complete:
        raise RuntimeError(f"{name}: incomplete download; the previous snapshot is kept.")
    table_id = f"{PROJECT_ID}.{DATASET}.{name}"
    # A load job with WRITE_TRUNCATE swaps the whole table at once, so readers never see it half-loaded.
    load_rows(client, conform_rows(rows, SCHEMAS[name]), table_id, SCHEMAS[name],
              bigquery.WriteDisposition.WRITE_TRUNCATE)
    print(f"{name}: replaced {table_id} with {len(rows)} rows.")


def commissions_schema(client: bigquery.Client, table_id: str) -> list[SchemaField]:
    table = client.get_table(table_id)
    if not any(field.name == "campaign" for field in table.schema):
        table.schema = list(table.schema) + [COMMISSIONS_CAMPAIGN_FIELD]
        table = client.update_table(table, ["schema"])
        print(f"commissions: added column `campaign` to {table_id}.")
    return list(table.schema)


def run_merge(client: bigquery.Client, rows: list[dict], complete: bool) -> None:
    if not complete:
        print("commissions: incomplete download; merging what was fetched (the merge never deletes).")
    table_id = f"{PROJECT_ID}.{COMMISSIONS_TABLE}"
    schema = commissions_schema(client, table_id)
    staging_id = f"{PROJECT_ID}.{STAGING_DATASET}.commissions__{EXECUTION_ID}"
    load_rows(client, conform_rows(rows, schema), staging_id, schema, bigquery.WriteDisposition.WRITE_TRUNCATE)

    columns = [field.name for field in schema]
    update_clause = ", ".join(f"target.`{c}` = source.`{c}`" for c in columns if c != "id")
    insert_columns = ", ".join(f"`{c}`" for c in columns)
    insert_values = ", ".join(f"source.`{c}`" for c in columns)
    try:
        client.query(f"""
        MERGE `{table_id}` AS target
        USING `{staging_id}` AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """).result()
        print(f"commissions: merged {len(rows)} rows into {table_id}.")
    finally:
        client.delete_table(staging_id, not_found_ok=True)


def main() -> None:
    running = find_earlier_running_execution()
    if running:
        print(f"Execution {running} is still running; skipping this run.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    session = ThrottledSession(get_secret(SECRET_NAME))
    failed = []

    for name, path, expand, mode, transform in ENDPOINTS:
        if ENDPOINTS_TO_RUN and name not in ENDPOINTS_TO_RUN:
            continue
        started = time.monotonic()
        try:
            by_id, complete = fetch_complete(session, path, expand)
            rows = [transform(row) if transform else row for row in by_id.values()]
            if mode == "replace":
                run_replace(client, name, rows, complete)
            else:
                run_merge(client, rows, complete)
        except Exception as exc:
            failed.append(name)
            print(f"{name}: FAILED: {exc}")
        print(f"{name}: done in {(time.monotonic() - started) / 60:.1f} min.")

    if failed:
        print(f"Run finished with failures: {', '.join(failed)}")
        sys.exit(1)
    print("Run finished.")


if __name__ == "__main__":
    main()
