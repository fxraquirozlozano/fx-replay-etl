import os
import time
from datetime import timezone

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, secretmanager
from requests.auth import HTTPBasicAuth

BASE_URL = "https://api.getrewardful.com"
ENDPOINT = "/v1/commissions?expand[]=sale&expand[]=campaign"
PROJECT_ID = "fxr-analytics"
PROJECT_NUMBER = "362612879927"
DATASET = os.getenv("REWARDFUL_DATASET")
TABLE = os.getenv("COMMISSIONS_TABLE")
DATASET_TABLE = f"{DATASET}.{TABLE}"
STAGING_DATASET = os.getenv("REWARDFUL_RAW_DATASET", "rewardful_raw")
STAGING_DATASET_TABLE = f"{STAGING_DATASET}.{TABLE}"
SECRET_NAME = os.getenv("REWARDFUL_API_KEY_SECRET")

PRIMARY_KEY = "id"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
BACKOFF_SECONDS = 5
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "0"))


def get_secret(secret_name: str) -> str:
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{PROJECT_NUMBER}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")
    except Exception as exc:
        raise RuntimeError(f"Error retrieving secret '{secret_name}': {exc}") from exc


def get_last_updated_at(client: bigquery.Client) -> str:
    query = f"SELECT MAX(updated_at) AS max_updated_at FROM `{PROJECT_ID}.{DATASET_TABLE}`"

    try:
        result = client.query(query).result()
        max_updated_at = next(result).max_updated_at
        if not max_updated_at:
            return "2000-01-01T00:00:00Z"

        if max_updated_at.tzinfo is None:
            max_updated_at = max_updated_at.replace(tzinfo=timezone.utc)

        return max_updated_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception as exc:
        print(f"Table not found or error fetching `updated_at`: {exc}")
        return "2000-01-01T00:00:00Z"


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


def reset_staging_table(client: bigquery.Client) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET_TABLE}` AS
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_TABLE}`
    WHERE 1 = 0
    """
    client.query(query).result()
    print(f"Staging table {PROJECT_ID}.{STAGING_DATASET_TABLE} reset for current batch.")


def _request_with_retry(url: str, headers: dict, params: dict, auth: HTTPBasicAuth) -> requests.Response:
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                auth=auth,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last_exception = exc
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

        sleep_seconds = BACKOFF_SECONDS * attempt
        print(
            f"Retryable Rewardful API error {response.status_code} on attempt "
            f"{attempt}/{MAX_RETRIES}. Retrying in {sleep_seconds}s."
        )
        time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch Rewardful data: {last_exception}")


def fetch_api_data(endpoint: str, updated_since: str, api_key: str) -> pd.DataFrame:
    url = f"{BASE_URL}{endpoint}&updated_since={updated_since}"
    headers = {"Accept": "*/*"}
    params = {"limit": 100, "page": 1}
    auth = HTTPBasicAuth(api_key, "")
    all_data = []

    while True:
        response = _request_with_retry(url, headers, params, auth)
        data = response.json()
        all_data.extend(data.get("data", []))

        pagination = data.get("pagination") or {}
        current_page = params["page"]
        total_pages = pagination.get("total_pages", current_page)
        print(f"Processing page {current_page} of {total_pages}")

        if MAX_PAGES_PER_RUN > 0 and current_page >= MAX_PAGES_PER_RUN:
            raise RuntimeError(
                f"Aborting run after page {current_page} because MAX_PAGES_PER_RUN={MAX_PAGES_PER_RUN}. "
                "Increase the limit, reduce the data window, or redesign the ingestion to chunk safely."
            )

        if current_page >= total_pages:
            break

        params["page"] += 1

    return pd.DataFrame(all_data)


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
        "currency": "STRING",
        "state": "STRING",
        "stripe_account_id": "STRING",
        "due_at": "TIMESTAMP",
        "paid_at": "TIMESTAMP",
        "voided_at": "STRING",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
        "amount": "INTEGER",
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


def main(request=None):
    if not DATASET or not TABLE or not SECRET_NAME:
        raise RuntimeError("Missing required environment variables for dataset, table, or secret name.")

    client = bigquery.Client(project=PROJECT_ID)
    ensure_staging_dataset_exists(client)

    last_updated_at = get_last_updated_at(client)
    print(f"Last updated_at in BigQuery: {last_updated_at}")

    api_key = get_secret(SECRET_NAME)
    data = fetch_api_data(ENDPOINT, last_updated_at, api_key)

    if data.empty:
        reset_staging_table(client)
        print("No new data to merge.")
        return "", 200

    final_table = get_final_table(client)
    prepared_df = prepare_dataframe_for_load(data, final_table.schema)
    load_batch_into_staging(client, prepared_df, final_table.schema)
    merge_staging_into_final(client, final_table.schema)

    return "", 200
