from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


CAMPAIGN_DAILY_SCHEMA = [
    bigquery.SchemaField("data_date", "DATE"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_status", "STRING"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("cost", "FLOAT"),
]
ROW_KEY = ("data_date", "customer_id", "platform", "device_type", "campaign_id")
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


def context_logical_date(context: dict[str, Any]) -> str:
    """Return an ISO logical date for scheduled and manual Airflow 2/3 runs."""
    if context.get("ds"):
        return str(context["ds"])
    for key in ("logical_date", "data_interval_end"):
        value = context.get(key)
        if value is not None:
            return value.date().isoformat()
    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "logical_date", None) is not None:
        return dag_run.logical_date.date().isoformat()
    return datetime.now(UTC).date().isoformat()


def request_with_retry(
    method: str,
    url: str,
    *,
    max_attempts: int = 5,
    **kwargs: Any,
) -> requests.Response:
    """Call an API with bounded backoff for throttling and transient failures."""
    response: requests.Response | None = None
    for attempt in range(max_attempts):
        response = requests.request(method, url, **kwargs)
        if response.status_code not in RETRYABLE_HTTP_STATUSES:
            response.raise_for_status()
            return response
        if attempt < max_attempts - 1:
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else min(
                2**attempt, 60
            )
            time.sleep(delay)
    assert response is not None
    response.raise_for_status()
    return response


def validate_and_deduplicate_rows(
    rows: list[dict[str, Any]], start_date: date, end_date: date,
    *, allow_negative_cost: bool = False,
) -> list[dict[str, Any]]:
    """Reject malformed/conflicting rows and collapse exact API duplicates."""
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        missing = [
            field
            for field in ("data_date", "customer_id", "platform", "campaign_id")
            if not row.get(field)
        ]
        if missing:
            raise ValueError(f"Campaign row is missing required fields: {missing}")
        row_date = date.fromisoformat(str(row["data_date"]))
        if not start_date <= row_date <= end_date:
            raise ValueError(f"Campaign row date {row_date} is outside the load window")
        if row["impressions"] < 0 or (row["clicks"] is not None and row["clicks"] < 0):
            raise ValueError("Campaign impressions and clicks cannot be negative")
        if row["cost"] < 0 and not allow_negative_cost:
            raise ValueError("Campaign cost cannot be negative")

        key = tuple(row.get(field) for field in ROW_KEY)
        existing = unique.get(key)
        if existing is not None and existing != row:
            raise ValueError(f"Conflicting campaign rows returned for key {key}")
        unique[key] = row
    return list(unique.values())


def resolve_daily_window(
    ds: str, conf: dict[str, Any] | None, default_lookback_days: int
) -> tuple[date, date]:
    """Return an inclusive window, excluding the incomplete current logical day."""
    conf = conf or {}
    default_end = datetime.fromisoformat(ds).date() - timedelta(days=1)
    end_date = date.fromisoformat(conf.get("end_date", default_end.isoformat()))
    start_date = date.fromisoformat(
        conf.get(
            "start_date",
            (end_date - timedelta(days=default_lookback_days - 1)).isoformat(),
        )
    )
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    return start_date, end_date


def replace_bigquery_window(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    *,
    allow_negative_cost: bool = False,
) -> None:
    """Atomically replace a date window after staging and validating new rows."""
    rows = validate_and_deduplicate_rows(
        rows, start_date, end_date, allow_negative_cost=allow_negative_cost,
    )
    dataset_ref = f"{project_id}.{dataset_id}"
    table_ref = f"{dataset_ref}.{table_id}"
    staging_ref = f"{dataset_ref}._staging_{table_id}_{uuid4().hex}"

    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset, exists_ok=True)

    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=CAMPAIGN_DAILY_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_date",
        )
        client.create_table(table)

    staging = bigquery.Table(staging_ref, schema=CAMPAIGN_DAILY_SCHEMA)
    client.create_table(staging)
    try:
        if rows:
            load_config = bigquery.LoadJobConfig(
                schema=CAMPAIGN_DAILY_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            client.load_table_from_json(rows, staging_ref, job_config=load_config).result()

        query = f"""
        BEGIN TRANSACTION;
        DELETE FROM `{table_ref}`
        WHERE data_date BETWEEN @start_date AND @end_date;
        INSERT INTO `{table_ref}`
        SELECT * FROM `{staging_ref}`;
        ASSERT NOT EXISTS (
          SELECT 1
          FROM `{table_ref}`
          WHERE data_date BETWEEN @start_date AND @end_date
          GROUP BY data_date, customer_id, platform, device_type, campaign_id
          HAVING COUNT(*) > 1
        ) AS 'Duplicate campaign daily rows detected';
        COMMIT TRANSACTION;
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        client.query(query, job_config=config).result()
    finally:
        client.delete_table(staging_ref, not_found_ok=True)
