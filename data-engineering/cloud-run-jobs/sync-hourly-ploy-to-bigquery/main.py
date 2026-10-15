"""Incremental PloyDB ingestion. Grain: one row per immutable Ploy row ID."""

import argparse
import json
import logging
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


UTC = timezone.utc
LOGGER = logging.getLogger(__name__)
GATEWAY = "https://ploydb-do-production.ploy.workers.dev"
# Immutable field IDs preserve the contract even when a Ploy label is renamed.
FIELDS = {
    "submission_id": "fld_89820d647e1f41e0",
    "email": "fld_71f1168bfc734ccf",
    "first_name": "fld_bec6ca4aa4f24dd7",
    "category": "fld_0f5435b7c3ef443b",
    "source_type": "fld_85082a2c661a47d9",
    "source_url": "fld_c41a718449cc4fec",
    "utm_source": "fld_039b9cc27bf442d2",
    "utm_medium": "fld_36c8d78d3d0f4017",
    "utm_campaign": "fld_4c934a68918140fe",
    "utm_content": "fld_3e2977e9df6e41db",
    "utm_term": "fld_8c5c70d3540d4eec",
    "referrer": "fld_0a25387e931044c0",
    "consent_marketing": "fld_f90c4ed3a8a945af",
    "submitted_at": "fld_38cf3145a9784284",
    "is_test": "fld_7aaa423b92f34b1b",
    "extra_fields": "fld_8836c1cdc9774282",
}
SCHEMA = [
    bigquery.SchemaField("ploy_row_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ploy_database_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ploy_table_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_updated_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_version", "INTEGER", mode="REQUIRED"),
    *[
        bigquery.SchemaField(name, "STRING")
        for name in FIELDS
        if name not in {"consent_marketing", "is_test", "submitted_at"}
    ],
    bigquery.SchemaField("consent_marketing", "BOOLEAN"),
    bigquery.SchemaField("is_test", "BOOLEAN"),
    bigquery.SchemaField("submitted_at", "TIMESTAMP"),
    bigquery.SchemaField("submitted_at_raw", "STRING"),
    bigquery.SchemaField("raw_values", "JSON"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]
STATE_SCHEMA = [
    bigquery.SchemaField("pipeline_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("watermark", "TIMESTAMP"),
    bigquery.SchemaField("last_success_at", "TIMESTAMP"),
    bigquery.SchemaField("last_row_count", "INTEGER"),
]


@dataclass(frozen=True)
class Config:
    project: str = "fxr-analytics"
    dataset: str = "ploy"
    table: str = "submissions"
    location: str = "US"
    database_id: str = "pdb_269c8fcd20e44d9d8f96"
    table_id: str = "tbl_53358e8d464d4fbe9712"
    lookback_hours: int = 168
    safety_lag_seconds: int = 60
    page_size: int = 500
    timeout_seconds: int = 60

    def __post_init__(self):
        for name in (self.project, self.dataset, self.table):
            if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
                raise ValueError("Invalid BigQuery identifier")
        if not re.fullmatch(r"pdb_[A-Za-z0-9]+", self.database_id):
            raise ValueError("Invalid Ploy database ID")
        if not re.fullmatch(r"tbl_[A-Za-z0-9]+", self.table_id):
            raise ValueError("Invalid Ploy table ID")
        if not 1 <= self.page_size <= 500 or self.lookback_hours < 1:
            raise ValueError("Invalid page size or lookback")
        if self.safety_lag_seconds < 0 or self.timeout_seconds < 1:
            raise ValueError("Invalid lag or timeout")

    @classmethod
    def from_env(cls):
        return cls(
            project=os.getenv("GCP_PROJECT", "fxr-analytics"),
            dataset=os.getenv("BQ_DATASET", "ploy"),
            table=os.getenv("BQ_TABLE", "submissions"),
            location=os.getenv("BQ_LOCATION", "US"),
            database_id=os.getenv("PLOY_DATABASE_ID", cls.database_id),
            table_id=os.getenv("PLOY_TABLE_ID", cls.table_id),
            lookback_hours=int(os.getenv("LOOKBACK_HOURS", "168")),
            safety_lag_seconds=int(os.getenv("SAFETY_LAG_SECONDS", "60")),
            page_size=int(os.getenv("PAGE_SIZE", "500")),
            timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "60")),
        )

    @property
    def target(self):
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def state(self):
        return f"{self.project}.{self.dataset}._sync_state"

    @property
    def pipeline_id(self):
        return f"{self.database_id}/{self.table_id}/{self.table}"


def parse_timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Source timestamp must include a timezone")
    return parsed.astimezone(UTC)


def timestamp(value):
    # Ploy stores millisecond ISO timestamps. Keep the same format for comparisons.
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def boolean(value):
    if value is None:
        return None
    if value in (True, 1, "1", "true"):
        return True
    if value in (False, 0, "0", "false"):
        return False
    raise ValueError("Unexpected Ploy checkbox value")


def transform(row, config, ingested_at):
    if not row.get("id") or not isinstance(row.get("values"), dict):
        raise ValueError("Ploy row is missing its ID or values")
    values = row["values"]
    result = {name: values.get(field_id) for name, field_id in FIELDS.items()}
    for name in ("consent_marketing", "is_test"):
        result[name] = boolean(result[name])
    result["submitted_at_raw"] = result["submitted_at"]
    try:
        result["submitted_at"] = timestamp(parse_timestamp(result["submitted_at"]))
    except (AttributeError, TypeError, ValueError):
        result["submitted_at"] = None
    if not isinstance(row.get("version"), int) or isinstance(row["version"], bool):
        raise ValueError("Ploy row is missing its integer version")
    result.update(
        ploy_row_id=row["id"],
        ploy_database_id=config.database_id,
        ploy_table_id=config.table_id,
        source_created_at=timestamp(parse_timestamp(row["createdAt"])),
        source_updated_at=timestamp(parse_timestamp(row["updatedAt"])),
        source_version=row["version"],
        raw_values=values,
        ingested_at=timestamp(ingested_at),
    )
    return result


class PloyClient:
    def __init__(self, config, access_key, session=None):
        if not access_key.strip():
            raise ValueError("PLOY_ACCESS_KEY is required")
        self.config = config
        self.session = session or requests.Session()
        retry = Retry(
            total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]), respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"Authorization": f"Bearer {access_key.strip()}"})
        self.base = f"{GATEWAY}/site/v1/ploydbs/{config.database_id}"

    def request(self, method, path, **kwargs):
        response = self.session.request(
            method, f"{self.base}{path}", timeout=self.config.timeout_seconds,
            allow_redirects=False, **kwargs,
        )
        if response.status_code != 200:
            # Do not log response bodies, credentials or submission payloads.
            raise RuntimeError(f"Ploy request failed: HTTP {response.status_code}")
        data = response.json()
        if data.get("ok") is not True:
            raise RuntimeError("Ploy returned an unsuccessful response")
        return data

    def validate_schema(self):
        tables = self.request("GET", "/tables")["tables"]
        table = next((t for t in tables if t["tableId"] == self.config.table_id), None)
        if table is None:
            raise RuntimeError("Ploy table is not in the key's read scope")
        available = {field["fieldId"] for field in table["fields"]}
        missing = set(FIELDS.values()) - available
        if missing:
            raise RuntimeError(f"Ploy schema is missing {len(missing)} expected fields")

    def pages(self, start, end):
        filters = [{"field": "updated_at", "op": "lt", "value": timestamp(end)}]
        if start is not None:
            filters.append({"field": "updated_at", "op": "gte", "value": timestamp(start)})
        body = {
            "filters": filters,
            "sort": [{"field": "updated_at", "dir": "asc"}, {"field": "id", "dir": "asc"}],
            "limit": self.config.page_size,
        }
        seen = set()
        while True:
            data = self.request("POST", f"/tables/{self.config.table_id}/query", json=dict(body))
            rows = data.get("rows")
            if not isinstance(rows, list) or not isinstance(data.get("hasMore"), bool):
                raise RuntimeError("Unexpected Ploy pagination response")
            for row in rows:
                updated = parse_timestamp(row["updatedAt"])
                if updated >= end or (start is not None and updated < start):
                    raise RuntimeError("Ploy returned a row outside the requested time window")
            yield rows
            if not data["hasMore"]:
                return
            cursor = data.get("nextCursor") or data.get("cursor")
            if not cursor or cursor in seen:
                raise RuntimeError("Missing or repeated Ploy pagination cursor")
            seen.add(cursor)
            body["cursor"] = cursor


def query_config(**params):
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter(name, kind, value)
        for name, (kind, value) in params.items()
    ])


def initialize(client, config):
    dataset = bigquery.Dataset(f"{config.project}.{config.dataset}")
    dataset.location = config.location
    dataset.description = "Raw PloyDB data loaded incrementally every hour."
    client.create_dataset(dataset, exists_ok=True)
    if client.get_dataset(dataset.reference).location.upper() != config.location.upper():
        raise ValueError("Existing dataset location does not match BQ_LOCATION")
    target = bigquery.Table(config.target, schema=SCHEMA)
    target.description = "Ploy Submissions: one current row per immutable ploy_row_id. UTC timestamps."
    target.time_partitioning = bigquery.TimePartitioning(field="source_created_at")
    target.clustering_fields = ["ploy_row_id", "submission_id"]
    client.create_table(target, exists_ok=True)
    state = bigquery.Table(config.state, schema=STATE_SCHEMA)
    state.description = "One checkpoint per Ploy source/target pipeline; committed with its data."
    client.create_table(state, exists_ok=True)
    client.query(
        f"""MERGE `{config.state}` T
        USING (SELECT @pipeline_id AS pipeline_id) S ON T.pipeline_id = S.pipeline_id
        WHEN NOT MATCHED THEN INSERT (pipeline_id) VALUES (S.pipeline_id)""",
        job_config=query_config(pipeline_id=("STRING", config.pipeline_id)),
    ).result()


def get_window(client, config, now):
    rows = list(client.query(
        f"SELECT watermark FROM `{config.state}` WHERE pipeline_id = @pipeline_id",
        job_config=query_config(pipeline_id=("STRING", config.pipeline_id)),
    ).result())
    if len(rows) != 1:
        raise RuntimeError("Expected one checkpoint row; run --init-only before ingestion")
    watermark = rows[0]["watermark"]
    end = parse_timestamp(timestamp(now - timedelta(seconds=config.safety_lag_seconds)))
    if watermark is not None and watermark > end:
        raise RuntimeError("Checkpoint is ahead of the current extraction cutoff")
    start = watermark - timedelta(hours=config.lookback_hours) if watermark else None
    return start, end


def commit_batch(client, config, stage, end, row_count):
    columns = [field.name for field in SCHEMA]
    updates = ", ".join(f"T.`{col}` = S.`{col}`" for col in columns if col != "ploy_row_id")
    merge = ""
    if stage:
        merge = f"""
        MERGE `{config.target}` T
        USING (
            SELECT * FROM `{stage}`
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ploy_row_id ORDER BY source_version DESC, source_updated_at DESC
            ) = 1
        ) S ON T.ploy_row_id = S.ploy_row_id
        WHEN MATCHED AND (S.source_version > T.source_version
            OR (S.source_version = T.source_version AND S.source_updated_at > T.source_updated_at))
        THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({', '.join(f'`{c}`' for c in columns)})
        VALUES ({', '.join(f'S.`{c}`' for c in columns)});
        """
    # Updating the shared checkpoint in the transaction also detects overlapping
    # writers: a conflicting transaction aborts instead of inserting duplicates.
    sql = f"""
    BEGIN TRANSACTION;
    UPDATE `{config.state}`
    SET watermark = GREATEST(COALESCE(watermark, @cutoff), @cutoff),
        last_success_at = CURRENT_TIMESTAMP(), last_row_count = @row_count
    WHERE pipeline_id = @pipeline_id;
    ASSERT @@row_count = 1 AS 'Expected exactly one checkpoint row';
    {merge}
    COMMIT TRANSACTION;
    """
    client.query(sql, job_config=query_config(
        cutoff=("TIMESTAMP", end), row_count=("INT64", row_count),
        pipeline_id=("STRING", config.pipeline_id),
    )).result()


def run(client, source, config, now=None):
    now = now or datetime.now(UTC)
    start, end = get_window(client, config, now)
    source.validate_schema()
    stage = f"{config.project}.{config.dataset}._submissions_stage_{uuid.uuid4().hex}"
    row_count = 0
    stage_created = False
    try:
        # Disk-backed NDJSON bounds memory even during the initial history load.
        with tempfile.TemporaryFile(mode="w+b") as spool:
            for page in source.pages(start, end):
                for row in page:
                    spool.write((json.dumps(transform(row, config, now)) + "\n").encode("utf-8"))
                    row_count += 1
            if row_count:
                table = bigquery.Table(stage, schema=SCHEMA)
                table.expires = datetime.now(UTC) + timedelta(hours=24)
                client.create_table(table)
                stage_created = True
                spool.seek(0)
                client.load_table_from_file(
                    spool, stage,
                    job_config=bigquery.LoadJobConfig(
                        schema=SCHEMA, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ),
                ).result()
            commit_batch(client, config, stage if row_count else None, end, row_count)
    finally:
        if stage_created:
            try:
                client.delete_table(stage, not_found_ok=True)
            except Exception:
                LOGGER.warning("Staging cleanup failed; table expires automatically within 24 hours")
    summary = {
        "target": config.target, "rows_fetched": row_count,
        "window_start": timestamp(start) if start else None, "window_end": timestamp(end),
        "initial_load": start is None,
    }
    LOGGER.info(json.dumps(summary))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--init-only", action="store_true", help="Create dataset, tables and checkpoint")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    config = Config.from_env()
    client = bigquery.Client(project=config.project, location=config.location)
    if args.init_only:
        initialize(client, config)
        LOGGER.info("Initialized %s", config.target)
    else:
        source = PloyClient(config, os.environ["PLOY_ACCESS_KEY"])
        run(client, source, config)


if __name__ == "__main__":
    main()
