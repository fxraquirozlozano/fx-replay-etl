import os
import csv
import gzip
import json
import tempfile
from io import BytesIO, StringIO, TextIOWrapper
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
import paramiko


PROJECT_ID = os.getenv("GCP_PROJECT", "fxr-analytics")
DATASET = os.getenv("BQ_DATASET", "sandbox")
TABLE = os.getenv("BQ_TABLE", "tracking_events_chargebee")
BUCKET = os.getenv("EXPORT_BUCKET", "fxr-chargebee-exports")
PREFIX = os.getenv("EXPORT_PREFIX", "tracking_events_chargebee")
EXPORT_FORMAT = os.getenv("EXPORT_FORMAT", "CSV").upper()
COMPRESSION = os.getenv("EXPORT_COMPRESSION", "GZIP").upper()
EXPORT_TIME_ZONE = os.getenv("EXPORT_TIME_ZONE", "America/Chicago")
SFTP_HOST = os.getenv("SFTP_HOST", "").strip()
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "").strip()
SFTP_REMOTE_PATH = os.getenv("SFTP_REMOTE_PATH", "usage_data")
SFTP_PRIVATE_KEY = os.getenv("SFTP_PRIVATE_KEY", "").strip()
SFTP_BATCH_TIME_ZONE = os.getenv("SFTP_BATCH_TIME_ZONE", "UTC").strip()
SFTP_BATCH_ID_OVERRIDE = os.getenv("SFTP_BATCH_ID_OVERRIDE", "").strip()
ROWS_PER_OUTPUT_FILE = int(os.getenv("ROWS_PER_OUTPUT_FILE", "250000"))
MIN_ROWS_PER_DAY = int(os.getenv("MIN_ROWS_PER_DAY", "100000"))

if MIN_ROWS_PER_DAY < 0:
    raise ValueError("MIN_ROWS_PER_DAY cannot be negative")

EXPORT_COLUMNS = [
    "subscription_id",
    "subscription_plan",
    "timestamp",
    "event_type",
    "customer_id",
    "user_id",
    "is_session_created",
    "is_quick_buy_sell",
    "is_partials_taken",
    "is_position_managed",
    "is_replay_activated",
    "is_news_visibility_clicked",
    "is_order_placed",
    "created_session_type",
    "quick_buy_sell_type",
    "partials_taken_percentage",
    "partials_taken_position_amount_type",
    "position_managed_type",
    "order_has_auto_be",
    "order_has_object_selected",
    "order_has_stop_loss",
    "order_tag_count",
    "order_has_tag_selected",
    "order_has_take_profit",
    "order_take_profit_type",
    "order_save_type",
]


def weekly_window() -> tuple[str, str]:
    local_now = datetime.now(ZoneInfo(EXPORT_TIME_ZONE))
    end_date = local_now.date() - timedelta(days=(local_now.weekday() - 1) % 7)
    start_date = end_date - timedelta(days=7)
    return start_date.isoformat(), end_date.isoformat()


def build_destination_uri(start_date: str, end_date: str, current_batch_id: str) -> str:
    extension = "csv.gz" if EXPORT_FORMAT == "CSV" else "parquet"
    return (
        f"gs://{BUCKET}/{PREFIX}/week_start={start_date}/week_end={end_date}/"
        f"batch_id={current_batch_id}/"
        f"tracking_events_chargebee-*.{extension}"
    )


def build_export_sql(destination_uri: str, start_date: str, end_date: str) -> str:
    options = [
        f"uri='{destination_uri}'",
        f"format='{EXPORT_FORMAT}'",
        "overwrite=true",
    ]

    if EXPORT_FORMAT == "CSV":
        options.append("header=true")
        options.append(f"compression='{COMPRESSION}'")
    elif EXPORT_FORMAT == "PARQUET":
        options.append("compression='SNAPPY'")

    event_ts = """
    CASE
      WHEN timestamp IS NULL THEN NULL
      WHEN timestamp >= 1000000000000 THEN TIMESTAMP_MILLIS(timestamp)
      ELSE TIMESTAMP_SECONDS(timestamp)
    END
    """
    selected_columns = ",\n      ".join(EXPORT_COLUMNS)

    start_ts = f"{start_date} 00:00:00+00"
    end_ts = f"{end_date} 00:00:00+00"

    return f"""
    EXPORT DATA
    OPTIONS (
      {", ".join(options)}
    ) AS
    SELECT
      {selected_columns}
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {event_ts} >= TIMESTAMP('{start_ts}')
      AND {event_ts} < TIMESTAMP('{end_ts}')
    """


def validate_export_window(
    client: bigquery.Client,
    start_date: str,
    end_date: str,
) -> dict[str, int]:
    event_ts = """
    CASE
      WHEN timestamp IS NULL THEN NULL
      WHEN timestamp >= 1000000000000 THEN TIMESTAMP_MILLIS(timestamp)
      ELSE TIMESTAMP_SECONDS(timestamp)
    END
    """
    query = f"""
    WITH expected_dates AS (
      SELECT event_date
      FROM UNNEST(
        GENERATE_DATE_ARRAY(
          DATE(@start_date),
          DATE_SUB(DATE(@end_date), INTERVAL 1 DAY)
        )
      ) AS event_date
    ),
    actual_counts AS (
      SELECT DATE({event_ts}, 'UTC') AS event_date, COUNT(*) AS row_count
      FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
      WHERE {event_ts} >= TIMESTAMP(@start_date)
        AND {event_ts} < TIMESTAMP(@end_date)
      GROUP BY event_date
    )
    SELECT expected_dates.event_date, COALESCE(actual_counts.row_count, 0) AS row_count
    FROM expected_dates
    LEFT JOIN actual_counts USING (event_date)
    ORDER BY event_date
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    rows = list(client.query(query, job_config=config).result())
    rows_by_date = {
        row["event_date"].isoformat(): row["row_count"]
        for row in rows
    }
    low_volume_days = {
        event_date: row_count
        for event_date, row_count in rows_by_date.items()
        if row_count < MIN_ROWS_PER_DAY
    }
    if MIN_ROWS_PER_DAY > 0 and low_volume_days:
        raise RuntimeError(
            "Weekly export blocked because one or more days are below "
            f"MIN_ROWS_PER_DAY={MIN_ROWS_PER_DAY}: {low_volume_days}"
        )
    return rows_by_date


def list_exported_blobs(bucket_name: str, prefix: str) -> list[storage.Blob]:
    client = storage.Client(project=PROJECT_ID)
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    return [blob for blob in blobs if not blob.name.endswith("/")]


def download_blob_to_buffer(blob: storage.Blob) -> BytesIO:
    buffer = BytesIO()
    try:
        blob.download_to_file(buffer)
    except NotFound:
        # EXPORT DATA can replace a listed object generation while finalizing
        # shards. Resolve the same object name again without pinning the stale
        # generation instead of restarting the entire Cloud Run execution.
        buffer.seek(0)
        buffer.truncate(0)
        latest_blob = blob.bucket.blob(blob.name)
        latest_blob.download_to_file(buffer)
    buffer.seek(0)
    return buffer


def sftp_enabled() -> bool:
    return all([SFTP_HOST, SFTP_USER, SFTP_PRIVATE_KEY])


def sftp_client() -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    private_key = paramiko.Ed25519Key.from_private_key(StringIO(SFTP_PRIVATE_KEY))
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, pkey=private_key)
    return paramiko.SFTPClient.from_transport(transport), transport


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    current = ""
    for part in remote_path.strip("/").split("/"):
        current = f"{current}/{part}" if current else f"/{part}"
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def upload_exports_to_sftp(destination_uri: str) -> list[str]:
    if not sftp_enabled():
        raise RuntimeError(
            "SFTP upload is not configured. Set SFTP_HOST, SFTP_USER, and "
            "SFTP_PRIVATE_KEY."
        )

    bucket_prefix = destination_uri.removeprefix("gs://")
    bucket_name, blob_pattern = bucket_prefix.split("/", 1)
    blob_prefix = blob_pattern.rsplit("/", 1)[0] + "/"
    blobs = list_exported_blobs(bucket_name, blob_prefix)

    uploaded_files: list[str] = []
    sftp, transport = sftp_client()
    try:
        ensure_remote_dir(sftp, SFTP_REMOTE_PATH)
        for blob in blobs:
            remote_file = f"{SFTP_REMOTE_PATH.rstrip('/')}/{blob.name.rsplit('/', 1)[-1]}"
            buffer = download_blob_to_buffer(blob)
            sftp.putfo(buffer, remote_file)
            uploaded_files.append(remote_file)
    finally:
        sftp.close()
        transport.close()

    return uploaded_files


def batch_id() -> str:
    value = SFTP_BATCH_ID_OVERRIDE or datetime.now(
        ZoneInfo(SFTP_BATCH_TIME_ZONE)
    ).strftime("%Y-%m-%d-%H-%M-%S")
    try:
        datetime.strptime(value, "%Y-%m-%d-%H-%M-%S")
    except ValueError as exc:
        raise ValueError(
            "SFTP_BATCH_ID_OVERRIDE must use YYYY-MM-DD-HH-MM-SS"
        ) from exc
    return value


def iter_export_rows(blobs: list[storage.Blob]):
    for blob in sorted(blobs, key=lambda item: item.name):
        if blob.name.endswith("/"):
            continue

        buffer = download_blob_to_buffer(blob)

        stream = gzip.GzipFile(fileobj=buffer, mode="rb") if blob.name.endswith(".gz") else buffer
        text_stream = TextIOWrapper(stream, encoding="utf-8", newline="")
        reader = csv.reader(text_stream)

        try:
            header = next(reader, None)
            if header is None:
                continue

            for row in reader:
                if any(cell not in ("", None) for cell in row):
                    yield row
        finally:
            text_stream.detach()
            if hasattr(stream, "close"):
                stream.close()


def prepare_batch_files(
    destination_uri: str,
    current_batch_id: str,
) -> tuple[list[tempfile.NamedTemporaryFile], dict]:
    bucket_prefix = destination_uri.removeprefix("gs://")
    bucket_name, blob_pattern = bucket_prefix.split("/", 1)
    blob_prefix = blob_pattern.rsplit("/", 1)[0] + "/"
    blobs = list_exported_blobs(bucket_name, blob_prefix)

    output_files: list[tempfile.NamedTemporaryFile] = []
    expected_file_names: list[str] = []
    row_counts: dict[str, int] = {}
    current_file = None
    current_file_name = None
    current_writer = None
    current_row_count = 0
    part_number = 0
    min_event_timestamp = None
    max_event_timestamp = None

    def open_next_part():
        nonlocal current_file, current_file_name, current_writer, current_row_count, part_number
        if current_file is not None:
            current_file.flush()
            current_file.seek(0)
            output_files.append(current_file)
            row_counts[current_file_name] = current_row_count

        part_number += 1
        file_name = f"usage_events_part_{part_number:03d}.csv"
        expected_file_names.append(file_name)
        current_file_name = file_name
        current_file = tempfile.NamedTemporaryFile(mode="w+", newline="", suffix=".csv", delete=False)
        current_writer = csv.writer(current_file)
        current_writer.writerow(EXPORT_COLUMNS)
        current_row_count = 0

    for row in iter_export_rows(blobs):
        if current_file is None or current_row_count >= ROWS_PER_OUTPUT_FILE:
            open_next_part()
        current_writer.writerow(row)
        current_row_count += 1

        event_timestamp = int(row[2]) if row[2] not in ("", None) else None
        if event_timestamp is not None:
            min_event_timestamp = (
                event_timestamp
                if min_event_timestamp is None
                else min(min_event_timestamp, event_timestamp)
            )
            max_event_timestamp = (
                event_timestamp
                if max_event_timestamp is None
                else max(max_event_timestamp, event_timestamp)
            )

    if current_file is not None:
        current_file.flush()
        current_file.seek(0)
        output_files.append(current_file)
        row_counts[current_file_name] = current_row_count

    metadata = {
        "batch_id": current_batch_id,
        "expected_file_count": len(expected_file_names),
        "expected_file_names": expected_file_names,
        "row_counts": row_counts,
        "min_event_timestamp": min_event_timestamp,
        "max_event_timestamp": max_event_timestamp,
    }
    return output_files, metadata


def upload_batch_to_sftp(
    destination_uri: str,
    current_batch_id: str,
) -> tuple[list[str], dict]:
    if not sftp_enabled():
        raise RuntimeError(
            "SFTP upload is not configured. Set SFTP_HOST, SFTP_USER, and "
            "SFTP_PRIVATE_KEY."
        )

    part_files, metadata = prepare_batch_files(destination_uri, current_batch_id)
    remote_batch_path = f"{SFTP_REMOTE_PATH.rstrip('/')}/{current_batch_id}"
    uploaded_files: list[str] = []
    sftp, transport = sftp_client()

    try:
        ensure_remote_dir(sftp, remote_batch_path)

        for part_file, file_name in zip(part_files, metadata["expected_file_names"]):
            part_file.seek(0)
            remote_file = f"{remote_batch_path}/{file_name}"
            sftp.putfo(part_file, remote_file)
            uploaded_files.append(remote_file)

        metadata_bytes = BytesIO(json.dumps(metadata, indent=2).encode("utf-8"))
        metadata_remote_file = f"{remote_batch_path}/metadata.json"
        sftp.putfo(metadata_bytes, metadata_remote_file)
        uploaded_files.append(metadata_remote_file)
    finally:
        for part_file in part_files:
            file_name = part_file.name
            part_file.close()
            if os.path.exists(file_name):
                os.unlink(file_name)
        sftp.close()
        transport.close()

    return uploaded_files, metadata


def main() -> None:
    start_date, end_date = weekly_window()
    current_batch_id = batch_id()
    destination_uri = build_destination_uri(start_date, end_date, current_batch_id)
    sql = build_export_sql(destination_uri, start_date, end_date)
    export_start_ts = f"{start_date} 00:00:00+00"
    export_end_ts = f"{end_date} 00:00:00+00"

    client = bigquery.Client(project=PROJECT_ID)
    rows_by_date = validate_export_window(client, start_date, end_date)
    job = client.query(sql)
    job.result()
    uploaded_files, metadata = upload_batch_to_sftp(
        destination_uri, current_batch_id
    )

    print(
        "Export completed",
        {
            "project": PROJECT_ID,
            "source_table": f"{PROJECT_ID}.{DATASET}.{TABLE}",
            "destination_uri": destination_uri,
            "format": EXPORT_FORMAT,
            "time_zone": EXPORT_TIME_ZONE,
            "week_start": start_date,
            "week_end": end_date,
            "export_start_ts": export_start_ts,
            "export_end_ts": export_end_ts,
            "min_rows_per_day": MIN_ROWS_PER_DAY,
            "rows_by_date": rows_by_date,
            "sftp_host": SFTP_HOST,
            "sftp_remote_path": SFTP_REMOTE_PATH,
            "uploaded_files": len(uploaded_files),
            "batch_id": metadata["batch_id"],
            "expected_file_count": metadata["expected_file_count"],
        },
    )


if __name__ == "__main__":
    main()
