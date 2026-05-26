import os
import csv
import gzip
import json
import tempfile
from io import BytesIO, StringIO, TextIOWrapper
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
EXPORT_START_DATE = os.getenv("EXPORT_START_DATE", "").strip()
EXPORT_END_DATE = os.getenv("EXPORT_END_DATE", "").strip()
SFTP_HOST = os.getenv("SFTP_HOST", "").strip()
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "").strip()
SFTP_REMOTE_PATH = os.getenv("SFTP_REMOTE_PATH", "usage_data")
SFTP_PRIVATE_KEY = os.getenv("SFTP_PRIVATE_KEY", "").strip()
SFTP_BATCH_TIME_ZONE = os.getenv("SFTP_BATCH_TIME_ZONE", "UTC").strip()
ROWS_PER_OUTPUT_FILE = int(os.getenv("ROWS_PER_OUTPUT_FILE", "250000"))
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


def export_window() -> tuple[str, str]:
    if EXPORT_START_DATE and EXPORT_END_DATE:
        return EXPORT_START_DATE, EXPORT_END_DATE
    return weekly_window()


def build_destination_uri(start_date: str, end_date: str) -> str:
    extension = "csv.gz" if EXPORT_FORMAT == "CSV" else "parquet"
    return (
        f"gs://{BUCKET}/{PREFIX}/week_start={start_date}/week_end={end_date}/"
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

    return f"""
    EXPORT DATA
    OPTIONS (
      {", ".join(options)}
    ) AS
    SELECT
      {selected_columns}
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE DATE({event_ts}, '{EXPORT_TIME_ZONE}') >= DATE('{start_date}')
      AND DATE({event_ts}, '{EXPORT_TIME_ZONE}') < DATE('{end_date}')
    """


def list_exported_blobs(bucket_name: str, prefix: str) -> list[storage.Blob]:
    client = storage.Client(project=PROJECT_ID)
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    return [blob for blob in blobs if not blob.name.endswith("/")]


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
            buffer = BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            sftp.putfo(buffer, remote_file)
            uploaded_files.append(remote_file)
    finally:
        sftp.close()
        transport.close()

    return uploaded_files


def batch_id() -> str:
    return datetime.now(ZoneInfo(SFTP_BATCH_TIME_ZONE)).strftime("%Y-%m-%d-%H-%M-%S")


def iter_export_rows(blobs: list[storage.Blob]):
    for blob in sorted(blobs, key=lambda item: item.name):
        if blob.name.endswith("/"):
            continue

        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)

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


def prepare_batch_files(destination_uri: str) -> tuple[str, list[tempfile.NamedTemporaryFile], dict]:
    bucket_prefix = destination_uri.removeprefix("gs://")
    bucket_name, blob_pattern = bucket_prefix.split("/", 1)
    blob_prefix = blob_pattern.rsplit("/", 1)[0] + "/"
    blobs = list_exported_blobs(bucket_name, blob_prefix)

    current_batch_id = batch_id()
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
    return current_batch_id, output_files, metadata


def upload_batch_to_sftp(destination_uri: str) -> tuple[list[str], dict]:
    if not sftp_enabled():
        raise RuntimeError(
            "SFTP upload is not configured. Set SFTP_HOST, SFTP_USER, and "
            "SFTP_PRIVATE_KEY."
        )

    current_batch_id, part_files, metadata = prepare_batch_files(destination_uri)
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
    start_date, end_date = export_window()
    destination_uri = build_destination_uri(start_date, end_date)
    sql = build_export_sql(destination_uri, start_date, end_date)

    client = bigquery.Client(project=PROJECT_ID)
    job = client.query(sql)
    job.result()
    uploaded_files, metadata = upload_batch_to_sftp(destination_uri)

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
            "sftp_host": SFTP_HOST,
            "sftp_remote_path": SFTP_REMOTE_PATH,
            "uploaded_files": len(uploaded_files),
            "batch_id": metadata["batch_id"],
            "expected_file_count": metadata["expected_file_count"],
        },
    )


if __name__ == "__main__":
    main()
