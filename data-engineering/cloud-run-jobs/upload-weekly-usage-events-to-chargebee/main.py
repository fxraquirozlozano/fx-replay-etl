import os
from io import BytesIO, StringIO
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
SFTP_HOST = os.getenv("SFTP_HOST", "")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER", "")
SFTP_REMOTE_PATH = os.getenv("SFTP_REMOTE_PATH", "usage_data")
SFTP_PRIVATE_KEY = os.getenv("SFTP_PRIVATE_KEY", "")
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


def main() -> None:
    start_date, end_date = weekly_window()
    destination_uri = build_destination_uri(start_date, end_date)
    sql = build_export_sql(destination_uri, start_date, end_date)

    client = bigquery.Client(project=PROJECT_ID)
    job = client.query(sql)
    job.result()
    uploaded_files = upload_exports_to_sftp(destination_uri)

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
        },
    )


if __name__ == "__main__":
    main()
