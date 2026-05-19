import sys
from pathlib import Path

HELPERS_DIR = Path("data-engineering/cloud-run-jobs").resolve()
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

from helpers import CloudRunApp, PROJECT_ID, print_commands, run_commands


app = CloudRunApp()


@app.job(
    "upload-weekly-usage-events-to-chargebee",
    schedule="0 8 * * 2",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/upload-weekly-usage-events-to-chargebee",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "sandbox",
        "BQ_TABLE": "tracking_events_chargebee",
        "EXPORT_BUCKET": "fxr-chargebee-exports",
        "EXPORT_PREFIX": "tracking_events_chargebee",
        "EXPORT_FORMAT": "CSV",
        "EXPORT_COMPRESSION": "GZIP",
        "EXPORT_TIME_ZONE": "UTC",
        "SFTP_PORT": "22",
        "SFTP_REMOTE_PATH": "usage_data",
    },
    secret_env_vars={
        "SFTP_HOST": "chargebee-sftp-host-runtime",
        "SFTP_USER": "chargebee-sftp-user-runtime",
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def upload_weekly_usage_events_to_chargebee(event=None):
    """Export weekly Chargebee usage data to GCS and then upload it to SFTP."""
    return event


@app.job(
    "load-weekly-chargebee-predictions-from-sftp",
    schedule="0 17 * * 3",
    timezone="UTC",
    source_dir="data-engineering/cloud-run-jobs/load-weekly-chargebee-predictions-from-sftp",
    env_vars={
        "GCP_PROJECT": PROJECT_ID,
        "BQ_DATASET": "chargebee",
        "BQ_TABLE": "chargebee_predictions",
        "RAW_BQ_DATASET": "chargebee_raw",
        "RAW_BQ_TABLE": "chargebee_predictions",
        "RAW_BUCKET": "fxr-chargebee-exports",
        "RAW_PREFIX": "chargebee_predictions_raw",
        "SFTP_PORT": "22",
        "SFTP_ROOT_PATH": "/predictions",
        "SFTP_FILE_GLOB": "predictions_part_*.csv",
        "WRITE_DISPOSITION": "WRITE_APPEND",
    },
    secret_env_vars={
        "SFTP_HOST": "chargebee-sftp-host-runtime",
        "SFTP_USER": "chargebee-sftp-user-runtime",
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def load_weekly_chargebee_predictions_from_sftp(event=None):
    """Fetch the latest Chargebee predictions batch from SFTP and load it into BigQuery."""
    return event


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "print"
    target_job = sys.argv[2] if len(sys.argv) > 2 else None

    if action == "deploy":
        run_commands(app, target_job)
    else:
        print_commands(app, target_job)
