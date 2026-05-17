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
        "SFTP_HOST": "data-accel.chargebee.com",
        "SFTP_PORT": "22",
        "SFTP_USER": "cb-prod-fxreplay-fileshare",
        "SFTP_REMOTE_PATH": "usage_data",
    },
    secret_env_vars={
        "SFTP_PRIVATE_KEY": "chargebee-sftp-private-key",
    },
)
def upload_weekly_usage_events_to_chargebee(event=None):
    """Export weekly Chargebee usage data to GCS and then upload it to SFTP."""
    return event


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "print"
    target_job = sys.argv[2] if len(sys.argv) > 2 else None

    if action == "deploy":
        run_commands(app, target_job)
    else:
        print_commands(app, target_job)
