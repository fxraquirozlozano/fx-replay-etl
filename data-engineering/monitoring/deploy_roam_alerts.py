import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import requests


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
PROJECT = "fxr-analytics"
REGION = "us-central1"
SERVICE = "forward-data-alerts-to-roam"
TOPIC = "data-alerts-roam"
SUBSCRIPTION = "data-alerts-roam-push"
CHANNEL_NAME = "Data Alerts - Roam via PubSub"
SECRET = "FXR_ROAM_API_KEY"
GCLOUD = shutil.which("gcloud.cmd") if os.name == "nt" else shutil.which("gcloud")


def gcloud(*args):
    if not GCLOUD:
        raise RuntimeError("Google Cloud CLI is required")
    result = subprocess.run(
        [GCLOUD, *args, f"--project={PROJECT}", "--quiet"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode:
        # Never include captured output: secret access commands can return secrets.
        raise RuntimeError(f"gcloud {' '.join(args[:2])} failed; check authentication and IAM permissions")
    return result.stdout.strip()


def google_api(method, path, body=None):
    token = gcloud("auth", "print-access-token")
    response = requests.request(
        method, "https://monitoring.googleapis.com/v3/" + path,
        headers={"Authorization": f"Bearer {token}"}, json=body, timeout=60,
    )
    if not response.ok:
        error = response.json().get("error", {}).get("message", "Request failed")
        raise RuntimeError(f"Monitoring {method} failed: HTTP {response.status_code}: {error}")
    return response.json()


def list_monitoring(collection):
    result, page = [], ""
    while True:
        suffix = f"?pageToken={page}" if page else ""
        data = google_api("GET", f"projects/{PROJECT}/{collection}{suffix}")
        result.extend(data.get(collection, []))
        page = data.get("nextPageToken", "")
        if not page:
            return result


def resolve_group(api_key, group_id):
    headers = {"Authorization": f"Bearer {api_key}", "Roam-Version": "2026-08-25"}
    groups, cursor = [], ""
    if group_id:
        uuid.UUID(group_id)
        response = requests.get("https://api.ro.am/v1/group.info", headers=headers,
                                params={"id": group_id}, timeout=30)
        if not response.ok:
            raise RuntimeError(f"Cannot verify Roam group: HTTP {response.status_code}")
        groups = [response.json()]
    else:
        while True:
            response = requests.get("https://api.ro.am/v1/group.list", headers=headers,
                                    params={"query": "Data Alerts", "limit": 100, "cursor": cursor}, timeout=30)
            if not response.ok:
                raise RuntimeError(f"Cannot find Roam Data Alerts: HTTP {response.status_code}; group:read access is required")
            data = response.json()
            groups.extend(data.get("groups", []))
            cursor = data.get("nextCursor", "")
            if not cursor:
                break
    matches = [g for g in groups if re.sub(r"[\s_-]+", " ", g.get("name", "").lower()).strip() == "data alerts"]
    if len(matches) != 1:
        raise RuntimeError("Expected exactly one accessible Roam group named Data Alerts")
    return str(uuid.UUID(matches[0]["id"]))


def ensure_account(name):
    email = f"{name}@{PROJECT}.iam.gserviceaccount.com"
    existing = json.loads(gcloud("iam", "service-accounts", "list", "--format=json"))
    if not any(item["email"] == email for item in existing):
        gcloud("iam", "service-accounts", "create", name)
    return email


def policy_documents(channel):
    policies = []
    for path in sorted(HERE.glob("*.template.json")):
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["notificationChannels"] = [channel]
        policies.append(policy)
    if len(policies) != 2:
        raise RuntimeError("Expected the Cloud Run and Datastream alert templates")
    return policies


def deploy(group_id, send_test):
    # All read-only preflight checks happen before provisioning.
    gcloud("auth", "print-access-token")
    group_id = resolve_group(gcloud("secrets", "versions", "access", "latest", f"--secret={SECRET}"), group_id)
    print("Verified Roam destination: Data Alerts")
    number = gcloud("projects", "describe", PROJECT, "--format=value(projectNumber)")
    gcloud("services", "enable", "run.googleapis.com", "cloudbuild.googleapis.com",
           "artifactregistry.googleapis.com", "pubsub.googleapis.com", "monitoring.googleapis.com",
           "logging.googleapis.com", "secretmanager.googleapis.com")
    runtime = ensure_account("data-alerts-roam-runtime")
    push = ensure_account("data-alerts-roam-push")
    gcloud("secrets", "add-iam-policy-binding", SECRET,
           f"--member=serviceAccount:{runtime}", "--role=roles/secretmanager.secretAccessor")
    topic_name = f"projects/{PROJECT}/topics/{TOPIC}"
    topics = json.loads(gcloud("pubsub", "topics", "list", "--format=json"))
    if not any(t["name"] == topic_name for t in topics):
        gcloud("pubsub", "topics", "create", TOPIC)
    subscription_name = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"
    env = f"GCP_PROJECT={PROJECT},ROAM_GROUP_ID={group_id},PUBSUB_SUBSCRIPTION={subscription_name}"
    print("Deploying authenticated Roam delivery service")
    gcloud("run", "deploy", SERVICE, f"--region={REGION}",
           f"--source={ROOT / 'data-engineering' / 'cloud-run-services' / SERVICE}",
           "--no-allow-unauthenticated", f"--service-account={runtime}",
           "--memory=256Mi", "--cpu=1", "--min-instances=0", "--max-instances=2",
           "--concurrency=8", "--timeout=60s", f"--set-env-vars={env}",
           f"--set-secrets=ROAM_API_KEY={SECRET}:latest")
    url = gcloud("run", "services", "describe", SERVICE, f"--region={REGION}", "--format=value(status.url)")
    gcloud("run", "services", "add-iam-policy-binding", SERVICE, f"--region={REGION}",
           f"--member=serviceAccount:{push}", "--role=roles/run.invoker")
    gcloud("iam", "service-accounts", "add-iam-policy-binding", push,
           f"--member=serviceAccount:service-{number}@gcp-sa-pubsub.iam.gserviceaccount.com",
           "--role=roles/iam.serviceAccountTokenCreator")
    subscriptions = json.loads(gcloud("pubsub", "subscriptions", "list", "--format=json"))
    existing = next((s for s in subscriptions if s["name"] == subscription_name), None)
    if existing and existing["topic"] != topic_name:
        raise RuntimeError("Existing subscription points to another topic")
    sub_args = ["pubsub", "subscriptions", "update" if existing else "create", SUBSCRIPTION]
    if not existing:
        sub_args.append(f"--topic={TOPIC}")
    gcloud(*sub_args, f"--push-endpoint={url}/", f"--push-auth-service-account={push}",
           f"--push-auth-token-audience={url}", "--ack-deadline=60",
           "--min-retry-delay=10s", "--max-retry-delay=600s", "--message-retention-duration=7d")
    channels = list_monitoring("notificationChannels")
    matches = [c for c in channels if c.get("type") == "pubsub" and c.get("labels", {}).get("topic") == topic_name]
    if len(matches) > 1:
        raise RuntimeError("Multiple notification channels target the alerts topic")
    if matches:
        channel = matches[0]
        if channel.get("enabled") is False:
            raise RuntimeError("Existing Roam notification channel is disabled")
    else:
        channel = google_api("POST", f"projects/{PROJECT}/notificationChannels", {
            "type": "pubsub", "displayName": CHANNEL_NAME,
            "labels": {"topic": topic_name}, "enabled": True,
        })
    gcloud("pubsub", "topics", "add-iam-policy-binding", TOPIC,
           f"--member=serviceAccount:service-{number}@gcp-sa-monitoring-notification.iam.gserviceaccount.com",
           "--role=roles/pubsub.publisher")
    if send_test:
        incident_id = "setup-test-" + uuid.uuid4().hex
        incident = {"incident_id": incident_id, "state": "open", "scoping_project_id": PROJECT,
                    "policy_user_labels": {"managed_by": "fx-replay-etl"},
                    "policy_name": "Prueba de entrega de Data Alerts", "summary": "Prueba de configuracion; no representa un fallo real.",
                    "resource_display_name": "Roam notification delivery"}
        # Pass JSON via a file; no command-line quoting or credentials in payload.
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test-flags.json"
            payload = json.dumps({"version": "1.2", "incident": incident})
            path.write_text(json.dumps({"--message": payload}), encoding="utf-8")
            gcloud("pubsub", "topics", "publish", TOPIC, f"--flags-file={path}")
        print("Test published:", incident_id)
        for attempt in range(60):
            entries = json.loads(gcloud("logging", "read",
                f'resource.type="cloud_run_revision" AND resource.labels.service_name="{SERVICE}" AND jsonPayload.event="roam_alert_delivered" AND jsonPayload.incident_id="{incident_id}"',
                "--freshness=10m", "--limit=1", "--format=json"))
            if entries:
                print("Roam acknowledged the test message")
                break
            if attempt % 6 == 0:
                print("Waiting for authenticated delivery and log ingestion", flush=True)
            time.sleep(5)
        else:
            raise RuntimeError("Test delivery was not confirmed; policies have not been activated")
    existing_policies = list_monitoring("alertPolicies")
    for policy in policy_documents(channel["name"]):
        matches = [p for p in existing_policies if p.get("displayName") == policy["displayName"]]
        if len(matches) > 1:
            raise RuntimeError("Duplicate alert policy names; resolve before updating")
        if matches:
            current = matches[0]
            if current.get("userLabels", {}).get("managed_by") != "fx-replay-etl":
                raise RuntimeError("Refusing to overwrite an unmanaged alert policy")
            policy["name"] = current["name"]
            mask = "displayName,combiner,enabled,notificationChannels,userLabels,conditions,alertStrategy,documentation"
            google_api("PATCH", policy["name"] + "?updateMask=" + mask, policy)
        else:
            google_api("POST", f"projects/{PROJECT}/alertPolicies", policy)
        print("Enabled:", policy["displayName"])


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description="Provision Monitoring -> Pub/Sub -> authenticated Cloud Run -> Roam Data Alerts")
    parser.add_argument("--apply", action="store_true", help="Provision and enable the alerts")
    parser.add_argument("--group-id", help="Verify this Roam group instead of searching by name")
    parser.add_argument("--test", action="store_true", help="Send and verify one labeled setup message before activating policies")
    args = parser.parse_args()
    if args.apply:
        deploy(args.group_id, args.test)
    else:
        print(json.dumps({"project": PROJECT, "region": REGION, "service": SERVICE,
            "topic": TOPIC, "destination": "Roam / Data Alerts", "secret": SECRET,
            "policies": policy_documents("REPLACE_WITH_CREATED_PUBSUB_NOTIFICATION_CHANNEL")}, indent=2))
