import base64
import binascii
import hashlib
import json
import os
from urllib.parse import urlparse

import requests


ROAM_URL = "https://api.ro.am/v1/chat.post"
MAX_BODY_BYTES = 1024 * 1024


def log(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def decode_incident(envelope, project, subscription):
    if not isinstance(envelope, dict) or envelope.get("subscription") != subscription:
        raise ValueError("Unexpected Pub/Sub subscription")
    message = envelope.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("data"), str):
        raise ValueError("Missing Pub/Sub data")
    payload = json.loads(base64.b64decode(message["data"], validate=True))
    incident = payload.get("incident") if isinstance(payload, dict) else None
    if not isinstance(incident, dict):
        raise ValueError("Missing Monitoring incident")
    if incident.get("scoping_project_id") != project:
        raise ValueError("Unexpected Monitoring project")
    labels = incident.get("policy_user_labels", {})
    if not isinstance(labels, dict) or labels.get("managed_by") != "fx-replay-etl":
        raise ValueError("Unexpected Monitoring policy")
    if not isinstance(incident.get("incident_id"), str) or not incident["incident_id"]:
        raise ValueError("Missing incident identifier")
    if incident.get("state") not in {"open", "closed"}:
        raise ValueError("Unexpected incident state")
    resource = incident.get("resource") or {}
    if not isinstance(resource, dict) or not isinstance(resource.get("labels") or {}, dict):
        raise ValueError("Invalid incident resource")
    return incident


def safe_text(value, limit=1000):
    # Alert content must not generate Roam mentions from log or resource text.
    return str(value or "").replace("<", "\u2039").replace(">", "\u203a")[:limit]


def format_message(incident):
    state = "ALERTA" if incident["state"] == "open" else "INCIDENTE CERRADO"
    resource = incident.get("resource") or {}
    labels = resource.get("labels") or {}
    name = labels.get("job_name") or labels.get("stream_id") or incident.get("resource_display_name", "")
    lines = [
        f"**{state} | Data pipelines**",
        f"**Regla:** {safe_text(incident.get('policy_name'))}",
        f"**Recurso:** {safe_text(name)}",
        f"**Proyecto:** {safe_text(incident['scoping_project_id'])}",
        safe_text(incident.get("summary"), 2500),
        f"**Incidente:** {safe_text(incident['incident_id'])}",
    ]
    url = str(incident.get("url", ""))
    parsed = urlparse(url)
    if parsed.scheme == "https" and parsed.netloc == "console.cloud.google.com":
        lines.append(url)
    if incident["state"] == "closed":
        lines.append("El cierre puede ser automatico; revisa la ejecucion o el stream para confirmar recuperacion.")
    return "\n\n".join(lines)


def send_to_roam(incident, api_key, group_id):
    # Stable threads group retries and close notifications, without claiming
    # exactly-once delivery: Pub/Sub can deliver a message more than once.
    thread_key = hashlib.sha256(
        (incident["scoping_project_id"] + ":" + incident["incident_id"]).encode()
    ).hexdigest()
    response = requests.post(
        ROAM_URL,
        headers={"Authorization": f"Bearer {api_key}", "Roam-Version": "2026-08-25"},
        json={"groupId": group_id, "threadKey": thread_key,
              "text": format_message(incident), "sync": True},
        timeout=(5, 25),
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Roam HTTP {response.status_code}")
    result = response.json()
    if not isinstance(result, dict) or result.get("ok") is not True:
        raise RuntimeError("Roam did not acknowledge the message")


def app(environ, start_response):
    # Cloud Run IAM authenticates the dedicated Pub/Sub push service account.
    # Deploy this service with --no-allow-unauthenticated.
    status, body = "204 No Content", b""
    if environ.get("REQUEST_METHOD") != "POST" or environ.get("PATH_INFO") != "/":
        status, body = "404 Not Found", b"Not found"
    else:
        try:
            project = os.environ["GCP_PROJECT"]
            subscription = os.environ["PUBSUB_SUBSCRIPTION"]
            api_key = os.environ["ROAM_API_KEY"].strip()
            group_id = os.environ["ROAM_GROUP_ID"].strip()
            if not all((project, subscription, api_key, group_id)):
                raise KeyError("Missing runtime configuration")
        except KeyError:
            log("configuration_error", severity="ERROR")
            start_response("503 Service Unavailable", [("Content-Type", "text/plain")])
            return [b"Configuration unavailable"]
        try:
            length = int(environ.get("CONTENT_LENGTH") or 0)
            if not 0 < length <= MAX_BODY_BYTES:
                raise ValueError("Invalid request length")
            envelope = json.loads(environ["wsgi.input"].read(length))
            incident = decode_incident(envelope, project, subscription)
        except (ValueError, TypeError, binascii.Error, UnicodeError):
            log("invalid_alert_payload", severity="WARNING")
            status, body = "400 Bad Request", b"Invalid alert payload"
        else:
            try:
                send_to_roam(incident, api_key, group_id)
            except (requests.RequestException, RuntimeError, ValueError) as error:
                # Do not log headers, credentials, response bodies or full payloads.
                log("roam_delivery_failed", severity="ERROR", incident_id=incident["incident_id"],
                    error_type=type(error).__name__)
                status, body = "503 Service Unavailable", b"Delivery failed; retry"
            else:
                log("roam_alert_delivered", incident_id=incident["incident_id"], state=incident["state"])
    start_response(status, [("Content-Type", "text/plain")])
    return [body]
