import base64
import copy
import io
import json
import unittest
from unittest.mock import Mock, patch

import requests

import main


ENV = {"GCP_PROJECT": "fxr-analytics", "PUBSUB_SUBSCRIPTION": "projects/fxr-analytics/subscriptions/data-alerts-roam-push",
       "ROAM_API_KEY": "private-test-key", "ROAM_GROUP_ID": "group-1"}
INCIDENT = {"incident_id": "incident-1", "state": "open", "scoping_project_id": "fxr-analytics",
            "policy_user_labels": {"managed_by": "fx-replay-etl"}, "policy_name": "Job execution failed",
            "summary": "Execution failed", "resource": {"labels": {"job_name": "example-job"}},
            "url": "https://console.cloud.google.com/monitoring/alerting/incidents/incident-1"}


def envelope(incident=None):
    return {"subscription": ENV["PUBSUB_SUBSCRIPTION"], "message": {
        "data": base64.b64encode(json.dumps({"incident": INCIDENT if incident is None else incident}).encode()).decode()}}


def invoke(payload, env=None):
    raw = json.dumps(payload).encode()
    response = Mock()
    with patch.dict(main.os.environ, ENV if env is None else env, clear=True):
        body = b"".join(main.app({"REQUEST_METHOD": "POST", "PATH_INFO": "/", "CONTENT_LENGTH": str(len(raw)),
                                 "wsgi.input": io.BytesIO(raw)}, response))
    return response.call_args.args[0], body


class DeliveryTests(unittest.TestCase):
    @patch.object(main, "send_to_roam")
    def test_acknowledges_only_after_delivery(self, send):
        status, _ = invoke(envelope())
        self.assertEqual(status, "204 No Content")
        send.assert_called_once_with(INCIDENT, ENV["ROAM_API_KEY"], ENV["ROAM_GROUP_ID"])

    @patch.object(main, "send_to_roam", side_effect=requests.Timeout("private-test-key"))
    def test_timeout_requests_redelivery_without_logging_secret(self, send):
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            status, body = invoke(envelope())
        self.assertEqual(status, "503 Service Unavailable")
        self.assertNotIn("private-test-key", output.getvalue() + body.decode())

    @patch.object(main, "send_to_roam")
    def test_secret_manager_trailing_newline_is_removed(self, send):
        status, _ = invoke(envelope(), env={**ENV, "ROAM_API_KEY": ENV["ROAM_API_KEY"] + "\n"})
        self.assertEqual(status, "204 No Content")
        self.assertEqual(send.call_args.args[1], ENV["ROAM_API_KEY"])

    @patch.object(main, "send_to_roam")
    def test_wrong_subscription_is_rejected(self, send):
        data = envelope()
        data["subscription"] = "another-subscription"
        self.assertEqual(invoke(data)[0], "400 Bad Request")
        send.assert_not_called()

    @patch.object(main, "send_to_roam")
    def test_wrong_project_and_unmanaged_policy_are_rejected(self, send):
        for key, value in [("scoping_project_id", "other-project"), ("policy_user_labels", {}), ("state", "unknown")]:
            incident = copy.deepcopy(INCIDENT)
            incident[key] = value
            with self.subTest(key=key):
                self.assertEqual(invoke(envelope(incident))[0], "400 Bad Request")
        send.assert_not_called()

    @patch.object(main, "send_to_roam")
    def test_invalid_base64_is_not_forwarded(self, send):
        data = envelope()
        data["message"]["data"] = "not-base64!"
        self.assertEqual(invoke(data)[0], "400 Bad Request")
        send.assert_not_called()

    @patch.object(main, "send_to_roam")
    def test_missing_credentials_does_not_acknowledge(self, send):
        self.assertEqual(invoke(envelope(), env={})[0], "503 Service Unavailable")
        send.assert_not_called()

    def test_message_contains_context_and_blocks_mentions(self):
        incident = copy.deepcopy(INCIDENT)
        incident["summary"] = "Bad payload <!channel> <@someone>"
        text = main.format_message(incident)
        self.assertIn("example-job", text)
        self.assertIn(INCIDENT["url"], text)
        self.assertNotIn("<!channel>", text)
        self.assertNotIn("<@someone>", text)

    def test_closed_incident_does_not_claim_recovery(self):
        incident = {**INCIDENT, "state": "closed"}
        self.assertIn("El cierre puede ser automatico", main.format_message(incident))

    @patch.object(main.requests, "post")
    def test_sends_to_configured_group_and_stable_thread(self, post):
        post.return_value = Mock(status_code=200, json=lambda: {"ok": True})
        main.send_to_roam(INCIDENT, "test-key", "group-1")
        first = post.call_args.kwargs
        main.send_to_roam({**INCIDENT, "state": "closed"}, "test-key", "group-1")
        self.assertEqual(first["json"]["groupId"], "group-1")
        self.assertEqual(first["json"]["threadKey"], post.call_args.kwargs["json"]["threadKey"])
        self.assertFalse(first["allow_redirects"])
        self.assertEqual(first["headers"]["Roam-Version"], "2026-08-25")

    @patch.object(main.requests, "post")
    def test_http_and_api_failures_are_not_acknowledged(self, post):
        for code, result in [(429, {}), (503, {}), (403, {}), (200, {"ok": False}), (200, [])]:
            post.return_value = Mock(status_code=code, json=lambda: result)
            with self.subTest(code=code, result=result), self.assertRaises(RuntimeError):
                main.send_to_roam(INCIDENT, "test-key", "group-1")


if __name__ == "__main__":
    unittest.main()
