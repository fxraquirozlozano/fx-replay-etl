import unittest
from unittest.mock import Mock, patch

import deploy_roam_alerts as deploy


GROUP_ID = "2c98090b-d3e0-406f-a2ef-211fbd3539ce"


class DestinationTests(unittest.TestCase):
    @patch.object(deploy.requests, "get")
    def test_group_id_is_verified_against_name(self, get):
        get.return_value = Mock(ok=True, json=lambda: {"id": GROUP_ID, "name": "Data Alerts"})
        self.assertEqual(deploy.resolve_group("test-key", GROUP_ID), GROUP_ID)
        self.assertEqual(get.call_args.kwargs["params"], {"id": GROUP_ID})

    @patch.object(deploy.requests, "get")
    def test_different_group_is_rejected(self, get):
        get.return_value = Mock(ok=True, json=lambda: {"id": GROUP_ID, "name": "Finance"})
        with self.assertRaises(RuntimeError):
            deploy.resolve_group("test-key", GROUP_ID)

    @patch.object(deploy.requests, "get")
    def test_duplicate_names_across_pages_are_rejected(self, get):
        get.side_effect = [
            Mock(ok=True, json=lambda: {"groups": [{"id": GROUP_ID, "name": "Data Alerts"}], "nextCursor": "page2"}),
            Mock(ok=True, json=lambda: {"groups": [{"id": "another-id", "name": "Data Alerts"}]}),
        ]
        with self.assertRaises(RuntimeError):
            deploy.resolve_group("test-key", None)

    @patch.object(deploy.requests, "get")
    def test_search_rejects_similar_names(self, get):
        get.return_value = Mock(ok=True, json=lambda: {"groups": [{"id": GROUP_ID, "name": "Data Alerts Archive"}]})
        with self.assertRaises(RuntimeError):
            deploy.resolve_group("test-key", None)


class PolicyScopeTests(unittest.TestCase):
    def test_only_cdc_is_configured(self):
        policies = deploy.policy_documents("test-channel")
        self.assertEqual(len(policies), 1)
        self.assertIn("Datastream", policies[0]["displayName"])
        self.assertIn("conditionMatchedLog", policies[0]["conditions"][0])

    @patch.object(deploy, "google_api")
    def test_retirement_leaves_other_job_policies_untouched(self, api):
        old = {"name": "projects/fxr-analytics/alertPolicies/11189439487452597550",
               "displayName": "Data Alerts - Cloud Run job execution failed",
               "userLabels": {"managed_by": "fx-replay-etl"}, "enabled": True}
        unrelated = {**old, "name": "projects/fxr-analytics/alertPolicies/another"}
        deploy.disable_legacy_job_alert([unrelated, old])
        api.assert_called_once_with("PATCH", old["name"] + "?updateMask=enabled",
                                    {"name": old["name"], "enabled": False})


if __name__ == "__main__":
    unittest.main()
