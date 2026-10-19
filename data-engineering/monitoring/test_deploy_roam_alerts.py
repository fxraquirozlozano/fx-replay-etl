import unittest
from unittest.mock import Mock, patch

import deploy_roam_alerts as deploy


GROUP_ID = "2c98090b-d3e0-406f-a2ef-211fbd3539ce"


class PolicyTests(unittest.TestCase):
    def test_all_policy_templates_use_the_selected_channel(self):
        channel = "projects/test-project/notificationChannels/test-channel"
        policies = deploy.policy_documents(channel)
        self.assertEqual(len(policies), 3)
        self.assertEqual(len({p["displayName"] for p in policies}), 3)
        for policy in policies:
            self.assertEqual(policy["notificationChannels"], [channel])
            self.assertTrue(policy["enabled"])
        lag = next(p for p in policies if "lag over" in p["displayName"])
        condition = lag["conditions"][0]["conditionThreshold"]
        self.assertEqual(condition["thresholdValue"], 3600)
        self.assertEqual(condition["duration"], "600s")
        self.assertIn("fxreplay-prod-mysql-bigquery-gtid-20261019", condition["filter"])
        self.assertNotIn("fxreplay-prod-mysql-bigquery-gtid-20261015", condition["filter"])


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


if __name__ == "__main__":
    unittest.main()
