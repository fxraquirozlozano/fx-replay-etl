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


if __name__ == "__main__":
    unittest.main()
