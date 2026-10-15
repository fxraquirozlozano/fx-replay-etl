import unittest
from unittest.mock import Mock, patch

import main


class CustomerUpdateTest(unittest.TestCase):
    def test_source_hash_is_stable(self):
        first = main.CustomerUpdate("customer-1", "native_beginner", "Beginner")
        second = main.CustomerUpdate("customer-1", "native_beginner", "Beginner")

        self.assertEqual(first.source_hash, second.source_hash)

    def test_source_hash_changes_with_payload(self):
        first = main.CustomerUpdate("customer-1", "native_beginner", "Beginner")
        second = main.CustomerUpdate(
            "customer-1", "former_paid_to_beginner", "Beginner"
        )

        self.assertNotEqual(first.source_hash, second.source_hash)


class ChargebeeClientTest(unittest.TestCase):
    def test_sends_both_customer_custom_fields(self):
        update = main.CustomerUpdate(
            "customer/1", "former_paid_to_beginner", "Beginner"
        )
        response = Mock(status_code=200)
        session = Mock()
        session.post.return_value = response
        client = main.ChargebeeClient()
        client._thread_local.session = session

        with (
            patch.object(main, "CHARGEBEE_SITE", "example-test"),
            patch.object(main, "CHARGEBEE_API_KEY", "secret"),
            patch.object(client._limiter, "wait"),
        ):
            result = client.update_customer(update, "run-1")

        self.assertTrue(result.succeeded)
        request = session.post.call_args
        self.assertEqual(
            request.args[0],
            "https://example-test.chargebee.com/api/v2/customers/customer%2F1",
        )
        self.assertEqual(
            request.kwargs["data"],
            {
                "cf_Beginner_Category": "former_paid_to_beginner",
                "cf_Plan_Type": "Beginner",
            },
        )
        self.assertEqual(request.kwargs["auth"], ("secret", ""))
        self.assertIn("chargebee-idempotency-key", request.kwargs["headers"])

    def test_does_not_retry_non_retryable_response(self):
        update = main.CustomerUpdate("customer-1", "unknown", "Beginner")
        response = Mock(status_code=400, text="invalid custom field")
        session = Mock()
        session.post.return_value = response
        client = main.ChargebeeClient()
        client._thread_local.session = session

        with patch.object(client._limiter, "wait"):
            result = client.update_customer(update, "run-1")

        self.assertFalse(result.succeeded)
        self.assertEqual(result.status_code, 400)
        self.assertEqual(session.post.call_count, 1)


class BigQueryStoreTest(unittest.TestCase):
    def test_refresh_query_uses_validated_source_columns_and_active_customers(self):
        freshness_job = Mock()
        freshness_job.result.return_value = [{"is_current": False}]
        refresh_job = Mock()
        refresh_job.result.return_value = []
        bq_client = Mock()
        bq_client.query.side_effect = [freshness_job, refresh_job]
        bq_client.get_table.return_value.num_rows = 100
        store = main.BigQueryStore(bq_client)

        row_count = store.refresh_source_snapshot()

        self.assertEqual(row_count, 100)
        sql = bq_client.query.call_args.args[0]
        self.assertIn("beginner_history_segment", sql)
        self.assertIn("last_subscription_plan_type", sql)
        self.assertIn("NOT COALESCE(chargebee.deleted, FALSE)", sql)

    def test_pending_query_uses_snapshot_and_sync_version(self):
        query_job = Mock()
        query_job.result.return_value = [
            {
                "customer_id": "customer-1",
                "beginner_category": "native_beginner",
                "plan_type": "Beginner",
            }
        ]
        bq_client = Mock()
        bq_client.query.return_value = query_job
        store = main.BigQueryStore(bq_client)

        updates = list(store.pending_updates())

        self.assertEqual(
            updates,
            [main.CustomerUpdate("customer-1", "native_beginner", "Beginner")],
        )
        sql = bq_client.query.call_args.args[0]
        self.assertIn("chargebee_customer_plan_fields_source_snapshot", sql)
        self.assertIn("state.sync_version IS DISTINCT FROM", sql)


class SyncUpdatesTest(unittest.TestCase):
    def test_preserves_success_and_failure_results(self):
        updates = [
            main.CustomerUpdate("customer-1", "native_beginner", "Beginner"),
            main.CustomerUpdate("customer-2", "not_applicable", "Premium"),
        ]
        client = Mock()
        client.update_customer.side_effect = [
            main.SyncResult(updates[0], True, status_code=200),
            main.SyncResult(updates[1], False, status_code=429, error="limited"),
        ]

        with patch.object(main, "HTTP_CONCURRENCY", 2):
            results = main.sync_updates(updates, client, "run-1")

        self.assertEqual(len(results), 2)
        self.assertEqual(sum(result.succeeded for result in results), 1)


if __name__ == "__main__":
    unittest.main()
