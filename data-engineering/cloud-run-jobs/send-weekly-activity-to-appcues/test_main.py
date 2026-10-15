import json
import unittest
from unittest.mock import Mock, patch

import main


class CountryProfileTest(unittest.TestCase):
    def test_country_fields_are_not_duplicated_as_event_attributes(self):
        normalized = main.normalize_row(
            {
                "user_id": "user-123",
                "event_name": "weekly_product_activity",
                "event_timestamp": "2026-09-14T00:00:00Z",
                "wpa_tier_week": "WPA10",
                "hit_wpa_week": True,
                "wpa_tier_week_0": "WPA10",
                "country_code": "PE",
                "country_name": "Peru",
            },
            "2026-09-14T00:00:00Z",
        )

        self.assertNotIn("country_code", normalized["attributes"])
        self.assertNotIn("country_name", normalized["attributes"])

    def test_normalizes_country_profile(self):
        profile = main.normalize_country_profile(
            {
                "user_id": "user-123",
                "country_code": "pe",
                "country_name": "Peru",
            }
        )

        self.assertEqual(
            profile,
            {
                "user_id": "user-123",
                "country": "Peru",
                "country_code": "PE",
                "country_name": "Peru",
            },
        )

    def test_falls_back_to_country_code(self):
        profile = main.normalize_country_profile(
            {"user_id": "user-123", "country_code": "us"}
        )

        self.assertEqual(profile["country"], "US")
        self.assertNotIn("country_name", profile)

    def test_skips_row_without_country(self):
        self.assertIsNone(main.normalize_country_profile({"user_id": "user-123"}))

    def test_dry_run_deduplicates_profiles(self):
        rows = [
            {"user_id": "user-123", "country_code": "PE", "country_name": "Peru"},
            {"user_id": "user-123", "country_code": "PE", "country_name": "Peru"},
            {"user_id": "user-456", "country_code": None, "country_name": None},
        ]

        with patch.object(main, "DRY_RUN", True):
            result = main.send_country_profiles(rows)

        self.assertEqual(result["unique_profiles"], 1)
        self.assertEqual(result["skipped_without_country"], 1)
        self.assertEqual(result["would_submit"], 1)


class BulkImportTest(unittest.TestCase):
    def test_chunks_ndjson_without_splitting_rows(self):
        documents = [
            {"user_id": "1", "country": "Peru"},
            {"user_id": "2", "country": "Canada"},
        ]
        max_line_size = max(
            len(json.dumps(document, separators=(",", ":")).encode())
            for document in documents
        )

        with patch.object(main, "APPCUES_IMPORT_CHUNK_BYTES", max_line_size):
            chunks = main.ndjson_chunks(documents)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(json.loads(chunks[0]), documents[0])
        self.assertEqual(json.loads(chunks[1]), documents[1])

    @patch.object(main, "wait_for_appcues_job")
    @patch.object(main, "appcues_request")
    def test_submits_profile_import_and_reports_completion(
        self,
        appcues_request,
        wait_for_appcues_job,
    ):
        response = Mock()
        response.json.return_value = {"job_url": "https://example.test/jobs/1"}
        appcues_request.return_value = response
        wait_for_appcues_job.return_value = {
            "id": "job-1",
            "events": [
                {
                    "name": "Completed",
                    "attributes": {
                        "ingestions_completed": 2,
                        "ingestions_failed": 0,
                        "requests_failed": 0,
                    },
                }
            ],
        }

        result = main.submit_bulk_import(
            "profiles",
            [
                {"user_id": "1", "country": "Peru"},
                {"user_id": "2", "country": "Canada"},
            ],
        )

        self.assertEqual(result["submitted"], 2)
        self.assertEqual(result["sent"], 2)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["appcues_job_ids"], ["job-1"])
        request_kwargs = appcues_request.call_args.kwargs
        self.assertEqual(request_kwargs["headers"]["Content-Type"], "application/ndjson")
        self.assertIn(b'"country":"Peru"', request_kwargs["data"])


class CountryQueryTest(unittest.TestCase):
    def test_weekly_event_query_does_not_depend_on_country_source(self):
        query = main.default_query()

        self.assertIn("weekly_user_activity_with_commitment`", query)
        self.assertNotIn("country_source", query)

    def test_weekly_country_query_filters_new_users_by_timestamp_window(self):
        query = main.country_profile_query()

        self.assertIn("dbt_cloud.dim_user`", query)
        self.assertIn("created_at >= @week_start_ts", query)
        self.assertIn("created_at < @week_end_ts", query)
        self.assertIn("PARTITION BY user_id", query)

    def test_backfill_query_returns_latest_country_per_user(self):
        query = main.country_backfill_query()

        self.assertIn("PARTITION BY user_id", query)
        self.assertIn("ORDER BY created_at DESC", query)
        self.assertIn("ROW_NUMBER()", query)
        self.assertIn("INNER JOIN appcues_managed_users", query)


if __name__ == "__main__":
    unittest.main()
