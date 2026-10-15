import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import main


NOW = datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)


def source_row(**changes):
    row = {
        "id": "row_test", "createdAt": "2026-09-24T12:00:00.000Z",
        "updatedAt": "2026-09-25T18:00:00.000Z", "version": 3,
        "values": {
            main.FIELDS["submission_id"]: "submission-1",
            main.FIELDS["consent_marketing"]: 0,
            main.FIELDS["is_test"]: 1,
            main.FIELDS["submitted_at"]: "2026-09-24T07:00:00-05:00",
            "fld_future_column": "preserved",
        },
    }
    row.update(changes)
    return row


class TransformTests(unittest.TestCase):
    def test_preserves_grain_raw_values_and_normalizes_types(self):
        result = main.transform(source_row(), main.Config(), NOW)
        self.assertEqual(result["ploy_row_id"], "row_test")
        self.assertEqual(result["submission_id"], "submission-1")
        self.assertIs(result["consent_marketing"], False)
        self.assertIs(result["is_test"], True)
        self.assertEqual(result["submitted_at"], "2026-09-24T12:00:00.000Z")
        self.assertEqual(result["raw_values"]["fld_future_column"], "preserved")

    def test_invalid_business_timestamp_is_retained_as_raw(self):
        row = source_row()
        row["values"][main.FIELDS["submitted_at"]] = "invalid"
        result = main.transform(row, main.Config(), NOW)
        self.assertIsNone(result["submitted_at"])
        self.assertEqual(result["submitted_at_raw"], "invalid")

    def test_missing_key_version_or_timezone_fails(self):
        for change in ({"id": ""}, {"version": None}, {"updatedAt": "2026-09-25T18:00:00"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                main.transform(source_row(**change), main.Config(), NOW)


class PaginationTests(unittest.TestCase):
    def source(self, payloads):
        session = Mock()
        responses = []
        for data in payloads:
            response = Mock(status_code=200)
            response.json.return_value = data
            responses.append(response)
        session.request.side_effect = responses
        return main.PloyClient(main.Config(page_size=1), "test-key", session), session

    def test_pages_preserve_filters_and_use_cursor(self):
        for cursor_key in ("cursor", "nextCursor"):
            with self.subTest(cursor_key=cursor_key):
                source, session = self.source([
                    {"ok": True, "rows": [source_row()], "hasMore": True, cursor_key: "page2"},
                    {"ok": True, "rows": [], "hasMore": False},
                ])
                start = NOW - timedelta(days=7)
                self.assertEqual(sum(len(p) for p in source.pages(start, NOW)), 1)
                first, second = [call.kwargs["json"] for call in session.request.call_args_list]
                self.assertNotIn("cursor", first)
                self.assertEqual(second["cursor"], "page2")
                self.assertEqual(first["filters"], second["filters"])
                self.assertEqual(first["sort"][0]["field"], "updated_at")

    def test_missing_or_repeated_cursor_aborts(self):
        for payloads in (
            [{"ok": True, "rows": [], "hasMore": True}],
            [{"ok": True, "rows": [], "hasMore": True, "cursor": "same"}] * 2,
        ):
            source, _ = self.source(payloads)
            with self.assertRaises(RuntimeError):
                list(source.pages(None, NOW))

    def test_out_of_window_row_aborts(self):
        source, _ = self.source([{"ok": True, "rows": [source_row()], "hasMore": False}])
        with self.assertRaises(RuntimeError):
            list(source.pages(NOW - timedelta(minutes=1), NOW))

    def test_authentication_error_does_not_log_payload(self):
        source, session = self.source([])
        session.request.side_effect = None
        session.request.return_value = Mock(status_code=401, text="sensitive payload")
        with self.assertRaisesRegex(RuntimeError, "HTTP 401") as caught:
            source.validate_schema()
        self.assertNotIn("sensitive", str(caught.exception))


class IncrementalTests(unittest.TestCase):
    def test_initial_and_incremental_windows(self):
        client = Mock()
        client.query.return_value.result.return_value = [{"watermark": None}]
        start, end = main.get_window(client, main.Config(), NOW)
        self.assertIsNone(start)
        self.assertEqual(end, NOW - timedelta(seconds=60))
        watermark = NOW - timedelta(hours=1)
        client.query.return_value.result.return_value = [{"watermark": watermark}]
        start, _ = main.get_window(client, main.Config(), NOW)
        self.assertEqual(start, watermark - timedelta(days=7))

    def test_partial_extraction_never_loads_or_advances_checkpoint(self):
        client, source = Mock(), Mock()

        def pages(*_):
            yield [source_row()]
            raise RuntimeError("second page failed")

        source.pages.side_effect = pages
        with patch.object(main, "get_window", return_value=(None, NOW)), \
                patch.object(main, "commit_batch") as commit:
            with self.assertRaisesRegex(RuntimeError, "second page failed"):
                main.run(client, source, main.Config(), NOW)
            commit.assert_not_called()
            client.load_table_from_file.assert_not_called()

    def test_failed_load_keeps_checkpoint_and_cleans_staging(self):
        client, source = Mock(), Mock()
        source.pages.return_value = [[source_row()]]
        client.load_table_from_file.return_value.result.side_effect = RuntimeError("load failed")
        with patch.object(main, "get_window", return_value=(None, NOW)), \
                patch.object(main, "commit_batch") as commit:
            with self.assertRaisesRegex(RuntimeError, "load failed"):
                main.run(client, source, main.Config(), NOW)
            commit.assert_not_called()
            client.delete_table.assert_called_once()

    def test_empty_incremental_window_advances_without_load(self):
        client, source = Mock(), Mock()
        source.pages.return_value = [[]]
        with patch.object(main, "get_window", return_value=(NOW - timedelta(days=7), NOW)), \
                patch.object(main, "commit_batch") as commit:
            result = main.run(client, source, main.Config(), NOW)
            commit.assert_called_once_with(client, main.Config(), None, NOW, 0)
            client.load_table_from_file.assert_not_called()
            self.assertFalse(result["initial_load"])

    def test_transaction_commits_checkpoint_and_deduplicated_merge_together(self):
        client = Mock()
        main.commit_batch(client, main.Config(), "fxr-analytics.ploy.stage", NOW, 2)
        sql = client.query.call_args.args[0]
        self.assertLess(sql.index("BEGIN TRANSACTION"), sql.index("UPDATE `"))
        self.assertLess(sql.index("UPDATE `"), sql.index("MERGE `"))
        self.assertLess(sql.index("MERGE `"), sql.index("COMMIT TRANSACTION"))
        self.assertIn("PARTITION BY ploy_row_id", sql)
        self.assertIn("S.source_version > T.source_version", sql)
        self.assertIn("GREATEST(COALESCE(watermark", sql)


if __name__ == "__main__":
    unittest.main()
