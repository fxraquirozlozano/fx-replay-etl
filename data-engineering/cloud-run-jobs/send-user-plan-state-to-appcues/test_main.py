import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import main


def row(**updates):
    value = dict(user_id="u1", state_as_of_date=date(2026, 11, 18),
                 current_plan_state="paid", state_changed_date=date(2026, 10, 24))
    value.update(updates)
    return value


def completed(n=1, failed=0):
    return {"status": "COMPLETED", "id": "j1", "events": [{"name": "Completed", "attributes": {
        "ingestions_completed": n, "ingestions_failed": failed, "requests_failed": 0}}]}


class PlanningTests(unittest.TestCase):
    def test_initial_is_full_even_in_daily_mode(self):
        p = main.make_plan("daily", date(2026, 11, 19), date(2026, 11, 18), None, None)
        self.assertTrue(p.full_history)
        self.assertEqual(p.window_start, main.HISTORY_START)

    def test_daily_reads_only_missing_days(self):
        p = main.make_plan("auto", date(2026, 11, 19), date(2026, 11, 18),
                           date(2026, 11, 16), date(2026, 11, 1))
        self.assertFalse(p.full_history)
        self.assertEqual(p.window_start, date(2026, 11, 17))

    def test_monthly_catches_up_if_first_day_missed(self):
        p = main.make_plan("auto", date(2026, 12, 3), date(2026, 12, 2),
                           date(2026, 11, 30), date(2026, 11, 1))
        self.assertTrue(p.full_history)
        self.assertEqual(p.historical_refresh_date, date(2026, 12, 3))

    def test_first_day_history_does_not_repeat_on_second_day(self):
        p = main.make_plan("auto", date(2026, 12, 2), date(2026, 12, 1),
                           date(2026, 11, 30), date(2026, 12, 1))
        self.assertFalse(p.full_history)

    def test_same_cutoff_retries_delivery_without_recalculation(self):
        p = main.make_plan("auto", date(2026, 11, 19), date(2026, 11, 18),
                           date(2026, 11, 18), date(2026, 11, 1))
        self.assertFalse(p.recalculate)

    def test_manual_history_can_recalculate_same_cutoff(self):
        p = main.make_plan("historical", date(2026, 11, 19), date(2026, 11, 18),
                           date(2026, 11, 18), date(2026, 11, 1))
        self.assertTrue(p.recalculate)

    def test_stale_and_backwards_cutoffs_fail(self):
        with self.assertRaises(ValueError):
            main.make_plan("auto", date(2026, 11, 19), date(2026, 11, 17), None, None)
        with self.assertRaises(ValueError):
            main.make_plan("daily", date(2026, 11, 19), date(2026, 11, 18), date(2026, 11, 19), None)


class PayloadTests(unittest.TestCase):
    def test_dates_are_chicago_midnight_and_payload_has_only_current_fields(self):
        p = main.profile(row())
        self.assertEqual(p["bq_plan_state_changed_date"], "2026-10-24T05:00:00Z")
        self.assertEqual(p["bq_plan_state_as_of_date"], "2026-11-18T06:00:00Z")
        self.assertEqual(set(p), {"user_id", "bq_plan_state", "bq_plan_state_as_of_date", "bq_plan_state_changed_date"})

    def test_corrected_date_is_explicit_null_not_empty_string(self):
        p = main.profile(row(state_changed_date=None))
        self.assertIn("bq_plan_state_changed_date", p)
        self.assertIsNone(p["bq_plan_state_changed_date"])
        self.assertNotIn("clear_upgrade", p)

    def test_cancelled_and_paused_are_sent(self):
        for state in ("cancelled", "paused", "trial", "unknown"):
            self.assertEqual(main.profile(row(current_plan_state=state))["bq_plan_state"], state)

    def test_chunk_limits_keep_exact_confirmation_rows(self):
        rows = [row(user_id=f"u{i}") for i in range(5)]
        c = main.Config(chunk_rows=2)
        chunks = list(main.batches(iter(rows), c))
        self.assertEqual([len(r) for _, r in chunks], [2, 2, 1])
        self.assertEqual([r for _, originals in chunks for r in originals], rows)
        for data, originals in chunks:
            self.assertEqual(len(data.splitlines()), len(originals))
        one = len(json.dumps(main.profile(rows[0]), ensure_ascii=True, separators=(",", ":")).encode())
        self.assertEqual(len(list(main.batches(rows[:2], main.Config(chunk_bytes=one)))), 2)
        with self.assertRaises(ValueError):
            list(main.batches(rows, main.Config(chunk_bytes=1)))


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.config = main.Config(account_id="123", api_key="key", api_secret="secret", poll_seconds=.001)
        self.api = main.Appcues(self.config)

    def test_acceptance_is_polled_before_confirmation(self):
        self.api.request = Mock(side_effect=[Mock(json=lambda: {"job_url": "https://api.appcues.com/job"}),
                                             Mock(json=lambda: {"status": "RUNNING"}),
                                             Mock(json=lambda: completed())])
        store = Mock()
        result = main.deliver([row()], store, self.api, self.config, False)
        self.assertEqual(result["profiles"], 1)
        self.assertEqual(self.api.request.call_count, 3)
        store.confirm.assert_called_once_with([row()])

    def test_partial_or_unknown_completion_never_confirms(self):
        for value in (completed(0, 1), completed(0), {"status": "COMPLETED"}, {"status": "FAILED"}):
            with self.subTest(value=value):
                self.api.request = Mock(side_effect=[Mock(json=lambda: {"job_url": "https://api.appcues.com/job"}),
                                                     Mock(json=lambda: value)])
                store = Mock()
                with self.assertRaises(RuntimeError):
                    main.deliver([row()], store, self.api, self.config, False)
                store.confirm.assert_not_called()

    def test_current_import_response_without_counters(self):
        self.api.request = Mock(side_effect=[
            Mock(json=lambda: {"job_id": "import-abc", "job_url": "https://api.appcues.com/job"}),
            Mock(json=lambda: {"id": "abc", "name": "Import", "status": "COMPLETED"}),
        ])
        store = Mock()
        result = main.deliver([row()], store, self.api, self.config, False)
        self.assertEqual(result["profiles"], 1)
        store.confirm.assert_called_once_with([row()])

    def test_counterless_response_must_match_submitted_import(self):
        self.api.request = Mock(side_effect=[
            Mock(json=lambda: {"job_id": "import-abc", "job_url": "https://api.appcues.com/job"}),
            Mock(json=lambda: {"id": "different", "name": "Import", "status": "COMPLETED"}),
        ])
        store = Mock()
        with self.assertRaises(RuntimeError):
            main.deliver([row()], store, self.api, self.config, False)
        store.confirm.assert_not_called()

    def test_successful_batches_remain_confirmed_when_next_batch_fails(self):
        api, store = Mock(), Mock()
        api.import_batch.side_effect = ["j1", RuntimeError("failure")]
        with self.assertRaises(RuntimeError):
            main.deliver([row(user_id="one"), row(user_id="two")], store, api,
                         main.Config(chunk_rows=1), False)
        store.confirm.assert_called_once_with([row(user_id="one")])

    def test_confirmation_uses_exact_sent_values_and_nulls(self):
        store = main.Store(self.config, Mock())
        store.query = Mock()
        store.confirm([row(state_changed_date=None)])
        args = store.query.call_args.args
        payload = json.loads(args[1][0].value)
        self.assertIsNone(payload[0]["state_changed_date"])
        self.assertEqual(set(payload[0]), set(main.FIELDS))
        self.assertNotIn("FROM `fxr-analytics.appcues_sync.user_plan_state`", args[0])

    @patch("main.time.sleep")
    @patch("main.requests.request")
    def test_429_honors_retry_after(self, request, sleep):
        request.side_effect = [Mock(status_code=429, headers={"Retry-After": "2"}),
                               Mock(status_code=200)]
        self.api.request("GET", "https://api.appcues.com/job")
        sleep.assert_called_once_with(2)

    @patch("main.requests.request")
    def test_rejected_auth_is_not_retried(self, request):
        request.return_value = Mock(status_code=401)
        with self.assertRaises(RuntimeError):
            self.api.request("GET", "https://api.appcues.com/job")
        self.assertEqual(request.call_count, 1)

    @patch("main.requests.request")
    def test_job_url_cannot_receive_credentials_on_other_host(self, request):
        with self.assertRaises(ValueError):
            self.api.request("GET", "https://other.example/job")
        request.assert_not_called()

    def test_timeout_does_not_confirm(self):
        self.api.request = Mock(return_value=Mock(json=lambda: {"job_url": "https://api.appcues.com/job"}))
        with patch("main.time.monotonic", side_effect=[0, 9999]):
            with self.assertRaises(TimeoutError):
                self.api.import_batch(b"{}", 1)


class RunnerTests(unittest.TestCase):
    def test_legacy_schema_forces_historical_recalculation(self):
        client = Mock()
        client.get_table.return_value.schema = [Mock(name="unused")]
        store = main.Store(main.Config(), client)
        store.exists = Mock(return_value=True)
        store.query = Mock(return_value=[{"cutoff": date(2026, 11, 19), "historical": None}])
        previous, historical = store.checkpoint()
        self.assertIn("CAST(NULL AS DATE)", store.query.call_args.args[0])
        self.assertTrue(main.make_plan("auto", date(2026, 11, 20), previous, previous, historical).full_history)

    def test_test_cohort_is_bound_as_query_parameter(self):
        store = main.Store(main.Config(max_profiles=100, test_user_ids=("u1", "u2")), Mock())
        store.query = Mock(return_value=[])
        plan = main.make_plan("daily", date(2026, 11, 19), date(2026, 11, 18),
                              date(2026, 11, 18), date(2026, 11, 1))
        store.pending(plan)
        sql, parameters = store.query.call_args.args
        self.assertIn("user_id IN UNNEST(@test_user_ids)", sql)
        self.assertEqual(list(parameters[-1].values), ["u1", "u2"])

    def test_profile_cap_applies_across_all_batches(self):
        api, store = Mock(), Mock()
        api.import_batch.return_value = "test-import"
        rows = [row(user_id=f"u{i:03}") for i in range(105)]
        cfg = main.Config(max_profiles=100, chunk_rows=30)
        result = main.deliver(iter(rows), store, api, cfg, False)
        self.assertEqual(result["profiles"], 100)
        self.assertEqual([c.args[1] for c in api.import_batch.call_args_list], [30, 30, 30, 10])
        confirmed = [r for c in store.confirm.call_args_list for r in c.args[0]]
        self.assertEqual(confirmed, rows[:100])

    def test_pending_query_applies_deterministic_profile_cap(self):
        store = main.Store(main.Config(max_profiles=100), Mock())
        store.query = Mock(return_value=[])
        plan = main.make_plan("daily", date(2026, 11, 19), date(2026, 11, 18),
                              date(2026, 11, 18), date(2026, 11, 1))
        store.pending(plan)
        self.assertTrue(store.query.call_args.args[0].endswith("ORDER BY user_id LIMIT 100"))

    def test_negative_profile_cap_is_rejected(self):
        with self.assertRaises(ValueError):
            main.Config(max_profiles=-1).validate()

    def test_dry_run_has_no_writes_or_appcues_requests(self):
        store, api = Mock(), Mock()
        store.checkpoint.return_value = (None, None)
        store.cutoff.return_value = date(2026, 11, 18)
        store.pending.return_value = iter([row()])
        result = main.run(main.Config(), store, "auto", True, date(2026, 11, 19), api)
        self.assertEqual(result["profiles"], 1)
        store.setup.assert_not_called()
        store.refresh.assert_not_called()
        store.confirm.assert_not_called()
        api.import_batch.assert_not_called()
        self.assertTrue(store.pending.call_args.kwargs["preview"])

    def test_retry_without_recalculation_still_delivers_pending(self):
        store, api = Mock(), Mock()
        store.checkpoint.return_value = (date(2026, 11, 18), date(2026, 11, 1))
        store.cutoff.return_value = date(2026, 11, 18)
        store.pending.return_value = iter([row()])
        api.import_batch.return_value = "j1"
        cfg = main.Config(account_id="123", api_key="key", api_secret="secret")
        main.run(cfg, store, "auto", False, date(2026, 11, 19), api)
        self.assertFalse(store.refresh.call_args.args[0].recalculate)
        store.confirm.assert_called_once()

    def test_preview_sql_contains_no_writes_and_works_before_setup(self):
        store = main.Store(main.Config(), Mock())
        store.exists = Mock(return_value=False)
        store.query = Mock(return_value=[])
        plan = main.make_plan("auto", date(2026, 11, 19), date(2026, 11, 18), None, None)
        store.pending(plan, preview=True)
        sql = store.query.call_args.args[0]
        self.assertNotIn("{{", sql)
        self.assertNotIn("`fxr-analytics.appcues_sync.", sql)
        self.assertTrue(sql.startswith("WITH"))
        for token in ("MERGE ", "INSERT ", "CREATE ", "DELETE "):
            self.assertNotIn(token, sql)


if __name__ == "__main__":
    unittest.main()

