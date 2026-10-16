from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock, patch


DAGS_DIR = Path(__file__).parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))


class _FakeTask:
    def __call__(self, function):
        def operator(*args, **kwargs):
            return _FakeOperator()

        operator.function = function
        return operator


class _FakeOperator:
    def __rshift__(self, other):
        return other

    def __rrshift__(self, other):
        return self


class _FakeDAG:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class _FakeVariable:
    @staticmethod
    def get(name, default_var=None):
        if default_var is not None:
            return default_var
        return "test-value"


def _install_airflow_stub() -> None:
    airflow = types.ModuleType("airflow")
    decorators = types.ModuleType("airflow.decorators")
    models = types.ModuleType("airflow.models")
    operators = types.ModuleType("airflow.operators")
    python_operator = types.ModuleType("airflow.operators.python")
    airflow.DAG = _FakeDAG
    decorators.task = _FakeTask()
    decorators.get_current_context = lambda: {}
    python_operator.get_current_context = lambda: {}
    models.Variable = _FakeVariable
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.decorators": decorators,
            "airflow.models": models,
            "airflow.operators": operators,
            "airflow.operators.python": python_operator,
        }
    )


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, DAGS_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_install_airflow_stub()
common = _load("campaign_daily_common")
openai_ads = _load("openai_ads_campaign_daily_to_bigquery")


def _openai_row(**overrides):
    return {"readable_time": "2026-09-13", "campaign_id": "cmpn_1",
            "campaign_name": "Brand", "campaign_status": "active",
            "impressions": 1200, "clicks": 36, "spend": 18.42, **overrides}



def test_openai_row_matches_schema_and_preserves_currency_units():
    row = openai_ads.map_row(_openai_row(), "adacct_1")
    assert set(row) == {field.name for field in common.CAMPAIGN_DAILY_SCHEMA}
    assert row["cost"] == 18.42
    assert row["platform"] == "openai"
    assert row["device_type"] is None
    assert row["customer_id"] == "adacct_1"



def test_openai_missing_and_nonfinite_metrics_fail_closed():
    for value in (None, float("nan"), float("inf"), float("-inf"), True):
        try:
            openai_ads.map_row(_openai_row(spend=value), "adacct_1")
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid metric accepted: {value}")



def test_openai_pagination_deduplicates_identical_rows():
    page1 = {"data": [_openai_row()], "has_more": True, "last_id": "cursor1"}
    page2 = {"data": [_openai_row()], "has_more": False}
    account = {"id": "adacct_1", "timezone": "America/New_York"}
    with patch.object(openai_ads, "get_json", side_effect=[page1, page2]) as request:
        rows = openai_ads.fetch_rows("token", account, date(2026, 9, 13), date(2026, 9, 13))
    assert len(rows) == 1
    params = request.call_args_list[0].args[2]
    assert ("time_granularity", "daily") in params
    assert ("aggregation_level", "campaign") in params
    time_range = json.loads(dict(params)["time_ranges[]"])
    assert time_range == {"type": "date_range", "since": "2026-09-13",
                          "until": "2026-09-13", "timezone": "America/New_York"}
    assert ("after", "cursor1") in request.call_args_list[1].args[2]



def test_openai_signed_spend_reaches_bigquery_unchanged():
    account = {"id": "test-value", "timezone": "America/Chicago", "currency_code": "USD"}
    source = _openai_row(readable_time="2026-10-13", impressions=0, clicks=0, spend=-6.61)
    context = {"data_interval_end": datetime(2026, 10, 16, 8, 30, tzinfo=UTC)}
    pages = [{"data": [], "has_more": False} for _ in range(7)]
    pages[4] = {"data": [source, source.copy()], "has_more": False}
    client = Mock()
    with patch.object(openai_ads, "get_json", side_effect=[account, *pages]), \
         patch.object(openai_ads, "get_current_context", return_value=context), \
         patch.object(openai_ads.bigquery, "Client", return_value=client):
        result = openai_ads.sync_openai_campaign_daily.function()
    assert result["rows"] == 1
    assert (result["start_date"], result["end_date"]) == ("2026-10-09", "2026-10-15")
    client.load_table_from_json.assert_called_once()
    loaded = client.load_table_from_json.call_args.args[0]
    assert loaded == [openai_ads.map_row(source, "test-value")]
    assert loaded[0]["cost"] == -6.61
    client.query.assert_called_once()
    client.delete_table.assert_called_once()



def test_negative_cost_still_requires_explicit_opt_in():
    row = openai_ads.map_row(_openai_row(spend=-6.61), "test-value")
    day = date(2026, 9, 13)
    client = Mock()
    try:
        common.replace_bigquery_window(client, "project", "dataset", "table", [row], day, day)
    except ValueError as error:
        assert "cost cannot be negative" in str(error)
    else:
        raise AssertionError("Default cost validation must stay strict")
    assert not client.mock_calls



def test_openai_counts_still_reject_negative_fractional_and_invalid_values():
    for field in ("impressions", "clicks"):
        for value in (-1, 0.5, None, True, float("nan"), float("inf")):
            try:
                openai_ads.map_row(_openai_row(**{field: value}), "test-value")
            except ValueError:
                pass
            else:
                raise AssertionError(f"Invalid {field} accepted: {value}")



def test_openai_incomplete_and_looping_pages_are_rejected():
    account = {"id": "adacct_1", "timezone": "UTC"}
    for page in ({"data": []}, {"data": [], "has_more": True},
                 {"data": [_openai_row()], "has_more": True, "last_id": "same"}):
        with patch.object(openai_ads, "get_json", return_value=page) as request:
            try:
                openai_ads.fetch_rows("token", account, date(2026, 9, 13), date(2026, 9, 13))
            except ValueError:
                pass
            else:
                raise AssertionError("Invalid page must not be loaded")
            assert request.call_count <= 2



def test_openai_failed_api_does_not_modify_bigquery():
    with patch.object(openai_ads, "get_json", side_effect=RuntimeError("HTTP 403")), \
         patch.object(openai_ads, "replace_bigquery_window") as load:
        try:
            openai_ads.sync_openai_campaign_daily.function()
        except RuntimeError:
            pass
        else:
            raise AssertionError("API error must propagate")
        load.assert_not_called()



def test_openai_empty_report_does_not_modify_bigquery():
    account = {"id": "test-value", "timezone": "UTC", "currency_code": "USD"}
    with patch.object(openai_ads, "get_json", return_value=account), \
         patch.object(openai_ads, "fetch_rows", return_value=[]), \
         patch.object(openai_ads, "replace_bigquery_window") as load:
        try:
            openai_ads.sync_openai_campaign_daily.function()
        except ValueError as error:
            assert "Empty OpenAI Ads" in str(error)
        else:
            raise AssertionError("Empty report must preserve existing data")
        load.assert_not_called()



def test_openai_http_uses_read_only_api_and_does_not_follow_redirects():
    response = Mock(status_code=200)
    response.json.return_value = {"id": "adacct_1"}
    with patch.object(openai_ads, "request_with_retry", return_value=response) as request:
        assert openai_ads.get_json("ad_account", "token") == {"id": "adacct_1"}
    assert request.call_args.args == ("GET", "https://api.ads.openai.com/v1/ad_account")
    assert request.call_args.kwargs["allow_redirects"] is False



def test_openai_completed_window_uses_interval_end_not_logical_start():
    account = {"id": "test-value", "timezone": "UTC", "currency_code": "USD"}
    row = openai_ads.map_row(_openai_row(), "test-value")
    context = {"logical_date": datetime(2026, 9, 13, 8, 30, tzinfo=UTC),
               "data_interval_end": datetime(2026, 9, 14, 8, 30, tzinfo=UTC)}
    with patch.object(openai_ads, "get_json", return_value=account), \
         patch.object(openai_ads, "get_current_context", return_value=context), \
         patch.object(openai_ads, "fetch_rows", return_value=[row]) as fetch, \
         patch.object(openai_ads.bigquery, "Client"), \
         patch.object(openai_ads, "replace_bigquery_window") as load:
        result = openai_ads.sync_openai_campaign_daily.function()
    assert fetch.call_args.args[-2:] == (date(2026, 9, 7), date(2026, 9, 13))
    assert load.call_args.args[3] == "openai_campaign_daily_stats"
    assert result["rows"] == 1



def test_openai_wrong_account_and_currency_stop_before_extraction():
    for account in ({"id": "wrong-account", "timezone": "UTC", "currency_code": "USD"},
                    {"id": "test-value", "timezone": "UTC", "currency_code": "EUR"}):
        with patch.object(openai_ads, "get_json", return_value=account), \
             patch.object(openai_ads, "fetch_rows") as fetch, \
             patch.object(openai_ads, "replace_bigquery_window") as load:
            try:
                openai_ads.sync_openai_campaign_daily.function()
            except ValueError:
                pass
            else:
                raise AssertionError("Account/currency mismatch must stop load")
        fetch.assert_not_called()
        load.assert_not_called()



def test_openai_partial_extraction_failure_never_loads_bigquery():
    account = {"id": "test-value", "timezone": "UTC", "currency_code": "USD"}
    with patch.object(openai_ads, "get_json", return_value=account), \
         patch.object(openai_ads, "fetch_rows", side_effect=RuntimeError("page failed")), \
         patch.object(openai_ads, "replace_bigquery_window") as load:
        try:
            openai_ads.sync_openai_campaign_daily.function()
        except RuntimeError:
            pass
        else:
            raise AssertionError("Partial extraction must fail")
        load.assert_not_called()

