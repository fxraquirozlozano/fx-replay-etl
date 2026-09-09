import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import main


def valid_row(**overrides):
    row = {column: None for column in main.REPORT_COLUMNS}
    row.update(
        {
            "month": "2026-08",
            "invoice_id": "inv_1",
            "invoice_date": datetime(2026, 8, 1),
            "status": "paid",
            "net_billing": Decimal("80.00"),
        }
    )
    row.update(overrides)
    return row


class MonthTest(unittest.TestCase):
    def test_previous_month_across_year_boundary(self):
        result = main.resolve_report_month(
            "", "America/Chicago", datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        )
        self.assertEqual(result, date(2025, 12, 1))

    def test_override_and_filename(self):
        report_month = main.resolve_report_month("2026-08", "UTC")
        self.assertEqual(report_month, date(2026, 8, 1))
        self.assertEqual(main.report_filename(report_month), "AUG 2026.csv")

    def test_invalid_override(self):
        with self.assertRaisesRegex(ValueError, "YYYY-MM"):
            main.resolve_report_month("08-2026", "UTC")


class ValidationTest(unittest.TestCase):
    def test_accepts_unique_invoice_snapshot(self):
        rows = [
            valid_row(),
            valid_row(
                invoice_id="inv_2",
                status="payment_due",
                net_billing=Decimal("20.00"),
            ),
        ]

        result = main.validate_report(rows, date(2026, 8, 1))

        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.net_billing, Decimal("100.00"))
        self.assertEqual(result.rows_by_status, {"paid": 1, "payment_due": 1})

    def test_rejects_duplicate_invoice_id(self):
        with self.assertRaisesRegex(ValueError, "Duplicate invoice_id"):
            main.validate_report(
                [valid_row(), valid_row()], date(2026, 8, 1)
            )

    def test_rejects_wrong_month(self):
        with self.assertRaisesRegex(ValueError, "expected 2026-08"):
            main.validate_report(
                [valid_row(month="2026-07")], date(2026, 8, 1)
            )

    def test_rejects_voided_invoice(self):
        with self.assertRaisesRegex(ValueError, "not reportable"):
            main.validate_report(
                [valid_row(status="voided")], date(2026, 8, 1)
            )

    def test_rejects_invoice_date_outside_month(self):
        with self.assertRaisesRegex(ValueError, "expected month 2026-08"):
            main.validate_report(
                [valid_row(invoice_date=datetime(2026, 7, 31))],
                date(2026, 8, 1),
            )


class QueryTest(unittest.TestCase):
    def test_report_query_excludes_only_zero_gross_zero_net_invoices(self):
        query = main.report_query(
            "fxr-analytics", "reporting", "vat_report_monthly_snapshots"
        )

        self.assertIn("COALESCE(total_billing, 0) = 0", query)
        self.assertIn("net_billing = 0", query)


class CsvTest(unittest.TestCase):
    def test_csv_is_deterministic_and_has_utf8_bom(self):
        with tempfile.TemporaryDirectory() as directory:
            output = main.write_report_csv(
                [valid_row()], Path(directory), "AUG 2026.csv"
            )
            content = output.read_bytes()

        self.assertTrue(content.startswith(b"\xef\xbb\xbf"))
        self.assertIn(b"invoice_id", content)
        self.assertIn(b"inv_1", content)

    def test_validates_and_writes_stream_without_materializing_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = (row for row in [valid_row()])
            output, summary = main.validate_and_write_report(
                rows, date(2026, 8, 1), Path(directory), "AUG 2026.csv"
            )

            self.assertTrue(output.exists())
            self.assertEqual(summary.row_count, 1)
            self.assertEqual(summary.net_billing, Decimal("80.00"))


class SharePointTest(unittest.TestCase):
    def setUp(self):
        self.config = main.Config(
            project_id="fxr-analytics",
            bq_dataset="reporting",
            source_table="fct_invoices",
            snapshot_table="vat_report_monthly_snapshots",
            business_timezone="America/Chicago",
            report_month_override="2026-08",
            source_as_of="",
            refresh_snapshot=False,
            output_dir=Path("/tmp"),
            sharepoint_hostname="fxreplay.sharepoint.com",
            sharepoint_site_path="sites/DataTeam",
            sharepoint_drive_id="drive-id",
            sharepoint_folder_path="Finance/VAT Report",
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
            roam_api_key="roam-key",
            roam_group_id="group-id",
            overwrite_existing=True,
            dry_run=False,
            request_timeout_seconds=30,
        )

    def test_identical_existing_file_is_not_uploaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "AUG 2026.csv"
            path.write_bytes(b"same-content")
            session = Mock()
            session.get.side_effect = [
                SimpleNamespace(status_code=200),
                SimpleNamespace(ok=True, content=b"same-content"),
            ]

            action = main.upload_report(
                session, self.config, "site-id", "token", path
            )

        self.assertEqual(action, "unchanged")
        session.put.assert_not_called()

    def test_different_existing_file_is_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "AUG 2026.csv"
            path.write_bytes(b"new-content")
            session = Mock()
            session.get.side_effect = [
                SimpleNamespace(status_code=200),
                SimpleNamespace(ok=True, content=b"old-content"),
            ]
            session.put.return_value = SimpleNamespace(
                status_code=200, text="", ok=True
            )

            action = main.upload_report(
                session, self.config, "site-id", "token", path
            )

        self.assertEqual(action, "replaced")
        session.put.assert_called_once()


if __name__ == "__main__":
    unittest.main()
