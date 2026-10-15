import stat
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import main


class FakeSftp:
    def __init__(self, entries):
        self.entries = entries

    def listdir_attr(self, _path):
        return self.entries


def directory(name, mtime):
    return SimpleNamespace(filename=name, st_mtime=mtime, st_mode=stat.S_IFDIR)


class LatestBatchPathTest(unittest.TestCase):
    def test_selects_folder_timestamp_instead_of_sftp_mtime(self):
        sftp = FakeSftp([
            directory("2026-05-27-15-00-00", 9999999999),
            directory("2026-07-21-13-40-00", 0),
            directory("2026-07-29-06-47-26", 0),
        ])

        selected = main.latest_batch_path(
            sftp, now=datetime(2026, 7, 31, tzinfo=UTC)
        )

        self.assertEqual(selected, "/predictions/2026-07-29-06-47-26")


class BatchAndMetadataValidationTest(unittest.TestCase):
    def test_accepts_matching_metadata(self):
        main.validate_metadata(
            {
                "batch_id": "2026-07-29-06-47-26",
                "expected_file_count": 1,
                "expected_file_names": ["predictions_part_001.csv"],
                "row_counts": {"predictions_part_001.csv": 40973},
            },
            "2026-07-29-06-47-26",
            ["predictions_part_001.csv"],
            {"predictions_part_001.csv": 40973},
        )

    def test_rejects_metadata_row_count_mismatch(self):
        with self.assertRaisesRegex(RuntimeError, "Metadata row count mismatch"):
            main.validate_metadata(
                {"row_counts": {"predictions_part_001.csv": 40973}},
                "2026-07-29-06-47-26",
                ["predictions_part_001.csv"],
                {"predictions_part_001.csv": 100},
            )

    def test_accepts_single_digit_hour(self):
        parsed = main.parse_batch_timestamp("2026-05-15-4-36-27")

        self.assertEqual(parsed, datetime(2026, 5, 15, 4, 36, 27, tzinfo=UTC))

    def test_rejects_stale_latest_batch(self):
        sftp = FakeSftp([directory("2026-05-27-15-00-00", 9999999999)])

        with patch.object(main, "MAX_BATCH_AGE_DAYS", 14):
            with self.assertRaisesRegex(RuntimeError, "older than 14 days"):
                main.latest_batch_path(
                    sftp, now=datetime(2026, 7, 31, tzinfo=UTC)
                )

    def test_ignores_non_timestamp_directories(self):
        sftp = FakeSftp([
            directory("archive", 9999999999),
            directory("2026-07-29-06-47-26", 0),
        ])

        selected = main.latest_batch_path(
            sftp, now=datetime(2026, 7, 31, tzinfo=UTC)
        )

        self.assertEqual(selected, "/predictions/2026-07-29-06-47-26")


if __name__ == "__main__":
    unittest.main()
