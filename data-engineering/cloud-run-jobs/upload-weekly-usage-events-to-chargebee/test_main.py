import unittest
from types import SimpleNamespace
from unittest.mock import patch

import main


class BatchPathTest(unittest.TestCase):
    def test_destination_uri_is_unique_per_batch(self):
        uri = main.build_destination_uri(
            "2026-07-28", "2026-08-04", "2026-08-04-18-00-00"
        )

        self.assertIn("week_start=2026-07-28/week_end=2026-08-04/", uri)
        self.assertIn("batch_id=2026-08-04-18-00-00/", uri)

    def test_uses_valid_batch_override(self):
        with patch.object(main, "SFTP_BATCH_ID_OVERRIDE", "2026-08-04-18-00-00"):
            self.assertEqual(main.batch_id(), "2026-08-04-18-00-00")

    def test_rejects_invalid_batch_override(self):
        with patch.object(main, "SFTP_BATCH_ID_OVERRIDE", "../invalid"):
            with self.assertRaisesRegex(ValueError, "YYYY-MM-DD-HH-MM-SS"):
                main.batch_id()

    def test_refreshes_blob_after_stale_generation(self):
        latest_blob = SimpleNamespace(
            download_to_file=lambda buffer: buffer.write(b"current generation")
        )
        bucket = SimpleNamespace(blob=lambda _name: latest_blob)

        class StaleBlob:
            name = "export-000.csv.gz"

            def __init__(self):
                self.bucket = bucket

            def download_to_file(self, _buffer):
                raise main.NotFound("stale generation")

        buffer = main.download_blob_to_buffer(StaleBlob())

        self.assertEqual(buffer.read(), b"current generation")


if __name__ == "__main__":
    unittest.main()
