import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from log_store import LogStore


class LogTimestampTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.log = LogStore(1, data_dir=self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_new_entries_have_timezone_aware_utc_timestamps(self):
        entry = self.log.append_proposal(1, 1, "WRITE", 42)
        timestamp = datetime.fromisoformat(
            entry["timestamp"].replace("Z", "+00:00")
        )

        self.assertEqual(timestamp.tzinfo, timezone.utc)

    def test_legacy_entries_load_and_gain_timestamp_on_sync(self):
        legacy_entry = {
            "epoch": 1,
            "seq": 1,
            "op": "WRITE",
            "value": 42,
            "status": "COMMITTED",
        }
        path = Path(self.temp_dir.name) / "node_2.log"
        path.write_text(json.dumps(legacy_entry) + "\n", encoding="utf-8")

        follower_log = LogStore(2, data_dir=self.temp_dir.name)
        self.assertNotIn("timestamp", follower_log.all_entries()[0])

        synchronized_entry = {
            **legacy_entry,
            "timestamp": "2026-10-17T09:30:00.123Z",
        }
        follower_log.install_entries([synchronized_entry])

        self.assertEqual(
            follower_log.all_entries()[0]["timestamp"],
            synchronized_entry["timestamp"],
        )

    def test_sync_rejects_conflicting_timestamps_for_same_entry(self):
        self.log.append_proposal(
            1,
            1,
            "WRITE",
            42,
            "2026-10-17T09:30:00.123Z",
        )
        conflicting_entry = {
            "epoch": 1,
            "seq": 1,
            "op": "WRITE",
            "value": 42,
            "status": "COMMITTED",
            "timestamp": "2026-10-17T09:31:00.123Z",
        }

        with self.assertRaisesRegex(ValueError, "Conflicting timestamps"):
            self.log.install_entries([conflicting_entry])


if __name__ == "__main__":
    unittest.main()
