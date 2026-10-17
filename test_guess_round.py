import tempfile
import unittest

from config import NodeConfig
from log_store import LogStore
from replication import ReplicationManager
from state import NodeState


class AcknowledgingRpc:
    def __init__(self, peers):
        self.config = NodeConfig(
            node_id=1,
            peers={peer_id: f"127.0.0.1:{8000 + peer_id}" for peer_id in peers},
        )
        self.proposal_timestamps = []

    def call(self, peer_id, method, *args, timeout=None):
        if method == "receive_proposal":
            term, epoch, seq, _, _, _, timestamp = args
            self.proposal_timestamps.append((epoch, seq, timestamp))
            return {
                "status": "ACK",
                "node_id": peer_id,
                "term": term,
                "epoch": epoch,
                "seq": seq,
                "timestamp": timestamp,
            }

        if method == "receive_commit":
            return True

        raise AssertionError(f"Unexpected RPC method: {method}")


class GuessRoundTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.log = LogStore(1, data_dir=self.temp_dir.name)
        self.state = NodeState(
            node_id=1,
            role="LEADER",
            leader_id=1,
            term=1,
        )
        self.replication = ReplicationManager(
            NodeConfig(
                node_id=1,
                peers={
                    peer_id: f"127.0.0.1:{8000 + peer_id}"
                    for peer_id in (2, 3, 4, 5)
                },
            ),
            self.state,
            self.log,
            AcknowledgingRpc((2, 3, 4, 5)),
            guesses_per_round=2,
        )
        self.replication.begin_epoch(1)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_round_selects_closest_guess_and_updates_target(self):
        self.assertEqual(self.replication.write(50)["status"], "committed")

        first_guess = self.replication.guess(30)
        self.assertEqual(first_guess["round"]["status"], "collecting")
        self.assertEqual(self.replication.read()["target"], 50)

        final_guess = self.replication.guess(62)
        self.assertEqual(final_guess["round"]["status"], "committed")
        self.assertEqual(final_guess["round"]["selected_guess"], 62)
        self.assertEqual(final_guess["round"]["old_target"], 50)
        self.assertEqual(final_guess["round"]["new_target"], 62)
        self.assertEqual(self.replication.read()["target"], 62)
        self.assertEqual(
            {
                (epoch, seq): timestamp
                for epoch, seq, timestamp
                in self.replication.rpc.proposal_timestamps
            },
            {
                (entry["epoch"], entry["seq"]): entry["timestamp"]
                for entry in self.log.all_entries()
            },
        )
        self.assertEqual(
            self.replication.read()["timestamp"],
            self.log.latest_target()["timestamp"],
        )

    def test_equally_close_guesses_choose_the_earlier_proposal(self):
        self.replication.write(50)
        self.replication.guess(45)

        final_guess = self.replication.guess(55)

        self.assertEqual(final_guess["round"]["selected_guess"], 45)
        self.assertEqual(self.replication.read()["target"], 45)

    def test_recovery_completes_a_fully_collected_round(self):
        self.replication.write(50)
        for seq, value in ((2, 30), (3, 62)):
            self.log.append_proposal(1, seq, "GUESS", value)
            self.log.mark_committed(1, seq)

        self.replication.begin_epoch(2)
        result = self.replication.complete_pending_round()

        self.assertEqual(result["status"], "committed")
        self.assertEqual(result["selected_guess"], 62)
        self.assertEqual(self.replication.read()["target"], 62)

    def test_invalid_guess_is_rejected(self):
        result = self.replication.guess(101)

        self.assertEqual(result["status"], "error")
        self.assertIsNone(self.log.latest_target())

    def test_round_uses_configured_guess_count(self):
        self.replication = ReplicationManager(
            NodeConfig(
                node_id=1,
                peers={
                    peer_id: f"127.0.0.1:{8000 + peer_id}"
                    for peer_id in (2, 3, 4, 5)
                },
            ),
            self.state,
            self.log,
            AcknowledgingRpc((2, 3, 4, 5)),
            guesses_per_round=3,
        )
        self.replication.begin_epoch(1)
        self.replication.write(50)

        self.assertEqual(
            self.replication.guess(30)["round"]["status"],
            "collecting",
        )
        self.assertEqual(
            self.replication.guess(55)["round"]["status"],
            "collecting",
        )
        final_guess = self.replication.guess(62)

        self.assertEqual(final_guess["round"]["status"], "committed")
        self.assertEqual(final_guess["round"]["guesses_received"], 3)
        self.assertEqual(final_guess["round"]["selected_guess"], 55)
        self.assertEqual(final_guess["round"]["old_target"], 50)
        self.assertEqual(final_guess["round"]["new_target"], 55)
        self.assertEqual(self.replication.read()["target"], 55)

    def test_round_requires_at_least_two_guesses(self):
        with self.assertRaisesRegex(ValueError, "at least 2"):
            ReplicationManager(
                NodeConfig(node_id=1),
                self.state,
                self.log,
                AcknowledgingRpc(()),
                guesses_per_round=1,
            )


if __name__ == "__main__":
    unittest.main()
