import threading

from config import NodeConfig
from log_store import LogStore, current_timestamp
from rpc import RpcTransport
from state import NodeState


class ReplicationManager:
    def __init__(
        self,
        config: NodeConfig,
        state: NodeState,
        log: LogStore,
        rpc: RpcTransport,
        guesses_per_round: int = 2,
    ):
        if type(guesses_per_round) is not int or guesses_per_round < 2:
            raise ValueError(
                "guesses_per_round must be an integer of at least 2"
            )

        self.config = config
        self.state = state
        self.log = log
        self.rpc = rpc
        self.guesses_per_round = guesses_per_round

        self._write_lock = threading.RLock()

        self.peer_timeout = 2.0
        self.forward_timeout = 15.0

        # Recovery sets the epoch before enabling writes.
        self.epoch = None
        self.seq = 0

    def begin_epoch(self, epoch):
        # Called during RECOVERING, while writes are disabled.
        if type(epoch) is not int or epoch < 1:
            raise ValueError("Epoch must be a positive integer")

        last_epoch, _ = self.log.latest_tag()

        if epoch <= last_epoch:
            raise ValueError(
                "New epoch must be greater than existing log epochs"
            )

        self.epoch = epoch
        self.seq = 0

    def write(self, value):
        if type(value) is not int or not 0 <= value <= 100:
            return {
                "status": "error",
                "message": "Value must be an integer between 0 and 100",
            }

        with self.state.lock:
            role = self.state.role
            leader_id = self.state.leader_id

        if role == "LEADER":
            return self._propose_and_commit("WRITE", value)

        return self._forward_or_unavailable("write", value, role, leader_id)

    def guess(self, value):
        if type(value) is not int or not 0 <= value <= 100:
            return {
                "status": "error",
                "message": "Guess must be an integer between 0 and 100",
            }

        with self.state.lock:
            role = self.state.role
            leader_id = self.state.leader_id

        if role == "LEADER":
            with self._write_lock:
                pending = self._pending_guesses()
                if len(pending["guesses"]) >= self.guesses_per_round:
                    completion = self._finish_round_locked(pending)
                    if completion["status"] != "committed":
                        return {
                            "status": "unavailable",
                            "message": (
                                "The previous round is awaiting a "
                                "quorum commit"
                            ),
                        }

                if self.log.latest_target() is None:
                    return {
                        "status": "error",
                        "message": "Set the target with Write before guessing",
                    }

                result = self._propose_and_commit("GUESS", value)
                if result.get("status") != "committed":
                    return result

                pending = self._pending_guesses()
                if len(pending["guesses"]) >= self.guesses_per_round:
                    round_result = self._finish_round_locked(pending)
                    result["round"] = round_result
                else:
                    result["round"] = {
                        "status": "collecting",
                        "guesses_received": len(pending["guesses"]),
                        "guesses_required": self.guesses_per_round,
                        "target": pending["target"]["value"],
                    }

                return result

        return self._forward_or_unavailable("guess", value, role, leader_id)

    def _forward_or_unavailable(self, method, value, role, leader_id):
        if role in {"CANDIDATE", "RECOVERING"}:
            return {
                "status": "unavailable",
                "message": (
                    "Election or recovery in progress; "
                    "client operations are unavailable"
                ),
            }

        if leader_id is None or leader_id == self.state.node_id:
            return {
                "status": "unavailable",
                "message": "No usable leader known",
            }

        try:
            return self.rpc.call(
                leader_id,
                method,
                value,
                timeout=self.forward_timeout,
            )

        except Exception as error:
            return {
                "status": "error",
                "message": (
                    f"Forwarding {method} failed; outcome unknown: "
                    f"{error}"
                ),
            }

    def read(self):
        entry = self.log.latest_target()

        return {
            "status": "ok",
            "node_id": self.state.node_id,
            "target": None if entry is None else entry["value"],
            "tag": None if entry is None else [
                entry["epoch"],
                entry["seq"],
            ],
            "timestamp": None if entry is None else entry.get("timestamp"),
        }

    def complete_pending_round(self):
        with self._write_lock:
            pending = self._pending_guesses()
            if len(pending["guesses"]) < self.guesses_per_round:
                return None

            return self._finish_round_locked(pending)

    def _pending_guesses(self):
        entries = self.log.all_entries()
        target_index = None

        for index, entry in enumerate(entries):
            if entry["op"] == "WRITE" and entry["status"] == "COMMITTED":
                target_index = index

        if target_index is None:
            return {"target": None, "guesses": []}

        target = entries[target_index]
        guesses = [
            entry
            for entry in entries[target_index + 1:]
            if entry["op"] == "GUESS" and entry["status"] == "COMMITTED"
        ]

        return {"target": target, "guesses": guesses}

    def _finish_round_locked(self, pending):
        target_value = pending["target"]["value"]
        winner = min(
            pending["guesses"],
            key=lambda entry: (
                abs(entry["value"] - target_value),
                entry["epoch"],
                entry["seq"],
            ),
        )
        result = self._propose_and_commit("WRITE", winner["value"])

        round_result = {
            "status": result["status"],
            "selected_guess": winner["value"],
            "target": (
                winner["value"]
                if result["status"] == "committed"
                else target_value
            ),
            "old_target": target_value,
            "new_target": (
                winner["value"]
                if result["status"] == "committed"
                else None
            ),
            "target_before_round": target_value,
            "guesses_received": len(pending["guesses"]),
            "guesses_required": self.guesses_per_round,
            "write_result": result,
        }

        if result["status"] == "committed":
            print(
                f"ROUND COMPLETE: old target = {target_value}, "
                f"new target = {winner['value']} "
                f"(selected guess = {winner['value']})"
            )

        return round_result

    def _propose_and_commit(self, op, value):
        with self._write_lock:
            with self.state.lock:
                if self.state.role != "LEADER":
                    return {
                        "status": "unavailable",
                        "message": "Node is no longer leader",
                    }

                if self.epoch is None:
                    return {
                        "status": "unavailable",
                        "message": "Transaction epoch is not initialized",
                    }

                term = self.state.term
                epoch = self.epoch

                self.seq += 1
                seq = self.seq
                timestamp = current_timestamp()

                self.log.append_proposal(
                    epoch,
                    seq,
                    op,
                    value,
                    timestamp,
                )

            print(f"PROPOSED ({epoch}, {seq}): {op} {value}")

            total_nodes = len(self.config.peers) + 1
            quorum = total_nodes // 2 + 1

            acknowledgements = {self.state.node_id}

            for peer_id in self.config.peers:
                try:
                    response = self.rpc.call(
                        peer_id,
                        "receive_proposal",
                        term,
                        epoch,
                        seq,
                        op,
                        value,
                        self.state.node_id,
                        timestamp,
                        timeout=self.peer_timeout,
                    )

                    if self._valid_ack(
                        response,
                        peer_id,
                        term,
                        epoch,
                        seq,
                        timestamp,
                    ):
                        acknowledgements.add(peer_id)

                        print(
                            f"ACK ({epoch}, {seq}) "
                            f"from Node {peer_id}"
                        )

                    else:
                        print(f"Invalid ACK from Node {peer_id}")

                except Exception as error:
                    print(
                        f"Proposal to Node {peer_id} failed: {error}"
                    )

            with self.state.lock:
                if (
                    self.state.role != "LEADER"
                    or self.state.term != term
                    or self.epoch != epoch
                ):
                    return {
                        "status": "error",
                        "epoch": epoch,
                        "seq": seq,
                        "message": (
                            "Leadership changed; "
                            "proposal outcome unknown"
                        ),
                    }

                if len(acknowledgements) < quorum:
                    print(
                        f"NO QUORUM ({epoch}, {seq}): "
                        f"{len(acknowledgements)}/{quorum}"
                    )

                    return {
                        "status": "no_quorum",
                        "epoch": epoch,
                        "seq": seq,
                        "acks": len(acknowledgements),
                        "quorum": quorum,
                        "message": (
                            "Proposal stored, but not committed"
                        ),
                    }

                self.log.mark_committed(epoch, seq)

            print(
                f"COMMITTED ({epoch}, {seq}): target = {value}"
            )

            pending_peers = []

            for peer_id in self.config.peers:
                if peer_id not in acknowledgements:
                    pending_peers.append(peer_id)
                    continue

                try:
                    result = self.rpc.call(
                        peer_id,
                        "receive_commit",
                        term,
                        epoch,
                        seq,
                        self.state.node_id,
                        timeout=self.peer_timeout,
                    )

                    if result is not True:
                        pending_peers.append(peer_id)

                except Exception as error:
                    pending_peers.append(peer_id)

                    print(
                        f"Commit to Node {peer_id} failed: {error}"
                    )

            return {
                "status": "committed",
                "node_id": self.state.node_id,
                "term": term,
                "epoch": epoch,
                "seq": seq,
                "value": value,
                "timestamp": timestamp,
                "acks": len(acknowledgements),
                "quorum": quorum,
                "pending_peers": pending_peers,
            }

    def receive_proposal(
        self,
        term,
        epoch,
        seq,
        op,
        value,
        leader_id,
        timestamp,
    ):
        with self.state.lock:
            self._check_leader(term, leader_id)

            entry = self.log.append_proposal(
                epoch,
                seq,
                op,
                value,
                timestamp,
            )

        print(f"PROPOSED ({epoch}, {seq}): {op} {value}")

        return {
            "status": "ACK",
            "node_id": self.state.node_id,
            "term": term,
            "epoch": entry["epoch"],
            "seq": entry["seq"],
            "timestamp": entry["timestamp"],
        }

    def receive_commit(self, term, epoch, seq, leader_id):
        with self.state.lock:
            self._check_leader(term, leader_id)

            entry = self.log.mark_committed(epoch, seq)

        print(
            f"COMMITTED ({epoch}, {seq}): "
            f"target = {entry['value']}"
        )

        return True

    def _check_leader(self, term, leader_id):
        # Caller holds state.lock.
        if (
            self.state.role != "FOLLOWER"
            or self.state.leader_id != leader_id
            or self.state.term != term
        ):
            raise ValueError(
                "Message is not from the current leader"
            )

    @staticmethod
    def _valid_ack(response, peer_id, term, epoch, seq, timestamp):
        return (
            isinstance(response, dict)
            and response.get("status") == "ACK"
            and response.get("node_id") == peer_id
            and response.get("term") == term
            and response.get("epoch") == epoch
            and response.get("seq") == seq
            and response.get("timestamp") == timestamp
        )