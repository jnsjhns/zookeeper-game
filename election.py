import random
import threading
import time

from config import NodeConfig
from log_store import LogStore
from rpc import RpcTransport
from state import NodeState


class ElectionManager:
    def __init__(
        self,
        config: NodeConfig,
        state: NodeState,
        log: LogStore,
        rpc: RpcTransport,
    ):
        self.config = config
        self.state = state
        self.log = log
        self.rpc = rpc

        self.quorum = (len(config.peers) + 1) // 2 + 1
        self.timeout = 4.0

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"election-{state.node_id}",
            daemon=True,
        )

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()

        if self._thread.is_alive():
            self._thread.join()

    def get_status(self):
        epoch, seq = self.log.latest_tag()

        with self.state.lock:
            return {
                "node_id": self.state.node_id,
                "role": self.state.role,
                "leader_id": self.state.leader_id,
                "term": self.state.term,
                "last_epoch": epoch,
                "last_seq": seq,
            }

    @staticmethod
    def _rank(status):
        return (
            status["last_epoch"],
            status["last_seq"],
            -status["node_id"],
        )

    # Determine if an election is needed based on the current role, leader status, and last heartbeat time. 
    # If the node is a leader or recovering, no election is needed. 
    # If there is no known leader or the last heartbeat has timed out, an election is needed.
    def needs_election(self):
        with self.state.lock:
            if self.state.role in {"LEADER", "RECOVERING"}:
                return False

            if self.state.leader_id is None:
                return True

            elapsed = time.monotonic() - self.state.last_heartbeat
            return elapsed > self.timeout

    def _run(self):
        while not self._stop_event.wait(random.uniform(1.0, 2.0)):
            if not self.needs_election():
                continue

            try:
                self.start_election()
            except Exception as error:
                print(f"Election attempt failed: {error}")

    # Start an election by querying the status of all peers, determining the best candidate, and requesting votes. 
    # If a quorum of votes is received, the node becomes the leader and enters the recovering state. 
    # If not enough votes are received or a higher term is encountered, the node reverts to a follower role.
    def start_election(self):
        statuses = [self.get_status()]

        for peer_id in self.config.peers:
            if self._stop_event.is_set():
                return False

            try:
                status = self.rpc.call(
                    peer_id,
                    "election_status",
                    timeout=0.5,
                )
                statuses.append(status)
            except Exception:
                pass

        if not self.needs_election():
            return False

        if len(statuses) < self.quorum:
            return False

        best = max(statuses, key=self._rank)

        # Let the best reachable candidate request votes.
        if best["node_id"] != self.state.node_id:
            return False

        with self.state.lock:
            # A heartbeat could have arrived during peer queries.
            if self.state.role in {"LEADER", "RECOVERING"}:
                return False

            if (
                self.state.leader_id is not None
                and time.monotonic() - self.state.last_heartbeat
                <= self.timeout
            ):
                return False

            self.state.term = max(
                self.state.term,
                max(status["term"] for status in statuses),
            ) + 1

            term = self.state.term
            self.state.role = "CANDIDATE"
            self.state.leader_id = None
            self.state.voted_for = self.state.node_id

        last_epoch, last_seq = self.log.latest_tag()
        votes = {self.state.node_id}

        print(
            f"Election term {term}: "
            f"Node {self.state.node_id} requests votes "
            f"with log ({last_epoch}, {last_seq})"
        )

        for peer_id in self.config.peers:
            if self._stop_event.is_set():
                return False

            try:
                response = self.rpc.call(
                    peer_id,
                    "request_vote",
                    term,
                    self.state.node_id,
                    last_epoch,
                    last_seq,
                    timeout=0.5,
                )

                response_term = response["term"]

                with self.state.lock:
                    if response_term > self.state.term:
                        self._advance_term(response_term)

                if (
                    response["granted"]
                    and response_term == term
                    and response["node_id"] == peer_id
                ):
                    votes.add(peer_id)

            except Exception:
                pass

        with self.state.lock:
            if (
                self.state.term != term
                or self.state.role != "CANDIDATE"
            ):
                return False

            if len(votes) < self.quorum:
                self.state.role = "FOLLOWER"
                print(
                    f"Election term {term} failed: "
                    f"{len(votes)}/{self.quorum} votes"
                )
                return False

            self.state.role = "RECOVERING"
            self.state.leader_id = self.state.node_id
            self.state.last_heartbeat = time.monotonic()

        print(
            f"Node {self.state.node_id} elected in term {term}: "
            f"{len(votes)} votes; role = RECOVERING"
        )

        return True

    def request_vote(
        self,
        term,
        candidate_id,
        last_epoch,
        last_seq,
    ):
        local_epoch, local_seq = self.log.latest_tag()

        candidate_rank = (last_epoch, last_seq, -candidate_id)
        local_rank = (local_epoch, local_seq, -self.state.node_id)

        with self.state.lock:
            granted = False

            if term >= self.state.term:
                leader_recent = (
                    self.state.leader_id is not None
                    and time.monotonic() - self.state.last_heartbeat
                    <= self.timeout
                )

                if not leader_recent:
                    if term > self.state.term:
                        self._advance_term(term)

                    if (
                        candidate_rank >= local_rank
                        and self.state.voted_for in (None, candidate_id)
                    ):
                        self.state.voted_for = candidate_id
                        granted = True

            return {
                "node_id": self.state.node_id,
                "term": self.state.term,
                "granted": granted,
            }

    def receive_heartbeat(self, term, leader_id):
        with self.state.lock:
            if term < self.state.term:
                return False

            if term > self.state.term:
                self._advance_term(term)

            if self.state.role in {"LEADER", "RECOVERING"}:
                return leader_id == self.state.node_id

            if self.state.leader_id not in (None, leader_id):
                return False

            self.state.role = "FOLLOWER"
            self.state.leader_id = leader_id
            self.state.last_heartbeat = time.monotonic()

        return True

    def _advance_term(self, term):
        # Caller holds state.lock.
        self.state.term = term
        self.state.role = "FOLLOWER"
        self.state.leader_id = None
        self.state.voted_for = None