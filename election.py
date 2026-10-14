import time

from config import NodeConfig
from rpc import RpcTransport
from state import NodeState


class ElectionManager:
    def __init__(
        self,
        config: NodeConfig,
        state: NodeState,
        rpc: RpcTransport,
    ):
        self.config = config
        self.state = state
        self.rpc = rpc

    def request_vote(self, candidate_id: int):
        with self.state.lock:
            if self.state.voted_for in (None, candidate_id):
                self.state.voted_for = candidate_id
                return True

            return False

    def become_leader(self):
        with self.state.lock:
            self.state.role = "LEADER"
            self.state.leader_id = self.state.node_id
            self.state.last_heartbeat = time.monotonic()

    def become_follower(self, leader_id: int):
        with self.state.lock:
            self.state.role = "FOLLOWER"
            self.state.leader_id = leader_id
            self.state.last_heartbeat = time.monotonic()

        return True

    def start_election(self):
        with self.state.lock:
            self.state.role = "CANDIDATE"
            self.state.leader_id = None
            self.state.voted_for = self.state.node_id

        votes = 1
        total_nodes = len(self.config.peers) + 1
        quorum = total_nodes // 2 + 1

        for peer_id in self.config.peers:
            try:
                granted = self.rpc.call(
                    peer_id,
                    "request_vote",
                    self.state.node_id,
                )

                if granted:
                    votes += 1

            except Exception as error:
                print(f"Vote request to Node {peer_id} failed: {error}")

        if votes < quorum:
            print(f"Election failed: {votes}/{quorum} votes")
            return False

        self.become_leader()

        for peer_id in self.config.peers:
            try:
                self.rpc.call(
                    peer_id,
                    "become_follower",
                    self.state.node_id,
                )
            except Exception as error:
                print(f"Leader announcement to Node {peer_id} failed: {error}")

        print(f"Node {self.state.node_id} became leader")
        return True