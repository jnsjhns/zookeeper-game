import time

from rpc import RpcTransport
from state import NodeState


class HeartbeatManager:
    def __init__(
        self,
        state: NodeState,
        rpc: RpcTransport,
        timeout: float = 3.0,
    ):
        self.state = state
        self.rpc = rpc
        self.timeout = timeout

    def receive(self, leader_id: int):
        with self.state.lock:
            self.state.last_heartbeat = time.monotonic()
            self.state.role = "FOLLOWER"
            self.state.leader_id = leader_id

        return True

    def check_timeout(self):
        with self.state.lock:
            elapsed = time.monotonic() - self.state.last_heartbeat

        return elapsed > self.timeout

    def send_once(self):
        with self.state.lock:
            if self.state.role != "LEADER":
                return

            leader_id = self.state.node_id

        for peer_id in self.rpc.config.peers:
            try:
                self.rpc.call(peer_id, "ping", leader_id)
            except Exception as error:
                print(f"Heartbeat to Node {peer_id} failed: {error}")