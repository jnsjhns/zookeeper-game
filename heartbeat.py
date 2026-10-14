import threading
import time

from rpc import RpcTransport
from state import NodeState


class HeartbeatManager:
    def __init__(
        self,
        state: NodeState,
        rpc: RpcTransport,
        interval: float = 1.0,
        timeout: float = 4.0,
        rpc_timeout: float = 0.5,
    ):
        self.state = state
        self.rpc = rpc

        self.interval = interval
        self.timeout = timeout
        self.rpc_timeout = rpc_timeout

        self._stop_event = threading.Event()
        self._timeout_reported = False
        self._unreachable_peers = set()

        self._sender_thread = threading.Thread(
            target=self._send_loop,
            name=f"heartbeat-sender-{state.node_id}",
            daemon=True,
        )

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"heartbeat-monitor-{state.node_id}",
            daemon=True,
        )

    def start(self):
        with self.state.lock:
            self.state.last_heartbeat = time.monotonic()
            self._timeout_reported = False

        self._sender_thread.start()
        self._monitor_thread.start()

    def stop(self):
        self._stop_event.set()

        for thread in (
            self._sender_thread,
            self._monitor_thread,
        ):
            if thread.is_alive():
                thread.join()

    def receive(self, leader_id: int):
        with self.state.lock:
            if (
                self.state.role != "FOLLOWER"
                or self.state.leader_id != leader_id
            ):
                return False

            was_timed_out = self._timeout_reported

            self.state.last_heartbeat = time.monotonic()
            self._timeout_reported = False

        if was_timed_out:
            print(f"Heartbeat from Leader {leader_id} restored")

        return True

    def check_timeout(self):
        with self.state.lock:
            if (
                self.state.role != "FOLLOWER"
                or self.state.leader_id is None
            ):
                return False

            elapsed = time.monotonic() - self.state.last_heartbeat

        return elapsed > self.timeout

    def send_once(self):
        with self.state.lock:
            if self.state.role != "LEADER":
                return

            leader_id = self.state.node_id

        for peer_id in self.rpc.config.peers:
            if self._stop_event.is_set():
                return

            try:
                result = self.rpc.call(
                    peer_id,
                    "ping",
                    leader_id,
                    timeout=self.rpc_timeout,
                )

                if result is not True:
                    raise RuntimeError("Heartbeat rejected")

            except Exception as error:
                if peer_id not in self._unreachable_peers:
                    print(
                        f"Heartbeat to Node {peer_id} failed: {error}"
                    )
                    self._unreachable_peers.add(peer_id)

            else:
                if peer_id in self._unreachable_peers:
                    print(f"Node {peer_id} responds to heartbeats again")
                    self._unreachable_peers.remove(peer_id)

    def _send_loop(self):
        while not self._stop_event.is_set():
            self.send_once()

            if self._stop_event.wait(self.interval):
                return

    def _monitor_loop(self):
        while not self._stop_event.wait(0.2):
            timed_out_leader = None

            with self.state.lock:
                if (
                    self.state.role == "FOLLOWER"
                    and self.state.leader_id is not None
                ):
                    elapsed = (
                        time.monotonic()
                        - self.state.last_heartbeat
                    )

                    if (
                        elapsed > self.timeout
                        and not self._timeout_reported
                    ):
                        self._timeout_reported = True
                        timed_out_leader = self.state.leader_id

            if timed_out_leader is not None:
                print(
                    f"Leader {timed_out_leader} heartbeat "
                    "timeout detected"
                )