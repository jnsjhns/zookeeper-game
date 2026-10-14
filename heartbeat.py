import threading
import time


class HeartbeatManager:
    def __init__(
        self,
        state,
        rpc,
        election,
        interval=1.0,
        timeout=4.0,
    ):
        self.state = state
        self.rpc = rpc
        self.election = election

        self.interval = interval
        self.timeout = timeout

        self._stop_event = threading.Event()
        self._timeout_reported = False
        self._unreachable_peers = set()

        self._sender_thread = threading.Thread(
            target=self._send_loop,
            daemon=True,
        )
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
        )

    def start(self):
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

    def receive(self, term, leader_id):
        accepted = self.election.receive_heartbeat(term, leader_id)

        if accepted:
            with self.state.lock:
                was_timed_out = self._timeout_reported
                self._timeout_reported = False

            if was_timed_out:
                print(f"Heartbeat from Leader {leader_id} received")

        return accepted

    def _send_loop(self):
        while not self._stop_event.is_set():
            with self.state.lock:
                is_leader = self.state.role in {
                    "LEADER",
                    "RECOVERING",
                }
                term = self.state.term
                leader_id = self.state.node_id

            if is_leader:
                for peer_id in self.rpc.config.peers:
                    if self._stop_event.is_set():
                        return

                    try:
                        accepted = self.rpc.call(
                            peer_id,
                            "ping",
                            term,
                            leader_id,
                            timeout=0.5,
                        )

                        if accepted is not True:
                            raise RuntimeError("Heartbeat rejected")

                    except Exception as error:
                        if peer_id not in self._unreachable_peers:
                            print(
                                f"Heartbeat to Node {peer_id} "
                                f"failed: {error}"
                            )
                            self._unreachable_peers.add(peer_id)

                    else:
                        if peer_id in self._unreachable_peers:
                            print(f"Node {peer_id} responds again")
                            self._unreachable_peers.remove(peer_id)

            if self._stop_event.wait(self.interval):
                return

    def _monitor_loop(self):
        while not self._stop_event.wait(0.2):
            leader_id = None

            with self.state.lock:
                if (
                    self.state.role == "FOLLOWER"
                    and self.state.leader_id is not None
                    and time.monotonic() - self.state.last_heartbeat
                    > self.timeout
                    and not self._timeout_reported
                ):
                    self._timeout_reported = True
                    leader_id = self.state.leader_id

            if leader_id is not None:
                print(
                    f"Leader {leader_id} heartbeat timeout detected"
                )