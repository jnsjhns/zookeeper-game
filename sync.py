import threading

from log_store import LogStore


class FollowerSyncManager:
    def __init__(
        self,
        config,
        state,
        log,
        rpc,
        replication,
        interval=3.0,
    ):
        self.config = config
        self.state = state
        self.log = log
        self.rpc = rpc
        self.replication = replication

        self.interval = interval
        self.peer_timeout = 1.0

        self._stop_event = threading.Event()
        self._last_errors = {}

        self._thread = threading.Thread(
            target=self._run,
            name=f"follower-sync-{state.node_id}",
            daemon=True,
        )

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()

        if self._thread.is_alive():
            self._thread.join()

    def _run(self):
        while not self._stop_event.wait(self.interval):
            with self.state.lock:
                is_leader = self.state.role == "LEADER"

            if not is_leader:
                continue

            self.sync_followers()

    def sync_followers(self):
        # Serialize synchronization with writes on this leader.
        with self.replication._write_lock:
            with self.state.lock:
                if self.state.role != "LEADER":
                    return

                term = self.state.term
                leader_id = self.state.node_id
                entries = self.log.all_entries()

            for peer_id in self.config.peers:
                if self._stop_event.is_set():
                    return

                with self.state.lock:
                    if (
                        self.state.role != "LEADER"
                        or self.state.term != term
                    ):
                        return

                try:
                    result = self.rpc.call(
                        peer_id,
                        "sync_install",
                        term,
                        leader_id,
                        entries,
                        timeout=self.peer_timeout,
                    )

                    if (
                        not isinstance(result, dict)
                        or result.get("status") != "ok"
                        or result.get("node_id") != peer_id
                        or result.get("term") != term
                    ):
                        raise RuntimeError(
                            "Invalid synchronization response"
                        )

                    previously_failed = peer_id in self._last_errors
                    self._last_errors.pop(peer_id, None)

                    if result["changed"] or previously_failed:
                        print(
                            f"Node {peer_id} synchronized: "
                            f"{result['entry_count']} entries"
                        )

                except Exception as error:
                    message = str(error)

                    if self._last_errors.get(peer_id) != message:
                        print(
                            f"Sync to Node {peer_id} failed: {message}"
                        )
                        self._last_errors[peer_id] = message

    def install(self, term, leader_id, entries):
        if not isinstance(entries, list):
            raise ValueError("Synchronization entries must be a list")

        incoming = {}
        previous_tag = None

        for entry in entries:
            LogStore._validate(entry)
            tag = (entry["epoch"], entry["seq"])

            if previous_tag is not None and tag <= previous_tag:
                raise ValueError(
                    "Synchronization entries must have unique, "
                    "increasing tags"
                )

            incoming[tag] = entry
            previous_tag = tag

        with self.state.lock:
            if (
                self.state.role != "FOLLOWER"
                or self.state.leader_id != leader_id
                or self.state.term != term
            ):
                raise ValueError(
                    "Synchronization is not from the current leader"
                )

            local_entries = self.log.all_entries()

            for local in local_entries:
                tag = (local["epoch"], local["seq"])
                leader_entry = incoming.get(tag)

                if leader_entry is None:
                    raise ValueError(
                        f"Local entry {tag} is missing from leader log"
                    )

                if (
                    local["op"] != leader_entry["op"]
                    or local["value"] != leader_entry["value"]
                ):
                    raise ValueError(
                        f"Conflicting entry for tag {tag}"
                    )

                if (
                    local["status"] == "COMMITTED"
                    and leader_entry["status"] != "COMMITTED"
                ):
                    raise ValueError(
                        f"Leader would downgrade committed entry {tag}"
                    )

            changed = local_entries != entries

            if changed:
                self.log.install_entries(entries)

            if self.log.all_entries() != entries:
                raise RuntimeError(
                    "Installed log does not match leader snapshot"
                )

            result = {
                "status": "ok",
                "node_id": self.state.node_id,
                "term": self.state.term,
                "changed": changed,
                "entry_count": len(entries),
            }

        if changed:
            print(
                f"Log synchronized from Leader {leader_id}: "
                f"{len(entries)} entries"
            )

        return result