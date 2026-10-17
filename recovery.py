import threading

from log_store import LogStore


class RecoveryManager:
    def __init__(
        self,
        config,
        state,
        log,
        rpc,
        replication,
    ):
        self.config = config
        self.state = state
        self.log = log
        self.rpc = rpc
        self.replication = replication

        self.quorum = (len(config.peers) + 1) // 2 + 1
        self.peer_timeout = 1.0

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"recovery-{state.node_id}",
            daemon=True,
        )

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()

        if self._thread.is_alive():
            self._thread.join()

    def get_log(self):
        with self.state.lock:
            return {
                "node_id": self.state.node_id,
                "term": self.state.term,
                "entries": self.log.all_entries(),
            }

    def _run(self):
        while not self._stop_event.wait(2.0):
            with self.state.lock:
                recovering = self.state.role == "RECOVERING"
                term = self.state.term

            if not recovering:
                continue

            try:
                self.recover(term)

            except Exception as error:
                print(f"Recovery not completed: {error}")

    def _still_recovering(self, term):
        with self.state.lock:
            return (
                self.state.role == "RECOVERING"
                and self.state.leader_id == self.state.node_id
                and self.state.term == term
            )

    @staticmethod
    def _merge(logs):
        merged = {}

        for entries in logs:
            for entry in entries:
                LogStore._validate(entry)

                tag = (entry["epoch"], entry["seq"])
                existing = merged.get(tag)

                if existing is None:
                    merged[tag] = entry.copy()
                    continue

                if (
                    existing["op"] != entry["op"]
                    or existing["value"] != entry["value"]
                ):
                    raise ValueError(
                        f"Conflicting entries for tag {tag}"
                    )

                existing_timestamp = existing.get("timestamp")
                incoming_timestamp = entry.get("timestamp")
                if (
                    existing_timestamp is not None
                    and incoming_timestamp is not None
                    and existing_timestamp != incoming_timestamp
                ):
                    raise ValueError(
                        f"Conflicting timestamps for tag {tag}"
                    )

                if existing_timestamp is None and incoming_timestamp is not None:
                    existing["timestamp"] = incoming_timestamp

                if entry["status"] == "COMMITTED":
                    existing["status"] = "COMMITTED"

        return sorted(
            merged.values(),
            key=lambda entry: (
                entry["epoch"],
                entry["seq"],
            ),
        )

    def recover(self, term):
        if not self._still_recovering(term):
            return

        print(
            f"RECOVERY START: Node {self.state.node_id}, "
            f"term {term}"
        )

        logs = [self.log.all_entries()]

        # Phase 1: collect logs.
        for peer_id in self.config.peers:
            if self._stop_event.is_set():
                return

            try:
                response = self.rpc.call(
                    peer_id,
                    "recovery_get_log",
                    timeout=self.peer_timeout,
                )

            except Exception as error:
                print(
                    f"Recovery read from Node {peer_id} "
                    f"failed: {error}"
                )
                continue

            if response["node_id"] != peer_id:
                raise ValueError(
                    f"Unexpected identity from Node {peer_id}"
                )

            if response["term"] > term:
                raise RuntimeError(
                    "A peer has a newer election term"
                )

            logs.append(response["entries"])

        if len(logs) < self.quorum:
            raise RuntimeError(
                "Not enough logs available for recovery"
            )

        entries = self._merge(logs)

        with self.state.lock:
            if not self._still_recovering(term):
                return

            self.log.install_entries(entries)

        synchronized = {self.state.node_id}

        # Phase 2: persist the selected history on peers.
        for peer_id in self.config.peers:
            if self._stop_event.is_set():
                return

            if not self._still_recovering(term):
                return

            try:
                result = self.rpc.call(
                    peer_id,
                    "recovery_install",
                    term,
                    self.state.node_id,
                    entries,
                    timeout=self.peer_timeout,
                )

                if result is True:
                    synchronized.add(peer_id)

            except Exception as error:
                print(
                    f"Recovery install on Node {peer_id} "
                    f"failed: {error}"
                )

        if len(synchronized) < self.quorum:
            raise RuntimeError(
                f"Recovery has only "
                f"{len(synchronized)}/{self.quorum} ACKs"
            )

        tags = [
            [entry["epoch"], entry["seq"]]
            for entry in entries
        ]

        # Explicit commit after quorum storage.
        with self.state.lock:
            if not self._still_recovering(term):
                return

            self.log.commit_entries(tags)

        committed = {self.state.node_id}

        # Phase 3: distribute the recovery commit.
        for peer_id in sorted(
            synchronized - {self.state.node_id}
        ):
            if self._stop_event.is_set():
                return

            if not self._still_recovering(term):
                return

            try:
                result = self.rpc.call(
                    peer_id,
                    "recovery_commit",
                    term,
                    self.state.node_id,
                    tags,
                    timeout=self.peer_timeout,
                )

                if result is True:
                    committed.add(peer_id)

            except Exception as error:
                print(
                    f"Recovery commit on Node {peer_id} "
                    f"failed: {error}"
                )

        if len(committed) < self.quorum:
            raise RuntimeError(
                "Recovery commits not confirmed by a quorum"
            )

        highest_epoch = max(
            (entry["epoch"] for entry in entries),
            default=0,
        )

        new_epoch = max(highest_epoch + 1, term)

        # Phase 4: enable writes only if leadership is unchanged.
        with self.state.lock:
            if not self._still_recovering(term):
                return

            self.replication.begin_epoch(new_epoch)
            self.state.role = "LEADER"

        pending_round = self.replication.complete_pending_round()
        if pending_round is not None and pending_round["status"] != "committed":
            print(
                "Pending guess round could not be completed: "
                f"{pending_round['write_result']}"
            )

        print(
            f"RECOVERY COMPLETE: Node {self.state.node_id} is LEADER, "
            f"term {term}, epoch {new_epoch}; "
            f"{len(committed)}/{len(self.config.peers) + 1} nodes synchronized"
        )

    def install(self, term, leader_id, entries):
        with self.state.lock:
            self._check_leader(term, leader_id)

            # Reject unobserved local entries rather than silently
            # acknowledging a different history.
            incoming_tags = {
                (entry["epoch"], entry["seq"])
                for entry in entries
            }

            local_tags = {
                (entry["epoch"], entry["seq"])
                for entry in self.log.all_entries()
            }

            if not local_tags.issubset(incoming_tags):
                raise ValueError(
                    "Local log contains entries missing "
                    "from recovery history; retry collection"
                )

            self.log.install_entries(entries)

        print(
            f"Recovery log received from Leader {leader_id}"
        )

        return True

    def commit(self, term, leader_id, tags):
        with self.state.lock:
            self._check_leader(term, leader_id)
            self.log.commit_entries(tags)

        print(
            f"Recovery entries committed "
            f"from Leader {leader_id}"
        )

        return True

    def _check_leader(self, term, leader_id):
        # Caller holds state.lock.
        if (
            self.state.role != "FOLLOWER"
            or self.state.term != term
            or self.state.leader_id != leader_id
        ):
            raise ValueError(
                "Recovery message is not from the current leader"
            )