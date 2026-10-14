import json
import os
import threading
from pathlib import Path


class LogStore:
    def __init__(self, node_id: int, data_dir: str = "data"):
        self.path = Path(data_dir) / f"node_{node_id}.log"
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._entries = self._load()

    def append_proposal(
        self,
        epoch: int,
        seq: int,
        op: str,
        value: int,
    ):
        entry = {
            "epoch": epoch,
            "seq": seq,
            "op": op,
            "value": value,
            "status": "PROPOSED",
        }
        self._validate(entry)

        with self._lock:
            existing = self._find(epoch, seq)

            if existing is not None:
                if existing["op"] != op or existing["value"] != value:
                    raise ValueError(
                        f"Conflicting proposal for tag ({epoch}, {seq})"
                    )

                return existing.copy()

            entries = [item.copy() for item in self._entries]
            entries.append(entry)
            entries.sort(key=self._tag)

            self._persist(entries)
            self._entries = entries

            return entry.copy()

    def mark_committed(self, epoch: int, seq: int):
        with self._lock:
            existing = self._find(epoch, seq)

            if existing is None:
                raise ValueError(
                    f"No proposal found for tag ({epoch}, {seq})"
                )

            if existing["status"] == "COMMITTED":
                return existing.copy()

            entries = [item.copy() for item in self._entries]

            for entry in entries:
                if self._tag(entry) == (epoch, seq):
                    entry["status"] = "COMMITTED"
                    committed = entry

            self._persist(entries)
            self._entries = entries

            return committed.copy()

    def latest_tag(self):
        with self._lock:
            if not self._entries:
                return (0, 0)

            return self._tag(self._entries[-1])

    def latest_committed(self):
        with self._lock:
            for entry in reversed(self._entries):
                if entry["status"] == "COMMITTED":
                    return entry.copy()

            return None

    def all_entries(self):
        with self._lock:
            return [entry.copy() for entry in self._entries]

    def _find(self, epoch: int, seq: int):
        for entry in self._entries:
            if self._tag(entry) == (epoch, seq):
                return entry

        return None

    @staticmethod
    def _tag(entry):
        return (entry["epoch"], entry["seq"])

    @staticmethod
    def _validate(entry):
        required = {"epoch", "seq", "op", "value", "status"}

        if not isinstance(entry, dict) or set(entry) != required:
            raise ValueError("Invalid log entry fields")

        for field in ("epoch", "seq", "value"):
            if type(entry[field]) is not int:
                raise ValueError(f"{field} must be an integer")

        if entry["epoch"] < 0 or entry["seq"] < 1:
            raise ValueError("epoch must be >= 0 and seq must be >= 1")

        if not 0 <= entry["value"] <= 100:
            raise ValueError("value must be between 0 and 100")

        if entry["op"] not in {"WRITE", "GUESS"}:
            raise ValueError("op must be WRITE or GUESS")

        if entry["status"] not in {"PROPOSED", "COMMITTED"}:
            raise ValueError("Invalid entry status")

    def _load(self):
        if not self.path.exists():
            return []

        entries = []
        seen_tags = set()

        with self.path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                try:
                    entry = json.loads(line)
                    self._validate(entry)
                    tag = self._tag(entry)

                    if tag in seen_tags:
                        raise ValueError(f"Duplicate tag: {tag}")

                except (ValueError, TypeError) as error:
                    raise ValueError(
                        f"Invalid log at {self.path}, "
                        f"line {line_number}: {error}"
                    ) from error

                seen_tags.add(tag)
                entries.append(entry)

        entries.sort(key=self._tag)
        return entries

    def _persist(self, entries):
        temporary_path = self.path.with_suffix(".log.tmp")

        try:
            with temporary_path.open("w", encoding="utf-8") as file:
                for entry in entries:
                    file.write(json.dumps(entry) + "\n")

                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary_path, self.path)

        finally:
            if temporary_path.exists():
                temporary_path.unlink()