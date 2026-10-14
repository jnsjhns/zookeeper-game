import threading
import time
from dataclasses import dataclass, field


@dataclass
class NodeState:
    node_id: int
    role: str = "FOLLOWER"
    leader_id: int | None = None

    term: int = 0
    voted_for: int | None = None

    last_heartbeat: float = field(default_factory=time.monotonic)

    lock: threading.RLock = field(
        default_factory=threading.RLock,
        repr=False,
    )