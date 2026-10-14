from dataclasses import dataclass, field


@dataclass
class NodeConfig:
    node_id: int
    host: str = "127.0.0.1"
    port: int = 8001
    peers: dict[int, str] = field(default_factory=dict)