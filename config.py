from dataclasses import dataclass, field


NODES = {
    1: "127.0.0.1:8001",
    2: "127.0.0.1:8002",
    3: "127.0.0.1:8003",
    4: "127.0.0.1:8004",
    5: "127.0.0.1:8005",
}


@dataclass
class NodeConfig:
    node_id: int
    host: str = "127.0.0.1"
    port: int = 8001
    peers: dict[int, str] = field(default_factory=dict)


def create_config(node_id: int):
    if node_id not in NODES:
        raise ValueError(f"Unknown node ID: {node_id}")

    host, port = NODES[node_id].split(":")

    peers = {
        peer_id: address
        for peer_id, address in NODES.items()
        if peer_id != node_id
    }

    return NodeConfig(
        node_id=node_id,
        host=host,
        port=int(port),
        peers=peers,
    )