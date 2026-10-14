from config import NodeConfig
from election import ElectionManager
from heartbeat import HeartbeatManager
from log_store import LogStore
from replication import ReplicationManager
from rpc import RpcTransport
from state import NodeState


class Node:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.state = NodeState(node_id=config.node_id)
        self.log = LogStore(config.node_id)
        self.rpc = RpcTransport(config)

        self.election = ElectionManager(
            config,
            self.state,
            self.rpc,
        )

        self.heartbeat = HeartbeatManager(
            self.state,
            self.rpc,
        )

        self.replication = ReplicationManager(
            config,
            self.state,
            self.log,
            self.rpc,
        )

        self.rpc.register("hello", self.hello)
        self.rpc.register("ping", self.heartbeat.receive)
        self.rpc.register("request_vote", self.election.request_vote)
        self.rpc.register("become_follower", self.election.become_follower)

        self.rpc.register("write", self.replication.write)
        self.rpc.register("read", self.replication.read)
        self.rpc.register(
            "receive_proposal",
            self.replication.receive_proposal,
        )
        self.rpc.register(
            "receive_commit",
            self.replication.receive_commit,
        )

    def start(self):
        self.rpc.start()
        print(
            f"Node {self.config.node_id} started "
            f"on {self.config.host}:{self.config.port}"
        )

    def stop(self):
        self.rpc.stop()
        print(f"Node {self.config.node_id} stopped")

    def hello(self, sender_id):
        with self.state.lock:
            return {
                "status": "ok",
                "node_id": self.state.node_id,
                "role": self.state.role,
                "leader_id": self.state.leader_id,
            }