from config import NodeConfig
from election import ElectionManager
from heartbeat import HeartbeatManager
from rpc import RpcTransport
from state import NodeState


class Node:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.state = NodeState(node_id=config.node_id)
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

        self.rpc.register("hello", self.hello)
        self.rpc.register("ping", self.heartbeat.receive)
        self.rpc.register("request_vote", self.election.request_vote)
        self.rpc.register("become_follower", self.election.become_follower)
        self.rpc.register("handle_client_request", self.handle_client_request)

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

    def handle_client_request(self, value):
        with self.state.lock:
            role = self.state.role
            leader_id = self.state.leader_id

        if role == "LEADER":
            return {
                "status": "accepted",
                "node_id": self.state.node_id,
                "value": value,
            }

        if leader_id is None or leader_id == self.state.node_id:
            return {
                "status": "error",
                "message": "No usable leader known",
            }

        try:
            return self.rpc.call(
                leader_id,
                "handle_client_request",
                value,
            )
        except Exception as error:
            return {
                "status": "error",
                "message": f"Leader unreachable: {error}",
            }