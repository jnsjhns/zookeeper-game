import threading
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer

from config import NodeConfig


class Node:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.role = "FOLLOWER"
        self.leader_id = None
        self.voted_for = None
        self.last_heartbeat = time.time()

        self.server = SimpleXMLRPCServer(
            (self.config.host, self.config.port),
            allow_none=True,
            logRequests=False
        )

        self.server.register_function(self.hello, "hello")
        self.server.register_function(self.ping, "ping")
        self.server.register_function(self.request_vote, "request_vote")
        self.server.register_function(self.become_leader, "become_leader")
        self.server.register_function(self.become_follower, "become_follower")
        self.server.register_function(self.handle_client_request, "handle_client_request")

        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self._thread.start()
        print(f"Node {self.config.node_id} started on {self.config.host}:{self.config.port}")

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        print(f"Node {self.config.node_id} stopped")

    def hello(self, sender_id):
        return {
            "status": "ok",
            "node_id": self.config.node_id,
            "role": self.role,
            "leader_id": self.leader_id,
            "message": "hello from node"
        }

    def ping(self, sender_id):
        self.last_heartbeat = time.time()
        self.role = "FOLLOWER"
        self.leader_id = sender_id
        return True

    def request_vote(self, candidate_id):
        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            return True
        return False

    def become_leader(self, leader_id):
        self.role = "LEADER"
        self.leader_id = leader_id
        self.last_heartbeat = time.time()
        return True

    def become_follower(self, leader_id):
        self.role = "FOLLOWER"
        self.leader_id = leader_id
        self.last_heartbeat = time.time()
        return True

    def handle_client_request(self, value):
        if self.role != "LEADER":
            if self.leader_id is not None:
                leader_peer = self._peer_by_id(self.leader_id)
                if leader_peer:
                    return self.call_peer(leader_peer, "handle_client_request", value)
            return {
                "status": "forward",
                "leader_id": self.leader_id,
                "value": value
            }

        return {
            "status": "accepted",
            "node_id": self.config.node_id,
            "value": value
        }

    def call_peer(self, peer: str, method: str, *args):
        host, port = peer.split(":")
        proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}", allow_none=True)
        return getattr(proxy, method)(*args)

    def _peer_by_id(self, node_id):
        for peer in self.config.peers:
            host, port = peer.split(":")
            if int(port) == 8000 + node_id:
                return peer
        return None