from config import NodeConfig
from election import ElectionManager
from heartbeat import HeartbeatManager
from log_store import LogStore
from recovery import RecoveryManager
from replication import ReplicationManager
from rpc import RpcTransport
from state import NodeState
from sync import FollowerSyncManager


class Node:
    def __init__(self, config: NodeConfig, guesses_per_round: int = 2):
        self.config = config

        self.state = NodeState(node_id=config.node_id)
        self.log = LogStore(config.node_id)
        self.rpc = RpcTransport(config)

        self.election = ElectionManager(
            config,
            self.state,
            self.log,
            self.rpc,
        )

        self.heartbeat = HeartbeatManager(
            self.state,
            self.rpc,
            self.election,
        )

        self.replication = ReplicationManager(
            config,
            self.state,
            self.log,
            self.rpc,
            guesses_per_round=guesses_per_round,
        )

        self.recovery = RecoveryManager(
            config,
            self.state,
            self.log,
            self.rpc,
            self.replication,
        )

        self.sync = FollowerSyncManager(
            config,
            self.state,
            self.log,
            self.rpc,
            self.replication,
        )

        # Register RPC handlers for various operations.
        # Each handler is associated with a specific method in the corresponding manager class.

        # Status
        self.rpc.register("hello", self.hello)

        # Election
        self.rpc.register(
            "election_status",
            self.election.get_status,
        )
        self.rpc.register(
            "request_vote",
            self.election.request_vote,
        )

        # Heartbeats
        self.rpc.register(
            "ping",
            self.heartbeat.receive,
        )

        # Client operations
        self.rpc.register(
            "write",
            self.replication.write,
        )
        self.rpc.register(
            "guess",
            self.replication.guess,
        )
        self.rpc.register(
            "read",
            self.replication.read,
        )

        # Replication
        self.rpc.register(
            "receive_proposal",
            self.replication.receive_proposal,
        )
        self.rpc.register(
            "receive_commit",
            self.replication.receive_commit,
        )

        # Recovery after election
        self.rpc.register(
            "recovery_get_log",
            self.recovery.get_log,
        )
        self.rpc.register(
            "recovery_install",
            self.recovery.install,
        )
        self.rpc.register(
            "recovery_commit",
            self.recovery.commit,
        )

        # Catch-up during normal leader operation
        self.rpc.register(
            "sync_install",
            self.sync.install,
        )

    def start(self):
        self.rpc.start()

        print(
            f"Node {self.config.node_id} started "
            f"on {self.config.host}:{self.config.port}"
        )

        self.heartbeat.start()
        self.recovery.start()
        self.sync.start()
        self.election.start()

    def stop(self):
        self.election.stop()
        self.sync.stop()
        self.recovery.stop()
        self.heartbeat.stop()
        self.rpc.stop()

        print(f"Node {self.config.node_id} stopped")

    def hello(self, sender_id):
        return self.election.get_status()