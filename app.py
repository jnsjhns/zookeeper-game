import time

from config import NodeConfig
from election import ElectionManager
from heartbeat import HeartbeatManager
from node import Node


def main():
    node1 = Node(
        NodeConfig(node_id=1, host="127.0.0.1", port=8001, peers=["127.0.0.1:8002"])
    )
    node2 = Node(
        NodeConfig(node_id=2, host="127.0.0.1", port=8002, peers=["127.0.0.1:8001"])
    )

    node1.start()
    node2.start()

    time.sleep(0.5)

    print("Connection test:")
    print(node1.call_peer("127.0.0.1:8002", "hello", 1))
    print(node2.call_peer("127.0.0.1:8001", "hello", 2))

    election = ElectionManager(node1)
    heartbeat = HeartbeatManager(node1)

    print("Starting election...")
    election.start_election()
    print(f"Node 1 role: {node1.role}, leader: {node1.leader_id}")

    print("Heartbeat check:")
    print("timeout reached?", heartbeat.check_timeout())

    print("Client request via leader:")
    print(node1.handle_client_request("test-value"))

    input("Press Enter to stop the nodes...")

    node1.stop()
    node2.stop()


if __name__ == "__main__":
    main()