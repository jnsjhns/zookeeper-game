from config import NodeConfig
from node import Node

def main():
    node1 = Node(NodeConfig(node_id=1, host="127.0.0.1", port=8001, peers=["127.0.0.1:8002"]))
    node2 = Node(NodeConfig(node_id=2, host="127.0.0.1", port=8002, peers=["127.0.0.1:8001"]))

    node1.start()
    node2.start()

    print("Connection test:")
    print(node1.call_peer("127.0.0.1:8002", "hello", 1))
    print(node2.call_peer("127.0.0.1:8001", "hello", 2))

    node1.become_leader(1)
    print(node1.handle_client_request("test-value"))
    print(node2.handle_client_request("test-value"))

    input("Press Enter to stop the nodes...")

    node1.stop()
    node2.stop()

if __name__ == "__main__":
    main()