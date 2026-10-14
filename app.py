from config import NodeConfig
from node import Node


def main():
    node1 = Node(
        NodeConfig(
            node_id=1,
            port=8001,
            peers={2: "127.0.0.1:8002"},
        )
    )

    node2 = Node(
        NodeConfig(
            node_id=2,
            port=8002,
            peers={1: "127.0.0.1:8001"},
        )
    )

    node1.start()
    node2.start()

    try:
        print("\nConnection test:")
        print(node1.rpc.call(2, "hello", 1))
        print(node2.rpc.call(1, "hello", 2))

        print("\nStarting election:")
        if not node1.election.start_election():
            return

        print("\nNode roles:")
        print(node1.hello(0))
        print(node2.hello(0))

        print("\nSending one heartbeat:")
        node1.heartbeat.send_once()
        print("Node 2 timeout:", node2.heartbeat.check_timeout())

        print("\nClient request via leader:")
        print(node1.handle_client_request(50))

        print("\nClient request via follower:")
        print(node2.handle_client_request(62))

        input("\nPress Enter to stop the nodes...")

    except KeyboardInterrupt:
        print("\nStopping...")

    finally:
        node1.stop()
        node2.stop()


if __name__ == "__main__":
    main()