import argparse
import time

from config import NODES, create_config
from node import Node


def main():
    parser = argparse.ArgumentParser(
        description="Start one ZooKeeper-like node"
    )

    parser.add_argument(
        "--node",
        type=int,
        required=True,
        choices=sorted(NODES),
        help="Node ID to start",
    )

    args = parser.parse_args()

    config = create_config(args.node)
    node = Node(config)

    # Temporary setup: fixed leader, no automatic election yet.
    if config.node_id == 1:
        node.election.become_leader()
    else:
        node.election.become_follower(leader_id=1)

    node.start()

    print(f"Role: {node.state.role}")
    print(f"Leader: {node.state.leader_id}")
    print(f"Log file: {node.log.path.resolve()}")
    print("Press Ctrl+C to stop this node.")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping node...")

    finally:
        node.stop()


if __name__ == "__main__":
    main()