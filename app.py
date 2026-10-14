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
    )

    args = parser.parse_args()
    node = Node(create_config(args.node))
    node.start()

    print(f"Log file: {node.log.path.resolve()}")
    print("Waiting for automatic leader election...")
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