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
    parser.add_argument(
        "--guesses-per-round",
        type=int,
        default=2,
        help=(
            "Committed guesses required to finish a round "
            "(minimum: 2, default: 2)"
        ),
    )

    args = parser.parse_args()
    if args.guesses_per_round < 2:
        parser.error("--guesses-per-round must be at least 2")

    node = Node(
        create_config(args.node),
        guesses_per_round=args.guesses_per_round,
    )
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