import argparse
import xmlrpc.client


NODE_IDS = (1, 2, 3, 4, 5)
BASE_PORT = 8000


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Send a write request to one cluster node and read the "
            "current target afterwards."
        )
    )

    parser.add_argument(
        "--node",
        type=int,
        required=True,
        choices=NODE_IDS,
        help="ID of the node to contact (1-5)",
    )

    parser.add_argument(
        "--value",
        type=int,
        required=True,
        help="Target value to write (0-100)",
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host of the target node (default: 127.0.0.1)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            "Port of the target node. "
            "If omitted, 8000 plus the node ID is used."
        ),
    )

    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Do not write; only read the current target",
    )

    return parser.parse_args()


def build_url(args):
    port = args.port if args.port is not None else BASE_PORT + args.node
    return f"http://{args.host}:{port}"


def main():
    args = parse_args()

    if not 0 <= args.value <= 100:
        raise SystemExit("Error: --value must be between 0 and 100")

    url = build_url(args)
    client = xmlrpc.client.ServerProxy(url, allow_none=True)

    print(f"Connecting to Node {args.node} at {url}")

    try:
        status = client.hello(args.node)
    except Exception as error:
        raise SystemExit(
            f"Error: could not reach Node {args.node} at {url}: {error}"
        )

    print(
        f"Node {status['node_id']} is reachable; "
        f"role={status['role']}, leader={status['leader_id']}"
    )

    if not args.read_only:
        print(f"Sending write({args.value})")

        try:
            write_result = client.write(args.value)
        except Exception as error:
            raise SystemExit(f"Error: write failed: {error}")

        print(f"Write result: {write_result}")

    print("Reading current target")

    try:
        read_result = client.read()
    except Exception as error:
        raise SystemExit(f"Error: read failed: {error}")

    print(f"Read result: {read_result}")


if __name__ == "__main__":
    main()