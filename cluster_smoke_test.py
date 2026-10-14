import argparse
import xmlrpc.client


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a write/read smoke test against one cluster node."
    )

    parser.add_argument(
        "--node",
        type=int,
        required=True,
        choices=[1, 2, 3, 4, 5],
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
            "If omitted, 8000 + node ID is used."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if not 0 <= args.value <= 100:
        parser.error("--value must be between 0 and 100")

    port = args.port if args.port is not None else 8000 + args.node
    url = f"http://{args.host}:{port}"

    print(f"Connecting to Node {args.node} at {url}")

    client = xmlrpc.client.ServerProxy(url, allow_none=True)

    try:
        status = client.hello(args.node)
    except Exception as error:
        raise SystemExit(
            f"Could not reach Node {args.node} at {url}: {error}"
        )

    print(
        f"Node {status['node_id']} is reachable; "
        f"role={status['role']}, leader={status['leader_id']}"
    )

    print(f"Sending write({args.value})")
    write_result = client.handle_client_request(args.value)
    print(f"Write result: {write_result}")

    print("Reading current target")
    read_result = client.read()
    print(f"Read result: {read_result}")


if __name__ == "__main__":
    main()