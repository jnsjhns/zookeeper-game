import xmlrpc.client

from config import NODES
from rpc import TimeoutTransport


def call_node(node_id, method, *args):
    with xmlrpc.client.ServerProxy(
        f"http://{NODES[node_id]}",
        allow_none=True,
        transport=TimeoutTransport(20.0),
    ) as proxy:
        return getattr(proxy, method)(*args)


def main():
    print("Writing 100 via Node 3:")

    try:
        result = call_node(3, "write", 100)
        print(result)
    except Exception as error:
        print(f"Write failed or outcome unknown: {error}")
        return

    print("\nReading target from all nodes:")

    for node_id in NODES:
        try:
            print(call_node(node_id, "read"))
        except Exception as error:
            print(f"Node {node_id} unreachable: {error}")


if __name__ == "__main__":
    main()