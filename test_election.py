import xmlrpc.client

from config import NODES
from rpc import TimeoutTransport


for node_id, address in NODES.items():
    try:
        with xmlrpc.client.ServerProxy(
            f"http://{address}",
            allow_none=True,
            transport=TimeoutTransport(2.0),
        ) as proxy:
            print(proxy.hello(0))

    except Exception as error:
        print(f"Node {node_id} unreachable: {error}")