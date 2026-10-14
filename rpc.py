import threading
import xmlrpc.client
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

from config import NodeConfig


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True


class TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout
        return connection


class RpcTransport:
    def __init__(self, config: NodeConfig, timeout: float = 2.0):
        self.config = config
        self.timeout = timeout

        self.server = ThreadedXMLRPCServer(
            (config.host, config.port),
            allow_none=True,
            logRequests=False,
        )

        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )

    def register(self, name: str, handler):
        self.server.register_function(handler, name)

    def start(self):
        self.thread.start()

    def stop(self):
        if self.thread.is_alive():
            self.server.shutdown()
            self.thread.join()

        self.server.server_close()

    def call(self, peer_id: int, method: str, *args, timeout=None):
        address = self.config.peers.get(peer_id)

        if address is None:
            raise ValueError(f"Unknown peer: Node {peer_id}")

        effective_timeout = self.timeout if timeout is None else timeout
        transport = TimeoutTransport(effective_timeout)

        with xmlrpc.client.ServerProxy(
            f"http://{address}",
            allow_none=True,
            transport=transport,
        ) as proxy:
            return getattr(proxy, method)(*args)