import time


class HeartbeatManager:
    def __init__(self, node, timeout=3.0):
        self.node = node
        self.timeout = timeout

    def check_timeout(self):
        return (time.time() - self.node.last_heartbeat) > self.timeout