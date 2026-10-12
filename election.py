class ElectionManager:
    def __init__(self, node):
        self.node = node

    def start_election(self):
        self.node.role = "CANDIDATE"
        self.node.voted_for = self.node.config.node_id
        votes = 1

        for peer in self.node.config.peers:
            try:
                if self.node.call_peer(peer, "request_vote", self.node.config.node_id):
                    votes += 1
            except Exception:
                pass

        if votes > (len(self.node.config.peers) + 1) / 2:
            self.node.become_leader(self.node.config.node_id)
            print(f"Node {self.node.config.node_id} became leader")
            return True

        return False