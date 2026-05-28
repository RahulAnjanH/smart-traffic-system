"""
config.py — Node Configuration
===============================
Stores the runtime configuration for a single distributed node.
Values are set via CLI arguments in run_node.py.

Distributed System Concept:
    Each node in a P2P system needs to know its own identity (node_id),
    which port to listen on, and the addresses of its peers.
"""


class NodeConfig:
    """Immutable configuration for one distributed node."""

    def __init__(self, node_id: str, host: str, port: int, peers: list):
        """
        Args:
            node_id:  Unique name for this node (e.g. "Node-1")
            host:     IP to bind the listener socket ("0.0.0.0" = all interfaces)
            port:     TCP port for the P2P listener
            peers:    List of (ip, port) tuples for other nodes
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers          # [(ip, port), ...]

    def __repr__(self):
        return (
            f"NodeConfig(node_id={self.node_id!r}, host={self.host!r}, "
            f"port={self.port}, peers={self.peers})"
        )
