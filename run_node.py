"""
run_node.py — CLI Entry Point for a Distributed Node
=====================================================
Start a single distributed traffic-system node from the command line.

Usage (Laptop A):
    python run_node.py --node-id Node-1 --port 9001 --peers 192.168.1.102:9001

Usage (Laptop B):
    python run_node.py --node-id Node-2 --port 9001 --peers 192.168.1.101:9001

To start the UI (separate terminal):
    streamlit run distributed_ui.py -- --port 9001

The node runs until you press Ctrl+C.
"""

import argparse
import signal
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from distributed.node import DistributedNode


def parse_peers(peer_strings: list) -> list:
    """Parse 'ip:port' strings into [(ip, port), ...] tuples."""
    peers = []
    raw_peers = []
    for item in peer_strings:
        raw_peers.extend(part.strip() for part in item.replace(",", " ").split())

    for p in raw_peers:
        parts = p.strip().split(":")
        if len(parts) == 2:
            peers.append((parts[0], int(parts[1])))
        else:
            print(f"Warning: Invalid peer format '{p}', expected 'ip:port'")
    return peers


def main():
    parser = argparse.ArgumentParser(
        description="Start a distributed traffic system node.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Node 1 (Laptop A, IP 192.168.1.101):
  python run_node.py --node-id Node-1 --port 9001 --peers 192.168.1.102:9001

  # Node 2 (Laptop B, IP 192.168.1.102):
  python run_node.py --node-id Node-2 --port 9001 --peers 192.168.1.101:9001

  # Two nodes on the SAME machine (for testing):
  python run_node.py --node-id Node-1 --port 9001 --peers 127.0.0.1:9002
  python run_node.py --node-id Node-2 --port 9002 --peers 127.0.0.1:9001
        """,
    )
    parser.add_argument("--node-id", required=True,
                        help="Unique name for this node (e.g. Node-1)")
    parser.add_argument("--host", default="0.0.0.0",
                        help="IP to bind (default: 0.0.0.0 = all interfaces)")
    parser.add_argument("--port", type=int, required=True,
                        help="TCP port for P2P listener")
    parser.add_argument("--peers", nargs="+", default=[],
                        help="Peer addresses as ip:port (space separated)")

    args = parser.parse_args()
    peers = parse_peers(args.peers)

    # ── Create and start the node ───────────────────────────────────
    node = DistributedNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        peers=peers,
    )
    node.start()

    # ── Handle Ctrl+C gracefully ────────────────────────────────────
    def shutdown(sig, frame):
        print(f"\n\nShutting down {args.node_id}...")
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    print(f"\nNode {args.node_id} is running. Press Ctrl+C to stop.\n")

    # Keep main thread alive
    try:
        while node.running:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
