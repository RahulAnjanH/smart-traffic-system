"""
run_node_with_ui.py — Start Node + Open UI Together
=====================================================
Starts the DistributedNode, saves config, then launches Streamlit
which will auto-create its own node in-process using the saved config.

Usage (Laptop A):
    python run_node_with_ui.py --node-id Node-1 --port 9001 --peers 192.168.1.102:9001

Usage (Same machine demo — 2 terminals):
    Terminal 1: python run_node_with_ui.py --node-id Node-1 --port 9001 --peers 127.0.0.1:9002
    Terminal 2: python run_node_with_ui.py --node-id Node-2 --port 9002 --peers 127.0.0.1:9001

SIMPLEST approach (recommended):
    Just run:  streamlit run distributed_ui.py
    And configure node-id, port, peers in the UI directly.
"""

import argparse
import subprocess
import sys
import os

def parse_peers(peer_strings: list) -> list:
    peers = []
    raw_peers = []
    for item in peer_strings:
        raw_peers.extend(part.strip() for part in item.replace(",", " ").split())

    for p in raw_peers:
        parts = p.strip().split(":")
        if len(parts) == 2:
            peers.append((parts[0], int(parts[1])))
    return peers


def main():
    parser = argparse.ArgumentParser(
        description="Start a distributed node AND open the Streamlit UI."
    )
    parser.add_argument("--node-id", required=True, help="Node name (e.g. Node-1)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, required=True, help="TCP listener port")
    parser.add_argument("--peers", nargs="*", default=[], help="Peer ip:port addresses")
    parser.add_argument("--ui-port", type=int, default=8501, help="Streamlit port")
    args = parser.parse_args()

    peer_str = ",".join(args.peers) if args.peers else ""

    # Launch Streamlit with node config passed as CLI args
    ui_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "distributed_ui.py")
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        ui_script,
        "--server.port", str(args.ui_port),
        "--server.headless", "true",
        "--",
        "--node-id", args.node_id,
        "--host", args.host,
        "--port", str(args.port),
        "--peers", peer_str,
    ]

    print(f"\n{'='*60}")
    print(f"  Starting Streamlit UI for {args.node_id}")
    print(f"  Open: http://localhost:{args.ui_port}")
    print(f"  The node will be created inside the UI.")
    print(f"  Click 'Create Node & Start' in the UI.")
    print(f"{'='*60}\n")

    try:
        proc = subprocess.Popen(cmd)
        proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
        proc.terminate()


if __name__ == "__main__":
    main()
