"""
run_demo.py — Launch 2 Nodes on the Same Machine (for Testing)
==============================================================
This script starts two distributed nodes on localhost with different
ports so you can test the P2P system without needing two laptops.

Usage:
    python run_demo.py

This will:
    1. Start Node-1 on port 9001 (peers with Node-2)
    2. Start Node-2 on port 9002 (peers with Node-1)
    3. Both nodes will exchange data via TCP on localhost

To also see the UI, open two separate terminals and run:
    Terminal 3:  streamlit run distributed_ui.py -- --port 9001
    Terminal 4:  streamlit run distributed_ui.py -- --port 9002
"""

import signal
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from distributed.node import DistributedNode


def main():
    print("=" * 60)
    print("  DEMO: Starting 2 distributed nodes on localhost")
    print("=" * 60)
    print()

    # ── Create Node 1 ──────────────────────────────────────────────
    node1 = DistributedNode(
        node_id="Node-1",
        host="0.0.0.0",
        port=9001,
        peers=[("127.0.0.1", 9002)],  # Node-2
    )

    # ── Create Node 2 ──────────────────────────────────────────────
    node2 = DistributedNode(
        node_id="Node-2",
        host="0.0.0.0",
        port=9002,
        peers=[("127.0.0.1", 9001)],  # Node-1
    )

    # ── Start both ─────────────────────────────────────────────────
    node1.start()
    time.sleep(1)       # Small delay to avoid port conflicts
    node2.start()

    print()
    print("─" * 60)
    print("  Both nodes are running! Watch the P2P communication.")
    print("  Press Ctrl+C to stop both nodes.")
    print()
    print("  To view the UI, open new terminals and run:")
    print("    streamlit run distributed_ui.py -- --port 9001")
    print("    streamlit run distributed_ui.py -- --port 9002")
    print("─" * 60)
    print()

    # ── Shutdown handler ───────────────────────────────────────────
    def shutdown(sig, frame):
        print("\n\nShutting down demo...")
        node1.stop()
        node2.stop()
        print("Demo stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    # ── Run for 60 seconds then offer failure demo ─────────────────
    try:
        time.sleep(20)

        print("\n" + "=" * 60)
        print("  DEMO: Simulating failure of Signal S2 on Node-1")
        print("=" * 60 + "\n")
        node1.fail_signal("S2")

        time.sleep(10)

        print("\n" + "=" * 60)
        print("  DEMO: Node-2 continues operating (fault tolerance)")
        print("  S2 status should show 'failed' on both nodes")
        print("=" * 60 + "\n")

        # Keep running until Ctrl+C
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
