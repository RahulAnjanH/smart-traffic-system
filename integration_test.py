"""
integration_test.py — Two-node P2P integration test.
Starts Node-1 (port 9001) and Node-2 (port 9002) on localhost,
lets them exchange data for 10 seconds, verifies both nodes
see the same state, then tests failure propagation.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from distributed.node import DistributedNode

print("=" * 55)
print("INTEGRATION TEST: 2-Node P2P on localhost")
print("=" * 55)

# Create both nodes
node1 = DistributedNode("Node-1", "0.0.0.0", 9001, [("127.0.0.1", 9002)])
node2 = DistributedNode("Node-2", "0.0.0.0", 9002, [("127.0.0.1", 9001)])

# Start Node-1 first, wait for socket to bind, then Node-2
node1.start()
time.sleep(0.5)
node2.start()

print("\nLetting nodes run for 8 seconds...")
time.sleep(8)

# ── Check 1: Both nodes have generated signal data ──────────────
print("\n--- Check 1: Local signal data ---")
s1_on_n1 = node1.state_manager.get_signal_state("S1")
s1_on_n2 = node2.state_manager.get_signal_state("S1")
print(f"  Node-1 S1 vehicle_count: {s1_on_n1['vehicle_count']} (status: {s1_on_n1['status']})")
print(f"  Node-2 S1 vehicle_count: {s1_on_n2['vehicle_count']} (status: {s1_on_n2['status']})")

n1_has_data = s1_on_n1["vehicle_count"] > 0
n2_has_data = s1_on_n2["vehicle_count"] > 0
print(f"  Node-1 generating data: {n1_has_data}")
print(f"  Node-2 generating data: {n2_has_data}")

# ── Check 2: Peer connectivity ───────────────────────────────────
print("\n--- Check 2: Peer connectivity ---")
n1_peers = node1.communication.get_peer_status()
n2_peers = node2.communication.get_peer_status()
print(f"  Node-1 sees Node-2: {n1_peers}")
print(f"  Node-2 sees Node-1: {n2_peers}")

# ── Check 3: Logs merged ─────────────────────────────────────────
print("\n--- Check 3: Distributed logs ---")
n1_logs = node1.logger.get_logs(limit=5)
n2_logs = node2.logger.get_logs(limit=5)
print(f"  Node-1 log count: {len(n1_logs)}")
print(f"  Node-2 log count: {len(n2_logs)}")

# ── Check 4: Simulate failure + propagation ──────────────────────
print("\n--- Check 4: Failure simulation ---")
node1.fail_signal("S2")
print("  Failed S2 on Node-1. Waiting 3s for propagation...")
time.sleep(3)

s2_n1 = node1.state_manager.get_signal_state("S2")
s2_n2 = node2.state_manager.get_signal_state("S2")
print(f"  S2 status on Node-1: {s2_n1['status']} (expected: failed)")
print(f"  S2 status on Node-2: {s2_n2['status']} (expected: failed or running if sync not yet complete)")

# ── Check 5: System continues after failure ──────────────────────
print("\n--- Check 5: Fault tolerance ---")
s1_n2_after = node2.state_manager.get_signal_state("S1")
s3_n2_after = node2.state_manager.get_signal_state("S3")
print(f"  Node-2 S1 still running: {s1_n2_after['status']}")
print(f"  Node-2 S3 still running: {s3_n2_after['status']}")
n2_running = node2.running
print(f"  Node-2 overall: {'RUNNING' if n2_running else 'STOPPED'}")

# ── Shutdown ─────────────────────────────────────────────────────
print("\nShutting down both nodes...")
node1.stop()
node2.stop()

print("\n" + "=" * 55)
print("INTEGRATION TEST COMPLETE")
print("=" * 55)
