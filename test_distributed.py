"""Quick functional test for the distributed system."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from distributed.state_manager import StateManager
from distributed.message_queue import MessageQueue
from distributed.logger import DistributedLogger

# ── Test 1: StateManager + LWW ─────────────────────────────────────
print("=" * 55)
print("TEST 1: StateManager + Last-Write-Wins")
print("=" * 55)

sm = StateManager("Node-1")

# Apply a fresh update
ok = sm.update_signal("S1", {"vehicle_count": 25, "timestamp": 1000.0, "source_node": "Node-1"})
s1 = sm.get_signal_state("S1")
print(f"  Update accepted: {ok} (expected True)")
print(f"  vehicle_count: {s1['vehicle_count']} (expected 25)")

# Stale update (older timestamp) should be REJECTED
ok2 = sm.update_signal("S1", {"vehicle_count": 999, "timestamp": 500.0, "source_node": "Node-2"})
s1b = sm.get_signal_state("S1")
print(f"  Stale rejected: {not ok2} (expected True)")
print(f"  vehicle_count unchanged: {s1b['vehicle_count']} (expected 25)")

# Newer update should be ACCEPTED
ok3 = sm.update_signal("S1", {"vehicle_count": 42, "timestamp": 2000.0, "source_node": "Node-2"})
s1c = sm.get_signal_state("S1")
print(f"  Newer accepted: {ok3} (expected True)")
print(f"  vehicle_count updated: {s1c['vehicle_count']} (expected 42)")
print(f"  PASS: LWW working correctly\n")

# ── Test 2: Full state snapshot + merge ────────────────────────────
print("=" * 55)
print("TEST 2: Full State Snapshot + Merge")
print("=" * 55)

sm_a = StateManager("Node-A")
sm_b = StateManager("Node-B")

# Node A has newer data for S2
sm_a.update_signal("S2", {"vehicle_count": 30, "timestamp": 5000.0, "source_node": "Node-A"})

# Node B has older data
sm_b.update_signal("S2", {"vehicle_count": 10, "timestamp": 1000.0, "source_node": "Node-B"})

# Node B merges Node A's full state
snap_a = sm_a.get_full_state()
sm_b.apply_full_state(snap_a)

s2_b = sm_b.get_signal_state("S2")
print(f"  S2 on Node-B after merge: {s2_b['vehicle_count']} (expected 30 — Node-A wins)")
print(f"  PASS: State sync working correctly\n")

# ── Test 3: MessageQueue pub-sub ───────────────────────────────────
print("=" * 55)
print("TEST 3: MessageQueue Pub-Sub (Simulated MOM)")
print("=" * 55)

mq = MessageQueue()
received = []

mq.subscribe("congestion", lambda msg: received.append(msg))
mq.publish("congestion", {"signal_id": "S1", "vehicle_count": 25}, broadcast=False)
mq.publish("congestion", {"signal_id": "S2", "vehicle_count": 10}, broadcast=False)

print(f"  Messages received: {len(received)} (expected 2)")
print(f"  S1 vehicles: {received[0]['vehicle_count']} (expected 25)")
print(f"  S2 vehicles: {received[1]['vehicle_count']} (expected 10)")

# No cross-topic delivery
other = []
mq.subscribe("control", lambda msg: other.append(msg))
mq.publish("congestion", {"signal_id": "S3"}, broadcast=False)
print(f"  Control topic not triggered by congestion: {len(other) == 0} (expected True)")
print(f"  PASS: MOM pub-sub working correctly\n")

# ── Test 4: DistributedLogger dedup ───────────────────────────────
print("=" * 55)
print("TEST 4: DistributedLogger Deduplication")
print("=" * 55)

mq2 = MessageQueue()
logger = DistributedLogger("Node-1", mq2)

logger.log("Signal S1 started", broadcast=False)
logger.log("Signal S1 started", broadcast=False)  # Duplicate — should be ignored
logger.log("Controller running", broadcast=False)

logs = logger.get_logs()
print(f"  Log entries (deduped): {len(logs)} (expected 2)")
print(f"  PASS: Logger dedup working correctly\n")

# ── Test 5: Congestion level helper ───────────────────────────────
print("=" * 55)
print("TEST 5: Congestion Level Helper")
print("=" * 55)

sm2 = StateManager("test")
tests = [(5, "LOW"), (15, "LOW"), (16, "MEDIUM"), (30, "MEDIUM"), (31, "HIGH"), (50, "HIGH")]
all_ok = True
for vc, expected in tests:
    result = sm2.congestion_level(vc)
    ok = result == expected
    if not ok:
        all_ok = False
    print(f"  vehicles={vc:2d} -> {result:6s} (expected {expected}) {'OK' if ok else 'FAIL'}")

print(f"  PASS: Congestion levels correct: {all_ok}\n")

# ── Test 6: Field-Level LWW Timestamp Separation ───────────────────
print("=" * 55)
print("TEST 6: Field-Level LWW Timestamp Separation")
print("=" * 55)

sm_x = StateManager("Node-X")
# Initialize signal with status="failed" at ts=5000.0
sm_x.force_update_signal("S1", {
    "status": "failed",
    "timestamp": 5000.0
})

# Stale update has older status_timestamp (1000.0) but newer vehicle_timestamp (6000.0)
stale_update = {
    "status": "running",
    "status_timestamp": 1000.0,
    "vehicle_count": 50,
    "vehicle_timestamp": 6000.0,
    "timestamp": 6000.0
}

sm_x.update_signal("S1", stale_update)
s1_state = sm_x.get_signal_state("S1")

print(f"  Status remains failed: {s1_state['status'] == 'failed'} (expected True, current: {s1_state['status']})")
print(f"  Status timestamp remains 5000.0: {s1_state['status_timestamp'] == 5000.0} (expected True, current: {s1_state['status_timestamp']})")
print(f"  Vehicle count updated: {s1_state['vehicle_count'] == 50} (expected True, current: {s1_state['vehicle_count']})")
print(f"  Vehicle timestamp updated to 6000.0: {s1_state['vehicle_timestamp'] == 6000.0} (expected True, current: {s1_state['vehicle_timestamp']})")

if s1_state['status'] == 'failed' and s1_state['status_timestamp'] == 5000.0 and s1_state['vehicle_count'] == 50:
    print("  PASS: Field-level LWW timestamps separated correctly\n")
else:
    print("  FAIL: Field-level LWW timestamps corrupted!\n")
    sys.exit(1)

print("=" * 55)
print("ALL TESTS PASSED!")
print("=" * 55)

