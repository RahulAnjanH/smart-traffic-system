"""
controller.py — Distributed Traffic Controller
===============================================
Subscribes to "congestion" events from the MOM and computes
green-time adjustments for each signal.

In the distributed version, every node runs its own controller.
Decisions are broadcast to all peers so every node converges
to the same green-time assignments (via state replication + LWW).

Decision logic (same as original):
    vehicle_count > 30  →  green_time = 60 sec  (HIGH congestion)
    vehicle_count > 15  →  green_time = 40 sec  (MEDIUM)
    otherwise           →  green_time = 20 sec  (LOW)
"""

import threading
import time


class DistributedController(threading.Thread):
    """
    Runs as a daemon thread.  Subscribes to the "congestion" topic
    on the local MessageQueue and publishes "control" decisions.
    """

    def __init__(self, node_id: str, message_queue, state_manager):
        super().__init__(daemon=True, name="Controller")
        self.node_id = node_id
        self.mq = message_queue
        self.state = state_manager
        self.running = True

        # Subscribe to congestion events
        self.mq.subscribe("congestion", self._on_congestion)

    def _on_congestion(self, event: dict):
        """Called when a congestion event arrives (local or remote)."""
        signal_id = event.get("signal_id")
        vehicle_count = event.get("vehicle_count", 0)
        source = event.get("source_node", "?")

        # ── Decision logic ──────────────────────────────────────
        if vehicle_count > 30:
            green_time = 60
        elif vehicle_count > 15:
            green_time = 40
        else:
            green_time = 20

        ts = time.time()

        # Update local state with the new green time
        self.state.update_signal(signal_id, {
            "green_time": green_time,
            "timestamp": ts,
            "source_node": self.node_id,
        })

        # Publish control decision through MOM
        control_msg = {
            "signal_id": signal_id,
            "green_time": green_time,
            "vehicle_count": vehicle_count,
            "timestamp": ts,
            "source_node": self.node_id,
        }
        self.mq.publish("control", control_msg)

        # Record in history
        ts_str = time.strftime("%H:%M:%S", time.localtime(ts))
        congestion = self.state.congestion_level(vehicle_count)

        self.state.add_control({
            "signal_id": signal_id,
            "green_time": green_time,
            "timestamp": ts_str,
            "source_node": self.node_id,
        })
        self.state.add_message_flow({
            "direction": f"Controller → {signal_id}",
            "payload": f"green_time={green_time}s (from {source})",
            "timestamp": ts_str,
            "source_node": self.node_id,
        })
        self.state.add_timeline({
            "time": ts_str,
            "event": f"[{self.node_id}] Controller set {signal_id} → "
                     f"green={green_time}s ({congestion})",
        })

        print(f"[{self.node_id}] Controller: {signal_id} -> "
              f"green={green_time}s (vehicles={vehicle_count})")

    def run(self):
        """Keep the thread alive until stopped."""
        while self.running:
            time.sleep(0.5)

    def stop(self):
        self.running = False
