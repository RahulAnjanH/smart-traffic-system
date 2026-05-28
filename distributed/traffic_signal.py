"""
traffic_signal.py — Distributed Traffic Signal
===============================================
Each signal runs in its own thread and periodically generates a
random vehicle count, simulating real-world traffic sensors.

Unlike the original single-machine version (which wrote to a shared
Python Queue), this distributed version publishes events through the
MessageQueue (MOM), which relays them both locally and to remote peers.

Distributed System Concept — Message Passing:
    Instead of shared memory (the old Queue), signals communicate
    via message passing.  This is fundamental in distributed systems
    because separate machines CANNOT share memory.
"""

import threading
import random
import time


class DistributedSignal(threading.Thread):
    """
    Simulates a traffic signal sensor at one lane of a junction.

    Every 2 seconds it:
        1. Generates a random vehicle count (5–50)
        2. Publishes a "congestion" message via the local MessageQueue
        3. The MOM + P2P layer ensures all nodes receive it
    """

    def __init__(self, signal_id: str, node_id: str, message_queue, state_manager):
        super().__init__(daemon=True, name=f"Signal-{signal_id}")
        self.signal_id = signal_id
        self.node_id = node_id
        self.mq = message_queue
        self.state = state_manager
        self.running = True

    def run(self):
        """Main loop — generate and publish congestion events."""
        while self.running:
            # Check if this signal has been failed (locally or remotely)
            sig_state = self.state.get_signal_state(self.signal_id)
            if sig_state.get("status") == "failed":
                self.running = False
                print(f"[{self.node_id}] Signal {self.signal_id} detected failure state. Stopping thread.")
                break

            vehicle_count = random.randint(5, 50)
            ts = time.time()
            congestion = self.state.congestion_level(vehicle_count)

            # Build the event message
            event = {
                "signal_id": self.signal_id,
                "vehicle_count": vehicle_count,
                "congestion": congestion,
                "timestamp": ts,
                "source_node": self.node_id,
            }

            # Update local state
            self.state.update_signal(self.signal_id, {
                "vehicle_count": vehicle_count,
                "congestion": congestion,
                "status": "running",
                "timestamp": ts,
                "source_node": self.node_id,
            })

            # Publish to MOM (will be broadcast to peers)
            self.mq.publish("congestion", event)

            # Record in history
            ts_str = time.strftime("%H:%M:%S", time.localtime(ts))
            self.state.add_event({
                "signal_id": self.signal_id,
                "vehicle_count": vehicle_count,
                "timestamp": ts_str,
                "source_node": self.node_id,
            })
            self.state.add_message_flow({
                "direction": f"{self.signal_id} → MOM → Controller",
                "payload": f"vehicles={vehicle_count} ({congestion})",
                "timestamp": ts_str,
                "source_node": self.node_id,
            })
            self.state.add_timeline({
                "time": ts_str,
                "event": f"[{self.node_id}] {self.signal_id} published congestion "
                         f"(vehicles={vehicle_count}, {congestion})",
            })

            print(f"[{self.node_id}] Signal {self.signal_id}: "
                  f"{vehicle_count} vehicles ({congestion})")

            time.sleep(2)

    def stop(self):
        """Stop the signal thread (simulates failure)."""
        self.running = False
        self.state.force_update_signal(self.signal_id, {"status": "failed"})
        print(f"[{self.node_id}] Signal {self.signal_id} STOPPED (failure simulated)")
