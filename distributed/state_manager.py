"""
state_manager.py — State Replication with Last-Write-Wins (LWW)
===============================================================
Maintains a replicated copy of the global traffic-signal state on
each node and resolves conflicts using timestamps.

Distributed System Concept — State Replication:
    In a distributed system each node keeps its own COPY of the data.
    When one node updates its copy, it broadcasts the change to peers.
    All nodes eventually converge to the same state (eventual consistency).

Distributed System Concept — Last-Write-Wins (LWW):
    When two nodes update the SAME signal at nearly the same time,
    we have a CONFLICT.  LWW resolves it simply:
        "The update with the HIGHER timestamp wins."
    This is easy to implement and works well for temporal data like
    traffic counts, where the most recent reading is what matters.
"""

import threading
import time
import copy


class StateManager:
    """
    Thread-safe replicated state store for traffic signals.

    State structure (per signal):
        {
            "signal_id":      "S1",
            "vehicle_count":  25,
            "green_time":     40,
            "congestion":     "MEDIUM",
            "status":         "running",       # running | failed | stopped
            "timestamp":      1716200000.123,
            "source_node":    "Node-1"
        }
    """

    def __init__(self, node_id: str, signal_ids=None):
        self.node_id = node_id
        self.signal_ids = signal_ids or ["S1", "S2", "S3"]
        self._lock = threading.Lock()

        # Initialize blank state for each signal
        self.state = {}
        for sid in self.signal_ids:
            self.state[sid] = {
                "signal_id": sid,
                "vehicle_count": 0,
                "green_time": None,
                "congestion": "LOW",
                "status": "initialized",
                "timestamp": 0.0,
                "vehicle_timestamp": 0.0,
                "control_timestamp": 0.0,
                "status_timestamp": 0.0,
                "source_node": node_id,
            }

        # History for UI display
        self.event_history = []       # congestion events
        self.control_history = []     # controller decisions
        self.message_flow_log = []    # message flow entries
        self.event_timeline = []      # combined timeline

        # Callbacks — UI can register to be notified of changes
        self._on_change_callbacks = []

    # ── Core LWW Logic ──────────────────────────────────────────────────

    def update_signal(self, signal_id: str, updates: dict) -> bool:
        """
        Apply *updates* to a signal using Last-Write-Wins (field-level).

        Returns True if the update was accepted (newer timestamp),
        False if it was rejected (stale).

        This is the CONSISTENCY MODEL of the system.
        """
        with self._lock:
            current = self.state.get(signal_id)
            if current is None:
                return False

            new_ts = updates.get("timestamp", 0.0)
            cur_ts = current.get("timestamp", 0.0)
            accepted = False

            # Check if this update has vehicle/congestion data
            if "vehicle_count" in updates or "congestion" in updates:
                remote_vehicle_ts = updates.get("vehicle_timestamp", new_ts)
                cur_vehicle_ts = current.get("vehicle_timestamp", 0.0)
                if remote_vehicle_ts > cur_vehicle_ts:
                    current["vehicle_count"] = updates.get("vehicle_count", current["vehicle_count"])
                    current["congestion"] = updates.get("congestion", current["congestion"])
                    current["vehicle_timestamp"] = remote_vehicle_ts
                    accepted = True

            # Check if this update has green_time data
            if "green_time" in updates:
                remote_control_ts = updates.get("control_timestamp", new_ts)
                cur_control_ts = current.get("control_timestamp", 0.0)
                if remote_control_ts > cur_control_ts:
                    current["green_time"] = updates.get("green_time", current["green_time"])
                    current["control_timestamp"] = remote_control_ts
                    accepted = True

            # Check if this update has status data
            if "status" in updates:
                remote_status_ts = updates.get("status_timestamp", new_ts)
                cur_status_ts = current.get("status_timestamp", 0.0)
                if remote_status_ts > cur_status_ts:
                    current["status"] = updates.get("status", current["status"])
                    current["status_timestamp"] = remote_status_ts
                    accepted = True

            # If it's a timestamp-only update or state_sync snapshot with timestamps
            if not accepted and not any(k in updates for k in ["vehicle_count", "congestion", "green_time", "status"]):
                if new_ts > cur_ts:
                    accepted = True

            if accepted:
                current["timestamp"] = max(current.get("timestamp", 0.0), new_ts)
                if "source_node" in updates:
                    current["source_node"] = updates["source_node"]
                self._notify_change(signal_id)

            print(f"[{self.node_id}] LWW {signal_id}: incoming_ts={new_ts} | existing_ts={cur_ts} | accepted={accepted}")
            return accepted

    def force_update_signal(self, signal_id: str, updates: dict):
        """Apply updates without LWW check (used for local-only changes like status)."""
        with self._lock:
            current = self.state.get(signal_id)
            if current is None:
                return
            for key, value in updates.items():
                current[key] = value
            ts = updates.get("timestamp", time.time())
            current["timestamp"] = ts
            if "status" in updates:
                current["status_timestamp"] = updates.get("status_timestamp", ts)
            if "vehicle_count" in updates:
                current["vehicle_timestamp"] = updates.get("vehicle_timestamp", ts)
            if "green_time" in updates:
                current["control_timestamp"] = updates.get("control_timestamp", ts)
            self._notify_change(signal_id)



    # ── Snapshot for Sync ───────────────────────────────────────────────

    def get_full_state(self) -> dict:
        """Return a deep copy of the entire state (for broadcasting)."""
        with self._lock:
            return copy.deepcopy(self.state)

    def apply_full_state(self, remote_state: dict):
        """
        Merge a full state snapshot from a peer using LWW per-signal.
        Called when a remote state_sync message arrives.
        """
        for signal_id, remote_entry in remote_state.items():
            self.update_signal(signal_id, remote_entry)

    # ── Read Helpers ────────────────────────────────────────────────────

    def get_signal_state(self, signal_id: str) -> dict:
        with self._lock:
            return copy.deepcopy(self.state.get(signal_id, {}))

    def get_all_signals(self) -> dict:
        return self.get_full_state()

    # ── History Helpers (for UI) ────────────────────────────────────────

    def add_event(self, entry: dict):
        """Append a congestion event to history (capped at 100)."""
        with self._lock:
            self.event_history.append(entry)
            if len(self.event_history) > 100:
                self.event_history.pop(0)

    def add_control(self, entry: dict):
        """Append a controller decision to history."""
        with self._lock:
            self.control_history.append(entry)
            if len(self.control_history) > 100:
                self.control_history.pop(0)

    def add_message_flow(self, entry: dict):
        """Append a message-flow log entry."""
        with self._lock:
            self.message_flow_log.append(entry)
            if len(self.message_flow_log) > 100:
                self.message_flow_log.pop(0)

    def add_timeline(self, entry: dict):
        """Append to the event timeline."""
        with self._lock:
            self.event_timeline.append(entry)
            if len(self.event_timeline) > 100:
                self.event_timeline.pop(0)

    # ── Change Callbacks ────────────────────────────────────────────────

    def on_change(self, callback):
        """Register a callback to be called when state changes."""
        self._on_change_callbacks.append(callback)

    def _notify_change(self, signal_id: str):
        for cb in self._on_change_callbacks:
            try:
                cb(signal_id)
            except Exception:
                pass

    # ── Congestion Helper ───────────────────────────────────────────────

    @staticmethod
    def congestion_level(vehicle_count: int) -> str:
        if vehicle_count > 30:
            return "HIGH"
        elif vehicle_count > 15:
            return "MEDIUM"
        return "LOW"
