"""
logger.py — Distributed Logging System
=======================================
Each node maintains local logs AND shares them with peers so that
every dashboard can display a combined, system-wide log view.

Distributed System Concept — Distributed Logging:
    In a centralized system, one log file captures everything.
    In a distributed system, each node generates its own logs.
    To get a unified view, we replicate log entries across nodes
    using the same MOM message-passing infrastructure.

    Logs are deduplicated using (node_id + timestamp + message) as key.
"""

import threading
import time


class DistributedLogger:
    """
    Collects log entries locally and synchronizes them with peers.
    """

    def __init__(self, node_id: str, message_queue):
        self.node_id = node_id
        self.mq = message_queue
        self._lock = threading.Lock()
        self.logs = []              # List of log dicts
        self._seen = set()          # Dedup set: (node_id, timestamp, msg)

        # Subscribe to incoming log syncs from peers
        self.mq.subscribe("log_sync", self._on_remote_logs)

    def log(self, message: str, broadcast: bool = True):
        """
        Create a local log entry and optionally broadcast to peers.
        """
        ts = time.time()
        ts_str = time.strftime("%H:%M:%S", time.localtime(ts))
        entry = {
            "node_id": self.node_id,
            "timestamp": ts,
            "time_str": ts_str,
            "message": message,
        }

        key = (self.node_id, ts, message)
        with self._lock:
            if key not in self._seen:
                self._seen.add(key)
                self.logs.append(entry)
                # Cap at 200 entries
                if len(self.logs) > 200:
                    self.logs.pop(0)

        if broadcast:
            self.mq.publish("log_sync", {"entries": [entry]})

    def _on_remote_logs(self, message: dict):
        """Merge log entries received from a peer."""
        entries = message.get("entries", [])
        with self._lock:
            for entry in entries:
                key = (entry.get("node_id"), entry.get("timestamp"),
                       entry.get("message"))
                if key not in self._seen:
                    self._seen.add(key)
                    self.logs.append(entry)
                    if len(self.logs) > 200:
                        self.logs.pop(0)

    def get_logs(self, limit: int = 50) -> list:
        """Return recent logs sorted by timestamp (combined from all nodes)."""
        with self._lock:
            sorted_logs = sorted(self.logs, key=lambda x: x.get("timestamp", 0))
            return list(sorted_logs[-limit:])
