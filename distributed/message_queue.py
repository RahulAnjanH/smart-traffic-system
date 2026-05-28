"""
message_queue.py — Simulated Message-Oriented Middleware (MOM)
=============================================================
Implements a lightweight in-process publish-subscribe message broker.

Distributed System Concept — Message-Oriented Middleware:
    MOM decouples message producers from consumers. Producers publish
    messages to a "topic" without knowing who will consume them.
    Consumers subscribe to topics they care about.

    Real-world MOM systems include Kafka, RabbitMQ, and ActiveMQ.
    Here we simulate the same concept with a simple dictionary of
    topic → list-of-callbacks, proving the pattern works without
    heavy external tools.

    Why not RPC/RMI?
    - RPC is synchronous: the caller blocks until the remote method returns.
    - In our traffic system, signals should fire-and-forget their data.
    - Asynchronous message passing (MOM) is more fault-tolerant:
      if the consumer is down, the message can be queued.
"""

import threading
import time
from queue import Queue


class MessageQueue:
    """
    In-process pub-sub broker simulating MOM.

    Topics used in this system:
        "congestion"   — signal → controller  (vehicle count events)
        "control"      — controller → signals  (green-time decisions)
        "state_sync"   — node → peers          (replicated state)
        "log_sync"     — node → peers          (distributed log entries)
        "heartbeat"    — node → peers          (liveness checks)
    """

    def __init__(self):
        self._subscribers = {}          # topic → [callback, ...]
        self._lock = threading.Lock()
        # Outgoing queue: messages that should be broadcast to peers
        self.outgoing = Queue()

    # ── Pub / Sub ───────────────────────────────────────────────────────

    def subscribe(self, topic: str, callback):
        """Register *callback(message)* for a topic."""
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)

    def publish(self, topic: str, message: dict, broadcast: bool = True):
        """
        Deliver *message* to all local subscribers of *topic*.

        If *broadcast* is True the message is also placed on the outgoing
        queue so the P2P layer can relay it to peer nodes.
        """
        # Deliver to local subscribers
        with self._lock:
            callbacks = list(self._subscribers.get(topic, []))

        for cb in callbacks:
            try:
                cb(message)
            except Exception as exc:
                print(f"[MOM] Subscriber error on topic '{topic}': {exc}")

        # Queue for network broadcast
        if broadcast:
            envelope = {
                "type": topic,
                "timestamp": time.time(),
                "payload": message,
            }
            self.outgoing.put(envelope)

    def deliver_remote(self, topic: str, message: dict):
        """
        Deliver a message that arrived from a remote peer.
        Same as publish() but does NOT re-broadcast (avoids infinite loop).
        """
        self.publish(topic, message, broadcast=False)
