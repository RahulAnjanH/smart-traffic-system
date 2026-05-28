"""
node.py — Distributed Node Orchestrator
========================================
This is the MAIN class that ties all distributed components together.
Each physical machine runs ONE DistributedNode instance.

Architecture (per node):
    ┌─────────────────────────────────────────────────────────┐
    │  DistributedNode                                        │
    │                                                         │
    │  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
    │  │ Signal S1│  │ Signal S2│  │ Signal S3│  (3 threads)  │
    │  └────┬─────┘  └────┬─────┘  └────┬─────┘              │
    │       │              │              │                    │
    │       ▼              ▼              ▼                    │
    │  ┌──────────────────────────────────┐                   │
    │  │    MessageQueue (MOM Broker)     │  (pub-sub)        │
    │  └────────────┬─────────────────────┘                   │
    │               │                                         │
    │       ┌───────┴───────┐                                 │
    │       ▼               ▼                                 │
    │  ┌──────────┐  ┌─────────────┐                          │
    │  │Controller│  │   Logger    │  (1 thread each)         │
    │  └────┬─────┘  └─────────────┘                          │
    │       │                                                 │
    │       ▼                                                 │
    │  ┌──────────────────────────────────┐                   │
    │  │      StateManager (LWW)         │  (replicated)      │
    │  └────────────┬─────────────────────┘                   │
    │               │                                         │
    │               ▼                                         │
    │  ┌──────────────────────────────────┐                   │
    │  │   P2P Communication Layer       │  (TCP sockets)     │
    │  │   (listener thread + senders)   │                    │
    │  └──────────────────────────────────┘                   │
    └─────────────────────────────────────────────────────────┘
                        ↕  TCP/JSON  ↕
                   [ Other Nodes on LAN ]

Concurrency Model:
    - Signal S1, S2, S3 → 3 threads
    - Controller         → 1 thread
    - P2P Listener       → 1 thread
    - Outgoing relay     → 1 thread
    - State sync timer   → 1 thread
    - Heartbeat timer    → 1 thread
    Total: ~8 concurrent threads per node
"""

import threading
import time

from .communication import P2PCommunication
from .message_queue import MessageQueue
from .state_manager import StateManager
from .controller import DistributedController
from .traffic_signal import DistributedSignal
from .logger import DistributedLogger


# ── Module-level reference for Streamlit to access ──────────────────────
_active_node = None


def get_active_node():
    """Return the running DistributedNode (used by the Streamlit UI)."""
    return _active_node


class DistributedNode:
    """
    Orchestrates all components of one distributed traffic-system node.
    """

    def __init__(self, node_id: str, host: str, port: int, peers: list):
        global _active_node

        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.running = False

        # ── Build components ────────────────────────────────────────
        self.message_queue = MessageQueue()
        self.state_manager = StateManager(node_id)
        self.communication = P2PCommunication(host, port, peers, node_id)
        self.controller = DistributedController(node_id, self.message_queue,
                                                 self.state_manager)
        self.logger = DistributedLogger(node_id, self.message_queue)

        self.signals = [
            DistributedSignal(sid, node_id, self.message_queue, self.state_manager)
            for sid in ["S1", "S2", "S3"]
        ]

        # Wire incoming network messages to the MOM
        self.communication.on_receive = self._on_network_message

        # Start communication listener immediately so the node is online on the network
        self.listener_started = False
        try:
            self.communication.start()
            self.listener_started = True
        except Exception as e:
            print(f"[{node_id}] Warning: Could not start P2P listener on port {port}: {e}")

        # Store as module-level reference
        _active_node = self

    # ── Lifecycle ───────────────────────────────────────────────────────

    def start(self):
        """Start all threads and begin operating."""
        if self.running:
            return
        self.running = True

        # Try to start P2P listener if it failed initially or isn't running
        if not self.listener_started:
            try:
                self.communication.start()
                self.listener_started = True
            except Exception as e:
                self.running = False
                raise OSError(f"Could not start P2P listener: {e}")

        # 2. Start outgoing relay thread (MOM → network)
        self._relay_thread = threading.Thread(
            target=self._relay_outgoing, daemon=True, name="Relay-Out"
        )
        self._relay_thread.start()

        # 3. Start signals
        for signal in self.signals:
            signal.start()

        # 4. Start controller
        self.controller.start()

        # 5. Start periodic full-state sync (every 5 seconds)
        self._sync_thread = threading.Thread(
            target=self._periodic_state_sync, daemon=True, name="State-Sync"
        )
        self._sync_thread.start()

        # 6. Start heartbeat
        self._heartbeat_thread = threading.Thread(
            target=self._periodic_heartbeat, daemon=True, name="Heartbeat"
        )
        self._heartbeat_thread.start()

        self.logger.log(f"Node {self.node_id} started on {self.host}:{self.port}")
        print(f"\n{'='*60}")
        print(f"  Node {self.node_id} is RUNNING")
        print(f"  Listening on {self.host}:{self.port}")
        print(f"  Peers: {self.peers}")
        print(f"  Threads: ~8 (3 signals + controller + listener + relay + sync + heartbeat)")
        print(f"{'='*60}\n")

    def stop(self, stop_p2p: bool = True):
        """Gracefully shut down all threads."""
        if not self.running and not stop_p2p:
            return
        self.running = False

        # Stop signals
        for signal in self.signals:
            signal.stop()

        # Stop controller
        self.controller.stop()

        # Stop P2P if requested
        if stop_p2p:
            self.communication.stop()
            self.listener_started = False

        self.logger.log(f"Node {self.node_id} stopped (stop_p2p={stop_p2p})")
        print(f"\n[{self.node_id}] Simulation stopped (stop_p2p={stop_p2p}).\n")


    # ── Network Message Router ──────────────────────────────────────────

    def _on_network_message(self, message: dict):
        """
        Called when a message arrives from a remote peer.
        Routes it to the correct handler based on message type.
        """
        msg_type = message.get("type", "")
        payload = message.get("payload", {})
        source = message.get("source_node", "?")

        print(f"[{self.node_id}] P2P Received network message: type={msg_type} | source={source}")

        if msg_type == "congestion":

            # Remote signal data → deliver to local MOM (no re-broadcast)
            self.message_queue.deliver_remote("congestion", payload)
            # Update state via LWW
            sid = payload.get("signal_id")
            if sid:
                self.state_manager.update_signal(sid, {
                    "vehicle_count": payload.get("vehicle_count", 0),
                    "congestion": payload.get("congestion", "LOW"),
                    "status": "running",
                    "timestamp": payload.get("timestamp", 0),
                    "source_node": source,
                })
                ts_str = time.strftime("%H:%M:%S",
                                       time.localtime(payload.get("timestamp", 0)))
                self.state_manager.add_timeline({
                    "time": ts_str,
                    "event": f"[REMOTE {source}] {sid} congestion="
                             f"{payload.get('vehicle_count', 0)} received",
                })

        elif msg_type == "control":
            action = payload.get("action", "")

            if action == "FAIL":
                # ── Handle remote failure event ─────────────────────
                sid = payload.get("signal_id")
                if sid:
                    print(f"[{self.node_id}] Received FAIL event for {sid} from {source}")
                    # Use force_update to bypass LWW race with running signals
                    ts = payload.get("timestamp", time.time())
                    self.state_manager.force_update_signal(sid, {
                        "status": "failed",
                        "timestamp": ts,
                        "source_node": source,
                    })
                    # Stop the local signal thread if it's running
                    for signal in self.signals:
                        if signal.signal_id == sid and signal.running:
                            signal.running = False
                            print(f"[{self.node_id}] Applying FAIL to signal {sid} (stopped local thread)")
                    # Record in timeline
                    ts_str = time.strftime("%H:%M:%S", time.localtime(ts))
                    self.state_manager.add_timeline({
                        "time": ts_str,
                        "event": f"[REMOTE {source}] {sid} FAILED (failure synced)",
                    })
                    self.logger.log(f"Signal {sid} FAILED (received from {source})")
            else:
                # ── Normal control message (green_time update) ─────
                self.message_queue.deliver_remote("control", payload)
                sid = payload.get("signal_id")
                if sid:
                    self.state_manager.update_signal(sid, {
                        "green_time": payload.get("green_time"),
                        "timestamp": payload.get("timestamp", 0),
                        "source_node": source,
                    })

        elif msg_type == "state_sync":
            # Full state merge using LWW
            self.state_manager.apply_full_state(payload)

        elif msg_type == "log_sync":
            self.message_queue.deliver_remote("log_sync", payload)

        elif msg_type == "heartbeat":
            # Just receiving it is enough — the communication layer
            # marks the peer as reachable
            pass

    # ── Background Workers ──────────────────────────────────────────────

    def _relay_outgoing(self):
        """
        Continuously read from MOM outgoing queue and broadcast to peers.
        This is the bridge between the in-process MOM and the network.
        """
        while self.running:
            try:
                # Block with timeout so we can check self.running
                message = self.message_queue.outgoing.get(timeout=0.5)
                message["source_node"] = self.node_id
                self.communication.broadcast(message)
            except Exception:
                pass  # Queue.get timeout — loop back

    def _periodic_state_sync(self):
        """
        Every 5 seconds, broadcast the full state snapshot to peers.
        This ensures new/recovering nodes get caught up.
        """
        while self.running:
            time.sleep(5)
            if self.running:
                full_state = self.state_manager.get_full_state()
                msg = {
                    "type": "state_sync",
                    "source_node": self.node_id,
                    "timestamp": time.time(),
                    "payload": full_state,
                }
                self.communication.broadcast(msg)

    def _periodic_heartbeat(self):
        """
        Every 3 seconds, send a heartbeat to all peers.
        This lets peers know we are alive (liveness detection).
        """
        while self.running:
            time.sleep(3)
            if self.running:
                msg = {
                    "type": "heartbeat",
                    "source_node": self.node_id,
                    "timestamp": time.time(),
                    "payload": {},
                }
                self.communication.broadcast(msg)

    # ── Public API for UI ───────────────────────────────────────────────

    def fail_signal(self, signal_id: str):
        """Simulate failure of a specific signal and broadcast to all peers."""
        # ── DIAGNOSTIC: log peer state before attempting broadcast ──────
        peer_count = len(self.communication.peers)
        peer_status = self.communication.get_peer_status()
        print(f"[{self.node_id}] fail_signal({signal_id}) called. "
              f"node.running={self.running} | peers configured={peer_count} | "
              f"peer_status={peer_status}")

        if peer_count == 0:
            print(f"[{self.node_id}] WARNING: No peers configured! "
                  f"Failure will be LOCAL ONLY. "
                  f"Fix: pass peers when creating DistributedNode.")

        for signal in self.signals:
            if signal.signal_id == signal_id:
                ts = time.time()

                # 1. Stop signal thread + update local state.
                #    signal.stop() already calls force_update_signal({status: failed}),
                #    so we do NOT call force_update_signal again here to avoid a
                #    duplicate write that can confuse LWW timestamps.
                signal.stop()
                self.logger.log(f"Signal {signal_id} FAILED (simulated)")

                # 2. Broadcast explicit FAIL control message to all peers
                fail_msg = {
                    "type": "control",
                    "source_node": self.node_id,
                    "timestamp": ts,
                    "payload": {
                        "action": "FAIL",
                        "signal_id": signal_id,
                        "timestamp": ts,
                        "source_node": self.node_id,
                    },
                }
                print(f"[{self.node_id}] Broadcasting FAIL for {signal_id} "
                      f"to {peer_count} peer(s)...")
                self.communication.broadcast(fail_msg)

                # 3. Send full state sync as backup (so LWW catches up on any
                #    signals the peer may have missed while offline)
                full_state = self.state_manager.get_full_state()
                sync_msg = {
                    "type": "state_sync",
                    "source_node": self.node_id,
                    "timestamp": ts,
                    "payload": full_state,
                }
                self.communication.broadcast(sync_msg)
                print(f"[{self.node_id}] State sync also broadcast after FAIL.")

                # 4. Record in timeline
                ts_str = time.strftime("%H:%M:%S", time.localtime(ts))
                self.state_manager.add_timeline({
                    "time": ts_str,
                    "event": f"[{self.node_id}] {signal_id} FAILED (failure triggered locally)",
                })

                break
        else:
            print(f"[{self.node_id}] fail_signal: signal '{signal_id}' not found in "
                  f"{[s.signal_id for s in self.signals]}")

    def get_info(self) -> dict:
        """Return summary info for the UI."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "peers": self.peers,
            "running": self.running,
            "peer_status": self.communication.get_peer_status(),
        }
