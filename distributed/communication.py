"""
communication.py — Peer-to-Peer TCP Socket Layer
=================================================
Handles all network communication between distributed nodes.

Distributed System Concept — Peer-to-Peer (P2P):
    In a P2P architecture every node is BOTH a client and a server.
    - Server side: listens for incoming TCP connections from peers.
    - Client side: connects to each peer to send messages.

    There is NO central server. If one node goes down, the others
    continue operating independently (fault tolerance).

Message Protocol (wire format):
    [4 bytes: payload length, big-endian uint32] + [UTF-8 JSON bytes]

    This "length-prefixed framing" ensures the receiver knows exactly
    how many bytes to read, even if TCP delivers data in chunks.
"""

import json
import socket
import struct
import threading
import time


class P2PCommunication:
    """
    Manages TCP socket connections to all peers.

    Public API:
        start()          — begin listening for incoming connections
        stop()           — shut down listener and all connections
        broadcast(msg)   — send a JSON message to every peer
        on_receive       — callback set by the node for incoming messages
    """

    def __init__(self, host: str, port: int, peers: list, node_id: str = ""):
        """
        Args:
            host:    Address to bind (use "0.0.0.0" to accept from any interface)
            port:    TCP port to listen on
            peers:   List of (ip, port) tuples of other nodes
            node_id: Identifier for logging
        """
        self.host = host
        self.port = port
        self.peers = list(peers)        # [(ip, port), ...]
        self.node_id = node_id
        self.running = False

        # Callback: called with (message_dict) when data arrives
        self.on_receive = None

        # Track peer connection health
        self.peer_status = {f"{ip}:{p}": False for ip, p in self.peers}

        self._server_socket = None
        self._listener_thread = None
        self._lock = threading.Lock()

    # ── Server Side (Listener) ──────────────────────────────────────────

    def start(self):
        """Start the TCP listener in a background thread."""
        if self.running:
            return
        self.running = True
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.settimeout(1.0)     # so we can check self.running
        self._server_socket.bind((self.host, self.port))
        print("Listening on", self.host, self.port)
        self._server_socket.listen(5)

        self._listener_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="P2P-Listener"
        )
        self._listener_thread.start()
        print(f"[P2P {self.node_id}] Listener started on {self.host}:{self.port}")

    def _accept_loop(self):
        """Accept incoming connections in a loop."""
        while self.running:
            try:
                conn, addr = self._server_socket.accept()
                # Handle each connection in its own thread
                handler = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True,
                    name=f"P2P-Handler-{addr}",
                )
                handler.start()
            except socket.timeout:
                continue    # check self.running and retry
            except OSError:
                break       # socket closed during shutdown

    def _handle_connection(self, conn: socket.socket, addr):
        """Read one length-prefixed JSON message from a peer."""
        try:
            conn.settimeout(5.0)
            # 1. Read 4-byte length header
            raw_len = self._recv_exact(conn, 4)
            if raw_len is None:
                return
            msg_len = struct.unpack("!I", raw_len)[0]

            # 2. Read the JSON payload
            raw_msg = self._recv_exact(conn, msg_len)
            if raw_msg is None:
                return
            message = json.loads(raw_msg.decode("utf-8"))

            print(f"[P2P {self.node_id}] Received message from {addr}: {message.get('type')}")

            # 3. Deliver to the node
            if self.on_receive:
                self.on_receive(message)

        except Exception as exc:
            print(f"[P2P {self.node_id}] Error handling {addr}: {exc}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


    @staticmethod
    def _recv_exact(sock: socket.socket, num_bytes: int):
        """Read exactly *num_bytes* from the socket (TCP may fragment)."""
        data = b""
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    # ── Client Side (Sender) ────────────────────────────────────────────

    def broadcast(self, message: dict):
        """Send *message* to every peer. Skip unreachable peers."""
        # Inject source node so receivers know who sent it
        message["source_node"] = self.node_id

        if not self.peers:
            print(f"[P2P {self.node_id}] broadcast() called but peers list is EMPTY. "
                  f"Message type='{message.get('type')}' will NOT be sent anywhere. "
                  f"Fix: configure peers when launching the node.")
            return

        raw = json.dumps(message).encode("utf-8")
        header = struct.pack("!I", len(raw))
        payload = header + raw

        for ip, port in self.peers:
            key = f"{ip}:{port}"
            threading.Thread(
                target=self._send_to_peer,
                args=(ip, port, payload, key),
                daemon=True,
                name=f"P2P-Send-{key}",
            ).start()

    def _send_to_peer(self, ip: str, port: int, payload: bytes, key: str):
        """Connect to a single peer and send the payload."""
        print("Connecting to peer", ip, port)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(3.0)
                sock.connect((ip, port))
                sock.sendall(payload)

            with self._lock:
                self.peer_status[key] = True
            print("Successfully connected")
            print(f"[P2P {self.node_id}] Successfully sent message to peer {key}")

        except (ConnectionRefusedError, TimeoutError, OSError) as exc:
            with self._lock:
                self.peer_status[key] = False
            print("Connection failed:", exc)


    # ── Shutdown ────────────────────────────────────────────────────────

    def stop(self):
        """Gracefully shut down the listener."""
        self.running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        if self._listener_thread:
            self._listener_thread.join(timeout=3)
        print(f"[P2P {self.node_id}] Stopped.")

    # ── Health ──────────────────────────────────────────────────────────

    def get_peer_status(self) -> dict:
        """Return {peer_key: bool} — True if last send succeeded."""
        with self._lock:
            return dict(self.peer_status)
