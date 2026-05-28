"""
distributed_ui.py — Streamlit Dashboard for the Distributed P2P System
=======================================================================
Matches the ORIGINAL streamlit_app.py design exactly, but reads data
from a DistributedNode instead of a local TrafficSystem.

The node is created INSIDE the Streamlit session_state (same pattern
as the original) so there are no cross-process issues.

Usage:
    streamlit run distributed_ui.py
"""

import streamlit as st
import sys
import os
import time
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from distributed.node import DistributedNode
from distributed.state_manager import StateManager


# ─── Helpers ────────────────────────────────────────────────────────────────

def get_node():
    """Get or create the DistributedNode from session state using CLI args or defaults."""
    if "distributed_node" not in st.session_state:
        defaults = parse_cli_args()
        node_id = defaults.get("node_id", "Node-1")
        host = defaults.get("host", "0.0.0.0")
        port = defaults.get("port", 9001)
        peer_str = defaults.get("peers", "")
        peers = parse_peer_string(peer_str)

        node = DistributedNode(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
        )
        st.session_state["distributed_node"] = node
        st.session_state["node_config"] = {
            "node_id": node_id,
            "host": host,
            "port": port,
            "peers": peers,
        }
    return st.session_state["distributed_node"]



def parse_cli_args():
    """Parse optional CLI args passed after '--'."""
    defaults = {"node_id": "Node-1", "host": "0.0.0.0", "port": 9001, "peers": ""}
    args = sys.argv
    for i, arg in enumerate(args):
        if arg == "--node-id" and i + 1 < len(args):
            defaults["node_id"] = args[i + 1]
        elif arg == "--host" and i + 1 < len(args):
            defaults["host"] = args[i + 1]
        elif arg == "--port" and i + 1 < len(args):
            defaults["port"] = int(args[i + 1])
        elif arg == "--peers" and i + 1 < len(args):
            defaults["peers"] = args[i + 1]
    return defaults


def parse_peer_string(peer_str: str) -> list:
    """Parse 'ip:port,ip:port' into [(ip, port), ...]."""
    peers = []
    for p in peer_str.replace(",", " ").split():
        p = p.strip()
        if ":" in p:
            parts = p.split(":")
            peers.append((parts[0], int(parts[1])))
    return peers


# ─── Custom CSS (SAME as original streamlit_app.py) ────────────────────────

def inject_css():
    st.markdown("""
    <style>
    /* ── Global ─────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }

    /* ── Panel card ─────────────────────────────────── */
    .panel {
        background: linear-gradient(135deg, #1e1e2f 0%, #29293d 100%);
        border: 1px solid #3a3a5c;
        border-radius: 14px;
        padding: 20px 24px;
        margin-bottom: 18px;
        box-shadow: 0 4px 20px rgba(0,0,0,.35);
    }
    .panel h3 {
        margin: 0 0 12px 0;
        font-size: 1.05rem;
        color: #e0e0ff;
    }

    /* ── Architecture boxes ─────────────────────────── */
    .arch-row {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        justify-content: center;
    }
    .arch-node {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 72px;
        padding: 10px 16px;
        border-radius: 10px;
        font-weight: 600;
        font-size: 0.9rem;
        color: #fff;
        text-align: center;
    }
    .node-ok   { background: #22c55e; }
    .node-fail { background: #ef4444; }
    .node-ctrl { background: #6366f1; }
    .node-queue{ background: #f59e0b; color: #1e1e2f; }
    .arch-arrow { font-size: 1.3rem; color: #94a3b8; }

    /* ── Peer badges ───────────────────────────────── */
    .peer-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 600;
    }
    .peer-online  { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e55; }
    .peer-offline { background: #ef444422; color: #ef4444; border: 1px solid #ef444455; }

    /* ── Status badges ──────────────────────────────── */
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    .badge-running   { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e55; }
    .badge-failed    { background: #ef444422; color: #ef4444; border: 1px solid #ef444455; }
    .badge-stopped   { background: #94a3b822; color: #94a3b8; border: 1px solid #94a3b855; }
    .badge-init      { background: #6366f122; color: #6366f1; border: 1px solid #6366f155; }

    /* ── Signal cards ───────────────────────────────── */
    .signal-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #232340 100%);
        border: 1px solid #3a3a5c;
        border-radius: 12px;
        padding: 18px;
        text-align: center;
    }
    .signal-card h4 { margin: 0 0 8px; color: #c4b5fd; font-size: 1rem; }
    .signal-card .value { font-size: 1.6rem; font-weight: 700; color: #f1f5f9; }
    .signal-card .meta  { font-size: 0.78rem; color: #94a3b8; margin-top: 6px; }

    /* ── Concept check ──────────────────────────────── */
    .concept-check {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
        gap: 8px;
    }
    .concept-item {
        background: #22c55e15;
        border: 1px solid #22c55e44;
        border-radius: 8px;
        padding: 8px 14px;
        font-size: 0.85rem;
        color: #86efac;
    }

    /* ── Transparency list ──────────────────────────── */
    .transp-item {
        background: #6366f115;
        border-left: 3px solid #6366f1;
        border-radius: 0 8px 8px 0;
        padding: 10px 16px;
        margin-bottom: 8px;
    }
    .transp-item strong { color: #c4b5fd; }
    .transp-item span   { color: #cbd5e1; font-size: 0.85rem; }

    /* ── Message / timeline rows ────────────────────── */
    .msg-row {
        display: flex;
        gap: 12px;
        align-items: baseline;
        padding: 5px 0;
        border-bottom: 1px solid #ffffff0d;
        font-size: 0.84rem;
        color: #cbd5e1;
    }
    .msg-row .ts { color: #6366f1; font-weight: 600; min-width: 70px; }
    .msg-row .dir { color: #f59e0b; }
    .msg-row .pay { color: #94a3b8; }
    .msg-row .src { color: #818cf8; font-size: 0.78rem; }

    /* ── Log rows ───────────────────────────────────── */
    .log-row {
        display: flex;
        gap: 10px;
        align-items: baseline;
        padding: 4px 0;
        border-bottom: 1px solid #ffffff08;
        font-size: 0.82rem;
    }
    .log-node { color: #818cf8; font-weight: 600; min-width: 60px; }
    .log-time { color: #6366f1; min-width: 70px; }
    .log-msg  { color: #cbd5e1; }

    /* ── Health bar ──────────────────────────────────── */
    .health-bar {
        height: 8px;
        border-radius: 4px;
        margin-top: 8px;
    }
    .health-stable  { background: linear-gradient(90deg, #22c55e, #4ade80); }
    .health-degraded{ background: linear-gradient(90deg, #f59e0b, #fbbf24); }
    .health-critical{ background: linear-gradient(90deg, #ef4444, #f87171); }
    </style>
    """, unsafe_allow_html=True)


# ─── 1. Distributed Architecture View ──────────────────────────────────────

def show_architecture_view(node):
    st.markdown("### 🔧 1 · Distributed Architecture (Live View)")

    signal_ids = node.state_manager.signal_ids
    signals = node.state_manager.get_all_signals()
    info = node.get_info()
    peer_status = info.get("peer_status", {})

    # Signal columns + arrow + MOM + arrow + Controller
    cols = st.columns([1, 1, 1, 0.2, 1, 0.2, 1])
    for idx, sid in enumerate(signal_ids):
        s = signals.get(sid, {})
        status = s.get("status", "initialized")
        icon = "🟢" if status == "running" else "🔴" if status == "failed" else "🟡"
        cols[idx].markdown(f"**{sid}**\n\n{icon} {status.capitalize()}")

    cols[3].markdown("### →")
    cols[4].markdown("**Message Queue**\n\n📦 MOM (Pub-Sub)")
    cols[5].markdown("### →")
    cols[6].markdown("**Controller**\n\n🧠 Distributed")

    # Architecture label + peer status
    peer_html = ""
    for peer_key, is_online in peer_status.items():
        cls = "peer-online" if is_online else "peer-offline"
        dot = "●"
        peer_html += f' <span class="peer-badge {cls}">{dot} {peer_key}</span>'

    st.markdown(
        f"**Architecture:** Peer-to-Peer (Decentralized) &nbsp;|&nbsp; "
        f"**Middleware:** Simulated MOM (Pub-Sub) &nbsp;|&nbsp; "
        f"**Node:** `{info['node_id']}` on port `{info['port']}`"
        f"<br/>**Peers:** {peer_html if peer_html else 'None configured'}",
        unsafe_allow_html=True,
    )


# ─── 2. Live Message Flow Panel ────────────────────────────────────────────

def show_message_flow(node):
    st.markdown("### 📡 2 · Live Message Flow")

    rows = list(reversed(node.state_manager.message_flow_log[-10:]))
    if not rows:
        st.info("No messages yet — start the simulation.")
        return

    html = '<div class="panel">'
    for r in rows:
        src = r.get("source_node", "")
        src_html = f'<span class="src">[{src}]</span> ' if src else ""
        html += (
            f'<div class="msg-row">'
            f'<span class="ts">{r["timestamp"]}</span>'
            f'{src_html}'
            f'<span class="dir">{r["direction"]}</span>'
            f'<span class="pay">({r["payload"]})</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 3. Node State Visualization ───────────────────────────────────────────

def show_signal_cards(node):
    st.markdown("### 🚦 3 · Node State Visualization")

    signal_ids = node.state_manager.signal_ids
    signals = node.state_manager.get_all_signals()

    cols = st.columns(len(signal_ids))
    for idx, sid in enumerate(signal_ids):
        s = signals.get(sid, {})
        status = s.get("status", "unknown")
        badge_cls = {
            "running": "badge-running",
            "failed": "badge-failed",
            "stopped": "badge-stopped",
        }.get(status, "badge-init")

        vc = s.get("vehicle_count", 0)
        gt = s.get("green_time")
        gt_str = f"{gt} sec" if gt is not None else "pending"
        source = s.get("source_node", "-")

        with cols[idx]:
            html = f"""
            <div class="signal-card">
                <h4>Signal {sid}</h4>
                <div class="value">{vc} <span style="font-size:.8rem;font-weight:400;">vehicles</span></div>
                <div style="margin-top:6px;">
                    <span class="badge {badge_cls}">{status}</span>
                </div>
                <div class="meta">
                    Role: Publisher<br/>
                    Green time: {gt_str}<br/>
                    Source: {source}
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)


# ─── 4. Controller Decision Logic Panel ────────────────────────────────────

def show_controller_logic(node):
    st.markdown("### 🧠 4 · Controller Decision Logic")

    sm = node.state_manager
    recent = list(reversed(sm.control_history[-6:]))
    if not recent:
        st.info("No controller decisions yet.")
        return

    html = '<div class="panel">'
    for r in recent:
        sid = r["signal_id"]
        gt = r["green_time"]
        src = r.get("source_node", "")
        # Find matching event for vehicle count
        vc_label = ""
        for e in reversed(sm.event_history):
            if e["signal_id"] == sid:
                vc = e["vehicle_count"]
                level = sm.congestion_level(vc)
                vc_label = f" | Vehicles: {vc} &rarr; Congestion: <b>{level}</b>"
                break
        src_html = f" <span class='src'>[{src}]</span>" if src else ""
        html += (
            f'<div class="msg-row">'
            f'<span class="ts">{r["timestamp"]}</span>'
            f'<span class="dir">Received: {sid}{vc_label}</span>'
            f'{src_html}'
            f'<span class="pay">&rarr; Green = {gt}s</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 5. Failure Monitor Panel ──────────────────────────────────────────────

def show_failure_monitor(node):
    st.markdown("### ⚠️ 5 · Failure Monitor")

    signals = node.state_manager.get_all_signals()
    failed = [sid for sid, s in signals.items() if s.get("status") == "failed"]
    active = [sid for sid, s in signals.items() if s.get("status") == "running"]
    total = len(signals)

    if not failed:
        st.success("All nodes are healthy — no failures detected.")
    else:
        html = '<div class="panel">'
        for sid in failed:
            source = signals[sid].get("source_node", "?")
            html += (
                f'<div class="msg-row">'
                f'<span style="color:#ef4444;font-weight:600;">{sid} &rarr; FAILED</span>'
                f'<span class="pay">Recovery: Ignored in scheduling (source: {source})</span>'
                f'</div>'
            )
        running_status = "Running" if active else "Degraded"
        html += (
            f'<div style="margin-top:10px;color:#94a3b8;font-size:0.85rem;">'
            f'System Status: <b>{running_status}</b> '
            f'({len(active)}/{total} nodes active)'
            f'</div></div>'
        )
        st.markdown(html, unsafe_allow_html=True)


# ─── 6. Distributed System Concepts Panel ──────────────────────────────────

def show_ds_concepts():
    st.markdown("### 📚 6 · Distributed System Concepts")

    concepts = [
        "✔ P2P Architecture (No Central Server)",
        "✔ Communication (TCP Socket Message Passing)",
        "✔ Concurrency (Multithreaded Nodes)",
        "✔ Pub-Sub via Simulated MOM",
        "✔ State Replication (LWW Consistency)",
        "✔ Fault Tolerance (Graceful Degradation)",
        "✔ Distributed Logging",
        "✔ Heartbeat Liveness Detection",
    ]
    html = '<div class="panel"><div class="concept-check">'
    for c in concepts:
        html += f'<div class="concept-item">{c}</div>'
    html += "</div></div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 7. Concurrency Indicator ──────────────────────────────────────────────

def show_concurrency_indicator(node):
    signals = node.state_manager.get_all_signals()
    active = sum(1 for s in signals.values() if s.get("status") == "running")
    # Threads: active signals + controller + listener + relay + sync + heartbeat
    extra = 5 if node.running else 0
    total_threads = active + extra
    st.markdown(
        f"### 🔄 7 · Concurrency&emsp;"
        f"<span style='color:#22c55e;font-size:0.95rem;'>"
        f"{active} signal(s) + {'1 controller + 1 listener + 1 relay + 1 sync + 1 heartbeat' if node.running else '0'} "
        f"= <b>{total_threads} threads</b> running concurrently</span>",
        unsafe_allow_html=True,
    )


# ─── 8. Event Timeline ─────────────────────────────────────────────────────

def show_event_timeline(node):
    st.markdown("### 📜 8 · Event Timeline")

    rows = list(reversed(node.state_manager.event_timeline[-12:]))
    if not rows:
        st.info("No events yet.")
        return

    html = '<div class="panel">'
    for r in rows:
        html += (
            f'<div class="msg-row">'
            f'<span class="ts">{r["time"]}</span>'
            f'<span>{r["event"]}</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 9. Transparency Explanation Panel ─────────────────────────────────────

def show_transparency_panel():
    st.markdown("### 🔍 9 · Transparency Demonstration")

    items = [
        ("Access Transparency", "User sees results without knowing inter-node TCP communication details."),
        ("Location Transparency", "Signals appear as a unified dashboard — physical machine location is hidden."),
        ("Replication Transparency", "State is replicated across all nodes. Users see one unified view."),
        ("Failure Transparency", "System continues operating after a node failure; remaining nodes adjust."),
        ("Concurrency Transparency", "Multiple threads and nodes operate simultaneously without user awareness."),
    ]

    html = '<div class="panel">'
    for title, desc in items:
        html += (
            f'<div class="transp-item">'
            f'<strong>{title}</strong><br/>'
            f'<span>{desc}</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 10. System Status Summary ─────────────────────────────────────────────

def show_system_status(node):
    st.markdown("### 🌐 10 · System Status Summary")

    signals = node.state_manager.get_all_signals()
    info = node.get_info()
    total = len(signals)
    active = sum(1 for s in signals.values() if s.get("status") == "running")
    failed = sum(1 for s in signals.values() if s.get("status") == "failed")
    peers_online = sum(1 for v in info.get("peer_status", {}).values() if v)
    peers_total = len(info.get("peer_status", {}))

    if failed == 0:
        health, h_cls = "Stable", "health-stable"
    elif failed < total:
        health, h_cls = "Degraded", "health-degraded"
    else:
        health, h_cls = "Critical", "health-critical"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Nodes", total)
    c2.metric("Active Nodes", active)
    c3.metric("Failed Nodes", failed)
    c4.metric("Peers Online", f"{peers_online}/{peers_total}" if peers_total > 0 else "0")
    c5.metric("System Health", health)

    st.markdown(f'<div class="health-bar {h_cls}"></div>', unsafe_allow_html=True)


# ─── 11. Distributed Logs ──────────────────────────────────────────────────

def show_distributed_logs(node):
    st.markdown("### 📋 11 · Distributed Logs (All Nodes)")

    logs = node.logger.get_logs(limit=15)
    if not logs:
        st.info("No logs yet.")
        return

    html = '<div class="panel">'
    for entry in reversed(logs):
        html += (
            f'<div class="log-row">'
            f'<span class="log-node">{entry.get("node_id", "?")}</span>'
            f'<span class="log-time">{entry.get("time_str", "")}</span>'
            f'<span class="log-msg">{entry.get("message", "")}</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── Tables ────────────────────────────────────────────────────────────────

def show_tables(node):
    sm = node.state_manager
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Recent Congestion Events")
        event_rows = list(reversed(sm.event_history[-12:]))
        if event_rows:
            st.table(event_rows)
        else:
            st.write("No events yet.")

    with col_b:
        st.subheader("Recent Controller Decisions")
        control_rows = list(reversed(sm.control_history[-12:]))
        if control_rows:
            st.table(control_rows)
        else:
            st.write("No control updates yet.")


# ─── Sidebar Controls ──────────────────────────────────────────────────────

def show_controls(node):
    st.sidebar.markdown("## 🎛️ Simulation Controls")

    info = node.get_info()

    if node.running:
        if st.sidebar.button("⏹ Stop Simulation", use_container_width=True):
            node.stop(stop_p2p=False)
            st.rerun()
    else:
        if st.sidebar.button("▶ Start Simulation", use_container_width=True):
            try:
                node.start()
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Failed to start node: {e}. Port {node.port} might be in use.")

    st.sidebar.divider()

    # Show node info
    st.sidebar.markdown(f"**Node:** `{info['node_id']}`")
    st.sidebar.markdown(f"**Host:** `{info['host']}`")
    st.sidebar.markdown(f"**Port:** `{info['port']}`")
    peer_status = info.get("peer_status", {})
    for pk, online in peer_status.items():
        icon = "🟢" if online else "🔴"
        st.sidebar.markdown(f"**Peer:** {icon} `{pk}`")

    st.sidebar.divider()

    # Network Config Expander
    with st.sidebar.expander("⚙️ Network Config"):
        node_id = st.text_input("Node ID", value=info["node_id"])
        host = st.text_input("Bind Host", value=info["host"])
        port = st.number_input("TCP Port", value=info["port"], min_value=1024, max_value=65535)
        curr_peer_str = ",".join(f"{ip}:{p}" for ip, p in info["peers"])
        peer_str = st.text_input("Peers (comma-separated ip:port)", value=curr_peer_str)
        if st.button("Apply & Recreate Node", use_container_width=True):
            node.stop()
            peers = parse_peer_string(peer_str)
            new_node = DistributedNode(
                node_id=node_id,
                host=host,
                port=int(port),
                peers=peers,
            )
            st.session_state["distributed_node"] = new_node
            st.session_state["node_config"] = {
                "node_id": node_id,
                "host": host,
                "port": int(port),
                "peers": peers,
            }
            st.success("Recreated! Click 'Start Simulation'.")
            st.rerun()

    st.sidebar.divider()

    signal_ids = node.state_manager.signal_ids
    selected_signal = st.sidebar.selectbox("Select signal to fail", signal_ids)

    # Warn if no peers configured — failure won't propagate anywhere
    peer_status = node.communication.get_peer_status()
    if not peer_status:
        st.sidebar.warning("⚠️ No peers configured. Failure will be local-only.")
    else:
        online_peers = [k for k, v in peer_status.items() if v]
        if not online_peers:
            st.sidebar.warning("⚠️ All peers offline. Failure won't propagate until peers reconnect.")

    if st.sidebar.button("💥 Simulate Failure", use_container_width=True):
        if not node.running:
            st.sidebar.error("Node is not running. Start the simulation first.")
        else:
            node.fail_signal(selected_signal)
            st.sidebar.success(f"Failure triggered for {selected_signal}. Check peer logs.")

    st.sidebar.divider()
    if st.sidebar.button("🔄 Reset Simulation", use_container_width=True):
        old_node = st.session_state.get("distributed_node")
        if old_node:
            old_node.stop()
        cfg = st.session_state.get("node_config", {})
        new_node = DistributedNode(
            node_id=cfg.get("node_id", "Node-1"),
            host=cfg.get("host", "0.0.0.0"),
            port=cfg.get("port", 9001),
            peers=cfg.get("peers", []),
        )
        st.session_state["distributed_node"] = new_node
        st.success("Simulation reset. Press **Start** to relaunch.")
        st.rerun()



# ─── Overview ───────────────────────────────────────────────────────────────

def show_overview(node):
    info = node.get_info()
    status_label = "🟢 Running" if node.running else "🔴 Stopped"
    peers_online = sum(1 for v in info.get("peer_status", {}).values() if v)
    peers_total = len(info.get("peer_status", {}))
    st.markdown(
        f"**Simulation status:** {status_label} &nbsp;|&nbsp; "
        f"**Peers:** {peers_online}/{peers_total} online &nbsp;|&nbsp; "
        f"Update interval: every **2 seconds** while running."
    )
    st.markdown(
        "This dashboard demonstrates a **Distributed P2P Smart Traffic Signal Coordination System**. "
        "Each node runs independently with its own signals, controller, and state. "
        "Data is synchronized across nodes using **TCP sockets + JSON messaging** "
        "with **Last-Write-Wins** conflict resolution. "
        "Use the sidebar to start, stop, fail, or reset the simulation."
    )


# ─── Setup Page (shown before node is configured) ──────────────────────────

def show_setup_page(defaults):
    """Show config form before creating the node."""
    st.markdown("### ⚙️ Node Configuration")
    st.markdown(
        "Configure this node before starting. "
        "Enter the peer address(es) of other machines on your WiFi network."
    )

    col1, col2 = st.columns(2)
    with col1:
        node_id = st.text_input("Node ID", value=defaults.get("node_id", "Node-1"))
    with col2:
        port = st.number_input("TCP Port", value=defaults.get("port", 9001),
                                min_value=1024, max_value=65535)
    host = st.text_input("Bind host", value=defaults.get("host", "0.0.0.0"))

    peer_str = st.text_input(
        "Peer addresses (comma-separated ip:port)",
        value=defaults.get("peers", ""),
        placeholder="e.g. 192.168.1.102:9001 or 127.0.0.1:9002",
    )

    st.markdown("""
    **Quick start examples:**
    - **Same machine demo:** Set port `9001`, peers `127.0.0.1:9002`.
      Open another terminal and run with port `9002`, peers `127.0.0.1:9001`.
    - **Two laptops:** Find IPs with `ipconfig`, enter the other laptop's IP.
    """)

    if st.button("🚀 Create Node & Start", use_container_width=True, type="primary"):
        peers = parse_peer_string(peer_str)
        node = DistributedNode(
            node_id=node_id,
            host=host,
            port=int(port),
            peers=peers,
        )
        node.start()
        st.session_state["distributed_node"] = node
        st.session_state["node_config"] = {
            "node_id": node_id, "host": host, "port": int(port), "peers": peers
        }
        st.rerun()

    st.divider()
    st.markdown("**Or** create node without starting (configure, then press Start in sidebar):")
    if st.button("📝 Create Node Only", use_container_width=True):
        peers = parse_peer_string(peer_str)
        node = DistributedNode(
            node_id=node_id,
            host=host,
            port=int(port),
            peers=peers,
        )
        st.session_state["distributed_node"] = node
        st.session_state["node_config"] = {
            "node_id": node_id, "host": host, "port": int(port), "peers": peers
        }
        st.rerun()


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Smart Traffic Signal Dashboard",
        page_icon="🚦",
        layout="wide",
    )
    inject_css()

    st.title("🚦 Smart Traffic Signal Coordination")

    node = get_node()


    # Normal dashboard (same layout as original)
    show_controls(node)
    show_overview(node)

    if node.running:
        st.button("🔃 Refresh Dashboard")

    # ── Row 1: Architecture + System Status ─────────────────
    show_architecture_view(node)
    show_system_status(node)

    st.divider()

    # ── Row 2: Signal cards ─────────────────────────────────
    show_signal_cards(node)

    st.divider()

    # ── Row 3: Message Flow + Controller Logic ──────────────
    left, right = st.columns(2)
    with left:
        show_message_flow(node)
    with right:
        show_controller_logic(node)

    st.divider()

    # ── Row 4: Failure Monitor + Concurrency ────────────────
    show_failure_monitor(node)
    show_concurrency_indicator(node)

    st.divider()

    # ── Row 5: Event Timeline ───────────────────────────────
    show_event_timeline(node)

    st.divider()

    # ── Row 6: DS Concepts + Transparency ───────────────────
    left2, right2 = st.columns(2)
    with left2:
        show_ds_concepts()
    with right2:
        show_transparency_panel()

    st.divider()

    # ── Row 7: Distributed Logs ─────────────────────────────
    show_distributed_logs(node)

    st.divider()

    # ── Row 8: Raw tables ───────────────────────────────────
    show_tables(node)

    # ── Auto-refresh ────────────────────────────────────────
    # Refresh every 2 seconds to reflect remote updates even if local simulation is stopped
    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()
