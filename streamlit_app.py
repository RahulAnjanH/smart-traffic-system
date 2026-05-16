import streamlit as st
from traffic_system import TrafficSystem


# ─── Helpers ────────────────────────────────────────────────────────────────

def get_system():
    if "traffic_system" not in st.session_state:
        st.session_state["traffic_system"] = TrafficSystem()
    return st.session_state["traffic_system"]


# ─── Custom CSS ─────────────────────────────────────────────────────────────

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

def show_architecture_view(system):
    st.markdown("### 🔧 1 · Distributed Architecture (Live View)")

    cols = st.columns([1, 1, 1, 0.2, 1, 0.2, 1])
    for idx, sid in enumerate(system.signal_ids):
        s = system.signal_status.get(sid, {})
        status = s.get("status", "initialized")
        icon = "🟢" if status == "running" else "🔴" if status == "failed" else "🟡"
        cols[idx].markdown(f"**{sid}**\n\n{icon} {status.capitalize()}")

    cols[3].markdown("### →")
    cols[4].markdown("**Message Queue**\n\n📦 Queue")
    cols[5].markdown("### →")
    cols[6].markdown("**Controller**\n\n🧠 Central Coordinator")

    st.markdown(
        "**Architecture:** Client-Server + Publisher-Subscriber  |  "
        "**Middleware:** Message-Oriented Middleware (Queue)"
    )


# ─── 2. Live Message Flow Panel ────────────────────────────────────────────

def show_message_flow(system):
    st.markdown("### 📡 2 · Live Message Flow")

    rows = list(reversed(system.message_flow_log[-10:]))
    if not rows:
        st.info("No messages yet — start the simulation.")
        return

    html = '<div class="panel">'
    for r in rows:
        html += (
            f'<div class="msg-row">'
            f'<span class="ts">{r["timestamp"]}</span>'
            f'<span class="dir">{r["direction"]}</span>'
            f'<span class="pay">({r["payload"]})</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 3. Node State Visualization ───────────────────────────────────────────

def show_signal_cards(system):
    st.markdown("### 🚦 3 · Node State Visualization")

    cols = st.columns(len(system.signal_ids))
    for idx, sid in enumerate(system.signal_ids):
        s = system.signal_status.get(sid, {})
        status = s.get("status", "unknown")
        badge_cls = {
            "running": "badge-running",
            "failed": "badge-failed",
            "stopped": "badge-stopped",
        }.get(status, "badge-init")

        vc = s.get("last_vehicle_count", 0)
        gt = s.get("last_green_time")
        gt_str = f"{gt} sec" if gt is not None else "pending"
        evt = s.get("last_event_time", "-")

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
                    Last event: {evt}
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)


# ─── 4. Controller Decision Logic Panel ────────────────────────────────────

def show_controller_logic(system):
    st.markdown("### 🧠 4 · Controller Decision Logic")

    recent = list(reversed(system.control_history[-6:]))
    if not recent:
        st.info("No controller decisions yet.")
        return

    html = '<div class="panel">'
    for r in recent:
        sid = r["signal_id"]
        gt = r["green_time"]
        # reverse-engineer vehicle count from the matching event
        vc_label = ""
        for e in reversed(system.event_history):
            if e["signal_id"] == sid:
                vc = e["vehicle_count"]
                level = TrafficSystem._congestion_level(vc)
                vc_label = f" | Vehicles: {vc} → Congestion: <b>{level}</b>"
                break
        html += (
            f'<div class="msg-row">'
            f'<span class="ts">{r["timestamp"]}</span>'
            f'<span class="dir">Received: {sid}{vc_label}</span>'
            f'<span class="pay">→ Green = {gt}s</span>'
            f'</div>'
        )
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 5. Failure Monitor Panel ──────────────────────────────────────────────

def show_failure_monitor(system):
    st.markdown("### ⚠️ 5 · Failure Monitor")

    failed = [sid for sid, s in system.signal_status.items() if s.get("status") == "failed"]
    active = [sid for sid, s in system.signal_status.items() if s.get("status") == "running"]
    total = len(system.signal_ids)

    if not failed:
        st.success("All nodes are healthy — no failures detected.")
    else:
        html = '<div class="panel">'
        for sid in failed:
            html += (
                f'<div class="msg-row">'
                f'<span style="color:#ef4444;font-weight:600;">{sid} → FAILED</span>'
                f'<span class="pay">Recovery: Ignored in scheduling</span>'
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
        "✔ Communication (Message Passing)",
        "✔ Concurrency (Multithreaded Nodes)",
        "✔ Coordination (Centralized Controller)",
        "✔ Fault Tolerance (Failure Recovery)",
        "✔ Pub-Sub Architecture",
        "✔ Message-Oriented Middleware",
    ]
    html = '<div class="panel"><div class="concept-check">'
    for c in concepts:
        html += f'<div class="concept-item">{c}</div>'
    html += "</div></div>"
    st.markdown(html, unsafe_allow_html=True)


# ─── 7. Concurrency Indicator ──────────────────────────────────────────────

def show_concurrency_indicator(system):
    active = sum(1 for s in system.signal_status.values() if s.get("status") == "running")
    # +1 for the controller when running
    total_threads = active + (1 if system.running else 0)
    st.markdown(
        f"### 🔄 7 · Concurrency&emsp;"
        f"<span style='color:#22c55e;font-size:0.95rem;'>"
        f"{active} signal node(s) + {'1 controller' if system.running else '0 controllers'} "
        f"= <b>{total_threads} threads</b> running concurrently</span>",
        unsafe_allow_html=True,
    )


# ─── 8. Event Timeline ─────────────────────────────────────────────────────

def show_event_timeline(system):
    st.markdown("### 📜 8 · Event Timeline")

    rows = list(reversed(system.event_timeline[-12:]))
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
        ("Access Transparency", "User sees results without knowing inter-thread communication details."),
        ("Location Transparency", "Signals appear as a unified dashboard — location of each thread is hidden."),
        ("Failure Transparency", "System continues operating after a node failure; remaining nodes adjust."),
        ("Concurrency Transparency", "Multiple signal nodes operate simultaneously without user awareness."),
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

def show_system_status(system):
    st.markdown("### 🌐 10 · System Status Summary")

    total = len(system.signal_ids)
    active = sum(1 for s in system.signal_status.values() if s.get("status") == "running")
    failed = sum(1 for s in system.signal_status.values() if s.get("status") == "failed")

    if failed == 0:
        health, h_cls = "Stable", "health-stable"
    elif failed < total:
        health, h_cls = "Degraded", "health-degraded"
    else:
        health, h_cls = "Critical", "health-critical"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Nodes", total)
    c2.metric("Active Nodes", active)
    c3.metric("Failed Nodes", failed)
    c4.metric("System Health", health)

    st.markdown(f'<div class="health-bar {h_cls}"></div>', unsafe_allow_html=True)


# ─── Sidebar Controls ──────────────────────────────────────────────────────

def show_controls(system):
    st.sidebar.markdown("## 🎛️ Simulation Controls")

    if st.sidebar.button("▶ Start Simulation", use_container_width=True):
        system.start()

    if st.sidebar.button("⏹ Stop Simulation", use_container_width=True):
        system.stop()

    st.sidebar.divider()
    selected_signal = st.sidebar.selectbox("Select signal to fail", system.signal_ids)
    if st.sidebar.button("💥 Simulate Failure", use_container_width=True):
        system.fail_signal(selected_signal)

    st.sidebar.divider()
    if st.sidebar.button("🔄 Reset Simulation", use_container_width=True):
        system.stop()
        st.session_state["traffic_system"] = TrafficSystem()
        st.success("Simulation reset. Press **Start** to relaunch.")


# ─── Overview ───────────────────────────────────────────────────────────────

def show_overview(system):
    status_label = "🟢 Running" if system.running else "🔴 Stopped"
    st.markdown(
        f"**Simulation status:** {status_label} &nbsp;|&nbsp; "
        f"Update interval: every **2 seconds** while running."
    )
    st.markdown(
        "This dashboard demonstrates a **Smart Traffic Signal Coordination System** "
        "built on distributed-systems principles. Traffic signals publish congestion "
        "events through a message queue to a central controller, which decides green-time "
        "adjustments. Use the sidebar to start, stop, fail, or reset the simulation."
    )


# ─── Tables (kept from original) ───────────────────────────────────────────

def show_tables(system):
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Recent Congestion Events")
        event_rows = list(reversed(system.event_history[-12:]))
        if event_rows:
            st.table(event_rows)
        else:
            st.write("No events yet.")

    with col_b:
        st.subheader("Recent Controller Decisions")
        control_rows = list(reversed(system.control_history[-12:]))
        if control_rows:
            st.table(control_rows)
        else:
            st.write("No control updates yet.")


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Smart Traffic Signal Dashboard",
        page_icon="🚦",
        layout="wide",
    )
    inject_css()

    st.title("🚦 Smart Traffic Signal Coordination")

    system = get_system()
    show_controls(system)
    show_overview(system)

    if system.running:
        st.button("🔃 Refresh Dashboard")

    # ── Row 1: Architecture + System Status ─────────────────
    show_architecture_view(system)
    show_system_status(system)

    st.divider()

    # ── Row 2: Signal cards ─────────────────────────────────
    show_signal_cards(system)

    st.divider()

    # ── Row 3: Message Flow + Controller Logic ──────────────
    left, right = st.columns(2)
    with left:
        show_message_flow(system)
    with right:
        show_controller_logic(system)

    st.divider()

    # ── Row 4: Failure Monitor + Concurrency ────────────────
    show_failure_monitor(system)
    show_concurrency_indicator(system)

    st.divider()

    # ── Row 5: Event Timeline ───────────────────────────────
    show_event_timeline(system)

    st.divider()

    # ── Row 6: DS Concepts + Transparency ───────────────────
    left2, right2 = st.columns(2)
    with left2:
        show_ds_concepts()
    with right2:
        show_transparency_panel()

    st.divider()

    # ── Row 7: Raw tables ───────────────────────────────────
    show_tables(system)


if __name__ == "__main__":
    main()
