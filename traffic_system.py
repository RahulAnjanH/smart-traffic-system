import time
import datetime
from controller import TrafficController
from shared_queue import event_queue, control_queue
from traffic_signal import TrafficSignal


class TrafficSystem:
    def __init__(self, signal_ids=None):
        self.signal_ids = signal_ids or ["S1", "S2", "S3"]
        self.event_history = []
        self.control_history = []
        self.message_flow_log = []
        self.event_timeline = []
        self.signal_status = {}
        self.signals = []
        self.controller = None
        self.running = False
        self._create_signal_status()

    def _create_signal_status(self):
        self.signal_status = {
            signal_id: {
                "signal_id": signal_id,
                "status": "initialized",
                "last_vehicle_count": 0,
                "last_green_time": None,
                "last_event_time": None,
            }
            for signal_id in self.signal_ids
        }

    def _format_timestamp(self, timestamp):
        return time.strftime("%H:%M:%S", time.localtime(timestamp))

    @staticmethod
    def _congestion_level(vehicle_count):
        if vehicle_count > 30:
            return "HIGH"
        elif vehicle_count > 15:
            return "MEDIUM"
        return "LOW"

    def _on_event(self, event):
        ts = self._format_timestamp(event["timestamp"])
        sid = event["signal_id"]
        vc = event["vehicle_count"]

        status = self.signal_status.get(sid)
        if status is not None:
            status["status"] = "running"
            status["last_vehicle_count"] = vc
            status["last_event_time"] = ts

        self.event_history.append({
            "signal_id": sid,
            "vehicle_count": vc,
            "timestamp": ts,
        })

        # Message flow: signal → queue → controller
        self.message_flow_log.append({
            "direction": f"{sid} → Queue → Controller",
            "payload": f"vehicle_count={vc}",
            "timestamp": ts,
        })

        # Event timeline
        self.event_timeline.append({
            "time": ts,
            "event": f"{sid} published congestion event (vehicles={vc})",
        })

        if len(self.event_history) > 100:
            self.event_history.pop(0)
        if len(self.message_flow_log) > 100:
            self.message_flow_log.pop(0)
        if len(self.event_timeline) > 100:
            self.event_timeline.pop(0)

    def _on_control(self, update):
        ts = self._format_timestamp(update["timestamp"])
        sid = update["signal_id"]
        gt = update["green_time"]

        self.control_history.append({
            "signal_id": sid,
            "green_time": gt,
            "timestamp": ts,
        })

        # Message flow: controller → signal
        self.message_flow_log.append({
            "direction": f"Controller → {sid}",
            "payload": f"green_time={gt}",
            "timestamp": ts,
        })

        # Event timeline
        self.event_timeline.append({
            "time": ts,
            "event": f"Controller updated {sid} (green_time={gt}s)",
        })

        if len(self.control_history) > 100:
            self.control_history.pop(0)
        if len(self.message_flow_log) > 100:
            self.message_flow_log.pop(0)
        if len(self.event_timeline) > 100:
            self.event_timeline.pop(0)

        status = self.signal_status.get(sid)
        if status is not None:
            status["last_green_time"] = gt

    def _on_signal_update(self, update):
        self._on_control(update)

    def start(self):
        if self.running:
            return

        while not event_queue.empty():
            event_queue.get()

        while not control_queue.empty():
            control_queue.get()

        self.event_history.clear()
        self.control_history.clear()
        self.message_flow_log.clear()
        self.event_timeline.clear()
        self._create_signal_status()

        self.signals = [
            TrafficSignal(
                signal_id,
                event_callback=self._on_event,
                update_callback=self._on_signal_update,
            )
            for signal_id in self.signal_ids
        ]

        self.controller = TrafficController(update_callback=self._on_control)

        for signal in self.signals:
            signal.start()

        self.controller.start()
        self.running = True

    def stop(self):
        if not self.running:
            return

        for signal in self.signals:
            signal.stop()

        if self.controller:
            self.controller.stop()

        for signal in self.signals:
            signal.join(timeout=2)

        if self.controller:
            self.controller.join(timeout=2)

        for status in self.signal_status.values():
            if status["status"] != "failed":
                status["status"] = "stopped"

        self.running = False

    def fail_signal(self, signal_id):
        for signal in self.signals:
            if signal.signal_id == signal_id:
                signal.stop()
                self.signal_status[signal_id]["status"] = "failed"
                break
