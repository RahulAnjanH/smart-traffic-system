import threading
import time
from shared_queue import event_queue, control_queue

class TrafficController(threading.Thread):
    def __init__(self, update_callback=None):
        threading.Thread.__init__(self)
        self.running = True
        self.update_callback = update_callback

    def run(self):
        while self.running:
            if not event_queue.empty():
                event = event_queue.get()

                signal_id = event["signal_id"]
                vehicle_count = event["vehicle_count"]

                print(f"🧠 [Controller] Received from {signal_id}: {vehicle_count}")

                # Decision logic
                if vehicle_count > 30:
                    green_time = 60
                elif vehicle_count > 15:
                    green_time = 40
                else:
                    green_time = 20

                update = {
                    "signal_id": signal_id,
                    "green_time": green_time,
                    "timestamp": time.time()
                }

                # Send control decision
                control_queue.put(update)

                print(f"🧠 [Controller] Set {signal_id} green time → {green_time}")

                if self.update_callback:
                    try:
                        self.update_callback(update)
                    except Exception:
                        pass

    def stop(self):
        self.running = False