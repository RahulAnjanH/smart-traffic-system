import threading
import time
import random
from shared_queue import event_queue, control_queue

class TrafficSignal(threading.Thread):
    def __init__(self, signal_id, event_callback=None, update_callback=None):
        threading.Thread.__init__(self)
        self.signal_id = signal_id
        self.running = True
        self.event_callback = event_callback
        self.update_callback = update_callback
        self.current_green_time = None

    def run(self):
        while self.running:
            # Simulate vehicle count
            vehicle_count = random.randint(5, 50)

            event = {
                "signal_id": self.signal_id,
                "vehicle_count": vehicle_count,
                "timestamp": time.time()
            }

            print(f"[Signal {self.signal_id}] Vehicles: {vehicle_count}")

            event_queue.put(event)

            if self.event_callback:
                try:
                    self.event_callback(event)
                except Exception:
                    pass

            self.check_for_updates()

            time.sleep(2)

    def check_for_updates(self):
        pending_updates = []

        while not control_queue.empty():
            update = control_queue.get()

            if update["signal_id"] == self.signal_id:
                self.current_green_time = update["green_time"]

                print(f"🚦 [Signal {self.signal_id}] New GREEN time: {update['green_time']} sec")

                if self.update_callback:
                    try:
                        self.update_callback(update)
                    except Exception:
                        pass
            else:
                pending_updates.append(update)

        for update in pending_updates:
            control_queue.put(update)

    def stop(self):
        self.running = False