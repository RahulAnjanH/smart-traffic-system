from queue import Queue

# Global event queue (acts like Pub/Sub broker)
event_queue = Queue()

# Queue for sending control updates back to signals
control_queue = Queue()