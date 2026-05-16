from traffic_signal import TrafficSignal
from controller import TrafficController
import time

def main():
    # Create signals
    signal1 = TrafficSignal("S1")
    signal2 = TrafficSignal("S2")
    signal3 = TrafficSignal("S3")

    # Create controller
    controller = TrafficController()

    # Start all threads
    signal1.start()
    signal2.start()
    signal3.start()
    controller.start()

    # Run system for some time
    time.sleep(20)

    # Simulate failure of one signal
    print("\n⚠️ Simulating failure of Signal S2...\n")
    signal2.stop()

    time.sleep(15)

    # Stop all
    signal1.stop()
    signal3.stop()
    controller.stop()

    print("\n✅ System shutting down...\n")

if __name__ == "__main__":
    main()