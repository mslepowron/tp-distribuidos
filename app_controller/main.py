import signal
from app_controller import AppController


CSV_FILE = "../transactions.csv"

def graceful_shutdown(signum, frame):
    """Handler para SIGTERM o SIGINT que llama al shutdown del controller."""
    if graceful_shutdown.controller:
        print(f"Signal:{signum} recieved, starting app-controller shutdown...")
        graceful_shutdown.controller.shutdown()

def main():
    controller = AppController(
        host="rabbitmq",
        exchange_name="filters",
        #queue_name="coffee_tasks",
        csv_file=CSV_FILE,
    )

    graceful_shutdown.controller = controller

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    controller.run()

if __name__ == "__main__":
    main()