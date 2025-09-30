import signal
import os
import configparser
from app_controller import AppController

def graceful_shutdown(signum, frame):
    """Handler para SIGTERM o SIGINT que llama al shutdown del controller."""
    if graceful_shutdown.controller:
        print(f"Signal:{signum} recieved, starting app-controller shutdown...")
        graceful_shutdown.controller.shutdown()

def main():
    config_path = os.getenv("CONFIG_PATH", "config/config.ini")
    config = configparser.ConfigParser()
    config.read(config_path)
    
    cfg = config["DEFAULT"]
    input_exchange = cfg["INPUT_EXCHANGE"]
    routing_keys = [rk.strip() for rk in cfg["ROUTING_KEYS"].split(",")]
    result_exchange = cfg["RESULT_EXCHANGE"]
    result_routing_keys = [q.strip() for q in cfg["RESULT_QUEUES"].split(",")]
    result_sink_queue = cfg["RESULT_SINK_QUEUE"]

    controller = AppController(
        host="rabbitmq",
        input_exchange=input_exchange,
        input_routing_keys=routing_keys,         # ["transactions", "transaction_items", "menu"] etc.
        result_exchange=result_exchange,
        result_routing_keys=result_routing_keys, # ["q_amount_75_tx", etc]
        result_sink_queue=result_sink_queue,
    )

    graceful_shutdown.controller = controller

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    controller.run()

if __name__ == "__main__":
    main()