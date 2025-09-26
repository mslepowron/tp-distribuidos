import signal
import os
import configparser
from app_controller import AppController


QUERY_SELECTED= "Q_AMOUNT_75_TX"   #TODO: Queda hardcodeado pero desp tiene que venir con el mensaje del cliente.

def graceful_shutdown(signum, frame):
    """Handler para SIGTERM o SIGINT que llama al shutdown del controller."""
    if graceful_shutdown.controller:
        print(f"Signal:{signum} recieved, starting app-controller shutdown...")
        graceful_shutdown.controller.shutdown()

def main():
    config_path = os.getenv("CONFIG_PATH", "config/config.ini")
    config = configparser.ConfigParser()
    config.read(config_path)
    

    config_section = QUERY_SELECTED
    input_exchange = config[config_section]["INPUT_EXCHANGE"]
    routing_keys = config[config_section]["ROUTING_KEYS"].split(",")
    result_exchange = config[config_section]["RESULT_EXCHANGE"]
    result_queue = config[config_section]["RESULT_QUEUE"]

    controller = AppController(
        host="rabbitmq",
        input_exchange=input_exchange,
        routing_keys=routing_keys,
        result_exchange=result_exchange,
        result_queue=result_queue
    )

    graceful_shutdown.controller = controller

    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    controller.run()

if __name__ == "__main__":
    main()