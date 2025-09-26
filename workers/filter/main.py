import logging
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from filter import Filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

def main():
    try:
        logger.error(f"Arranca nomas")
        mw = MessageMiddlewareQueue(host="rabbitmq", queue_name="coffee_tasks")
        result_mw = MessageMiddlewareQueue(host="rabbitmq", queue_name="coffee_results")
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    logger.error(f"Inicializo filter")
    f = Filter(mw, result_mw)
    logger.error(f"Filter start")
    f.start()

if __name__ == "__main__":
    main()
