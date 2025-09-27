import logging
import os
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from lib.filter import Filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

def main():
    try:
        filter_type = os.getenv("FILTER_TYPE")
        next_worker = os.getenv("RESULT_QUEUE")
        mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name="filters",
            exchange_type="direct",
            route_keys=["filters_year", "filters_hour", "filters_amount"]  # creo que puedo declarar todas pero consumimos solo "year"
        )
        #result_mw = mw #este year filter se lo pasa al hour filter. Publicamos los resultados en el mismo exchange
        
        if next_worker == "coffee_results":
            logger.error(f"ESTA MANDA LA DATAAAA")
            result_mw = MessageMiddlewareExchange(
                host="rabbitmq",
                exchange_name="results",
                exchange_type="direct",
                route_keys=["coffee_results"]  # el app_controller está bindeado a esto
            )
        else:
            logger.info(f"No va al result wueeeee")
            result_mw = mw
            
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    f = Filter(mw, result_mw, next_worker, filter_type)

    f.start()

if __name__ == "__main__":
    main()
