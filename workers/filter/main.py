import logging
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from filter import Filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

def main():
    try:

        mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name="filters",
            exchange_type="direct",
            route_keys=["filters_year", "filters_hour", "filters_amount"]  # creo que puedo declarar todas pero consumimos solo "year"
        )
        #result_mw = mw #este year filter se lo pasa al hour filter. Publicamos los resultados en el mismo exchange
        
        result_mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name="results",
            exchange_type="direct",
            route_keys=["coffee_results"]  # el app_controller está bindeado a esto
        )
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    f = Filter(mw, result_mw)

    f.start()

if __name__ == "__main__":
    main()
