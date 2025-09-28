import logging
import os
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from lib.filter import Filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

def main():
    try:
        filter_type = os.getenv("FILTER_TYPE")

        # caso especial filter hour que tiene dos exchanges input
        # Los joins siempre tienen dos exchanges input

        input_exchange = None
        input_routing_key = None
        input_exchange_type = None
        
        output_exchange = None
        output_routing_key = None
        output_exchange_type = None


        mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name=input_exchange,
            exchange_type=input_exchange_type,
            route_keys=input_routing_key  
        )
        
        result_mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name=output_exchange,
            exchange_type=output_exchange_type,
            route_keys=output_routing_key  
        )
            
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    f = Filter(mw, result_mw, filter_type, input_routing_key, output_routing_key)

    f.start()

if __name__ == "__main__":
    main()
