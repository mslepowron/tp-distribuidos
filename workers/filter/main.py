import logging
import os
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from lib.filter import Filter
from lib.filterFactory import FilterFactory


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

def get_env_list(var_name, default=None):
    """Convierte '[a, b]' en ['a', 'b'] y 'a' en ['a']"""
    value = os.getenv(var_name, default)
    if not value:
        return []
    value = value.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]  # quita corchetes
    # separa por coma y limpia espacios
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items

def main():
    try:
        filter_type = os.getenv("FILTER_TYPE")
        # Inputs
        input_exchange = os.getenv("INPUT_EXCHANGE")
        input_exchange_type = os.getenv("INPUT_EXCHANGE_TYPE")
        input_routing_key = get_env_list("INPUT_ROUTING_KEY")

        # Outputs
        output_exchange = os.getenv("OUTPUT_EXCHANGE")
        output_exchange_type = os.getenv("OUTPUT_EXCHANGE_TYPE")
        output_routing_key = get_env_list("OUTPUT_ROUTING_KEY")
        
        # Inputs (casp filter hour)
        input_exchange2 = os.getenv("OUTPUT_EXCHANGE2", None)
        input_exchange_type2 = os.getenv("OUTPUT_EXCHANGE_TYPE2", None)
        input_routing_key2 = get_env_list("OUTPUT_ROUTING_KEY2", None)

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

    f = FilterFactory.create(filter_type, mw, result_mw, input_routing_key, output_routing_key)

    f.start()

if __name__ == "__main__":
    main()
