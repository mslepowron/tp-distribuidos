import json
import logging
import os
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from lib.filterFactory import FilterFactory


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("filter-main")

def parse_json_env(name, default=None):
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        logger.warning(f"Could not parse {name}='{raw}', using default={default}")
        return default

def main():
    try:
        filter_type = os.getenv("FILTER_TYPE")
        queue_name = os.getenv("QUEUE_NAME", f"{filter_type.lower()}_q")

        input_bindings  = parse_json_env("INPUT_BINDINGS", [])
        output_exchange = os.getenv("OUTPUT_EXCHANGE", "")
        output_rks      = parse_json_env("OUTPUT_RKS", [])

        mw_in = MessageMiddlewareExchange(
            host="rabbitmq",
            queue_name=queue_name,
            bindings=[(ex, ex_type, rk) for ex, ex_type, rk in input_bindings]
        )

        mw_out = MessageMiddlewareExchange(
            host="rabbitmq",
            queue_name=f"{queue_name}.out"  # cola solo para tener canal; no se consume
        )

        logger.info(f"ROUTING KEYS IN MAIN {output_rks}")
        f = FilterFactory.create(filter_type, mw_in, mw_out, output_exchange, output_rks, input_bindings)

        f.start()

    except Exception as e:
        logger.error(f"Fallo al inicializar filter: {e}")

if __name__ == "__main__":
    main()
