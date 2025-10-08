import json
import logging
import os
from pathlib import Path
from middleware.rabbitmq.mom import MessageMiddlewareExchange
from lib.joinFactory import JoinFactory


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("join-main")

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
        join_type = os.getenv("JOIN_TYPE")
        queue_name = os.getenv("QUEUE_NAME", f"{join_type.lower()}_q")

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
        

        storage = Path(os.getenv("STORAGE_DIR", "storage"))
        logger.info(f"STORAGE: {storage}")
        logger.info(f"routing keys: {output_rks}")

        j = JoinFactory.create(join_type, mw_in, mw_out, output_exchange, output_rks, input_bindings, storage)

        j.start()

    except Exception as e:
        logger.error(f"Failed to initialize join: {e}")

if __name__ == "__main__":
    main()
