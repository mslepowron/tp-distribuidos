import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("envar-parser")

def parse_json_env(name, default=None):
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        logger.warning(f"Could not parse {name}='{raw}', using default={default}")
        return default