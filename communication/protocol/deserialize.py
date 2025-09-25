import csv
import io
from typing import Dict, List
from .schemas import RAW_SCHEMAS

# Deserializa un mensaje en bytes
def deserialize_batch(message_bytes: bytes) -> List[Dict[str, str]]:
    print(f"\n Deserializando mensaje recibido ({len(message_bytes)} bytes)...")

    byte_stream = io.BytesIO(message_bytes)
    text_reader = io.TextIOWrapper(byte_stream, encoding="utf-8", newline="")

    reader = csv.DictReader(text_reader, RAW_SCHEMAS["transactions.raw"])
    rows = list(reader)

    print(f"Batch deserializado: {len(rows)} filas\n")
    return rows