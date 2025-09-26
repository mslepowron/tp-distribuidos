import csv
import io
from typing import Dict, List, Tuple
from .schemas import SCHEMAS
from .message import Header, HeaderError, PayloadError, ProtocolError

def deserialize_message(message_bytes: bytes, schema: List[str]) -> Tuple[Header, List[Dict[str, str]]]:
    try:
        header_bytes, payload_bytes = message_bytes.split(b"\n===\n", 1)
    except Exception as e:
        raise ProtocolError("Formato de mensaje inválido: separador '\\n===\\n' no encontrado")

    header = Header.decode(header_bytes)
    try:
        byte_stream = io.BytesIO(payload_bytes)
        text_reader = io.TextIOWrapper(byte_stream, encoding="utf-8", newline="")
        reader = csv.DictReader(text_reader, fieldnames=schema)
        rows = list(reader)
    except Exception as e:
        raise PayloadError(f"Error deserializando payload: {e}")

    return header, rows