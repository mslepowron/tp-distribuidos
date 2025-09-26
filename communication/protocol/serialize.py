import csv
import io
from typing import Dict, List, Tuple
from .schemas import SCHEMAS
from .message import Header, HeaderError, PayloadError

def serialize_message(header: Header, rows: List[Dict[str, str]], schema: List[str]) -> bytes:
    try:
        # serializamos payload CSV
        byte_buffer = io.BytesIO()
        text_writer = io.TextIOWrapper(byte_buffer, encoding="utf-8", newline="")

        writer = csv.DictWriter(text_writer, fieldnames=schema, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)
        text_writer.flush()

        payload = byte_buffer.getvalue()
        return header.encode() + b"\n===\n" + payload  # separador entre header y payload
    except HeaderError:
        raise  
    except Exception as e:
        raise PayloadError(f"Error serializando payload: {e}")


