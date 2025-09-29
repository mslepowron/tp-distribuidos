import csv
import io
from typing import Dict, List, Tuple
from .schemas import SCHEMAS
from .message import Header, HeaderError, PayloadError, ProtocolError
import logging

def deserialize_message(message_bytes: bytes) -> Tuple[Header, List[Dict[str, str]]]:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("deserialize")
    
    if b"\n===\n" not in message_bytes:
        try:
            header = Header.decode(message_bytes)
            return header, []
        except Exception:
            raise ProtocolError("Formato de mensaje inválido: no se encontró separador '\\n===\\n' ni se pudo decodificar el header.")

    try:
        header_bytes, payload_bytes = message_bytes.split(b"\n===\n", 1)
    except Exception:
        raise ProtocolError("Formato de mensaje inválido: separador '\\n===\\n' no encontrado")

    header = Header.decode(header_bytes)

    try:
        raw_fieldnames = header.fields["schema"]
        raw_fieldnames = raw_fieldnames.strip()[1:-1]

        # dividir por coma
        parts = raw_fieldnames.split(",")

        # limpiar cada valor (sacar comillas simples/dobles y espacios extra)
        fieldnames = [p.strip().strip("'").strip('"') for p in parts]
    except KeyError:
        raise PayloadError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    try:
        if not payload_bytes.strip():
            return header, []

        byte_stream = io.BytesIO(payload_bytes)
        text_reader = io.TextIOWrapper(byte_stream, encoding="utf-8", newline="")
        reader = csv.DictReader(text_reader, fieldnames=fieldnames)
        rows = [row for row in reader if any((v or "") != "" for v in row.values())]
    except Exception as e:
        raise PayloadError(f"Error deserializando payload: {e}")

    return header, rows