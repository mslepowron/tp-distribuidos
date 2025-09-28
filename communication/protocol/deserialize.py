import csv
import io
from typing import Dict, List, Tuple
from .schemas import SCHEMAS
from .message import Header, HeaderError, PayloadError, ProtocolError
import logging

# def deserialize_message(message_bytes: bytes, schema: List[str]) -> Tuple[Header, List[Dict[str, str]]]:
#     try:
#         header_bytes, payload_bytes = message_bytes.split(b"\n===\n", 1)
#     except Exception as e:
#         raise ProtocolError("Formato de mensaje inválido: separador '\\n===\\n' no encontrado")

#     header = Header.decode(header_bytes)
#     try:
#         byte_stream = io.BytesIO(payload_bytes)
#         text_reader = io.TextIOWrapper(byte_stream, encoding="utf-8", newline="")
#         reader = csv.DictReader(text_reader, fieldnames=schema)
#         rows = list(reader)
#     except Exception as e:
#         raise PayloadError(f"Error deserializando payload: {e}")

#     return header, rows

def _resolve_raw_schema_name(header: Header) -> str:
    """
    Usa el campo `schema` directamente como clave de SCHEMAS.
    No lo modifica ni le agrega/quita sufijos.
    """
    s = header.fields.get("schema") or header.fields.get("source")
    if not s:
        raise HeaderError("Header sin 'schema' ni 'source' para resolver el esquema.")
    return s.strip()

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

    schema_key = _resolve_raw_schema_name(header)  # ej: "transactions.raw"
    try:
        fieldnames = SCHEMAS[schema_key]
    except KeyError:
        raise PayloadError(f"Schema '{schema_key}' no encontrado en SCHEMAS")

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