import csv
import io
from typing import Dict
from .schemas import RAW_SCHEMAS

# Serializa una fila del diciconario como mensaje de bytes
def serialize_row(rows: Dict[str, str]) -> bytes:

    #if file_type not in RAW_SCHEMAS:
    #    raise ValueError(f"Tipo de archivo desconocido: {file_type}")

    print(f"\nSerializando batch de {len(rows)} filas")

    byte_buffer = io.BytesIO()
    text_writer = io.TextIOWrapper(byte_buffer, encoding="utf-8", newline="")

    # Creamos un escritor CSV que va a escribir una fila en el orden indicado por el esquema
    writer = csv.DictWriter(text_writer, RAW_SCHEMAS["transactions.raw"], quoting=csv.QUOTE_MINIMAL)
    for row in rows :
        writer.writerow(row)
    text_writer.flush()  # asegura que se escriba en el buffer 

    # Construimos el mensaje final: file_type|csv_row 
    csv_bytes = byte_buffer.getvalue().strip()
    
    print(f"Batch serializado ({len(csv_bytes)} bytes)\n")

    return csv_bytes
