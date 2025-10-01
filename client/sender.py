import csv
import socket
import logging
from pathlib import Path
from uuid import uuid4
from typing import List, Dict

from communication.protocol.schemas import RAW_SCHEMAS
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message

logger = logging.getLogger("sender")

QUERY_IDS_BY_FILE = {
    "transactions": ["q_amount_75_tx", "q_semester_tpv", "q_top3_birthdays"],
    "transaction_items": ["q_month_top_qty", "q_month_top_rev"],
    "menu_items": ["q_month_top_qty", "q_month_top_rev"],
    "stores": ["q_semester_tpv", "q_top3_birthdays"],
    "users": ["q_top3_birthdays"],
}

class Sender:
    def __init__(self, host: str, port: int, batch_size: int, input_dir: str):
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.input_dir = Path(input_dir)
        self.sock = None
        self.running = False

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port))
            self.running = True
            logger.info(f"Conectado al Gateway en {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"No se pudo conectar al Gateway: {e}")
            raise

    def shutdown(self):
        self.running = False
        if self.sock:
            self.sock.close()
            logger.info("Conexión cerrada")

    def send_bytes(self, data: bytes):
        if not self.sock:
            logger.warning("Socket no inicializado")
            return
        try:
            self.sock.sendall(data)
        except Exception as e:
            logger.error(f"Error enviando datos: {e}")

    def send_batches(self):
        file_basenames = ["transactions", "transaction_items", "menu_items", "stores", "users"]

        for file_basename in file_basenames:
            file_path = self.input_dir / f"{file_basename}.csv"
            if not file_path.exists():
                logger.warning(f"Archivo {file_path.name} no encontrado, se omite")
                continue

            schema_key = f"{file_basename}.raw"
            if schema_key not in RAW_SCHEMAS:
                logger.warning(f"Schema no definido para {schema_key}")
                continue

            schema = RAW_SCHEMAS[schema_key]

            try:
                with file_path.open(newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames is None:
                        logger.warning(f"Encabezado ausente en archivo {file_path.name}")
                        continue

                    batch: List[Dict[str, str]] = []
                    for row in reader:
                        if not self.running:
                            break
                        filtered_row = {col: row.get(col, "").strip() for col in schema}
                        batch.append(filtered_row)

                        if len(batch) >= self.batch_size:
                            self._send_batch(batch, file_basename, schema)
                            batch = []

                    if batch:
                        self._send_batch(batch, file_basename, schema)

                self._send_eof(file_basename, schema)

            except Exception as e:
                logger.error(f"Error procesando {file_path.name}: {e}")

    def _send_batch(self, rows: List[Dict[str, str]], file_basename: str, schema: List[str]):
        query_ids = QUERY_IDS_BY_FILE.get(file_basename, [])
        for query_id in query_ids:
            header_fields = [
                ("message_type", "DATA"),
                ("query_id", query_id),
                ("stage", "INIT"),
                ("part", f"{file_basename}.raw"),
                ("seq", str(uuid4())),
                ("schema", str(schema)),
                ("source", f"{file_basename}.raw")
            ]
            header = Header(header_fields)
            message_bytes = serialize_message(header, rows, schema)
            self.send_bytes(message_bytes)
            logger.info(f"Batch enviado: {file_basename} → {query_id} ({len(rows)} filas)")

    def _send_eof(self, file_basename: str, schema: List[str]):
        query_ids = QUERY_IDS_BY_FILE.get(file_basename, [])
        for query_id in query_ids:
            header_fields = [
                ("message_type", "EOF"),
                ("query_id", query_id),
                ("stage", "INIT"),
                ("part", f"{file_basename}.raw"),
                ("seq", str(uuid4())),
                ("schema", str(schema)),
                ("source", f"{file_basename}.raw")
            ]
            header = Header(header_fields)
            message_bytes = serialize_message(header, [], schema)
            self.send_bytes(message_bytes)
            logger.info(f"EOF enviado: {file_basename} → {query_id}")
















#
#import csv
#import socket
#import logging
#from pathlib import Path
#from uuid import uuid4
#from typing import List, Dict
#
#from communication.protocol.schemas import RAW_SCHEMAS
#from communication.protocol.message import Header
#from communication.protocol.serialize import serialize_message
#
#logger = logging.getLogger("sender")
#
#QUERY_IDS_BY_FILE = {
#    "transactions": ["q_amount_75_tx", "q_semester_tpv", "q_top3_birthdays"],
#    "transaction_items": ["q_month_top_qty", "q_month_top_rev"],
#    "menu_items": ["q_month_top_qty", "q_month_top_rev"],
#    "stores": ["q_semester_tpv", "q_top3_birthdays"],
#    "users": ["q_top3_birthdays"],
#}
#
#
#class Sender:
#    def __init__(self, host: str, port: int, batch_size: int, input_dir: str):
#        self.host = host
#        self.port = port
#        self.batch_size = batch_size
#        self.input_dir = Path(input_dir)
#        self.sock = None
#        self.running = False
#
#    def connect(self):
#        try:
#            self.sock = socket.create_connection((self.host, self.port))
#            self.running = True
#            logger.info(f"Conectado al Gateway en {self.host}:{self.port}")
#        except Exception as e:
#            logger.error(f"No se pudo conectar al Gateway: {e}")
#            raise
#
#    def shutdown(self):
#        self.running = False
#        if self.sock:
#            self.sock.close()
#            logger.info("Conexión cerrada")
#
#    def send_bytes(self, data: bytes):
#        if not self.sock:
#            logger.warning("Socket no inicializado")
#            return
#        try:
#            self.sock.sendall(data)
#        except Exception as e:
#            logger.error(f"Error enviando datos: {e}")
#
#    def send_batches(self):
#        file_basenames = ["transactions", "transaction_items", "menu_items", "stores", "users"]
#
#        for file_basename in file_basenames:
#            file_path = self.input_dir / f"{file_basename}.csv"
#            if not file_path.exists():
#                logger.warning(f"Archivo {file_path.name} no encontrado, se omite")
#                continue
#
#            schema_key = f"{file_basename}.raw"
#            if schema_key not in RAW_SCHEMAS:
#                logger.warning(f"Schema no definido para {schema_key}")
#                continue
#
#            schema = RAW_SCHEMAS[schema_key]
#
#            try:
#                with file_path.open(newline="", encoding="utf-8") as f:
#                    reader = csv.DictReader(f)
#                    if reader.fieldnames is None:
#                        logger.warning(f"Encabezado ausente en archivo {file_path.name}")
#                        continue
#
#                    batch: List[Dict[str, str]] = []
#                    for row in reader:
#                        if not self.running:
#                            break
#                        filtered_row = {col: row.get(col, "").strip() for col in schema}
#                        batch.append(filtered_row)
#
#                        if len(batch) >= self.batch_size:
#                            self._send_batch(batch, file_basename, schema)
#                            batch = []
#
#                    if batch:
#                        self._send_batch(batch, file_basename, schema)
#
#                self._send_eof(file_basename, schema)
#
#            except Exception as e:
#                logger.error(f"Error procesando {file_path.name}: {e}")
#
#
#    def _send_batch(self, rows: List[Dict[str, str]], file_basename: str, schema: List[str]):
#        query_ids = QUERY_IDS_BY_FILE.get(file_basename, [])
#        for query_id in query_ids:
#            header_fields = [
#                ("message_type", "DATA"),
#                ("query_id", query_id),
#                ("stage", "INIT"),
#                ("part", f"{file_basename}.raw"),
#                ("seq", str(uuid4())),
#                ("schema", str(schema)),
#                ("source", f"{file_basename}.raw")
#            ]
#            header = Header(header_fields)
#            message_bytes = serialize_message(header, rows, schema)
#            self.send_bytes(message_bytes)
#            logger.info(f"Batch enviado: {file_basename} → {query_id} ({len(rows)} filas)")
#
#    def _send_eof(self, file_basename: str, schema: List[str]):
#        query_ids = QUERY_IDS_BY_FILE.get(file_basename, [])
#        for query_id in query_ids:
#            header_fields = [
#                ("message_type", "EOF"),
#                ("query_id", query_id),
#                ("stage", "INIT"),
#                ("part", f"{file_basename}.raw"),
#                ("seq", str(uuid4())),
#                ("schema", str(schema)),
#                ("source", f"{file_basename}.raw")
#            ]
#            header = Header(header_fields)
#            message_bytes = serialize_message(header, [], schema)
#            self.send_bytes(message_bytes)
#            logger.info(f"EOF enviado: {file_basename} → {query_id}")
#
#