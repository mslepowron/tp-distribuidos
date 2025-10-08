import os
import socket
from pathlib import Path
import logging

from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.schemas import RAW_SCHEMAS  # key = filename, value = list of fields

logger = logging.getLogger("Sender")

MAX_ROWS = 50000
SEPARATOR = b"\n===\n"
MAX_PAYLOAD_SIZE = 8 * 1024  # 8 KB por mensaje

RAW_PREFIXES = {
    "transactions": "transactions.raw",
    "transaction_items": "transaction_items.raw",
    "stores": "stores.raw",
    "menu_items": "menu.raw",
    "users": "users.raw",
}

class Sender:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._client_id = None  # Se recibe del Gateway

    def set_client_id(self, client_id: str):
        self._client_id = client_id

    def is_connected(self) -> bool:
        return self._sock is not None

    def send_dataset(self, filepath: Path):
        filename = filepath.name
        logger.info(f"Serializing dataset: {filename}")
        
        matched_schema_key = None
        for key in RAW_SCHEMAS:
            base_key = key.split(".")[0]  # ej: "transactions"
            if filename.startswith(base_key):
                matched_schema_key = key
                break

        if not matched_schema_key:
            logger.warning(f"Skipping {filename}: no matching schema")
            return

        schema = RAW_SCHEMAS[matched_schema_key]

        with filepath.open("r", encoding="utf-8") as f:
            _ = f.readline()  
            rows = []
            for line in f:
                fields = line.strip().split(",")
                if len(fields) != len(schema):
                    continue
                row = dict(zip(schema, fields))
                rows.append(row)

                if len(rows) >= MAX_ROWS:
                    self._send_rows_as_message(rows, schema, filename)
                    rows = []

            # Último batch si quedó algo
            if rows:
                self._send_rows_as_message(rows, schema, filename)

        logger.info(f"Finished sending {filename}")

    def _send_rows_as_message(self, rows, schema, source_filename):
        header = Header([
            ("message_type", "DATA"),
            ("query_id", "BROADCAST"), 
            ("stage", "init"),
            ("part", source_filename),
            ("seq", "0"),  
            ("schema", str(schema)),
            ("source", source_filename),
        ])

        message = serialize_message(header, rows, schema)
        if len(message) > MAX_PAYLOAD_SIZE:
            logger.info(f"Message size {len(message)} exceeds max payload.")
        self._send_all(message)

    def _send_all(self, data: bytes):
        try:
            self._sock.sendall(data)
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            raise

    def send_eof(self):
        header = Header([
            ("message_type", "EOF"),
            ("query_id", "BROADCAST"),
            ("stage", "init"),
            ("part", "EOF"),
            ("seq", "end"),
            ("schema", "[]"),
            ("source", "eof"),
        ])
        message = serialize_message(header, [], [])
        self._send_all(message)
        logger.info("EOF message sent.")

