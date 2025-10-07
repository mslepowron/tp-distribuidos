import ast
import socket
import threading
import logging
from communication.protocol.deserialize import deserialize_message
from initializer import clean_rows, get_schema_columns_by_source, get_routing_key_from_filename
from io import StringIO
import csv

logger = logging.getLogger("gateway-receiver")

BUFFER_SIZE = 8192  # máx. 8 KB por mensaje
SEPARATOR = b"\n===\n"

def start_tcp_listener(port, controller):
    logger.info(f"AppController TCP listening on port {port}...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("", port))
        server_sock.listen()

        while True:
            conn, addr = server_sock.accept()
            logger.info(f"Conexión entrante desde {addr}")
            threading.Thread(target=handle_connection, args=(conn, addr, controller), daemon=True).start()

def handle_connection(conn, addr, controller, client_id=None):
    try:
        with conn:
            data = b""

            while True:
                chunk = conn.recv(BUFFER_SIZE)
                if not chunk:
                    break
                data += chunk

                if client_id is None and SEPARATOR in data:
                    parts = data.split(SEPARATOR, 1)
                    client_id = parts[0].decode().strip()
                    data = parts[1]
                    logger.info(f"[GATEWAY] Conexión asociada a client_id={client_id}")

                data = _process_buffered_data(data, addr, controller, client_id)
    except Exception as e:
        logger.error(f"Error en conexión con {addr}: {e}")

def _process_buffered_data(buffer: bytes, addr, controller, client_id=None):
    while True:
        sep_index = buffer.find(SEPARATOR)
        if sep_index == -1:
            break 

        header_part = buffer[:sep_index]
        remaining = buffer[sep_index + len(SEPARATOR):]

        next_header_index = remaining.find(b"message_type,")
        if next_header_index != -1:
            payload_part = remaining[:next_header_index]
            buffer = remaining[next_header_index:]
        else:
            payload_part = remaining
            buffer = b""

        full_msg = header_part + SEPARATOR + payload_part

        try:
            header, rows = deserialize_message(full_msg)
            _handle_deserialized_message(header, rows, addr, controller, client_id)
        except Exception as e:
            logger.error(f"Error deserializando o procesando mensaje: {e}")
            break

    return buffer

def _handle_deserialized_message(header, rows, addr, controller, client_id=None):
    logger.info(f"[GATEWAY] Mensaje recibido de {addr}")
    msg_type = header.fields["message_type"]
    source = header.fields["source"]

    if client_id:
        controller.register_client_for_source(source, client_id)
    
    try:
        routing_key = get_routing_key_from_filename(source)
        clean_columns, raw_columns = get_schema_columns_by_source(source)
    except ValueError as e:
        logger.info(str(e))
        return

    if msg_type == "DATA":
        cleaned_rows = list(clean_rows(rows, clean_columns, raw_columns))
        if cleaned_rows:
            controller.send_batch(
                batch=cleaned_rows,
                routing_key=routing_key,
                source=source,
                schema=clean_columns,
            )
    elif msg_type == "EOF":
        controller.send_end_of_file(routing_key=routing_key)