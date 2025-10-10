import ast
import socket
import threading
import logging
from communication.protocol.deserialize import deserialize_message
from initializer import clean_rows, get_schema_columns_by_source, get_routing_key_from_filename
from io import StringIO
import csv
from app_controller import AppController
import time

logger = logging.getLogger("gateway-receiver")

MAX_ROWS = 50000
BUFFER_SIZE = 1024
SEPARATOR = b"\n===\n"
client_streams = {}

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

def handle_connection(conn: socket.socket, addr, controller: AppController):
    logger.info(f"[GATEWAY] Esperando client_id desde {addr}")

    buffer = b""
    while buffer.count(SEPARATOR) < 1:
        chunk = conn.recv(BUFFER_SIZE)
        if not chunk:
            logger.error("[GATEWAY] No se recibió client_id inicial.")
            return
        buffer += chunk

    parts = buffer.split(SEPARATOR, 1)
    client_id = parts[0].decode().strip()
    remainder = parts[1] if len(parts) > 1 else b""

    logger.info(f"[GATEWAY] Conexión asociada a client_id={client_id}")
    client_streams[client_id] = conn
    controller.register_client_stream(client_id, conn)
    logger.info(f"[GATEWAY] Registrado stream de client_id={client_id} en AppController")

    routing_keys_vistos = set()
    buffer = remainder
    accumulated_rows = []
    msg_counter = 0

    try:
        while True:
            chunk = conn.recv(8192)
            if not chunk:
                logger.info(f"[GATEWAY] Fin de stream para client_id={client_id}")
                break

            buffer += chunk
            buffer, new_msgs = _extract_complete_messages(buffer)
            for header, row in new_msgs:
                msg_counter += 1
                msg_type = header.fields["message_type"].strip().upper()
                source = header.fields["source"].strip()

                if msg_type == "DATA":
                    accumulated_rows.append((header, row))

                elif msg_type == "EOF":
                    # Flush pendiente antes de EOF
                    if accumulated_rows:
                        try:
                            _handle_deserialized_message(
                                header=accumulated_rows[0][0],
                                rows=[r for _, r in accumulated_rows],
                                addr=addr,
                                controller=controller,
                                client_id=client_id,
                                routing_keys_vistos=routing_keys_vistos,
                            )
                            logger.info(f"[GATEWAY] Flush final de {len(accumulated_rows)} filas antes de EOF ({source})")
                        except Exception as e:
                            logger.error(f"Error procesando batch antes de EOF: {e}")
                        finally:
                            accumulated_rows.clear()

                    # Manejar EOF normalmente
                    _handle_deserialized_message(
                        header=header,
                        rows=[],
                        addr=addr,
                        controller=controller,
                        client_id=client_id,
                        routing_keys_vistos=routing_keys_vistos,
                    )

            # Flush por tamaño (para archivos grandes)
            if len(accumulated_rows) >= MAX_ROWS:
                try:
                    _handle_deserialized_message(
                        header=accumulated_rows[0][0],
                        rows=[r for _, r in accumulated_rows],
                        addr=addr,
                        controller=controller,
                        client_id=client_id,
                        routing_keys_vistos=routing_keys_vistos,
                    )
                    logger.info(f"[GATEWAY] Flush intermedio de {len(accumulated_rows)} filas")
                except Exception as e:
                    logger.error(f"Error procesando batch intermedio: {e}")
                finally:
                    accumulated_rows.clear()

        # Flush si quedó algo pendiente al final de la conexión
        if accumulated_rows:
            try:
                _handle_deserialized_message(
                    header=accumulated_rows[0][0],
                    rows=[r for _, r in accumulated_rows],
                    addr=addr,
                    controller=controller,
                    client_id=client_id,
                    routing_keys_vistos=routing_keys_vistos,
                )
                logger.info(f"[GATEWAY] Flush final al cerrar conexión ({len(accumulated_rows)} filas)")
            except Exception as e:
                logger.error(f"Error procesando batch final: {e}")

    except Exception as e:
        logger.error(f"[GATEWAY] Error en stream de {client_id}: {e}")
    finally:
        conn.close()
        client_streams.pop(client_id, None)
        logger.info(f"[GATEWAY] Conexión cerrada para client_id={client_id}")

def _extract_complete_messages(buffer: bytes):
    """
    Extrae todos los mensajes completos posibles del buffer.
    Devuelve el resto del buffer y una lista de tuplas (header, fila) o (header, []) si EOF.
    """
    messages = []

    while True:
        sep_index = buffer.find(SEPARATOR)
        if sep_index == -1:
            break  # no hay mensaje completo todavía

        header_part = buffer[:sep_index]
        remaining = buffer[sep_index + len(SEPARATOR):]

        next_sep = remaining.find(SEPARATOR)
        if next_sep == -1:
            break  # el payload está incompleto

        payload_part = remaining[:next_sep]
        buffer = remaining[next_sep + len(SEPARATOR):]
        full_msg = header_part + SEPARATOR + payload_part

        try:
            header, rows = deserialize_message(full_msg)
            if rows:
                for row in rows:
                    messages.append((header, row))
            else:
                messages.append((header, []))  # para EOF o mensajes vacíos
        except Exception as e:
            logger.error(f"Error deserializando mensaje: {e}")
            break

    return buffer, messages

def _handle_deserialized_message(header, rows, addr, controller, client_id=None, routing_keys_vistos=None):
    msg_type = header.fields["message_type"].strip().upper()
    source = header.fields["source"].strip()
    logger.info(f"[GATEWAY] Mensaje recibido de {addr}, source={source}, type={msg_type}")

    # Registrar client_id para la fuente, incluso si no es DATA
    if client_id and client_id in client_streams:
        controller.register_client_for_source(source, client_id, client_streams[client_id])

    if msg_type == "DATA":
        try:
            routing_key = get_routing_key_from_filename(source)
            if routing_keys_vistos is not None:
                routing_keys_vistos.add(routing_key)
                logger.info(f"[GATEWAY] Routing key registrado: {routing_key}")

            clean_columns, _ = get_schema_columns_by_source(source)
        except ValueError as e:
            logger.info(f"[GATEWAY] Error obteniendo schema para {source}: {e}")
            return

        cleaned_rows = list(clean_rows(source, rows))
        logger.info(f"[GATEWAY CLEAN] {source}: filas originales={len(rows)} → limpias={len(cleaned_rows)}")

        if cleaned_rows:
            controller.send_batch(
                batch=cleaned_rows,
                routing_key=routing_key,
                source=source,
                schema=clean_columns,
            )
            logger.info(
                f"[GATEWAY → APP_CONTROLLER] PUBLICANDO BATCH: "
                f"source={source}, routing_key={routing_key}, rows={len(cleaned_rows)}"
            )

    elif msg_type == "EOF":
        routing_key = source  # en EOF, source ya es la routing key
        logger.info(f"[GATEWAY] EOF recibido para routing_key={routing_key}, reenviando a AppController")
        controller.send_end_of_file(routing_key=routing_key)
