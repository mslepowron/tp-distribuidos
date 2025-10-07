import ast
import socket
import threading
import logging
from communication.protocol.deserialize import deserialize_message

logger = logging.getLogger("gateway-receiver")

BUFFER_SIZE = 8192  # máx. 8 KB por mensaje

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

def handle_connection(conn, addr, controller):
    try:
        with conn:
            data = b""
            while True:
                chunk = conn.recv(BUFFER_SIZE)
                if not chunk:
                    break
                data += chunk

                # Procesar todos los mensajes que estén completos (delimitados por \n===\n)
                while b"\n===\n" in data:
                    raw_msg, data = data.split(b"\n===\n", 1)
                    raw_msg += b"\n===\n"  # restaurar delimitador para deserialización

                    try:
                        header, rows = deserialize_message(raw_msg)
                        logger.info(f"✅ [GATEWAY] Mensaje recibido de {addr}")
                        logger.info(f"Header: {header.as_dictionary()}")
                        logger.info(f"Ejemplo de fila cruda: {rows[0] if rows else 'VACÍO'}")

                        schema = ast.literal_eval(header.fields["schema"])  # convierte string de lista en lista real
                        controller.send_batch(
                            rows=rows,
                            routing_key=header.fields["source"],
                            source=header.fields["source"],
                            schema=schema
                        )

                    except Exception as e:
                        logger.error(f"❌ Error deserializando o procesando mensaje: {e}")
    except Exception as e:
        logger.error(f"❌ Error en conexión con {addr}: {e}")