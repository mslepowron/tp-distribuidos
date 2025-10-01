import logging
from communication.protocol.deserialize import deserialize_message

logger = logging.getLogger("receiver")

class Receiver:
    def __init__(self, sock):
        self.sock = sock

    def shutdown(self):
        if self.sock:
            self.sock.close()
        logger.info("Receiver cerrado")

    def start_listening(self):
        logger.info("Esperando resultados del AppController...")
        buffer = b""
        while True:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            buffer += chunk
            while b"\n===\n" in buffer:
                try:
                    header_bytes, buffer = buffer.split(b"\n===\n", 1)
                    payload_bytes = buffer
                    message = header_bytes + b"\n===\n" + payload_bytes
                    header, rows = deserialize_message(message)
                    logger.info(f"Resultados recibidos: {header.as_dictionary()} ({len(rows)} filas)")
                    for row in rows:
                        logger.info(row)
                    return  # salir después de recibir respuesta
                except Exception as e:
                    logger.error(f"Error deserializando resultado: {e}")
                    break
