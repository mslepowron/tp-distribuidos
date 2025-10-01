import socket
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

HOST = "0.0.0.0"
PORT = 9000

def handle_client(conn, addr):
    logger.info(f"Cliente conectado desde {addr}")
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            # TODO: reenviar al AppController
            logger.info(f"Recibido {len(data)} bytes")
    finally:
        conn.close()
        logger.info(f"Cliente desconectado: {addr}")