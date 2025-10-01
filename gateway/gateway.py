import socket
import threading
import logging
import os
from communication.transport.tcp import tcp_send_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

#HOST = "0.0.0.0"
#PORT = 9000
APP_CONTROLLER_HOST = os.getenv("APP_CONTROLLER_HOST", "app_controller")
APP_CONTROLLER_PORT = int(os.getenv("APP_CONTROLLER_PORT", 9100))

def handle_client(conn, addr):
    logger.info(f"Cliente conectado desde {addr}")
    try:
        while True:
            data = conn.recv(65536)  # leer hasta 64KB
            if not data:
                break
            logger.info(f"Recibido {len(data)} bytes del cliente")

            # Reenviar el mensaje completo al AppController
            success = tcp_send_all(APP_CONTROLLER_HOST, APP_CONTROLLER_PORT, data)
            if not success:
                logger.error("Fallo el reenvío al AppController")
    except Exception as e:
        logger.error(f"Error manejando conexión: {e}")
    finally:
        conn.close()
        logger.info(f"Cliente desconectado: {addr}")

#def handle_client(conn, addr):
#    logger.info(f"Cliente conectado desde {addr}")
#    try:
#        while True:
#            data = conn.recv(4096)
#            if not data:
#                break
#            # TODO: reenviar al AppController
#            logger.info(f"Recibido {len(data)} bytes")
#    finally:
#        conn.close()
#        logger.info(f"Cliente desconectado: {addr}")