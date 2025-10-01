import socket
import threading
import logging
import os
from communication.transport.tcp import tcp_send_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

APP_CONTROLLER_HOST = os.getenv("APP_CONTROLLER_HOST", "app_controller")
APP_CONTROLLER_PORT = int(os.getenv("APP_CONTROLLER_PORT", 9100))

def handle_client(conn, addr):
    global client_conn
    logger.info(f"Cliente conectado desde {addr}")
    client_conn = conn

    try:
        while True:
            data = conn.recv(65536)  # leer hasta 64KB
            if not data:
                break
            logger.info(f"Recibido {len(data)} bytes del cliente")

            # Reenviar al AppController
            success = tcp_send_all(APP_CONTROLLER_HOST, APP_CONTROLLER_PORT, data)
            if not success:
                logger.error("Fallo el reenvío al AppController")
    except Exception as e:
        logger.error(f"Error manejando conexión: {e}")
    finally:
        conn.close()
        client_conn = None
        logger.info(f"Cliente desconectado: {addr}")

def start_result_listener(client_host="client", client_port=9300):
    def listener_loop():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("0.0.0.0", 9200))
            sock.listen()
            logger.info("Gateway escuchando reportes en 0.0.0.0:9200")
            while True:
                conn, addr = sock.accept()
                with conn:
                    data = conn.recv(65536)
                    if not data:
                        continue
                    logger.info(f"Reporte recibido del AppController ({len(data)} bytes)")
                    if client_conn:
                        try:
                            client_conn.sendall(data)
                            logger.info("Reporte reenviado al cliente")
                        except Exception as e:
                            logger.error(f"No se pudo reenviar al cliente: {e}")

    threading.Thread(target=listener_loop, daemon=True).start()
