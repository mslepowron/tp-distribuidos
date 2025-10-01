import socket
import threading
import logging

logger = logging.getLogger("tcp")

def start_tcp_listener(port: int, handler_fn, host: str = "0.0.0.0", name: str = "tcp-listener"):
    def listener_loop():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listening_sock:
            listening_sock.bind((host, port))
            listening_sock.listen()
            logger.info(f"[{name}] Escuchando en {host}:{port}")
            while True:
                conn, addr = listening_sock.accept()
                threading.Thread(target=handler_fn, args=(conn, addr), daemon=True).start()

    threading.Thread(target=listener_loop, daemon=True).start()


def tcp_send_all(host: str, port: int, data: bytes) -> bool:
    try:
        with socket.create_connection((host, port)) as sock:
            sock.sendall(data)
        return True
    except Exception as e:
        logger.error(f"Error enviando datos a {host}:{port}: {e}")
        return False


def tcp_connect(host: str, port: int) -> socket.socket:
    try:
        sock = socket.create_connection((host, port))
        logger.info(f"Conectado a {host}:{port}")
        return sock
    except Exception as e:
        logger.error(f"No se pudo conectar a {host}:{port}: {e}")
        return None
