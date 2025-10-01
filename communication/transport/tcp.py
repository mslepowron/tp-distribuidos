import socket
import threading
import logging
import time

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

def tcp_send_all(host: str, port: int, data: bytes, retries: int = 5, base_delay: float = 1.0) -> bool:

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Intentando conectar a {host}:{port} (intento {attempt}) para reenviar {len(data)} bytes")
            with socket.create_connection((host, port), timeout=5) as sock:
                sock.sendall(data)
            logger.info(f"Reenvío exitoso en intento {attempt}")
            return True
        except Exception as e:
            logger.error(f"Error enviando datos a {host}:{port} en intento {attempt}: {e}")
            if attempt < retries:
                sleep_time = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s...
                logger.info(f"Reintentando en {sleep_time:.1f}s...")
                time.sleep(sleep_time)
    return False

def tcp_connect(host: str, port: int) -> socket.socket:
    try:
        sock = socket.create_connection((host, port))
        logger.info(f"Conectado a {host}:{port}")
        return sock
    except Exception as e:
        logger.error(f"No se pudo conectar a {host}:{port}: {e}")
        return None
