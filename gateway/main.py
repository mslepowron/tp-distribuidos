import socket
import threading
import logging
from gateway import handle_client, start_result_listener
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

def main():
    
    host = os.getenv("GATEWAY_HOST", "0.0.0.0")
    port = int(os.getenv("GATEWAY_PORT", 9000))

    start_result_listener()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.bind((host, port))
        server_sock.listen()
        logger.info(f"Gateway escuchando en {host}:{port}")
        while True:
            conn, addr = server_sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()