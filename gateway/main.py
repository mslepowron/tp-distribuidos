import socket
import threading
import logging
from gateway import handle_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

HOST = "0.0.0.0"
PORT = 9000

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.bind((HOST, PORT))
        server_sock.listen()
        logger.info(f"Gateway escuchando en {HOST}:{PORT}")
        while True:
            conn, addr = server_sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
