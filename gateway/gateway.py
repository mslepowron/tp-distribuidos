import socket
import logging
from uuid import uuid4
from client_handler import ClientHandler
from client_manager import ClientManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Gateway")

class Gateway:
    def __init__(self, host="0.0.0.0", port=9000, report_port=9200):
        self.host = host
        self.port = port
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.registry = ClientManager()
        self.running = True

    def start(self):
        try:
            self.listener_socket.bind((self.host, self.port))
            self.listener_socket.listen()
            logger.info(f"Gateway listening for CLIENTS on {self.host}:{self.port}")
        
            while self.running:
                client_socket, client_address = self.listener_socket.accept()
                client_id = str(uuid4())
                logger.info(f"Accepted connection from {client_address}, assigned ID {client_id}")

                client_handler = ClientHandler(client_id, client_socket, client_address)
                self.registry.add(client_handler)
                client_handler.start()

        except KeyboardInterrupt:
            logger.info("Gateway shutting down")
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("Shutting down listener socket and connected clients")
        self.running = False
        self.listener_socket.close()
        self.registry.clear()
