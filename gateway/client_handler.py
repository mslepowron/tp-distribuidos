import socket
import threading
import logging
import os
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.deserialize import deserialize_message  
logger = logging.getLogger("ClientHandler")

class ClientHandler(threading.Thread):

    def __init__(self, client_id: str, client_socket: socket.socket, client_addr):
        super().__init__(daemon=True)
        self._client_id = client_id
        self._client_socket = client_socket
        self._client_addr = client_addr
        self._stop_flag = threading.Event()
        self.was_closed = False

        self.app_controller_host = os.getenv("APP_CONTROLLER_HOST", "app_controller")
        self.app_controller_port = int(os.getenv("APP_CONTROLLER_PORT", 9100))

    def get_client_id(self) -> str:
        return self._client_id

    def _client_is_connected(self) -> bool:
        return not self.was_closed

    def _stop_client(self):
        logger.info(f"Stopping client {self._client_id}")
        self._stop_flag.set()
        self.was_closed = True
        try:
            self._client_socket.close()
        except Exception as e:
            logger.info(f"Error closing client socket: {e}")

    def run(self):
        logger.info(f"ClientHandler thread started for {self._client_id}")
        try:
            ack_header = Header([
                ("message_type", "ACK"),
                ("query_id", "BROADCAST"),
                ("stage", "init"),
                ("part", ""),
                ("seq", "0"),
                ("schema", "[]"),
                ("source", self._client_id),
            ])
            ack_message = serialize_message(ack_header, [], [])
            self._client_socket.sendall(ack_message)
            logger.info(f"Sent ACK with client ID {self._client_id} to {self._client_addr}")
            
            with socket.create_connection((self.app_controller_host, self.app_controller_port)) as controller_sock:
                logger.info(f"Connected to AppController at {self.app_controller_host}:{self.app_controller_port}")

                first = True
                while not self._stop_flag.is_set():
                    data = self._client_socket.recv(8192)
                    if not data:
                        logger.info(f"Client {self._client_id} closed connection.")
                        break

                    if first:
                        # Prefix client_id + separador al primer mensaje
                        prefix = f"{self._client_id}\n===\n".encode("utf-8")
                        data = prefix + data
                        first = False

                    logger.info(f"Received {len(data)} bytes from client {self._client_id}")
                    controller_sock.sendall(data)  
                    logger.info(f"Forwarded {len(data)} bytes to AppController")

        except Exception as e:
            logger.error(f"Error in ClientHandler {self._client_id}: {e}")
        finally:
            self._stop_client()

        def send_raw(self, data: bytes):
            if self._client_is_connected():
                try:
                    self._client_socket.sendall(data)
                except Exception as e:
                    logger.error(f"Error sending raw data to client {self._client_id}: {e}")
                    self._stop_client()
