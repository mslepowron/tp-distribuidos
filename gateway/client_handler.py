import socket
import threading
import logging
import os
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.deserialize import deserialize_message  

logger = logging.getLogger("ClientHandler")
SEPARATOR = b"\n===\n"

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
            with socket.create_connection((self.app_controller_host, self.app_controller_port)) as controller_sock:
                logger.info(f"Connected to AppController at {self.app_controller_host}:{self.app_controller_port}")
                # Esperar el mensaje de "CONTROLLER OK"
                ok_msg = controller_sock.recv(1024)
                if b"CONTROLLER OK" not in ok_msg:
                    logger.error("No se recibió CONTROLLER OK del AppController. Cerrando conexión.")
                    return
                logger.info("Recibido CONTROLLER OK del AppController.")

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

                # 2. Enviar CONTROLLER OK al cliente
                self._client_socket.sendall(ok_msg)
                logger.info("Reenviado CONTROLLER OK al cliente.")

                first = True
                threading.Thread(target=self._recv_from_controller_and_send_to_client, args=(controller_sock,), daemon=True).start()
                while not self._stop_flag.is_set():
                    data = self._client_socket.recv(8192)
                    if not data:
                        logger.info(f"Client {self._client_id} closed connection.")
                        break

                    if first:
                        prefix = f"{self._client_id}\n===\n".encode("utf-8")
                        data = prefix + data
                        first = False

                    logger.info(f"Received {len(data)} bytes from client {self._client_id}")
                    controller_sock.sendall(data)
                    logger.info(f"Forwarded {len(data)} bytes to AppController")
            
            # with socket.create_connection((self.app_controller_host, self.app_controller_port)) as controller_sock:
            #     logger.info(f"Connected to AppController at {self.app_controller_host}:{self.app_controller_port}")

            #     first = True
            #     threading.Thread(target=self._recv_from_controller_and_send_to_client, args=(controller_sock,), daemon=True).start()
            #     while not self._stop_flag.is_set():
            #         data = self._client_socket.recv(8192)
            #         if not data:
            #             logger.info(f"Client {self._client_id} closed connection.")
            #             break

            #         if first:
            #             # Prefix client_id + separador al primer mensaje
            #             prefix = f"{self._client_id}\n===\n".encode("utf-8")
            #             data = prefix + data
            #             first = False

            #         logger.info(f"Received {len(data)} bytes from client {self._client_id}")
            #         controller_sock.sendall(data)  
            #         logger.info(f"Forwarded {len(data)} bytes to AppController")


        except Exception as e:
            logger.error(f"Error in ClientHandler {self._client_id}: {e}")
        finally:
            self._stop_client()
    
    def _recv_from_controller_and_send_to_client(self, controller_sock: socket.socket):
        logger.info(f"Started forwarding thread for client {self._client_id}")
        try:
            while not self._stop_flag.is_set():
                data = controller_sock.recv(8192)
                if not data:
                    logger.info(f"[REPORT] Connection to AppController closed.")
                    break
                self._client_socket.sendall(data)
                logger.info(f"[REPORT] Forwarded {len(data)} bytes to client {self._client_id}")
                logger.info(f"Raw report data:\n{data.decode('utf-8', errors='replace')}")
        except Exception as e:
            logger.error(f"[REPORT] Error forwarding to client: {e}")
            self._stop_client()

