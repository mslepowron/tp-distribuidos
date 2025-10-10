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
                logger.info("Connected to AppController")

                # 1. Enviar client_id al AppController
                client_id_msg = f"{self._client_id}\n===\n".encode("utf-8")
                controller_sock.sendall(client_id_msg)
                logger.info("Sent client_id to AppController")

                # 2. Esperar CONTROLLER OK
                ok_msg = controller_sock.recv(1024)
                if b"CONTROLLER OK" not in ok_msg:
                    logger.error("No CONTROLLER OK received")
                    return

                # 3. Enviar ACK + CONTROLLER OK al Client
                ack_header = Header([
                    ("message_type", "ACK"),
                    ("query_id", "BROADCAST"),
                    ("stage", "init"),
                    ("part", ""),
                    ("seq", "0"),
                    ("schema", "[]"),
                    ("source", self._client_id),
                ])
                ack_msg = serialize_message(ack_header, [], [])
                self._client_socket.sendall(ack_msg)
                self._client_socket.sendall(ok_msg)

                # 4. Forward controller → client (REPORT)
                threading.Thread(
                    target=self._recv_from_controller_and_send_to_client,
                    args=(controller_sock,),
                    daemon=True
                ).start()

                # 5. Forward client → controller (con buffer y separación por SEPARATOR)
                buffer = b""
                msg_counter = 0
                while not self._stop_flag.is_set():
                    chunk = self._client_socket.recv(8192)
                    if not chunk:
                        logger.info(f"[Client→Controller] Client {self._client_id} closed connection")
                        break

                    logger.info(f"[Client→Controller] Recibidos {len(chunk)} bytes de client {self._client_id}")
                    logger.info(f"[Client→Controller] Chunk crudo:\n{chunk.decode('utf-8', errors='replace')}")

                    buffer += chunk
                    while buffer.count(SEPARATOR) >= 2:
                        # Encontrar mensaje completo
                        first_sep = buffer.find(SEPARATOR)
                        header_part = buffer[:first_sep]
                        remainder = buffer[first_sep + len(SEPARATOR):]

                        second_sep = remainder.find(SEPARATOR)
                        if second_sep == -1:
                            # mensaje incompleto
                            break

                        payload_part = remainder[:second_sep]
                        buffer = remainder[second_sep + len(SEPARATOR):]

                        complete_msg = header_part + SEPARATOR + payload_part + SEPARATOR
                        msg_counter += 1

                        header_str = header_part.decode('utf-8', errors='replace')
                        payload_str = payload_part.decode('utf-8', errors='replace')
                        logger.info(f"[Client→Controller] ↪ MSG #{msg_counter} listo para enviar (len={len(complete_msg)})")
                        logger.info(f"  ├─ Header:\n{header_str}")
                        logger.info(f"  └─ Payload:\n{payload_str if payload_str else '<VACÍO>'}")
                        
                        controller_sock.sendall(complete_msg)
                        logger.info(f"[Client→Controller] MSG #{msg_counter} reenviado al AppController")
                
                if buffer.strip():
                    logger.warning(f"[Client→Controller] Buffer residual sin enviar:\n{buffer.decode('utf-8', errors='replace')}")

        except Exception as e:
            logger.error(f"Error in ClientHandler: {e}")
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
        except Exception as e:
            logger.error(f"[REPORT] Error forwarding to client: {e}")
            self._stop_client()

