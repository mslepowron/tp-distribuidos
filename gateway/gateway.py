import socket
import logging
from uuid import uuid4
from client_handler import ClientHandler
from client_manager import ClientManager
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Gateway")
SEPARATOR = b"\n===\n"

class Gateway:
    def __init__(self, host="0.0.0.0", port=9000, report_port=9200):
        self.host = host
        self.port = port
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.report_port = report_port
        self.report_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.registry = ClientManager()
        self.running = True

    def start(self):
        try:
            self.listener_socket.bind((self.host, self.port))
            self.listener_socket.listen()
            logger.info(f"Gateway listening for CLIENTS on {self.host}:{self.port}")

            # Listener para reportes desde AppController
            self.report_socket.bind((self.host, self.report_port))
            self.report_socket.listen()
            logger.info(f"Gateway listening for REPORTS on {self.host}:{self.report_port}")

            # Thread para reportes
            threading.Thread(target=self._report_listener, daemon=True).start()

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

    def _report_listener(self):
        """Escucha conexiones entrantes desde el AppController con streams CSV."""
        while self.running:
            try:
                conn, addr = self.report_socket.accept()
                threading.Thread(target=self._handle_report_stream, args=(conn, addr), daemon=True).start()
            except OSError:
                break  # socket cerrado

    def _handle_report_stream(self, conn, addr):
        try:
            logger.info(f"[REPORT] Conexión entrante desde AppController {addr}")

            buffer = b""
            while buffer.count(SEPARATOR) < 2:
                chunk = conn.recv(1024)
                if not chunk:
                    return
                buffer += chunk

            parts = buffer.split(SEPARATOR, 2)
            if len(parts) < 3:
                logger.error(f"[REPORT] Encabezado inválido: {buffer[:100]!r}")
                return

            client_id = parts[0].decode().strip()
            query_id = parts[1].decode().strip()
            remainder = parts[2]

            logger.info(f"[REPORT] Stream recibido para client_id={client_id}, query_id={query_id}")

            client_handler = self.registry.get_by_uuid(client_id)
            if not client_handler:
                logger.warning(f"[REPORT] No se encontró cliente con ID {client_id}")
                return

            client_sock = client_handler._client_socket
            client_sock.sendall(f"{client_id}\n===\n{query_id}\n===\n".encode("utf-8"))

            # Mandamos el resto inicial del CSV
            if remainder:
                client_sock.sendall(remainder)
                logger.info(f"[REPORT] Enviado chunk inicial de {len(remainder)} bytes al cliente {client_id}")

            while True:
                data = conn.recv(8192)
                if not data:
                    break
                client_sock.sendall(data)
                logger.info(f"[REPORT] Enviado chunk de {len(data)} bytes al cliente {client_id}")

            logger.info(f"[REPORT] Stream para client_id={client_id}, query_id={query_id} finalizado")

        except Exception as e:
            logger.error(f"[REPORT] Error reenviando stream: {e}")
        finally:
            conn.close()

    def shutdown(self):
        logger.info("Shutting down listener socket and connected clients")
        self.running = False
        self.listener_socket.close()
        self.report_socket.close()
        self.registry.clear()
