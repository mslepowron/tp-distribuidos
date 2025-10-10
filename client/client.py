import socket
import signal
import time
import logging
import csv
from pathlib import Path
import io
import os
from pathlib import Path
from communication.protocol.deserialize import deserialize_message  
from sender import Sender
logger = logging.getLogger("Client")
logging.basicConfig(level=logging.INFO)

MAX_RETRIES = 5
DELAY_BETWEEN_RETRIES = 50
SEPARATOR = b"\n===\n"

FILE_BASENAMES = [
    "transactions",
    "transaction_items",
    "menu_items",
    "stores",
    "users",
]

class Client:
    def __init__(self, host, port, max_batch_size):
        self.client_id = 0
        self.host = host
        self.port = port
        self.max_batch_size = max_batch_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.close_socket = False
        self.input_dir = Path(os.getenv("RAW_DATA_DIR", "data"))
        self.gateway_ready = False

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def run(self):
        self._connect()
        if self.close_socket:
            return

        sender = Sender(self.socket)
        sender.set_client_id(self.client_id)
        logger.info("Sender initialized. Sending datasets...")

        ordered_bases = ["menu_items", "stores"] + [b for b in FILE_BASENAMES if b not in ("menu_items", "stores")]

        for base in ordered_bases:
            matching_files = sorted(self.input_dir.glob(f"{base}*.csv"))
            if not matching_files:
                logger.info(f"No files found for prefix: {base}")
                continue

            for file_path in matching_files:
                logger.info(f"Sending file: {file_path.name}")
                sender.send_dataset(file_path)

            sender.send_eof_for_routing_key(base)

        logger.info("All datasets sent successfully.")
        self._wait_for_reports()

    def _connect(self, retries=MAX_RETRIES, delay=DELAY_BETWEEN_RETRIES):
        attempt = 0
        while attempt < retries:
            try:
                self.socket.connect((self.host, self.port))
                logger.info(f"Connected to gateway at {self.host}:{self.port}")
                self._recv_handshake()
                return
            except Exception as e:
                attempt += 1
                logger.info(f"Connection attempt {attempt} failed: {e}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds")
                    time.sleep(delay)
                else:
                    logger.error("Max connection attempts reached.")

    def _recv_handshake(self):
        try:
            buffer = b""
            while buffer.count(SEPARATOR) < 2:
                chunk = self.socket.recv(1024)
                if not chunk:
                    raise ConnectionError("Disconnected during handshake.")
                buffer += chunk

            result = deserialize_message(buffer)
            if len(result) == 3:
                header, rows, schema = result
            else:
                header, rows = result
            self.client_id = header.fields.get("source", "")
            logger.info(f"Received ACK from Gateway. Assigned client_id: {self.client_id}")

            # Buscar CONTROLLER OK al final
            if b"CONTROLLER OK" not in buffer:
                logger.error("CONTROLLER OK not received after ACK.")
                self.close_socket = True
                return
            logger.info("Recibido CONTROLLER OK del Gateway.")

        except Exception as e:
            logger.error(f"Error during handshake: {e}")
            self.close_socket = True
    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping client...")
        self._stop_client()

    def _wait_for_reports(self):
        logger.info("Waiting for reports from Gateway...")
        
        try:
            buffer = b""
            while True:
                # Leer hasta tener un header completo (client_id === query_id ===)
                while buffer.count(SEPARATOR) < 2:
                    chunk = self.socket.recv(1024)
                    if not chunk:
                        logger.info("Connection closed by Gateway (no more reports).")
                        return
                    buffer += chunk
                
                # Extraer header (client_id y query_id)
                parts = buffer.split(SEPARATOR, 2)
                recv_client_id = parts[0].decode().strip()
                query_id = parts[1].decode().strip()
                buffer = parts[2] # El resto es el inicio del CSV

                logger.info(f"Report header received. client_id={recv_client_id}, query_id={query_id}")

                base_report_dir = Path(os.getenv("OUTPUT_DIR", "/report")) / f"client_{recv_client_id}"
                base_report_dir.mkdir(parents=True, exist_ok=True)
                output_path = base_report_dir / f"{query_id}.csv"

                with output_path.open("wb") as f:
                    # Bucle para recibir los datos de UN solo reporte
                    while SEPARATOR not in buffer:
                        f.write(buffer)
                        buffer = self.socket.recv(8192)
                        if not buffer:
                            logger.warning(f"Connection closed unexpectedly while receiving report for {query_id}")
                            break
                    
                    # Escribir la última parte del reporte (antes del delimitador)
                    if SEPARATOR in buffer:
                        end_index = buffer.find(SEPARATOR)
                        f.write(buffer[:end_index])
                        # Guardar lo que sobró para la siguiente vuelta del bucle principal
                        buffer = buffer[end_index + len(SEPARATOR):]

                logger.info(f"Report saved for query_id={query_id} at {output_path}")

        except Exception as e:
            # No es un error si el socket se cierra normalmente al final
            if isinstance(e, (ConnectionResetError, BrokenPipeError)):
                logger.info("All reports received. Connection finished.")
            else:
                logger.error(f"Error while waiting for reports: {e}", exc_info=True)
        finally:
            self._stop_client()
            
    def _stop_client(self):
        try:
            if self.socket:
                self.close_socket = True
                try:
                    self.socket.shutdown(socket.SHUT_RDWR)
                except OSError as e:
                    logger.error(f"Socket already shutted")
                finally:
                    if self.socket:
                        self.socket.close()
                        logger.info("Socket closed.")
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")
