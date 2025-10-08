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

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def run(self):
        self._connect()
        if self.close_socket:
            return

        sender = Sender(self.socket)
        sender.set_client_id(self.client_id)
        logger.info("Sender initialized. Sending datasets...")

        for base in FILE_BASENAMES:
            matching_files = sorted(self.input_dir.glob(f"{base}*.csv"))
            if not matching_files:
                logger.info(f"No files found for prefix: {base}")
                continue

            for file_path in matching_files:
                logger.info(f"Sending file: {file_path.name}")
                sender.send_dataset(file_path)

        logger.info("All datasets sent successfully.")
        self._wait_for_reports()

    def _connect(self, retries=MAX_RETRIES, delay=DELAY_BETWEEN_RETRIES):
        attempt = 0
        while attempt < retries:
            try:
                self.socket.connect((self.host, self.port))
                logger.info(f"Connected to gateway at {self.host}:{self.port}")
                self._receive_ack()
                return
            except Exception as e:
                attempt += 1
                logger.info(f"Connection attempt {attempt} failed: {e}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds")
                    time.sleep(delay)
                else:
                    logger.error("Max connection attempts reached.")

    # recibe el ack del gateway y guarda el client_id
    def _receive_ack(self):
        try:
            buffer = b""
            while b"\n===\n" not in buffer:
                chunk = self.socket.recv(1024)
                if not chunk:
                    raise ConnectionError("Disconnected before receiving ACK.")
                buffer += chunk

            result = deserialize_message(buffer)
            if len(result) == 3:
                header, rows, schema = result
            else:
                header, rows = result
            client_id = header.fields.get("source", "")
            logger.info(f"Received ACK from Gateway. Assigned client_id: {client_id}")
            self.client_id = client_id
        except Exception as e:
            logger.error(f"Failed to receive ACK: {e}")
            logger.info("Message contained:")
            logger.info(buffer.decode("utf-8"))
            self.close_socket = True

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, stopping client...")
        self._stop_client()

    def _wait_for_reports(self):
        logger.info("Waiting for reports from Gateway...")
        
        try:
            buffer = b""
            logger.info(f"🔎 Raw header buffer:\n{buffer.decode('utf-8', errors='replace')}")
            while True:
                # Leer más datos si no tenemos al menos 2 separadores
                while buffer.count(SEPARATOR) < 2:
                    chunk = self.socket.recv(1024)
                    if not chunk:
                        logger.info("Connection closed by Gateway (no more reports).")
                        return
                    buffer += chunk
                
                # Extraer header completo (client_id + query_id)
                header_end = buffer.find(SEPARATOR)
                part1 = buffer[:header_end]
                rest = buffer[header_end + len(SEPARATOR):]

                second_sep = rest.find(SEPARATOR)
                if second_sep == -1:
                    logger.warning("Incomplete header. Waiting for more data...")
                    continue

                part2 = rest[:second_sep]
                csv_remainder = rest[second_sep + len(SEPARATOR):]

                recv_client_id = part1.decode().strip()
                query_id = part2.decode().strip()

                logger.info(f"Report header received. client_id={recv_client_id}, query_id={query_id}")

                base_report_dir = Path(os.getenv("OUTPUT_DIR", "/report")) / f"client_{recv_client_id}"
                base_report_dir.mkdir(parents=True, exist_ok=True)
                output_path = base_report_dir / f"{query_id}.csv"

                with output_path.open("wb") as f:
                    f.write(csv_remainder)

                    # Continuar leyendo el resto del archivo hasta que empiece otro reporte
                    while True:
                        chunk = self.socket.recv(8192)
                        if not chunk:
                            logger.info(f"Stream finished for query_id={query_id}")
                            return
                        if chunk.count(SEPARATOR) >= 2:
                            buffer = chunk
                            break
                        f.write(chunk)

                logger.info(f"Report saved for query_id={query_id} at {output_path}")

        except Exception as e:
            logger.error(f"Error while waiting for reports: {e}")
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
