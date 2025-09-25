import logging
import csv
import sys
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from communication.protocol.serialize import serialize_row
from communication.protocol.schemas import RAW_SCHEMAS

from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BASE_DIR = Path(__file__).resolve().parent.parent  # sube de app_controller a tp-distribuidos
CSV_FILE = BASE_DIR / "transactions.csv"

BATCH_SIZE = 2

class AppController:
    def __init__(self, host, queue_name, csv_file, batch_size=10):
        self.host = host
        self.queue_name = queue_name
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.mw = None
        self._running = False

    def connect_to_middleware(self):
        try:
            self.mw = MessageMiddlewareQueue(host=self.host, queue_name=self.queue_name)
        except Exception as e:
            logger.error(f"No se pudo conectar a RabbitMQ: {e}")
            sys.exit(1)
    
    def shutdown(self):
        """Closing middleware connection for graceful shutdown"""
        logger.info("Shutting down AppController gracefully...")
        self._running = False
        if self.mw:
            try:
                self.mw.close()
                logger.info("Middleware connection closed.")
            except Exception as e:
                logger.warning(f"Error closing middleware: {e}")

    def run(self):
        self.connect_to_middleware()
        self._running = True

        try:
            with open(CSV_FILE, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                for row in reader:
                    if not self._running:
                        break
                    batch.append(row)
                    if len(batch) >= self.batch_size:
                        self.send_batch(batch)
                        batch = []

                if batch and self._running:
                    self.send_batch(batch)

        except FileNotFoundError:
            logger.error(f"CSV file {self.csv_file} not found.")
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
        finally:
            self.shutdown()

    def send_batch(self, batch):
        for record in batch:
            if not self._running:
                break
            try:
                # Serializar usando RAW_SCHEMAS["transactions.raw"]
                message_bytes = serialize_row([record])
                self.mw.send(message_bytes)
                logger.info(f"Message Sent: {record}")
            except Exception as e:
                logger.error(f"Error sending message: {e}")