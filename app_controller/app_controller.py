import logging
import csv
import sys
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from communication.protocol.serialize import serialize_row
from communication.protocol.deserialize import deserialize_batch
from communication.protocol.schemas import RAW_SCHEMAS

from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BASE_DIR = Path(__file__).resolve().parent.parent  # sube de app_controller a tp-distribuidos
CSV_FILE = BASE_DIR / "transactions.csv"

BATCH_SIZE = 2

class AppController:
    def __init__(self, host, queue_name, csv_file, batch_size=10, result_queue="coffee_results"):
        self.host = host
        self.queue_name = queue_name
        self.result_queue = result_queue
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.mw = None
        self.result_mw = None
        self._running = False

    def connect_to_middleware(self):
        try:
            self.mw = MessageMiddlewareQueue(host=self.host, queue_name=self.queue_name)
            self.result_mw = MessageMiddlewareQueue(host=self.host, queue_name=self.result_queue)
        except Exception as e:
            logger.error(f"No se pudo conectar a RabbitMQ: {e}")
            sys.exit(1)
    
    def shutdown(self):
        """Closing middleware connection for graceful shutdown"""
        logger.info("Shutting down AppController gracefully...")
        self._running = False
        for conn in [self.mw, self.result_mw]:
            if conn:
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

        def callback(ch, method, properties, body):
            try:
                from communication.protocol.deserialize import deserialize_batch
                rows = deserialize_batch(body)
                for row in rows:
                    logger.info(f"Resultado recibido: {row}")
            except Exception as e:
                logger.error(f"Error deserializando resultado: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        logger.info("Esperando resultados...")
        try:
            self.result_mw.start_consuming(callback)
        except Exception as e:
            logger.error(f"Error consumiendo resultados: {e}")
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
