import logging
from uuid import uuid4
import csv
import sys
from middleware.rabbitmq.mom import MessageMiddlewareExchange
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.schemas import RAW_SCHEMAS
from communication.protocol.deserialize import deserialize_message

from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BASE_DIR = Path(__file__).resolve().parent.parent  # sube de app_controller a tp-distribuidos
CSV_FILE = BASE_DIR / "transactions.csv"

BATCH_SIZE = 5 #TODO> Varialbe de entorno

class AppController:
    def __init__(self, host, input_exchange, routing_keys, 
                 result_exchange, result_queue):
        self.host = host
        self.input_exchange = input_exchange
        self.routing_keys = routing_keys
        self.result_exchange = result_exchange
        self.result_queue = result_queue
        self.mw_exchange = None
        self.result_mw = None
        self._running = False

    def connect_to_middleware(self):
        try:
            self.mw_exchange = MessageMiddlewareExchange(
                host=self.host,
                exchange_name=self.input_exchange,
                exchange_type="direct", #TODO: Esto va a depender de la query creo...
                route_keys= self.routing_keys
            )

            #Exchange con binding a la cola de donde va a recibir los resultados de las queries
            self.result_mw = MessageMiddlewareExchange(
                host=self.host,
                exchange_name=self.result_exchange,
                exchange_type="direct",
                route_keys=[self.result_queue]
            ) 
        except Exception as e:
            logger.error(f"No se pudo conectar a RabbitMQ: {e}")
            sys.exit(1)
    
    def shutdown(self):
        """Closing middleware connection for graceful shutdown"""
        logger.info("Shutting down AppController gracefully...")
        self._running = False
        for conn in [self.mw_exchange, self.result_mw]:
            if conn:
                try:
                    self.mw_exchange.close()
                    self.result_mw.close()
                    logger.info("Middleware connection closed.")
                except Exception as e:
                    logger.warning(f"Error closing middleware: {e}")

    def run(self):
        self.connect_to_middleware()
        self._running = True

        try:
            with open(CSV_FILE, newline="") as csvfile: #TODO: El csv file debe ser dinamico, viene como un stream de datos dsde el gateway
                reader = csv.DictReader(csvfile)
                batch = []
                for row in reader:
                    if not self._running:
                        break
                    batch.append(row)
                    if len(batch) >= BATCH_SIZE:
                        self.send_batch(batch)
                        batch = []

                if batch and self._running:
                    self.send_batch(batch)
            self.send_end_of_file()

        except FileNotFoundError:
            logger.error(f"CSV file {CSV_FILE} not found.")
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")

        def callback(ch, method, properties, body):
            try:
                # Deserializar con tu nuevo protocolo
                header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
                logger.info(f"Header resultado: {header.as_dictionary()}")
                for row in rows:
                    logger.info(f"Resultado recibido: {row}")
            except Exception as e:
                logger.error(f"Error deserializando resultado: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        logger.info("Esperando resultados...")
        try:
            self.result_mw.start_consuming(callback, queues=[self.result_queue]) #No me da confianza dejar definido la queue_name en el callback
        except Exception as e:
            logger.error(f"Error consumiendo resultados: {e}")
        finally:
            self.shutdown()

    def send_batch(self, batch):
        if not self._running or not batch:
            return
        try:
            header_fields = [
                ("message_type", "DATA"),
                ("query_id", "q_amount_75_tx"),  # TODO: Cambiar según la query - Esto esta hardcodeado
                ("stage", "FILTER"),
                ("part", "transactions.raw"),
                ("seq", str(uuid4())),
                ("schema", "transactions.raw"),
                ("source", "app_controller")
            ]
            header = Header(header_fields)

            message_bytes = serialize_message(header, batch, RAW_SCHEMAS["transactions.raw"])
            route_key = self.routing_keys[0] if self.routing_keys else ""
            self.mw_exchange.send(message_bytes, route_key=route_key)

            logger.info(f"Batch enviado con {len(batch)} filas")

        except Exception as e:
            logger.error(f"Error enviando batch: {e}")

    def send_end_of_file(self):
        try:
            header_fields = [
                ("message_type", "EOF"),
                ("query_id", "q_amount_75_tx"),
                ("stage", "FILTER"),
                ("part", "transactions.raw"),
                ("seq", str(uuid4())),
                ("schema", "transactions.raw"),
                ("source", "app_controller")
            ]
            header = Header(header_fields)
            # Enviamos un mensaje vacío, el filtro lo va a interpretar
            message_bytes = serialize_message(header, [], RAW_SCHEMAS["transactions.raw"])
            route_key = self.routing_keys[0] if self.routing_keys else ""
            self.mw_exchange.send(message_bytes, route_key=route_key)
            logger.info("Mensaje END_OF_FILE enviado al filtro")
        except Exception as e:
            logger.error(f"Error enviando END_OF_FILE: {e}")

    