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

BATCH_SIZE = 2 #TODO> Varialbe de entorno

class AppController:
    # def __init__(self, host, exchange_name, /*queue_name*/, csv_file, result_queue="coffee_results"):
    def __init__(self, host, exchange_name, csv_file, result_queue="coffee_results"):
        self.host = host
        self.exchange_name = exchange_name
        #self.queue_name = queue_name    #TODO Remove -> ahora manda a exchange
        self.result_queue = result_queue #TODO> Check si recibe directo de una results queue o hay que bindearlo a un exchange
        self.csv_file = csv_file
        self.batch_size = BATCH_SIZE
        self.mw = None  #TODO> Mejorar este nombre
        self.result_mw = None
        self._running = False

    def connect_to_middleware(self):
        try:
            # self.mw = MessageMiddlewareQueue(host=self.host, queue_name=self.queue_name)
            # self.result_mw = MessageMiddlewareQueue(host=self.host, queue_name=self.result_queue)
            self.mw = MessageMiddlewareExchange(
                host=self.host,
                exchange_name=self.exchange_name,
                exchange_type="direct",
                route_keys=["filters_year", "filters_hour", "filters_amount"]   #TODO: Parametrizar route keys. Por ahora solo funca con filters de Q1
            )

            #Exchange con binding a la cola de donde va a recibir los resultados de las queries
            self.result_mw = MessageMiddlewareExchange(
                host=self.host,
                exchange_name="results",
                exchange_type="direct",
                route_keys=[self.result_queue]
            ) #TODO: Parametrizar nombre del exchange de results,
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
        for record in batch:
            if not self._running:
                break
            try:
                # Serializar usando RAW_SCHEMAS["transactions.raw"]
                #message_bytes = serialize_row([record])
                #self.mw.send(message_bytes, route_key="filters_year") #TODO: Esto esta hardcodeado para el filter d anios
                #comento lo de arriba para probar el nuevo protocolo
                header_fields = [
                    ("message_type", "DATA"),
                    ("query_id", "q_amount_75_tx"),  # ejemplo fijo
                    ("stage", "FILTER"),
                    ("part", "transactions.raw"),
                    ("seq", str(uuid4())),  # podés poner contador si preferís
                    ("schema", "transactions.raw"),
                    ("source", "app_controller")
                ]
                header = Header(header_fields)

                message_bytes = serialize_message(header, [record], RAW_SCHEMAS["transactions.raw"])
                self.mw.send(message_bytes, route_key="filters_year")
                #hasta aca pruebo lo mio
                logger.info(f"Message Sent: {record}")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
