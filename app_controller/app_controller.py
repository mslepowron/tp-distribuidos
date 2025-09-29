import logging
from uuid import uuid4
import csv
import sys
import os
import time
from typing import Dict, List
import pika
from middleware.rabbitmq.mom import MessageMiddlewareExchange
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.schemas import CLEAN_SCHEMAS, SCHEMA_EOF
from communication.protocol.deserialize import deserialize_message

from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BASE_DIR = Path(__file__).resolve().parent.parent  # sube de app_controller a tp-distribuidos
CSV_FILE = BASE_DIR / "transactions.csv"

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "storage"))

BATCH_SIZE = 5 #TODO> Varialbe de entorno

SCHEMA_BY_RK = {
    "transactions": "transactions.clean",
    "transaction_items": "transaction_items.clean",
    "menu_items": "menu_items.clean",
    "users": "users.clean",
    "stores": "stores.clean",
} #TODO esto desp sacarle el clean? No me convence xq van a ir cambiando los schemas.

class AppController:
    def __init__(self, host, input_exchange, input_routing_keys,
                 result_exchange, result_routing_keys, result_sink_queue):
        self.host = host
        
        self.input_exchange = input_exchange
        self.input_routing_keys = input_routing_keys

        self.result_exchange = result_exchange
        self.result_routing_keys = result_routing_keys
        self.result_sink_queue = result_sink_queue

        self.mw_input = None
        self.mw_results = None
        self._running = False

    def connect_to_middleware(self):
        try:
            self.mw_input = MessageMiddlewareExchange(
                host=self.host,
                queue_name="app_controller_input",
                #bindings=[(self.input_exchange, "direct", rk) for rk in self.input_routing_keys]
            ) #bindings = exchange name, tipo y las routing keys asociadas a donde va a mandar cosas.

            self.mw_input.ensure_exchange(self.input_exchange, "direct")
            
            #Exchange con binding a la cola de donde va a recibir los resultados de las queries desde diferentes
            #routing keys.
            self.result_mw = MessageMiddlewareExchange(
                host=self.host,
                queue_name=self.result_sink_queue,
                bindings=[(self.result_exchange, "direct", rk) for rk in self.result_routing_keys]
            ) 
        except Exception as e:
            logger.error(f"No se pudo conectar a RabbitMQ: {e}")
            sys.exit(1)
    
    def shutdown(self):
        """Closing middleware connection for graceful shutdown"""
        logger.info("Shutting down AppController gracefully...")
        self._running = False
        for conn in [self.mw_input, self.result_mw]:
            if conn:
                try:
                    self.mw_input.close()
                    self.result_mw.close()
                    logger.info("Middleware connection closed.")
                except Exception as e:
                    logger.warning(f"Error closing middleware: {e}")

    def _routing_key_by_files(self) -> Dict[str, List[Path]]:
        files_by_rk: Dict[str, List[Path]] = {
            "transactions": [],
            "transaction_items": [],
            "menu_items": [],
            "users": [],
            "stores": [],
        }

        if not STORAGE_DIR.exists():
            logger.warning(f"Storage dir '{STORAGE_DIR}' no existe.")
            return files_by_rk

        for p in STORAGE_DIR.glob("*.csv"):
            name = p.name.lower().strip()

            if name == "transactions.csv" or name.startswith("transactions_"):
                files_by_rk["transactions"].append(p)
            elif name == "transaction_items.csv" or name.startswith("transaction_items_"):
                files_by_rk["transaction_items"].append(p)
            elif name == "menu_items.csv":
                files_by_rk["menu_items"].append(p)
            elif name == "users.csv":
                files_by_rk["users"].append(p)
            elif name == "stores.csv" or name == "stores .csv":  # por si viene con espacio
                files_by_rk["stores"].append(p)
    
        for rk in files_by_rk:
            files_by_rk[rk].sort(key=lambda x: x.name)

        return files_by_rk
    
    def _wait_for_queues(self, queue_names, timeout=30):
        """Verifica que las colas existan (passive=True). Reabre el canal si el broker lo cierra."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                for q in queue_names:
                    # passive=True: no crea la cola, solo falla si no existe
                    self.mw_input.channel.queue_declare(queue=q, passive=True)
                logger.info(f"Ready: all target queues exist: {queue_names}")
                return True
            except pika.exceptions.ChannelClosedByBroker:
                # El broker cierra el canal si la cola no existe en modo passive - reabrimos y reintentamos
                self.mw_input.channel = self.mw_input.connection.channel()
                time.sleep(1)
            except Exception:
                time.sleep(1)
        logger.warning(f"Timeout esperando colas {queue_names}; enviando igual")
        return False
    
    def run(self):
        self.connect_to_middleware()
        self._running = True

        # time.sleep(10)
        self._wait_for_queues(["filter_year_q"])
        files_by_rk = self._routing_key_by_files()
        try:
            for rk in ["transactions", "transaction_items", "menu_items", "users", "stores"]:
                file_list = files_by_rk.get(rk, [])
                if not file_list:
                    logger.info(f"No hay archivos para '{rk}' en {STORAGE_DIR}")
                    continue

                logger.info(f"Procesando {len(file_list)} archivo(s) para rk='{rk}'")
                for csv_path in file_list:
                    if not self._running:
                        break
                    self._send_csv_in_batches(csv_path, routing_key=rk)

                # Un único EOF por tipo de archivo
                self.send_end_of_file(routing_key=rk)

        except Exception as e:
            logger.error(f"Error processing files: {e}")

        def callback_results(ch, method, properties, body):
            try:
                # Deserializar con tu nuevo protocolo
                rk = method.routing_key  # ej: "q_amount_75_tx"
                logger.info(f"[results:{rk}] len={len(body)} bytes")

                # Elegir el schema por rk si difiere por query dejo hardocdeado de ej transactions.raw:
                header, rows = deserialize_message(body)
                logger.info(f"Header: {header.as_dictionary()}")
                for row in rows:
                    logger.info(f"Resultado recibido: {row}")
            except Exception as e:
                logger.error(f"Error deserializando resultado: {e}")
            finally:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        logger.info(f"Esperando resultados en queue '{self.result_sink_queue}'...")
        try:
            self.result_mw.start_consuming(callback_results)
        except Exception as e:
            logger.error(f"Error consumiendo resultados: {e}")
        finally:
            self.shutdown()

    def _send_csv_in_batches(self, csv_path: Path, routing_key: str):
        """Lee los CSV de a batcges y lo manda al exchange con la routing key para ese archivo"""
        source = SCHEMA_BY_RK[routing_key]
        try:
            with csv_path.open(newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                for row in reader:
                    if not self._running:
                        break
                    batch.append(row)
                    if len(batch) >= BATCH_SIZE:
                        self.send_batch(batch, routing_key=routing_key, source=source)
                        batch = []
                if batch and self._running:
                    self.send_batch(batch, routing_key=routing_key, source=source)
            logger.info(f"Archivo enviado completo: {csv_path.name} a rk={routing_key}")
        except FileNotFoundError:
            logger.error(f"CSV no encontrado: {csv_path}")
        except Exception as e:
            logger.error(f"Error leyendo {csv_path}: {e}")

    def send_batch(self, batch, routing_key: str, source: str):
        if not self._running or not batch:
            return
        try:
            header_fields = [
                ("message_type", "DATA"),
                ("query_id", "q_amount_75_tx"),  # TODO: Creo que no lo vamos a necesitar
                ("stage", "INIT"),
                ("part", "transactions.raw"), #Esto es para reducers, aca no serviria
                ("seq", str(uuid4())),
                ("schema", source),
                ("source", source)
            ]
            header = Header(header_fields)

            message_bytes = serialize_message(header, batch, CLEAN_SCHEMAS[source])
            route_key = self.input_routing_keys[0] if self.input_routing_keys else ""
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)

            logger.info(f"Batch {self.input_exchange}:{routing_key} ({len(batch)} filas)")

        except Exception as e:
            logger.error(f"Error enviando batch: {e}")

    def send_end_of_file(self, routing_key: str):
        try:
            source = SCHEMA_BY_RK[routing_key]
            header_fields = [
                ("message_type", "EOF"),
                ("query_id", "q_amount_75_tx"),
                ("stage", "INIT"),
                ("part", "transactions.raw"),
                ("seq", str(uuid4())),
                ("schema", "EOF"),
                ("source", source)
            ]
            header = Header(header_fields)
            # Enviamos un mensaje vacío, el filtro lo va a interpretar
            message_bytes = serialize_message(header, [], SCHEMA_EOF)
            route_key = self.input_routing_keys[0] if self.input_routing_keys else ""
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)
            logger.info(f"EOF → {self.input_exchange}:{routing_key}")
        except Exception as e:
            logger.error(f"Error enviando END_OF_FILE: {e}")

    