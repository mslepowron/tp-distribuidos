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
from communication.protocol.schemas import CLEAN_SCHEMAS
from communication.protocol.deserialize import deserialize_message
from initializer import clean_all_files_grouped
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BATCH_SIZE = 50000

SCHEMA_BY_RK = {
    "transactions": "transactions.clean",
    "transaction_items": "transaction_items.clean",
    "menu_items": "menu_items.clean",
    "users": "users.clean",
    "stores": "stores.clean",
} #TODO esto desp sacarle el clean? No me convence xq van a ir cambiando los schemas.

QUERY_IDS_BY_FILE = {
    "transactions": ["q_amount_75_tx", "q_semester_tpv", "q_top3_birthdays"],
    "transaction_items": ["q_month_top_qty", "q_month_top_rev"],
    "menu_items": ["q_month_top_qty", "q_month_top_rev"],
    "stores": ["q_semester_tpv", "q_top3_birthdays"],
    "users": ["q_top3_birthdays"],
}

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
            ) 

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

        # self._wait_for_queues(["filter_year_q", "join_store_q"])
        self._wait_for_queues(["filter_year_q", "join_menu_q"])
        try:
            files_grouped = clean_all_files_grouped()
            for routing_key, filename, row_iterator in files_grouped:
                queries = QUERY_IDS_BY_FILE.get(routing_key, [])
                logger.info(f"Procesando archivo '{filename}' con routing_key='{routing_key}' para queries: {queries}")
                self._send_batches_from_iterator(row_iterator, routing_key)
                self.send_end_of_file(routing_key=routing_key)

        except Exception as e:
            logger.error(f"Error processing files: {e}")

        def callback_results(ch, method, properties, body):
            try:
                # Deserializar con tu nuevo protocolo
                rk = method.routing_key  
                logger.info(f"[results:{rk}] len={len(body)} bytes")
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

    def _send_batches_from_iterator(self, row_iterator, routing_key: str):
        source = SCHEMA_BY_RK[routing_key]
        schema = CLEAN_SCHEMAS[source]  
        batch = []
        for row in row_iterator:
            if not self._running:
                break
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                self.send_batch(batch, routing_key, source, schema)
                batch = []
        if batch and self._running:
            self.send_batch(batch, routing_key, source, schema)
    
    def send_batch(self, batch, routing_key: str, source: str, schema: List[str]):
        if not self._running or not batch:
            return
        try:
            header_fields = [
                ("message_type", "DATA"),
                ("query_id", "BROADCAST"),
                ("stage", "INIT"),
                ("part", source),
                ("seq", str(uuid4())),
                ("schema", schema),
                ("source", source)
            ]
            header = Header(header_fields)

            message_bytes = serialize_message(header, batch, schema)
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)
            logger.info(f"[SEND] {self.input_exchange}:{routing_key} ({len(batch)} filas)")
        except Exception as e:
            logger.error(f"Error enviando batch para routing_key={routing_key}: {e}")

    def send_end_of_file(self, routing_key: str):
        try:
            schema = []
            source = SCHEMA_BY_RK[routing_key]
            
            header_fields = [
                ("message_type", "EOF"),
                ("query_id", "BROADCAST"),
                ("stage", "INIT"),
                ("part", source),
                ("seq", str(uuid4())),
                ("schema", schema),
                ("source", source)
            ]
            header = Header(header_fields)
            message_bytes = serialize_message(header, [], schema)
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)
            logger.info(f"[EOF] {self.input_exchange}:{routing_key}")
        except Exception as e:
            logger.error(f"Error enviando END_OF_FILE para routing_key={routing_key}: {e}")
