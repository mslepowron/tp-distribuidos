import logging
from uuid import uuid4
import sys
import os
import io
import time
import ast
import socket
import csv
from pathlib import Path
from typing import Dict, List
import pika
from middleware.rabbitmq.mom import MessageMiddlewareExchange
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.schemas import CLEAN_SCHEMAS
from communication.protocol.deserialize import deserialize_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

BATCH_SIZE = 50000
REPORTS_DIR = Path(os.getenv("STORAGE_DIR", "/storage"))
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

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

        self.gateway_host = os.getenv("GATEWAY_HOST", "gateway")
        self.gateway_port = int(os.getenv("GATEWAY_PORT", "9200"))
        self._report_streams = {}
        self._client_ids_by_source = {}

        self.mw_input = None
        self.mw_results = None
        self._running = False
    
    def register_client_for_source(self, source: str, client_id: str):
        self._client_ids_by_source[source] = client_id
        logger.info(f"[REGISTER] source={source} → client_id={client_id}")

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
                    logger.info(f"Error closing middleware: {e}")

    # def _wait_for_queues(self, queue_names, timeout=30):
    #     """Verifica que las colas existan (passive=True). Reabre el canal si el broker lo cierra."""
    #     deadline = time.time() + timeout
    #     while time.time() < deadline:
    #         try:
    #             for q in queue_names:
    #                 # passive=True: no crea la cola, solo falla si no existe
    #                 self.mw_input.channel.queue_declare(queue=q, passive=True)
    #             logger.info(f"Ready: all target queues exist: {queue_names}")
    #             return True
    #         except pika.exceptions.ChannelClosedByBroker:
    #             # El broker cierra el canal si la cola no existe en modo passive - reabrimos y reintentamos
    #             self.mw_input.channel = self.mw_input.connection.channel()
    #             time.sleep(1)
    #         except Exception:
    #             time.sleep(1)
    #     logger.info(f"Timeout esperando colas {queue_names}; enviando igual")
    #     return False
    
    def run(self):
        self.connect_to_middleware()
        self._running = True

        logger.info("AppController listo para recibir mensajes desde el Gateway")

        def callback_results(ch, method, properties, body):
            try:
                rk = method.routing_key  
                header, rows = deserialize_message(body)
                header_dict = header.as_dictionary()
                query_id = header_dict["query_id"]
                msg_type = header_dict["message_type"]

                if msg_type == "DATA":
                    self._stream_result_rows(query_id, header_dict, rows)
                elif msg_type == "EOF":
                    logger.info(f"[EOF RESULT] Finalizó query_id={query_id}. Reporte listo.")
                    self._close_report_stream(query_id)
          
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
                ("schema", str(schema)),
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
                ("schema", str(schema)),
                ("source", source)
            ]
            header = Header(header_fields)
            message_bytes = serialize_message(header, [], schema)
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)
            logger.info(f"[EOF] {self.input_exchange}:{routing_key}")
        except Exception as e:
            logger.error(f"Error enviando END_OF_FILE para routing_key={routing_key}: {e}")

    def _stream_result_rows(self, query_id: str, header_dict: dict, rows: List[Dict]):
        if not rows:
            return

        schema_str = header_dict["schema"]
        try:
            schema = ast.literal_eval(schema_str)
        except Exception:
            logger.info(f"Schema inválido en header: {schema_str}")
            schema = list(rows[0].keys())

        sock, text_io, writer = self._get_or_create_report_stream(query_id, schema, header_dict)
        for row in rows:
            writer.writerow(row)
        text_io.flush()
        logger.info(f"[REPORT STREAM] Enviadas {len(rows)} filas para query_id={query_id}")

    # Devuelve la tupla (socket, csv_writer) para un determinado query_id
    def _get_or_create_report_stream(self, query_id: str, schema: List[str], header_dict: dict):
        if query_id in self._report_streams:
            return self._report_streams[query_id]

        try:
            source = header_dict.get("source")
            client_id = self._client_ids_by_source.get(source)
            if not client_id:
                raise RuntimeError(f"No se registró client_id para source={source}")

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.gateway_host, self.gateway_port))
            # MAndamos primero el query_id para que el Gateway sepa de qué reporte es
            sock.sendall(f"{client_id}\n===\n{query_id}\n===\n".encode("utf-8"))

            raw = sock.makefile('wb', buffering=0)  # raw binario sin buffer
            text_io = io.TextIOWrapper(raw, encoding='utf-8', newline='', write_through=True)
            writer = csv.DictWriter(text_io, fieldnames=schema)
            writer.writeheader()
            text_io.flush()

            self._report_streams[query_id] = (sock, text_io, writer)
            logger.info(f"[REPORT STREAM] Iniciada transmisión para query_id={query_id}")
            return self._report_streams[query_id]

        except Exception as e:
            logger.error(f"Error creando stream de reporte para {query_id}: {e}")
            raise

    def _close_report_stream(self, query_id: str):
        stream = self._report_streams.pop(query_id, None)
        if stream:
            sock, text_io, writer = stream
            try:
                text_io.flush()
                sock.shutdown(socket.SHUT_WR)
                sock.close()
                logger.info(f"[REPORT STREAM] Cerrado para query_id={query_id}")
            except Exception as e:
                logger.error(f"Error cerrando stream de reporte {query_id}: {e}")
