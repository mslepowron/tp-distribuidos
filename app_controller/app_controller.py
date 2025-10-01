import logging
from uuid import uuid4
import csv
import sys
import os
import time
import io
from typing import Dict, List
import pika
from middleware.rabbitmq.mom import MessageMiddlewareExchange
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.deserialize import deserialize_message
from communication.transport.tcp import start_tcp_listener, tcp_send_all
from initializer import initialize_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

TCP_PORT = int(os.getenv("APP_CONTROLLER_PORT", 9000))
BATCH_SIZE = 5 #TODO> Variable de entorno
MAX_REPORT_CHUNK = 8192  # 8KB por chunk

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

        self.results_buffer: Dict[str, List[Dict[str, str]]] = {}
        self.received_eofs: set[str] = set()

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
        self._wait_for_queues(["filter_year_q"])

        tcp_port = int(os.getenv("APP_CONTROLLER_PORT", 9100))
        start_tcp_listener(tcp_port, self.handle_tcp_batch, name="app-controller-tcp")
        logger.info(f"AppController escuchando batches por TCP en puerto {tcp_port}...")

        #def callback_results(ch, method, properties, body): LO DEJO COMENTADO PARA VOLVER A PROBAR SIN TODOS EOFS
        #    try:
        #        rk = method.routing_key
        #        logger.info(f"[results:{rk}] len={len(body)} bytes")
#
        #        header, rows = deserialize_message(body)
        #        logger.info(f"Header: {header.as_dictionary()}")
        #        for row in rows:
        #            logger.info(f"Resultado recibido: {row}")
        #        self._store_result(rk, rows)
#
        #        # Forzar envío inmediato para debug
        #        self._flush_reports()
#
        #    except Exception as e:
        #        logger.error(f"Error deserializando resultado: {e}")
        #    finally:
        #        ch.basic_ack(delivery_tag=method.delivery_tag)

        def callback_results(ch, method, properties, body):
            try:
                rk = method.routing_key  
                logger.info(f"[results:{rk}] len={len(body)} bytes")

                header, rows = deserialize_message(body)
                logger.info(f"Header: {header.as_dictionary()}")
                for row in rows:
                    logger.info(f"Resultado recibido: {row}")

                msg_type = header.fields.get("message_type")
                query_id = header.fields.get("query_id", rk)

                if msg_type == "DATA":
                    self._store_result(rk, rows)
                elif msg_type == "EOF":
                    logger.info(f"EOF recibido para {query_id}")
                    self.received_eofs.add(query_id)
                    # si ya tengo EOF de todas las queries esperadas, flush
                    if self._all_eofs_received():
                        logger.info("Todos los EOF recibidos, generando reportes...")
                        self._flush_reports()
                else:
                    logger.warning(f"Mensaje inesperado: {msg_type}")
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
            self._flush_reports()
            self.shutdown()
    
    def _all_eofs_received(self):
    # cada query_id corresponde a un routing key esperado
        return self.received_eofs.issuperset(set(self.result_routing_keys))

    def send_batch(self, batch, routing_key: str, source: str, schema: List[str]):
        """
        Envía un batch ya inicializado al middleware.
        """
        if not self._running or not batch:
            return
        try:
            header_fields = [
                ("message_type", "DATA"),
                ("query_id", "q_amount_75_tx"),  # TODO: ajustarlo si hace falta
                ("stage", "INIT"),
                ("part", source),  # usamos el mismo source del header recibido
                ("seq", str(uuid4())),
                ("schema", schema),
                ("source", source)
            ]
            header = Header(header_fields)

            message_bytes = serialize_message(header, batch, schema)
            route_key = self.input_routing_keys[0] if self.input_routing_keys else ""
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)

            logger.info(f"Batch {self.input_exchange}:{routing_key} ({len(batch)} filas)")
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")

    def send_end_of_file(self, routing_key: str, source: str):
        try:
            schema = []  # sin payload
            header_fields = [
                ("message_type", "EOF"),
                ("query_id", "q_amount_75_tx"),
                ("stage", "INIT"),
                ("part", source),
                ("seq", str(uuid4())),
                ("schema", schema),
                ("source", source)
            ]
            header = Header(header_fields)

            message_bytes = serialize_message(header, [], schema)
            route_key = self.input_routing_keys[0] if self.input_routing_keys else ""
            self.mw_input.send_to(self.input_exchange, routing_key, message_bytes)
            logger.info(f"EOF → {self.input_exchange}:{routing_key}")
        except Exception as e:
            logger.error(f"Error enviando END_OF_FILE: {e}")

    def handle_tcp_batch(self, conn, addr):
        logger.info(f"Conexión TCP abierta desde {addr}")
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break

                header, rows = deserialize_message(data)
                routing_key = header.fields["source"].split(".")[0]  # ej: "transactions"
                source = header.fields["source"]

                cleaned_rows = initialize_rows(rows, source)

                if header.fields["message_type"] == "DATA":
                    self.send_batch(cleaned_rows, routing_key, source, list(cleaned_rows[0].keys()) if cleaned_rows else [])
                elif header.fields["message_type"] == "EOF":
                    self.send_end_of_file(routing_key, source)

        except Exception as e:
            logger.error(f"Error procesando datos TCP: {e}")
        finally:
            conn.close()
            logger.info(f"Conexión TCP cerrada desde {addr}")
    
    def _store_result(self, routing_key, rows):
        if routing_key not in self.results_buffer:
            self.results_buffer[routing_key] = []
        self.results_buffer[routing_key].extend(rows)

    def _flush_reports(self, gw_host="gateway", gw_port=9200):
        files = self._generate_csv_reports()
        for name, content in files.items():
            data = content.encode("utf-8")
            total_size = len(data)
            logger.info(f"Enviando reporte {name} ({total_size} bytes) en chunks...")

            for i in range(0, total_size, MAX_REPORT_CHUNK):
                chunk = data[i:i+MAX_REPORT_CHUNK]
                header = f"FILENAME:{name};BATCH:{i//MAX_REPORT_CHUNK}\n"
                payload = header.encode("utf-8") + chunk
                tcp_send_all(gw_host, gw_port, payload)

            eof_msg = f"EOF:{name}\n".encode("utf-8")
            tcp_send_all(gw_host, gw_port, eof_msg)
            logger.info(f"Reporte {name} completado y enviado en {((total_size-1)//MAX_REPORT_CHUNK)+1} chunks")

    def _generate_csv_reports(self):
        files = {}
        for rk, rows in self.results_buffer.items():
            if not rows:
                continue
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            files[f"{rk}.csv"] = output.getvalue()
        return files

    

