import logging
import csv
import os
import ast
from pathlib import Path
from typing import Dict, List, Tuple
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header

logger = logging.getLogger("join")

class Join:
    REQUIRED_STREAMS: Tuple[str, str] = () #define que dos streams de datos debe esperar para hacer el join
    OUTPUT_FILE_BASENAME: str = None  #archivo donde se guarda el resultado del join

    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings, storage):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        self.storage = Path(storage)
        self.source_file_index: Dict[str, str] = {}
        self.source_file_closed = False
        eof_count = 0

        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_join()
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            self.mw.stop_consuming()
            self._close_mw()
        except Exception as e:
            logger.error(f"Error durante consumo de mensajes: {e}")
            self._close_mw()

    def _close_mw(self):
        try:
            self.mw.close()
            self.result_mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexion: {e}")

    def start_join(self):
        self.mw.start_consuming(self.callback)

    def define_schema(self, header, rk=None):
        try:
            schema = header.fields["schema"]
            raw_fieldnames = schema.strip()[1:-1]

            # dividir por coma
            parts = raw_fieldnames.split(",")

            # limpiar cada valor (sacar comillas simples/dobles y espacios extra)
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")
    
    def _send_rows(self, header, source, rows, routing_keys=None):
        if not rows:
            return

        try:
            schema = self.define_schema(header, routing_keys)

            logger.info(f"schema {schema}")

            out_header = Header({
                "message_type": "DATA",
                "query_id": header.fields.get("query_id"),
                "stage": "JOIN",
                "part": header.fields.get("part"),
                "seq": header.fields.get("seq"),
                "schema": schema,
                "source": source,
            })

            payload = serialize_message(out_header, rows, schema)
            
            if not routing_keys:
                logger.info(f"Sending batch to next worker through: {self.output_exchange} with rk={routing_keys}")
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in routing_keys:
                    logger.info(f"Sending batch to next worker through: {self.output_exchange} with rk={rk}")
                    self.result_mw.send_to(self.output_exchange, rk, payload)

        except Exception as e:
            logger.error(f"Error sending results: {e}")
    
    def _forward_eof(self, header, stage, routing_keys=None):
        try:
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": header.fields["query_id"],
                "stage": stage,
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema":  header.fields["schema"],
                "source": stage,
            })
            eof_payload = serialize_message(out_header, [], header.fields["schema"])

            if not routing_keys:  
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                for rk in routing_keys:
                    logger.info(f"Envio eof a --->{rk}")
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")

    def _append_rows(self, file_path: Path, rows: List[Dict[str, str]]):
        """ Persiste un batch de filas en CSV """
        if not rows:
            return

        # Crear el directorio si no existe
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = file_path.exists() and file_path.stat().st_size > 0
        
        fieldnames = list(rows[0].keys())
        
        with file_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)


    # para cada fila en el batch creo una nueva
    def _join_batch(self, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """ Hace el join sobre un batch de transacciones usando el menu indexado """
        if not self.source_file_index:
            return []

        out = []
        for row in rows:
            item_id = row.get("item_id")
           
            if not item_id:
                continue

            out.append({
                "transaction_id": row.get("transaction_id", ""),
                "created_at": row.get("created_at", ""),
                "item_id": item_id,
                "subtotal": row.get("subtotal",""),
                "item_name": self.source_file_index.get(item_id, ""),
            })

        return out

    def _read_batches(self, file_path: Path, batch_size: int):
        """ Itera el CSV en batches para no cargar todo en memoria """
        if not file_path.exists() or file_path.stat().st_size == 0:
            return

        with file_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def update_index(self, rows, col1, col2):
        for row in rows:
            item_id = row.get(col1)
            item_name = row.get(col2)
            if item_id and item_name:
                self.source_file_index[item_id] = item_name

    def on_eof_message(self, source, header):
        if source.startswith(self.PIVOT_FILE):
            self.source_file_closed = True
            storage_path = self.storage / f"{self.FILE_IN_BATCHS}.csv"

            for batch in self._read_batches(storage_path, self.BATCH_SZ):
                # Joineo los batches que llegaron antes del EOF del archivo a almacenar
                joined = self._join_batch(batch)
                self.print_joined_rows(joined)
                self._send_rows(header, self.SOURCE, joined, self.output_rk)

        elif source.startswith(self.FILE_IN_BATCHS):
            # si todavia ya me llego completo el archivo a almacenar entonces fowardeo el EOF
            if self.source_file_closed:
                self._forward_eof(header, self.SOURCE, routing_keys=self.output_rk)

            return True

        return False

    def print_joined_rows(self, rows):
        logger.info(f"-----------------------------------------------------------------------------------------------------")
        logger.info(f"Imprimo rows")
        n=0
        for row in rows:
            n+=1
            logger.info(f"Row_{n}: {row}")
        logger.info(f"-----------------------------------------------------------------------------------------------------")

# ----------------- SUBCLASES -----------------

class MenuJoin(Join):
    PIVOT_FILE = "menu_items"
    FILE_IN_BATCHS = "transaction_items"
    SOURCE = "menu_join"
    BATCH_SZ = 1000

    def define_schema(self, header, rk=None):
        try:
            schema = header.fields["schema"] 
            raw_fieldnames = schema.strip()[1:-1]
            parts = raw_fieldnames.split(",")

            # limpiar cada valor
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]

            fieldnames.append("item_name")

            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def on_data_message(self, source, header, rows):
        if source.startswith(self.PIVOT_FILE):
                self.update_index(rows, "item_id", "item_name")

        elif source.startswith(self.FILE_IN_BATCHS):
            if not self.source_file_closed:
                storage_path = self.storage / f"{self.FILE_IN_BATCHS}.csv"
                self._append_rows(storage_path, rows)
            else:
                joined = self._join_batch(rows)
                self.print_joined_rows(joined)
                self._send_rows(header, self.SOURCE, joined, self.output_rk)

            return True
        return False
    
    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            
            source = (header.fields.get("source") or "").lower()
            message_type = header.fields.get("message_type")
            message_stage = header.fields.get("stage")

            # EOF
            if message_type == "EOF":
                logger.info(f"EOF recibido del archivo: '{source}' proveniente de: {message_stage}")

                res = self.on_eof_message(source, header)

                if res:
                    if ch is not None and hasattr(method, "delivery_tag"):
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            res = self.on_data_message(source, header, rows)
            if res:
                if ch is not None and hasattr(method, "delivery_tag"):
                        ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"MenuJoin error: {e}")
            if ch is not None and hasattr(method, "delivery_tag"):
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def _join_batch(self, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """ Hace el join sobre un batch de transacciones usando el menu indexado """
        if not self.source_file_index:
            return []

        out = []
        for row in rows:
            item_id = row.get("item_id")
           
            if not item_id:
                continue

            out.append({
                "transaction_id": row.get("transaction_id", ""),
                "created_at": row.get("created_at", ""),
                "item_id": item_id,
                "subtotal": row.get("subtotal",""),
                "item_name": self.source_file_index.get(item_id, ""),
            })

        return out


class StoreJoin(Join):
    PIVOT_FILE = "stores"
    FILE_IN_BATCHS = "transactions"
    SOURCE = "store_join"
    BATCH_SZ = 1000

    def define_schema(self, header, rk=None):
        try:
            if rk[0] == "stores_joined": 
                fieldnames = ['transaction_id', 'created_at', 'store_id', 'final_amount', 'store_name']
            else:
                fieldnames = ['transaction_id', 'store_id', 'store_name', 'user_id']

            return fieldnames
        except KeyError:
            raise KeyError(f"Schema no encontrado en SCHEMAS")

    def on_eof_message(self, source, header):
        if source.startswith(self.PIVOT_FILE):
            self.source_file_closed = True
            storage_path = self.storage / f"{self.FILE_IN_BATCHS}.csv"

            for batch in self._read_batches(storage_path, self.BATCH_SZ):
                # Joineo los batches que llegaron antes del EOF del archivo a almacenar
                joined_filter, joined_reduce = self._join_batch(batch)
                self.print_joined_rows(joined_filter)
                self.print_joined_rows(joined_reduce)
                self._send_rows(header, self.SOURCE, joined_filter, [self.output_rk[0]])
                self._send_rows(header, self.SOURCE, joined_reduce, [self.output_rk[1]])

        elif source.startswith(self.FILE_IN_BATCHS):
            # si todavia ya me llego completo el archivo a almacenar entonces fowardeo el EOF
            if self.source_file_closed:
                self._forward_eof(header, self.SOURCE, routing_keys=self.output_rk)

            return True

        return False

    def on_data_message(self, source, header, rows):
        if source.startswith(self.PIVOT_FILE):
                self.update_index(rows, "store_id", "store_name")

        elif source.startswith(self.FILE_IN_BATCHS):
            if not self.source_file_closed:
                storage_path = self.storage / f"{self.FILE_IN_BATCHS}.csv"
                self._append_rows(storage_path, rows)
            else:
                joined_filter, joined_reduce = self._join_batch(rows)
                self.print_joined_rows(joined_filter)
                self.print_joined_rows(joined_reduce)
                self._send_rows(header, self.SOURCE, joined_filter, [self.output_rk[0]])
                self._send_rows(header, self.SOURCE, joined_reduce, [self.output_rk[1]])

            return True
        return False
    
    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            
            source = (header.fields.get("source") or "").lower()
            message_type = header.fields.get("message_type")
            message_stage = header.fields.get("stage")

            # EOF
            if message_type == "EOF":
                logger.info(f"EOF recibido del archivo: '{source}' proveniente de: {message_stage}")

                res = self.on_eof_message(source, header)

                if res:
                    if ch is not None and hasattr(method, "delivery_tag"):
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            res = self.on_data_message(source, header, rows)
            if res:
                if ch is not None and hasattr(method, "delivery_tag"):
                        ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"{self.SOURCE} error: {e}")
            if ch is not None and hasattr(method, "delivery_tag"):
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def _join_batch(self, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """ Hace el join sobre un batch de transacciones usando el menu indexado """
        if not self.source_file_index:
            return []

        out_filter = []
        out_reduce = []
        for row in rows:
            store_id = row.get("store_id")
           
            if not store_id:
                continue

            out_filter.append({
                    "transaction_id": row.get("transaction_id"),
                    "created_at": row.get("created_at", ""),
                    "store_id": store_id,
                    "final_amount": row.get("final_amount"),
                    "store_name": self.source_file_index.get(store_id, ""),
                })

            out_reduce.append({
                "transaction_id": row.get("transaction_id"),
                "store_id": store_id,
                "store_name": self.source_file_index.get(store_id, ""),
                "user_id": row.get("user_id"),
            })

        return out_filter, out_reduce

class UserJoin(Join):

    def callback(self, ch, method, properties, body):
        logger.info(f"No IMPLEMENTADO")