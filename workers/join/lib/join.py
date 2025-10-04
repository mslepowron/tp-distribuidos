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
        
        self.source_file_index = None  # dict con  un _id y un nombre (ej item_id, item_name o store_id, store_name)
        self.source_file_closed = False
        eof_count = 0
        #define un archivo para almacenar en c/archivo un tipo de stream de datos que necesita
        self.files: Dict[str, Path] = {s: storage / f"{s}.csv" for s in self.REQUIRED_STREAMS}

        # chequea que existan los archivos vacios
        for f in self.files.values():
            if not f.exists():
                f.touch()

        self.join_out_file: Path = storage / self.OUTPUT_FILE_BASENAME

        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []

        for f in self.files.values():
            if not f.exists():
                f.touch()

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

    def define_schema(self, header):
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
            schema = self.define_schema(header)

            if not schema or any(s == "" for s in schema):
                first_row = rows[0]
                schema = list(first_row.keys())

            normalized_rows = []
            for row in rows:
                norm_row = {}
                for field in schema:
                    value = row.get(field, "")
                    norm_row[field] = str(value)
                normalized_rows.append(norm_row)

            out_header = Header({
                "message_type": "DATA",
                "query_id": header.fields.get("query_id"),
                "stage": "JOIN",
                "part": header.fields.get("part"),
                "seq": header.fields.get("seq"),
                "schema": schema,
                "source": source,
            })

            payload = serialize_message(out_header, normalized_rows, schema)

            if not routing_keys:
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in routing_keys:
                    self.result_mw.send_to(self.output_exchange, rk, payload)

            logger.info(f"Sending batch to next worker through: {self.output_exchange} with rk={routing_keys}")
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
                "source": header.fields["source"],
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

        file_exists = file_path.exists() and file_path.stat().st_size > 0
        
        fieldnames = list(rows[0].keys())
        
        with file_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    def _read_rows(self, file_path: Path) -> List[Dict[str, str]]:
        """Carga TODO el CSV (no usar para transacciones grandes, solo para menu)"""
        if not file_path.exists() or file_path.stat().st_size == 0:
            return []
        with file_path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    
    def _build_index(self, col1, col2) -> Dict[str, str]:
        """ Construye el índice de item_id + item_name una vez que tengo todo el menu """
        menu_rows = self._read_rows(self.files[self.REQUIRED_STREAMS[self.INPUT_FILE_TO_STORAGE]])

        index = {}
        for m in menu_rows:
            mid = m.get(col1)
            name = m.get(col2)
            if mid and name:
                index[mid] = name

        logger.info(f"Menu index construido con {len(index)} items")
        return index

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

# ----------------- SUBCLASES -----------------

class MenuJoin(Join):
    REQUIRED_STREAMS = ("menu_items", "transaction_items")
    OUTPUT_FILE_BASENAME = "menu_join.csv"
    INPUT_FILE_TO_STORAGE = 0
    INPUT_FILE_IN_BATCHS = 1
    SOURCE = "menu_join"
    BATCH_SZ = 1000

    def print_joined_rows(self, rows):
        logger.info(f"-----------------------------------------------------------------------------------------------------")
        logger.info(f"Imprimo rows")
        n=0
        for row in rows:
            n+=1
            logger.info(f"Row_{n}: {row}")
        logger.info(f"-----------------------------------------------------------------------------------------------------")

    def define_schema(self, header):
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
    
    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            
            source = (header.fields.get("source") or "").lower()
            message_type = header.fields.get("message_type")
            message_stage = header.fields.get("stage")

            # EOF
            if message_type == "EOF":
                logger.info(f"EOF recibido del archivo: '{source}' proveniente de: {message_stage}")

                if source.startswith(self.REQUIRED_STREAMS[self.INPUT_FILE_TO_STORAGE]):
                    self.source_file_index = self._build_index("item_id","item_name")
                    self.source_file_closed = True

                    for batch in self._read_batches(self.files[self.REQUIRED_STREAMS[self.INPUT_FILE_IN_BATCHS]], self.BATCH_SZ): #TODO: Esta hardcodeado el batch
                        # Joineo los batches que llegaron antes del EOF del archivo a almacenar
                        joined = self._join_batch(batch)
                        self.print_joined_rows(joined)
                        self._send_rows(header, self.SOURCE, joined, self.output_rk)

                elif source.startswith(self.REQUIRED_STREAMS[self.INPUT_FILE_IN_BATCHS]):
                    # si todavia ya me llego completo el archivo a almacenar entonces fowardeo el EOF
                    if self.source_file_closed:
                        self._forward_eof(header, self.SOURCE, routing_keys=self.output_rk)

                    if ch is not None and hasattr(method, "delivery_tag"):
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            # DATA
            if source.startswith(self.REQUIRED_STREAMS[self.INPUT_FILE_TO_STORAGE]):
                # almacenamos las filas
                self._append_rows(self.files[self.REQUIRED_STREAMS[self.INPUT_FILE_TO_STORAGE]], rows)

            elif source.startswith(self.REQUIRED_STREAMS[self.INPUT_FILE_IN_BATCHS]):
                if not self.source_file_closed:
                    self._append_rows(self.files[self.REQUIRED_STREAMS[self.INPUT_FILE_IN_BATCHS]], rows)
                else:
                    joined = self._join_batch(rows)
                    self.print_joined_rows(joined)
                    self._send_rows(header, self.SOURCE, joined, self.output_rk)

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

    def callback(self, ch, method, properties, body):
        logger.info(f"No IMPLEMENTADO")

class UserJoin(Join):

    def callback(self, ch, method, properties, body):
        logger.info(f"No IMPLEMENTADO")