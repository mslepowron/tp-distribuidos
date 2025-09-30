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

#TODO: Este output esta hardcodeade para este caso. Habria que hacer algo mas dinamic.

class Join:
    REQUIRED_STREAMS: Tuple[str, str] = () #define que dos streams de datos debe esperar para hacer el join
    OUTPUT_FIELDS: List[str] = []
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
                    if isinstance(value, list): #TODO Fix. 
                        value = ",".join(map(str, value))
                    norm_row[field] = str(value)
                normalized_rows.append(norm_row)

            out_header = Header({
                "message_type": "DATA",
                "query_id": header.fields.get("query_id"),
                "stage": header.fields.get("stage"),
                "part": header.fields.get("part"),
                "seq": header.fields.get("seq"),
                "schema": schema,
                "source": source,
            })

            payload = serialize_message(out_header, normalized_rows, schema)

            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, payload)

            logger.info(f"Sending batch to next worker through: {self.output_exchange} with rk={rks}")
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
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                for rk in rks:
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

# ----------------- SUBCLASES -----------------

class MenuJoin(Join):
    REQUIRED_STREAMS = ("menu_items", "transaction_items")
    OUTPUT_FIELDS = ["transaction_id", "created_at", "item_id", "item_name", "subtotal"]
    OUTPUT_FILE_BASENAME = "menu_join.csv"

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
            message_schema = header.fields.get("schema")

            # EOF
            if message_type == "EOF":
                logger.info(f"EOF recibido del archivo: '{source}' proveniente de: {message_stage}")

                if source.startswith("menu_items"):
                    self.source_file_index = self._build_menu_index()
                    self.source_file_closed = True

                    for batch in self._read_batches(self.files["transaction_items"], 1000): #TODO: Esta hardcodeado el batch
                        joined = self._join_batch(batch)
                        self._send_rows(header, "menu_join", joined, self.output_rk)

                elif source.startswith("transaction_items"):
                    if self.source_file_closed:
                        self._forward_eof(header, "menu_join", routing_keys=self.output_rk)

                if ch is not None and hasattr(method, "delivery_tag"):
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # DATA
            if source.startswith("menu_items"):
                self._append_rows(self.files["menu_items"], rows)

            elif source.startswith("transaction_items"):
                if not getattr(self, "source_file_closed", False):
                    self._append_rows(self.files["transaction_items"], rows)
                else:
                    joined = self._join_batch(rows)
                    self._send_rows(header, "menu_join", joined, self.output_rk)

            if ch is not None and hasattr(method, "delivery_tag"):
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"MenuJoin error: {e}")
            if ch is not None and hasattr(method, "delivery_tag"):
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def _build_menu_index(self) -> Dict[str, str]:
        """ Construye el índice de item_id + item_name una vez que tengo todo el menu """
        menu_rows = self._read_rows(self.files["menu_items"])

        def pick_item_id(row):
            return row.get("item_id") or row.get("menu_item_id") or ""

        def pick_item_name(row):
            raw_name = row.get("item_name") or row.get("name") or ""
            # Si es un string que parece lista, extraigo el primer elemento
            try:
                parsed = ast.literal_eval(raw_name)
                if isinstance(parsed, list) and parsed:
                    return parsed[0]
            except (ValueError, SyntaxError):
                pass
            return raw_name

        index = {}
        for m in menu_rows:
            mid = pick_item_id(m)
            nm = pick_item_name(m)
            if mid and nm:
                index[mid] = nm
        logger.info(f"Menu index construido con {len(index)} items")
        return index

    def _join_batch(self, trx_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """ Hace el join sobre un batch de transacciones usando el menú indexado """
        if not self.source_file_index:
            return []
        out = []
        for t in trx_rows:
            item_id = t.get("item_id") or t.get("menu_item_id") or ""
            if not item_id:
                continue
            out.append({
                "transaction_id": t.get("transaction_id", t.get("trx_id", "")),
                "created_at": t.get("created_at", ""),
                "item_id": item_id,
                "subtotal": t.get("subtotal", t.get("amount", "")),
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

class StoreJoin(Join):
    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self._forward_eof(header, routing_keys=[])  # fanout
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            filtered = []
            for row in rows:
                hour = int(row["created_at"].split(" ")[1].split(":")[0])
                if 6 <= hour < 23:
                    filtered.append(row)

            self._send_rows(header, filtered, routing_keys=[])  # fanout
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"HourFilter error: {e}")

class UserJoin(Join):
    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self._forward_eof(header, routing_keys=["q_amount_75_trx"])
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            filtered = []
            for row in rows:
                final_amount = float(row.get("final_amount") or 0.0)
                if final_amount >= 75.0:
                    filtered.append(row)

            self._send_rows(header, filtered, routing_keys=["q_amount_75_trx"])
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"AmountFilter error: {e}")

