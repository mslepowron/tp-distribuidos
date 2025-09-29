import logging
import csv
import os
from pathlib import Path
from typing import Dict, List, Tuple
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import CLEAN_SCHEMAS

logger = logging.getLogger("join")

MENU_OUTPUT_FIELDS = ["transaction_id", "created_at", "item_id", "item_name", "subtotal"]

#TODO: Este output esta hardcodeade para este caso. Habria que hacer algo mas dinamic.

class Join:
    REQUIRED_STREAMS: Tuple[str, str] = () #define que dos streams de datos debe esperar para hacer el join
    OUTPUT_FIELDS: List[str] = []
    OUTPUT_JOINED_FILE: str = None  #archivo donde se guarda el resultado del join

    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings, storage):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        storage_path = storage
        eof_count = 0
        #define un archivo para almacenar en c/archivo un tipo de stream de datos que necesita
        self.files: Dict[str, Path] = {s: storage / f"{s}.csv" for s in self.REQUIRED_STREAMS}
        self.join_out_file: Path = storage / self._output_filename()
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

    def _serialize_rows(self, header, rows):
        """
        Serializa las filas usando el schema que venga en el header.
        Funciona tanto para .raw como para .clean porque usa SCHEMAS.
        """
        schema_name = header.fields["schema"]
        if schema_name not in CLEAN_SCHEMAS:
            raise ValueError(f"Schema '{schema_name}' no encontrado en CLEAN_SCHEMAS (header={header.fields})")
        schema_fields = CLEAN_SCHEMAS[schema_name]
        return serialize_message(header, rows, schema_fields)
    
    def _send_rows(self, header, rows, routing_keys=None):
        if not rows:
            return
        try:
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": header.fields["query_id"],
                "stage": self.__class__.__name__,
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": header.fields["schema"],
                "source": header.fields["source"],
            })
            payload = self._serialize_rows(out_header, rows)
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, payload)
        except Exception as e:
            logger.error(f"Error enviando resultado: {e}")
    
    def _forward_eof(self, header, routing_keys=None):
        try:
            eof_payload = self._serialize_rows(header, [])
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  # fanout
                #logger.info(f"ENTRA AL FANOUT")
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                #logger.info(f"ENTRA AL ROUT CON KEY")
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")

    def _append_rows(self, file_path: Path, rows: List[Dict[str, str]]):
        """
        Anexa filas al csv. Un csv por tipo de stream de datos
          Si el archivo no existe/es vacío, escribe header con las keys que nos llegan en el schema del header.
        """
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
        if not file_path.exists() or file_path.stat().st_size == 0:
            return []
        with file_path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    
    def _output_basename(join_class) -> str:
        return join_class.OUTPUT_FILE_BASENAME
    
    def _stream_from_source(self, header: Header) -> str:
        """
        Toma el header y se fija que tipo de stream de datos es
        """
        src = (header.fields.get("source") or "").lower()
        if "transaction_items" in src:
            return "transaction_items"
        if "menu" in src:  #cubre menu y trx items, pero otros joins usan otros cosos
            return "menu"
        return ""
    
    def _on_all_eofs_then_join(self, last_header: Header):
        """
        Cuando EOF de todos los streams requeridos hace el join y manda la 
        salida al output exchange
        """
        if self.eof_count >= len(self.REQUIRED_STREAMS):
            try:
                out_rows = self._do_join()
                # persiste el join en storage
                with self.join_out_file.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=Join.OUTPUT_FIELDS)
                    writer.writeheader()
                    writer.writerows(out_rows)
                # TODO: Deberia levantar el storage de a batches/mandar lo joienado de a mabatches
                self._send_rows(last_header, out_rows)
                self._send_eof(last_header)
                logger.info(f"{self.__class__.__name__}: join emitido ({len(out_rows)} filas) + EOF")
            except Exception as e:
                logger.error(f"{self.__class__.__name__} join error: {e}")
            finally:
                self.eof_count = 0

# ----------------- SUBCLASES -----------------

class MenuJoin(Join):
    REQUIRED_STREAMS = ("menu_items", "transaction_items")
    OUTPUT_FIELDS = ["transaction_id", "created_at", "item_id", "item_name", "subtotal"]
    OUTPUT_FILE_BASENAME = "menu_join.csv"

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)  # seguimos usando tu deserializador
            stream = self._stream_from_source(header)
            if not stream:
                logger.warning(f"Stream desconocido. source={header.fields.get('source')} rk={getattr(method,'routing_key','')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if header.fields.get("message_type") == "EOF":
                self.eof_count += 1
                logger.info(f"EOF recibido de '{stream}'")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                # si ambos EOF => join y emitir
                self._on_all_eofs_then_join(header)
                return

            # DATA: persistimos al CSV correspondiente, sin validar schema
            self._append_rows(self.files[stream], rows)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"YearFilter error: {e}")
    
    #TODO: Creo que esta levantando todo en memoria.
    def _do_join(self) -> List[Dict[str, str]]:
        # lee los csv del storage
        menu_rows = self._read_rows(self.files["menu"])
        trx_rows  = self._read_rows(self.files["transaction_items"])

        if not menu_rows or not trx_rows:
            logger.warning("No hay datos suficientes para join (menu o transaction_items vacío).")
            return []
    
        def pick_item_id(row: Dict[str, str]) -> str:
                if "item_id" in row and row["item_id"] != "":
                    return row["item_id"]
                if "menu_item_id" in row and row["menu_item_id"] != "":
                    return row["menu_item_id"]
                return ""

        def pick_item_id(row: Dict[str, str]) -> str:
                if "item_id" in row and row["item_id"] != "":
                    return row["item_id"]
                if "menu_item_id" in row and row["menu_item_id"] != "":
                    return row["menu_item_id"]
                return ""

        # construye el indice de nombres: item_id -> item_name
        def pick_item_name(row: Dict[str, str]) -> str:
            if "item_name" in row and row["item_name"] != "":
                return row["item_name"]
            if "name" in row and row["name"] != "":
                return row["name"]
            return ""

        name_by_item: Dict[str, str] = {}
        for m in menu_rows:
            mid = pick_item_id(m)
            if not mid:
                continue
            nm = pick_item_name(m)
            if nm:
                name_by_item[mid] = nm

        # mapear a OUTPUT_FIELDS
        out: List[Dict[str, str]] = []
        for t in trx_rows:
            item_id = pick_item_id(t)
            if not item_id:
                continue
            out.append({
                "transaction_id": t.get("transaction_id", t.get("trx_id", "")),
                "created_at":     t.get("created_at", ""),
                "item_id":        item_id,
                "item_name":      name_by_item.get(item_id, ""),
                "subtotal":       t.get("subtotal", t.get("amount", "")),
            })

        return out
    


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

