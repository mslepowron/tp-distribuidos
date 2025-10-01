import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
import csv
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("reduce")

class Reduce:
    REQUIRED_STREAMS: Tuple[str, str] = ()

    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings, storage):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []
        self.schema_out = None
        #self.files: Dict[str, Path] = {s: storage / f"{s}.csv" for s in self.REQUIRED_STREAMS}

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_reducer()
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

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def define_schema(self, header):
        try:
            schema = header.fields["schema"]
            raw_fieldnames = schema.strip()[1:-1]

            # dividir por coma
            parts = raw_fieldnames.split(",")

            # limpiar cada valor (sacar comillas simples/dobles y espacios extra)
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]
            logging.info(f"DEFINE SCHEMA fieldnames: {fieldnames}")
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def _send_rows(self, header, source, rows, routing_keys, query_id=None):
        if not rows:
            return
        try:
            schema = self.define_schema(header)
            out_header = Header({
                "message_type": "DATA",
                "query_id": query_id if query_id is not None else header.fields["query_id"],
                "stage": header.fields.get("stage"),
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": schema,
                "source": source,
            })
            payload = serialize_message(out_header, rows, schema)
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  #caso fanout; 
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in rks:
                    # logging.info(f"SEND ROWS sent header: {out_header.__dict__}")
                    # logging.info(f"SEND ROWS sent rows: {rows}")
                    logging.info(f"Reduced rows sent to: {rk}")
                    self.result_mw.send_to(self.output_exchange, rk, payload)
        except Exception as e:
            logger.error(f"ERROR SEND ROWS. HEADER: {out_header}")
            logger.error(f"ERROR SEND ROWS. PAYLOAD: {payload}")
            logger.error(f"Error enviando resultado: {e}")
    
    def _forward_eof(self, header, stage, routing_keys=None, query_id=None):
        try:
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": query_id if query_id is not None else header.fields["query_id"],
                "stage": stage,
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": header.fields["schema"],
                "source": header.fields["source"],
            })
            eof_payload = serialize_message(out_header, [], header.fields["schema"])
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  # fanout
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                for rk in rks:
                    logging.info(f"SEND EOF sent header: {out_header}")
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")
        

# ----------------- SUBCLASES -----------------

class QuantityReducer(Reduce):
    #REQUIRED_STREAMS = ["menu_items_by_month"]
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["year_month_created_at", "item_name", "selling_qty"]
        self.storage_file = Path("storage/reduce_quantity.csv")
        self.eofs_received = 0
        self.eofs_expected = 1

        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        if self.storage_file.exists():
            self.storage_file.unlink()

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def _update_storage(self, ym, item):
        """Lee el archivo y actualiza el contador para (ym,item)."""
        rows = {}
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows[(r["year_month_created_at"], r["item_name"])] = int(r["selling_qty"])

        # actualizar acumulado
        key = (ym, item)
        old_val = rows.get(key, 0)
        rows[key] = old_val + 1
        logger.info(f"[QuantityReducer] Update ({ym}, {item}): {old_val} -> {rows[key]}")

        # reescribir archivo con todos los acumulados
        with open(self.storage_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema_out_fields)
            writer.writeheader()
            for (ym, item), qty in rows.items():
                writer.writerow({"year_month_created_at": ym, "item_name": item, "selling_qty": qty})
        logger.debug(f"[QuantityReducer] Storage reescrito con {len(rows)} filas.")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                logger.info(f"[QuantityReducer] EOF recibido ({self.eofs_received}/{self.eofs_expected})")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                    self._forward_eof(header, "QuantityReducer", self.output_rk)
                return

            for row in rows:
                ym = row.get("year_month_created_at")
                item = row.get("item_name")
                if ym and item:
                    self._update_storage(ym, item)
                else:
                    logger.warning(f"[QuantityReducer] Fila inválida: {row}")
              
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en QuantityReducer: {e}")

    def _emit_result(self, header):
        result_rows = []
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    result_rows.append(r)
        logger.info(f"[QuantityReducer] Emisión final con {len(result_rows)} filas.")

        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceQuantity"
        self._send_rows(header, "quantity_reduce", result_rows, routing_keys=self.output_rk)


class ProfitReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["year_month_created_at", "item_name", "profit_sum"]
        self.storage_file = Path("storage/reduce_profit.csv")
        self.eofs_received = 0
        self.eofs_expected = 1

        # inicializamos archivo vacío
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        if self.storage_file.exists():
            self.storage_file.unlink()

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def _update_storage(self, ym, item, subtotal):
        """Lee el archivo y actualiza la suma para (ym,item)."""
        rows = {}
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows[(r["year_month_created_at"], r["item_name"])] = float(r["profit_sum"])

        # actualizar acumulado
        key = (ym, item)
        old_val = rows.get(key, 0.0)
        rows[key] = old_val + subtotal
        logger.debug(f"[ProfitReducer] Update ({ym}, {item}): {old_val} -> {rows[key]}")

        # reescribir archivo con todos los acumulados
        with open(self.storage_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema_out_fields)
            writer.writeheader()
            for (ym, item), profit_sum in rows.items():
                writer.writerow({
                    "year_month_created_at": ym,
                    "item_name": item,
                    "profit_sum": round(profit_sum, 2)
                })
        logger.info(f"[ProfitReducer] Storage reescrito con {len(rows)} filas.")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                logger.info(f"[ProfitReducer] EOF recibido ({self.eofs_received}/{self.eofs_expected})")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                    self._forward_eof(header, "ProfitReducer", self.output_rk)
                return

            # procesar filas e ir actualizando archivo
            for row in rows:
                ym = row.get("year_month_created_at")
                item = row.get("item_name")
                if not ym or not item:
                    logger.warning(f"[ProfitReducer] Fila inválida: {row}")
                    continue
                subtotal = float(row.get("subtotal") or 0.0)
                self._update_storage(ym, item, subtotal)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en ProfitReducer: {e}")

    def _emit_result(self, header):
        result_rows = []
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    result_rows.append(r)
        logger.info(f"[ProfitReducer] Emisión final con {len(result_rows)} filas.")

        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceProfit"
        self._send_rows(header, "profit_reduce", result_rows, routing_keys=self.output_rk)

class UserPurchasesReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["store_name", "user_id", "user_purchases"]
        self.storage_file = Path("storage/reduce_user_purchases.csv")
        self.eofs_received = 0
        self.eofs_expected = 1

        # inicializamos archivo vacío
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        if self.storage_file.exists():
            self.storage_file.unlink()

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def _update_storage(self, store_name, user_id, final_amount):
        """Lee el archivo y actualiza la suma para (yh,store)."""
        rows = {}
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows[(r["store_name"], r["user_id"])] = float(r["user_purchases"])

        # actualizar acumulado
        key = (store_name, user_id)
        old_val = rows.get(key, 0.0)
        rows[key] = old_val + 1 # incremento la cantidad de compras del usr en 1
        logger.debug(f"[UserPurchasesReducer] Update ({store_name}, {user_id}): {old_val} -> {rows[key]}")

        # reescribir archivo con todos los acumulados
        with open(self.storage_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema_out_fields)
            writer.writeheader()
            for (store_name, user), user_purchases in rows.items():
                writer.writerow({
                    "store_name": store_name,
                    "user_id": user,
                    "user_purchases": user_purchases
                })
        logger.info(f"[UserPurchasesReducer] Storage reescrito con {len(rows)} filas.")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                logger.info(f"[UserPurchasesReducer] EOF recibido ({self.eofs_received}/{self.eofs_expected})")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected: #esta al pedo
                    self._emit_result(header)
                    self._forward_eof(header, "UserPurchasesReducer", self.output_rk)
                return

            # procesar filas e ir actualizando archivo
            for row in rows:
                logger.info(f"[UserPurchasesReducer] Fila : {row}")
                store_name = row.get("store_name")
                user = row.get("user_id")
                if not user:
                    logger.warning(f"NO TIENE ID")
                if not store_name:
                    logger.warning(f"NO TIENE NOMBRE DE STORE")


                if not store_name or not user:
                    logger.warning(f"[UserPurchasesReducer] Fila inválida: {row}")
                    continue
                self._update_storage(store_name, user, 0)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en UserPurchasesReducer: {e}")

    def _emit_result(self, header):
        result_rows = []
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    result_rows.append(r)
        logger.info(f"[UserPurchasesReducer] Emisión final con {len(result_rows)} filas.")

        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceuserPurchases"
        self._send_rows(header, "user_purchases_reduce", result_rows, routing_keys=self.output_rk)


class TpvReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["year_half_created_at", "store_name", "tpv"]
        self.storage_file = Path("storage/reduce_tpv.csv")
        self.eofs_received = 0
        self.eofs_expected = 1

        # inicializamos archivo vacío
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        if self.storage_file.exists():
            self.storage_file.unlink()

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def _update_storage(self, yh, store, final_amount):
        """Lee el archivo y actualiza la suma para (yh,store)."""
        rows = {}
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows[(r["year_half_created_at"], r["store_name"])] = float(r["tpv"])

        # actualizar acumulado
        key = (yh, store)
        old_val = rows.get(key, 0.0)
        rows[key] = old_val + final_amount
        logger.debug(f"[TpvReducer] Update ({yh}, {store}): {old_val} -> {rows[key]}")

        # reescribir archivo con todos los acumulados
        with open(self.storage_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.schema_out_fields)
            writer.writeheader()
            for (yh, store), tpv in rows.items():
                writer.writerow({
                    "year_half_created_at": yh,
                    "store_name": store,
                    "tpv": round(tpv, 2)
                })
        logger.info(f"[TpvReducer] Storage reescrito con {len(rows)} filas.")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                logger.info(f"[TpvReducer] EOF recibido ({self.eofs_received}/{self.eofs_expected})")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                    self._forward_eof(header, "TpvReducer", self.output_rk, query_id=self.output_rk[0])
                return

            # procesar filas e ir actualizando archivo
            for row in rows:
                # logger.info(f"[TpvReducer] Fila : {row}")
                yh = row.get("year_half_created_at")
                store = row.get("store_name")
                if not yh or not store:
                    logger.warning(f"[TpvReducer] Fila inválida: {row}")
                    continue
                final_amount = float(row.get("final_amount") or 0.0)
                self._update_storage(yh, store, final_amount)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en TpvReducer: {e}")

    def _emit_result(self, header):
        result_rows = []
        if self.storage_file.exists():
            with open(self.storage_file, newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    result_rows.append(r)
        logger.info(f"[TpvReducer] Emisión final con {len(result_rows)} filas.")

        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceTpv"
        self._send_rows(header, "tpv_reduce", result_rows, routing_keys=self.output_rk, query_id=self.output_rk[0])