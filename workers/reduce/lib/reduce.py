import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header

logger = logging.getLogger("reduce")

class Reduce:
    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []
        self.schema_out = None

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
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def _send_rows(self, header, rows, routing_keys):
        if not rows:
            return
        try:
            schema = self.define_schema(header)
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": header.fields["query_id"],
                "stage": "FilterYear",
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": schema,
                "source": header.fields["source"],
            })
            payload = serialize_message(out_header, rows, schema)
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  #caso fanout; 
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, payload)
        except Exception as e:
            logger.error(f"Error enviando resultado: {e}")
    
    def _forward_eof(self, header, stage, routing_keys=None):
        try:
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": header.fields["query_id"],
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
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")
        

# ----------------- SUBCLASES -----------------

class UserPurchasesReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["store_name", "user_id", "user_purchases"]
        self.aggregates = {}  # clave: (user_id, store_id) → valor: total final_amount
        self.eofs_received = 0
        self.eofs_expected = 1  

    def start_reduce(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                user_id = row.get("user_id")
                store_key = row.get("store_name") or row.get("store_id")  # preferimos nombre
                if user_id is None or store_key is None:
                    # si viene una fila incompleta, la ignoramos para no romper
                    continue
                amount = float(row.get("final_amount") or 0.0)
                key = (user_id, store_key)
                self.aggregates[key] = self.aggregates.get(key, 0.0) + amount

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en UserPurchasesReducer: {e}")

    def _emit_result(self, header):
        result_rows = [
            {
                "store_name": store_key,      # si no existía nombre, cayó a store_id arriba
                "user_id": user_id,
                "user_purchases": round(total, 2)
            }
            for (user_id, store_key), total in self.aggregates.items()
        ]

        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceUserPurchases"
        self._send_rows(header, result_rows, routing_keys=self.output_rk)

class TpvReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["store_name", "year_semester", "tpv"]
        self.aggregates = {}  # key: (store_name, year_semester) → sum(final_amount)
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                store = row.get("store_name") or row.get("store_id")
                semester = row.get("year_semester")
                if not store or not semester:
                    continue
                amount = float(row.get("final_amount") or 0.0)
                key = (store, semester)
                self.aggregates[key] = self.aggregates.get(key, 0.0) + amount

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en TpvReducer: {e}")

    def _emit_result(self, header):
        result_rows = [
            {
                "store_name": store,
                "year_semester": semester,
                "tpv": round(amount, 2)
            }
            for (store, semester), amount in self.aggregates.items()
        ]
        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceTPV"

        self._send_rows(header, result_rows, routing_keys=self.output_rk)

class QuantityReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["year_month", "item_name", "selling_qty"]
        self.aggregates = {}  # key: (year_month, item_name) → count
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                ym = row.get("year_month")
                item = row.get("item_name")
                if not ym or not item:
                    continue
                key = (ym, item)
                self.aggregates[key] = self.aggregates.get(key, 0) + 1

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en QuantityReducer: {e}")

    def _emit_result(self, header):
        result_rows = [
            {
                "year_month": ym,
                "item_name": item,
                "selling_qty": qty
            }
            for (ym, item), qty in self.aggregates.items()
        ]
        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceQuantity"
        self._send_rows(header, result_rows, routing_keys=self.output_rk)

class ProfitReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out_fields = ["year_month", "item_name", "profit_sum"]
        self.aggregates = {}  # clave: (year_month, item_name) → suma de subtotales
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                ym = row.get("year_month")
                item = row.get("item_name")
                if not ym or not item:
                    continue
                subtotal = float(row.get("subtotal") or 0.0)
                key = (ym, item)
                self.aggregates[key] = self.aggregates.get(key, 0.0) + subtotal

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en ProfitReducer: {e}")

    def _emit_result(self, header):
        result_rows = [
            {
                "year_month": ym,
                "item_name": item,
                "profit_sum": round(amount, 2)
            }
            for (ym, item), amount in self.aggregates.items()
        ]
        header.fields["schema"] = str(self.schema_out_fields)
        header.fields["stage"] = "ReduceProfit"
        self._send_rows(header, result_rows, routing_keys=self.output_rk)