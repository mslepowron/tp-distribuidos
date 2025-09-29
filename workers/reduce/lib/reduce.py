import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import RAW_SCHEMAS

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

    def _serialize_rows(self, header, rows):
        schema_name = header.fields["schema"]
        if schema_name not in RAW_SCHEMAS:
            raise ValueError(f"Schema '{schema_name}' no encontrado en RAW_SCHEMAS (header={header.fields})")
        schema_fields = RAW_SCHEMAS[schema_name]
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
            if not rks:  #caso fanout; aca nos aplica para el hours
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
        self.schema_out = "user_purchases.aggregated"
        self.aggregates = {}  # clave: (user_id, store_id) → valor: total final_amount
        self.eofs_received = 0
        self.eofs_expected = 1  

    def start_reduce(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                key = (row["user_id"], row["store_id"])
                final_amount = float(row.get("final_amount") or 0.0)
                if key in self.aggregates:
                    self.aggregates[key] += final_amount
                else:
                    self.aggregates[key] = final_amount

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error en UserPurchasesReducer: {e}")

    def _emit_result(self, header):
        result_rows = [
            {
                "user_id": uid,
                "store_id": sid,
                "total_purchased": round(total, 2)
            }
            for (uid, sid), total in self.aggregates.items()
        ]

        header.fields["schema"] = self.schema_out
        self._send_rows(header, result_rows)

class TpvReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out = "tpv.aggregated"
        self.aggregates = {}  # key: (store_name, year_semester) → sum(final_amount)
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                key = (row["store_name"], row["year_semester"])
                amount = float(row.get("final_amount") or 0.0)
                if key in self.aggregates:
                    self.aggregates[key] += amount
                else:
                    self.aggregates[key] = amount

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
        header.fields["schema"] = self.schema_out
        self._send_rows(header, result_rows)

class QuantityReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out = "quantity.aggregated"
        self.aggregates = {}  # key: (year_month, item_name) → count
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                key = (row["year_month"], row["item_name"])
                if key in self.aggregates:
                    self.aggregates[key] += 1
                else:
                    self.aggregates[key] = 1

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
        header.fields["schema"] = self.schema_out
        self._send_rows(header, result_rows)

class ProfitReducer(Reduce):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema_out = "profit.aggregated"
        self.aggregates = {}  # clave: (year_month, item_name) → suma de subtotales
        self.eofs_received = 0
        self.eofs_expected = 1

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self.eofs_received += 1
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if self.eofs_received >= self.eofs_expected:
                    self._emit_result(header)
                return

            for row in rows:
                key = (row["year_month"], row["item_name"])
                subtotal = float(row.get("subtotal") or 0.0)
                if key in self.aggregates:
                    self.aggregates[key] += subtotal
                else:
                    self.aggregates[key] = subtotal

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
        header.fields["schema"] = self.schema_out
        self._send_rows(header, result_rows)