import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import CLEAN_SCHEMAS

logger = logging.getLogger("top")


class Top:
    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []
        self.top_lenght = 3
        self.top = []

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_top()
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

    def start_top(self):
        # """Método abstracto, lo redefine cada hijo."""
        raise NotImplementedError

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
                "stage": "Top",
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": schema,
                "source": header.fields["source"],
            })
            payload = serialize_message(out_header, rows, schema)
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  #caso fanout; aca nos aplica para el hours
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

class TopSellingItems(Top):
    def start_top(self):
        self.mw.start_consuming(self.callback)

    def update(self, item, count):
        # Caso: aún no llegué a n elementos
        if len(self.top) < self.top_lenght:
            self.top.append((item, count))
            self.top.sort(key=lambda x: x[1], reverse=True)
            return

        # Caso: lista llena, comparo con el minimo (ultimo elemento)
        if count > self.top[-1][1]:
            self.top[-1] = (item, count)
            self.top.sort(key=lambda x: x[1], reverse=True)

    def get_top(self):
        return self.top
    
    def to_csv(self):
        """Convierte el top a formato CSV en un string"""
        lines = []
        for item, count in self.top:
            lines.append(f"{2024},{item},{count}")
        return "\n".join(lines)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            logger.info(f"recibo rows")

            if header.fields.get("message_type") == "EOF":
                toped = self.get_top()
                #csv_str = self.to_csv()
                result_rows = [
                    {"year_month_created_at": "2024-10", "item_name": item, "selling_qty": count}
                    for item, count in toped
                ]
                header.fields["schema"] = str(["year_month_created_at", "item_name", "selling_qty"])
                header.fields["stage"] = "TopSellingItems"
                logger.info(f"RESULT ROWS: {result_rows}")
                self._send_rows(header, result_rows, self.output_rk)
                self._forward_eof(header, "TopSellingItems", routing_keys=self.output_rk)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            for row in rows:
                item = row.get("item_name")       # clave del row (ejemplo)
                count = row.get("selling_qty", 1)     # cantidad (ejemplo)
                # logger.info(f"row {item}: {count}")
                if item is not None and count is not None:
                    self.update(item, count)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"TopSellingItems error: {e}")


class TopRevenueGeneratinItems(Top):
    def start_top(self):
        self.mw.start_consuming(self.callback)

    def define_schema(self, header):
        try:
            schema = header.fields["schema"] 
            raw_fieldnames = schema.strip()[1:-1]
            parts = raw_fieldnames.split(",")

            # limpiar cada valor
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]

            if header.fields["source"].startswith("transactions"): 
                fieldnames.remove("created_at")
                fieldnames.remove("store_id")
                fieldnames.remove("user_id")
           
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def update(self, item, count):
        # Caso: aún no llegué a n elementos
        if len(self.top) < self.top_lenght:
            self.top.append((item, count))
            self.top.sort(key=lambda x: x[1], reverse=True)
            return

        # Caso: lista llena, comparo con el minimo (ultimo elemento)
        if count > self.top[-1][1]:
            self.top[-1] = (item, count)
            self.top.sort(key=lambda x: x[1], reverse=True)

    def get_top(self):
        return self.top

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                toped = self.get_top()
                logger.info(f"Top {self.top_lenght}: {toped}")

                self._send_rows(header, toped, self.output_rk) #envio el top

                self._forward_eof(header, "TopRevenue",routing_keys=self.output_rk)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            for row in rows:
                item = row.get("item_name")       # clave del row (ejemplo)
                count = row.get("profit_sum", 1)     # cantidad (ejemplo)
                if item is not None and count is not None:
                    self.top.update(item, count)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            # self._send_rows(header, toped, routing_keys=out_rk)

        except Exception as e:
            logger.error(f"TopRevenueItems error: {e}")



