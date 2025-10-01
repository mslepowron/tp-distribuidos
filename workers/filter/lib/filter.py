import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import CLEAN_SCHEMAS

logger = logging.getLogger("filter")


class Filter:
    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_filter()
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

    def start_filter(self):
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
                "stage": "FilterYear",
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

class YearFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                source = header.fields.get("source", "")
                if source.startswith("transactions"):
                # Propago EOF a ambas RKs para completar el “tipo”
                    routing_keys=["transactions"]
                    self._forward_eof(header, "FilterYear", routing_keys)
                    logger.info(f"EOF for: {source} sent to routing key: {routing_keys}")
                else:
                    routing_keys=["transaction_items"]
                    self._forward_eof(header, "FilterYear", routing_keys)
                    logger.info(f"EOF for: {source} sent to routing key: {routing_keys}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            filtered = []

            for row in rows:
                year = int(row["created_at"].split("-")[0])
                if year in (2024, 2025):
                    filtered.append(row)

            # Determino RK de salida segun cual es el archivo que me llego
            source = header.fields.get("source", "")
            if source.startswith("transaction_items") or method.routing_key.startswith("transaction_items"):
                out_rk = ["transaction_items"]
                logger.info(f"out_rk selected: transaction_items for source: {source}")
            else:
                out_rk = ["transactions"]
                logger.info(f"out_rk selected: transactions for source: {source}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._send_rows(header, filtered, routing_keys=out_rk)

        except Exception as e:
            logger.error(f"YearFilter error: {e}")


class HourFilter(Filter):
    def start_filter(self):
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
            elif header.fields["source"].startswith("store_join"): 
                fieldnames.remove("transaction_id")
           
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                logger.info("Todo enviado")
                self._forward_eof(header, "FilterHour",routing_keys=[])  # fanout
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            filtered = []
            for row in rows:
                hour = int(row["created_at"].split(" ")[1].split(":")[0])
                if 6 <= hour < 23:
                    if header.fields["source"].startswith("transactions"):
                        row.pop("created_at", None)
                        row.pop("store_id", None)
                        row.pop("user_id", None)
                    elif header.fields["source"].startswith("store_join"):
                        row.pop("transaction_id", None)
                        
                    filtered.append(row)

            self._send_rows(header, filtered, routing_keys=[])  # fanout
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"HourFilter error: {e}")

class AmountFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                self._forward_eof(header, "Results", routing_keys=["q_amount_75_trx"])
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

