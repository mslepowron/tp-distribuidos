import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import RAW_SCHEMAS
from datetime import datetime

logger = logging.getLogger("map")


class Map:
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
            self.start_map()
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

    def start_map(self):
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
                "stage": "Map",
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

class MapYearMonth(Map):
    def start_map(self):
        self.mw.start_consuming(self.callback)

    def define_schema(self, header):
        try:
            schema = header.fields["schema"] 
            raw_fieldnames = schema.strip()[1:-1]
            parts = raw_fieldnames.split(",")

            # limpiar cada valor
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]

            if header.fields["source"].startswith("menu_join"): 
                fieldnames.remove("created_at")
                fieldnames.append("year_month_created_at")
            # elsif:
            logger.info(f"SHEMAAA --->{fieldnames}")
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            if header.fields.get("message_type") == "EOF":
                # Propago EOF a ambas RKs para completar el “tipo”
                self._forward_eof(header, "MapYearMonth", routing_keys=self.output_rk)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            mapped = []

            for row in rows:
                try:
                    date_part = row["created_at"].split(" ")[0]   # 2024-10-01
                    year_month = "-".join(date_part.split("-")[:2])  # [2024,10] -> 2024-10

                    row["year_month_created_at"] = year_month
                    if header.fields["source"].startswith("menu_join"):
                        row.pop("created_at", None)
                    mapped.append(row)
                except Exception as e:
                    logger.warning(f"No se pudo mapear fila {row}: {e}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._send_rows(header, mapped, routing_keys=self.output_rk)

        except Exception as e:
            logger.error(f"{self.__class__.__name__} error: {e}")


class MapYearHalf(Map):
    def start_map(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                # Propago EOF a ambas RKs para completar el “tipo”
                self._forward_eof(header, "MapYearHalf", routing_keys=self.output_rk)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            mapped = []

            for row in rows:
                try:
                    date_part = row["created_at"].split(" ")[0]  
                    year, month, _ = date_part.split("-")        # "2024", "10", "01"
                    half = "H1" if int(month) <= 6 else "H2"

                    row["year_half_created_at"] = f"{year}-{half}"
                    mapped.append(row)
                except Exception as e:
                    logger.warning(f"No se pudo mapear fila {row}: {e}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._send_rows(header, mapped, routing_keys=self.output_rk)

        except Exception as e:
            logger.error(f"{self.__class__.__name__} error: {e}")

