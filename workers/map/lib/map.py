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

    def _serialize_rows(self, header, rows):
        """
        Serializa las filas usando el schema que venga en el header.
        Funciona tanto para .raw como para .clean porque usa SCHEMAS.
        """
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
                logger.info(f"ENTRA AL FANOUT")
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                logger.info(f"ENTRA AL ROUT CON KEY")
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")
        

# ----------------- SUBCLASES -----------------

class MapYearMonth(Map):
    def start_filter(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                # Propago EOF a ambas RKs para completar el “tipo”
                self._forward_eof(header, routing_keys=self.output_rk)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            mapped = []

            for row in rows:
                try:
                    date_part = row["created_at"].split(" ")[0]   # 2024-10-01
                    year_month = "-".join(date_part.split("-")[:2])  # [2024,10] -> 2024-10

                    row["year_month_created_at"] = year_month
                    mapped.append(row)
                except Exception as e:
                    logger.warning(f"No se pudo mapear fila {row}: {e}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._send_rows(header, mapped, routing_keys=self.output_rk)

        except Exception as e:
            logger.error(f"{self.__class__.__name__} error: {e}")


class MapYearHalf(Map):
    def start_filter(self):
        self.mw.start_consuming(self.callback)

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                # Propago EOF a ambas RKs para completar el “tipo”
                self._forward_eof(header, routing_keys=self.output_rk)
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

