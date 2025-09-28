import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import RAW_SCHEMAS

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
        # raise NotImplementedError
        self.mw.start_consuming(self.callback)

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
        

    # def send_to_next_step(self, filtered_rows, header):
    #     if filtered_rows:
    #         try:
    #             result_header = Header({
    #                 "message_type": header.fields["message_type"],
    #                 "query_id": header.fields["query_id"],
    #                 "stage": self.__class__.__name__,  # Nombre de la clase como stage
    #                 "part": header.fields["part"],
    #                 "seq": header.fields["seq"],
    #                 "schema": header.fields["schema"],
    #                 "source": header.fields["source"]
    #             })
    #             schema_name = header.fields["schema"]
    #             schema_fields = RAW_SCHEMAS[schema_name]
    #             csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)
                
    #             # logica vieja
    #             # self.result_mw.send(csv_bytes, route_key=self.output_rk)

    #             # envio el mensaje a cada una de las routings keys
    #             for rk in self.output_rk:
    #                 self.result_mw.send(csv_bytes, route_key=rk)

    #         except Exception as e:
    #             logger.error(f"Error enviando resultado: {e}")

    # def handle_EOF(self, body, header):
    #     if header.fields.get("message_type") == "EOF":
    #         logger.info("All messages recieved. Sending EOF...")
    #         self.result_mw.send(body, route_key=self.output_rk)
    #         logger.info("Mensaje EOF reenviado al siguiente paso")
    #         return True
    #     return False


# ----------------- SUBCLASES -----------------

class YearFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback) #, queues=self.input_bindings)
        # self.mw.start_consuming(self.callback , queues="filter_year_q")

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)

            if header.fields.get("message_type") == "EOF":
                # Propago EOF a ambas RKs para completar el “tipo”
                self._forward_eof(header, routing_keys=["transactions", "transaction_items"])
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            filtered = []

            for row in rows:
                year = int(row["created_at"].split("-")[0])
                if year in (2024, 2025):
                    filtered.append(row)

            # Determino RK de salida segun cual es el archivo que me llego
            source = header.fields.get("source", "")
            if "transaction_items" in source or method.routing_key.startswith("transaction_items"):
                out_rk = ["transaction_items"]
            else:
                out_rk = ["transactions"]

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._send_rows(header, filtered, routing_keys=out_rk)

        except Exception as e:
            logger.error(f"YearFilter error: {e}")

    # Si es el archivo transaccion lo mando con una routing_key, sino con la otra
    # def send_to_next_step(self, filtered_rows, header):
    #     if filtered_rows:
    #         try:
    #             result_header = Header({
    #                 "message_type": header.fields["message_type"],
    #                 "query_id": header.fields["query_id"],
    #                 "stage": self.__class__.__name__,  # Nombre de la clase como stage
    #                 "part": header.fields["part"],
    #                 "seq": header.fields["seq"],
    #                 "schema": header.fields["schema"],
    #                 "source": header.fields["source"]
    #             })
    #             schema_name = header.fields["schema"]
    #             schema_fields = RAW_SCHEMAS[schema_name]
    #             csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)

    #             if True: #cambiar condicion --> if file == 'Transaction'
    #                 self.result_mw.send(csv_bytes, route_key=self.output_rk[0])
    #             else:
    #                 self.result_mw.send(csv_bytes, route_key=self.output_rk[1])
    #         except Exception as e:
    #             logger.error(f"Error enviando resultado: {e}")


class HourFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback) #, queues=self.input_bindings)

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

class AmountFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback) #, queues=self.input_bindings)

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

