import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import RAW_SCHEMAS

logger = logging.getLogger("filter")


class Filter:
    def __init__(self, mw, result_mw, input_rk, output_rk):
        self.mw = mw
        self.result_mw = result_mw
        self.input_rk = input_rk
        self.output_rk = output_rk

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
        """Método abstracto, lo redefine cada hijo."""
        raise NotImplementedError

    def send_to_next_step(self, filtered_rows, header):
        if filtered_rows:
            try:
                result_header = Header({
                    "message_type": header.fields["message_type"],
                    "query_id": header.fields["query_id"],
                    "stage": self.__class__.__name__,  # Nombre de la clase como stage
                    "part": header.fields["part"],
                    "seq": header.fields["seq"],
                    "schema": header.fields["schema"],
                    "source": header.fields["source"]
                })
                schema_name = header.fields["schema"]
                schema_fields = RAW_SCHEMAS[schema_name]
                csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)
                
                # logica vieja
                # self.result_mw.send(csv_bytes, route_key=self.output_rk)

                # envio el mensaje a cada una de las routings keys
                for rk in self.output_rk:
                    self.result_mw.send(csv_bytes, route_key=rk)

            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")

    def handle_EOF(self, body, header):
        if header.fields.get("message_type") == "EOF":
            logger.info("All messages recieved. Sending EOF...")
            self.result_mw.send(body, route_key=self.output_rk)
            logger.info("Mensaje EOF reenviado al siguiente paso")
            return True
        return False


# ----------------- SUBCLASES -----------------

class AmountFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback, queues=self.input_rk)

    def callback(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            if not self.handle_EOF(body, header):
                for row in rows:
                    final_amount = float(row["final_amount"]) if row["final_amount"] else 0.0
                    if final_amount > 75:
                        filtered_rows.append(row)
                self.send_to_next_step(filtered_rows, header)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando mensaje en AmountFilter: {e}")


class YearFilter(Filter):
    def start_filter(self):
        self.mw.start_consuming(self.callback, queues=self.input_rk)

    def callback(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            if not self.handle_EOF(body, header):
                for row in rows:
                    year = int(row["created_at"].split("-")[0])
                    if year in [2024, 2025]:
                        filtered_rows.append(row)
                self.send_to_next_step(filtered_rows, header)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando mensaje en YearFilter: {e}")

    # Si es el archivo transaccion lo mando con una routing_key, sino con la otra
    def send_to_next_step(self, filtered_rows, header):
        if filtered_rows:
            try:
                result_header = Header({
                    "message_type": header.fields["message_type"],
                    "query_id": header.fields["query_id"],
                    "stage": self.__class__.__name__,  # Nombre de la clase como stage
                    "part": header.fields["part"],
                    "seq": header.fields["seq"],
                    "schema": header.fields["schema"],
                    "source": header.fields["source"]
                })
                schema_name = header.fields["schema"]
                schema_fields = RAW_SCHEMAS[schema_name]
                csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)

                if True: #cambiar condicion --> if file == 'Transaction'
                    self.result_mw.send(csv_bytes, route_key=self.output_rk[0])
                else:
                    self.result_mw.send(csv_bytes, route_key=self.output_rk[1])
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")


class HourFilter(Filter):
    def __init__(self, mw, result_mw, result_mw2, input_rk, output_rk, output_rk2):
        # llamo al init de Filter con los primeros valores
        super().__init__(mw, result_mw, input_rk, output_rk)
        # guardo los extras propios de HourFilter
        self.result_mw2 = result_mw2
        self.output_rk2 = output_rk2
        
    def start_filter(self):
        self.mw.start_consuming(self.callback, queues=self.input_rk)

    def callback(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            if not self.handle_EOF(body, header):
                for row in rows:
                    hour = int(row["created_at"].split(" ")[1].split(":")[0])
                    if 6 <= hour < 23:
                        filtered_rows.append(row)
                self.send_to_next_step(filtered_rows, header)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando mensaje en HourFilter: {e}")
