import logging
import logging
from uuid import uuid4
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header, HeaderError, PayloadError
from communication.protocol.schemas import RAW_SCHEMAS

logger = logging.getLogger("filter")


class Filter:
    def __init__(self, mw, result_mw, next_worker, filter_type='Year'):
        self.next_worker = next_worker
        self.type = filter_type
        self.mw = mw
        self.result_mw = result_mw
    
    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_filter()
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            self.mw.stop_consuming()
            self.close_mw()
        except Exception as e:
            logger.error(f"Error durante consumo de mensajes: {e}")
            self.close_mw()
            # no hay que cerrar la de result?

    def _close_mw(self):
        try:
            self.mw.close()
            self.result_mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexion: {e}")

    def start_filter(self):
        if self.type == 'Amount':
            self.mw.start_consuming(self.callback_filter_amount, queues=["filters_amount"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        elif self.type == 'Year':
            self.mw.start_consuming(self.callback_filter_year, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        elif self.type == 'Hour':
            self.mw.start_consuming(self.callback_filter_hour, queues=["filters_hour"])
        else:
            logger.error(f"QUE PUEDO SABER EU DESA SITUASAUN")


    def callback_filter_year(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            # logger.info(f"Header recibido: {header.as_dictionary()}")

            for row in rows:
                year = int(row["created_at"].split("-")[0])
                if year in [2024, 2025]:
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_to_next_step(filtered_rows, header)
    
    def callback_filter_amount(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            # logger.info(f"Header recibido: {header.as_dictionary()}")

            for row in rows:
                final_amount = float(row["final_amount"]) if row["final_amount"] else 0.0
                if final_amount > 75:
                    # almaceno fila del batch
                    filtered_rows.append(row)


        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_to_next_step(filtered_rows, header)
    
    def callback_filter_hour(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            
            for row in rows:
                hour = int(row["created_at"].split(" ")[1].split(":")[0])
                if hour >= 6 and hour < 23:
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_to_next_step(filtered_rows, header)


    def send_to_next_step(self, filtered_rows, header):
         if filtered_rows:
            try:
                # construir nuevo header
                result_header = Header({
                    "message_type": "DATA",
                    "query_id": header.fields["query_id"],
                    "stage": "FILTERED",
                    "part": "transactions.filtered",
                    "seq": str(uuid4()),
                    "schema": header.fields["schema"],
                    "source": "filter"
                })
                # serializar correctamente
                schema_name = header.fields["schema"]
                schema_fields = RAW_SCHEMAS[schema_name]
                csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)

                self.result_mw.send(csv_bytes, route_key=self.next_worker)
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")