import logging
import logging
from uuid import uuid4
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header, HeaderError, PayloadError
from communication.protocol.schemas import RAW_SCHEMAS

logger = logging.getLogger("filter")


class Filter:
    def __init__(self, mw_filter, mw_join, result_mw, next_worker, filter_type='Year'):
        self.next_worker = next_worker
        self.type = filter_type
        self.mw_filter = mw_filter
        self.mw_join = mw_join
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
            self.mw_filter.close()
            self.result_mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexion: {e}")

    def start_filter(self):
        if self.type == 'Amount':
            self.mw_filter.start_consuming(self.callback_filter_amount, queues=["filters_amount"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        elif self.type == 'Year':
            self.mw_filter.start_consuming(self.callback_filter_year, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        elif self.type == 'Hour':
            self.mw_filter.start_consuming(self.callback_filter_hour, queues=["filters_hour"])
        else:
            logger.error(f"Non valid filter type: {self.type}")


    def callback_filter_year(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            is_eof = self.handle_EOF(body, header)
            if not is_eof:

                for row in rows:
                    year = int(row["created_at"].split("-")[0])
                    if year in [2024, 2025]:
                        filtered_rows.append(row)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.send_to_next_step(filtered_rows, header)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
    
    def callback_filter_amount(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            is_eof = self.handle_EOF(body, header)
            if not is_eof:

                for row in rows:
                    final_amount = float(row["final_amount"]) if row["final_amount"] else 0.0
                    if final_amount > 75:
                        # almaceno fila del batch
                        filtered_rows.append(row)
                        
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.send_to_next_step(filtered_rows, header)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

    
    def callback_filter_hour(self, ch, method, properties, body):
        filtered_rows = []
        try:
            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            is_eof = self.handle_EOF(body, header)
            if not is_eof:
            
                for row in rows:
                    hour = int(row["created_at"].split(" ")[1].split(":")[0])
                    if hour >= 6 and hour < 23:
                        filtered_rows.append(row)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.send_to_next_step(filtered_rows, header)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")



    def create_header(self, header):
        result_header = Header({
                    "message_type": header.fields["message_type"],
                    "query_id": header.fields["query_id"],
                    "stage": self.type,
                    "part": header.fields["part"],
                    "seq": header.fields["seq"],
                    "schema": header.fields["schema"],
                    "source": header.fields["source"],
                    # esto seria el paso en el que estoy, entonces le sumo 1 para el prox
                    "stage_number": header.fields["stage_number"]+1, 
                    # estos son todos los workers por los que debo pasar secuencialmente
                    "stages": header.fields["stages"], # [filer_year, filter_hour, join_1, result]
                })
        return result_header

    def send_to_next_step(self, filtered_rows, header):
         if filtered_rows:
            try:
                # construir nuevo header
                result_header = self.create_header(header)
                # serializar correctamente
                schema_name = header.fields["schema"]
                schema_fields = RAW_SCHEMAS[schema_name]
                csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)

                mw, route_key = self.get_exchange(result_header)
                
                self.mw.send(csv_bytes, route_key=route_key)
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")

    def get_exchange(self, header):
        "stage_number": header.fields["stage_number"]+1, 
        "stages": header.fields["stages"], # [filer_year, filter_hour, join_1, result]
        stage_number =  int(header.fields["stages_number"])
        # tomo el proximo stage
        next_stage =  header.fields["stages"][stage_number]
        if next_stage == 'filter'
            return self.mw_filter, next_stage
        if next_stage == 'join'
            return self.mw_join, next_stage

    def handle_EOF(self, body, header):
        if header.fields.get("message_type") == "EOF":
            logger.info("All messages recieved. Sending EOF...")
            self.result_mw.send(body, route_key=self.next_worker)
            logger.info("Mensaje EOF reenviado al siguiente paso")
            return True
        return False

 