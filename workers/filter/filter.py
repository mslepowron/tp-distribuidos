import logging

from communication.protocol.deserialize import deserialize_batch
from communication.protocol.serialize import serialize_row

logger = logging.getLogger("filter")


class Filter:
    def __init__(self, mw, result_mw, type='Amount'):
        self.type = type
        self.mw = mw
        self.result_mw = result_mw
    
    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_filter()
            self.mw.start_consuming(self.callback_filter_amount, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
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
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexión: {e}")

    def start_filter(self):
        if self.type == 'Amount':
            self.mw.start_consuming(self.callback_filter_amount, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        elif self.type == 'Year':
            self.mw.start_consuming(self.callback_filter_year, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
        else:
            logger.error(f"QUE PUEDO SABER EU DESA SITUASAUN")


    def callback_filter_amount(self, ch, method, properties, body):
        filtered_rows = []
        try:
            #rows = deserialize_batch(body) comento para probar nuevo protocolo

            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            logger.info(f"Header recibido: {header.as_dictionary()}")

            for row in rows:
                original_amount = float(row["original_amount"]) if row["original_amount"] else 0.0
                if original_amount > 75:
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

        logger.info("Imprimo filas")
        for row in filtered_rows:
            logger.info(f"F:{row})")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if filtered_rows:
            try:
                csv_bytes = serialize_row(filtered_rows)
                self.result_mw.send(csv_bytes)
                logger.info(f"Resultado enviado con {len(filtered_rows)} filas")
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")
    
    
    def callback_filter_year(self, ch, method, properties, body):
        filtered_rows = []
        try:
            rows = deserialize_batch(body)

            for row in rows:
                year = int(row["created_at"].split("-")[0])
                if year in [2024, 2025]:
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
        
        logger.info(f"Imprimo filas")
        for row in filtered_rows:
            logger.info(f"F:{row})")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if filtered_rows:
            try:
                csv_bytes = serialize_row(filtered_rows)  
                self.result_mw.send(csv_bytes, route_key="coffee_results") #esta es la key que usa el app controller para bindearse con el exchange de results y consumir resultados
                logger.info(f"Resultado enviado con {len(filtered_rows)} filas")
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")