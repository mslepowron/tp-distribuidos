import logging
from communication.protocol.deserialize import deserialize_batch
from communication.protocol.serialize import serialize_row

logger = logging.getLogger("filter")

class Filter:
    def __init__(self, mw, result_mw):
        self.mw = mw
        self.result_mw = result_mw


    def callback(self, ch, method, properties, body):
        filtered_rows = []
        try:
            rows = deserialize_batch(body)

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

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.mw.start_consuming(self.callback)
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
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexión: {e}")
