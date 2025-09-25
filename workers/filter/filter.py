import logging
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from communication.protocol.deserialize import deserialize_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("filter")

def main():
    try:
        mw = MessageMiddlewareQueue(host="rabbitmq", queue_name="coffee_tasks")
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    def callback(ch, method, properties, body):
        filtered_rows = []
        try:
            rows = deserialize_batch(body)

            for row in rows:
                original_amount = float(row["original_amount"]) if row["original_amount"] else 0.0

                if original_amount > 75:
                    # almaceno fila del batch
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
        
        logger.info(f"Imprimo filas")
        for row in filtered_rows:
            logger.info(f"F:{row})")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Iniciamos consumo
    try:
        logger.info("Waiting for messages...")
        mw.start_consuming(callback)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        mw.stop_consuming()
        try:
            mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexión: {e}")
    except Exception as e:
        logger.error(f"Error durante consumo de mensajes: {e}")
        try:
            mw.close()
        except:
            pass

if __name__ == "__main__":
    main()
