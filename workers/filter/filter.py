import logging
from middleware.rabbitmq.mom import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("filter")

def main():
    try:
        mw = MessageMiddlewareQueue(host="rabbitmq", queue_name="coffee_tasks")
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    # Callback para procesar los mensajes
    def callback(ch, method, properties, body):
        logger.info(f"Received message: {body.decode()}")
        
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
