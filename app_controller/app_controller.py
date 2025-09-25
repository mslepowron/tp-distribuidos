import logging
from middleware.rabbitmq.mom import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

def main():
    try:
        mw = MessageMiddlewareQueue(host="rabbitmq", queue_name="coffee_tasks")
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    for i in range(5):
        message = f"Mensaje {i} desde app_controller"
        try:
            mw.send(message)
            logger.info(f"Mensaje enviado: {message}")
        except Exception as e:
            logger.error(f"No se pudo enviar el mensaje: {e}")
    try:
        mw.close()
    except Exception as e:
        logger.warning(f"No se pudo cerrar correctamente la conexión: {e}")

if __name__ == "__main__":
    main()
