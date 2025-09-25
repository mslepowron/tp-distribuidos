import pika
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("filter")

def connect_rabbitmq(max_retries=5, delay=3):
    """Intenta conectarse a RabbitMQ con reintentos."""
    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="rabbitmq",   # 👈 nombre del servicio en docker-compose
                    port=5672,
                    #credentials=pika.PlainCredentials("admin", "admin")
                )
            )
            logger.info("Connected to RabbitMQ")
            return connection
        except Exception as e:
            logger.warning(f"Attempt {attempt}: Could not connect to RabbitMQ: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    raise RuntimeError("Failed to connect to RabbitMQ after several attempts")

def main():
    connection = connect_rabbitmq()
    channel = connection.channel()

    # Aseguramos que la cola exista
    channel.queue_declare(queue="coffee_tasks")

    logger.info("Waiting for messages...")

    def callback(ch, method, properties, body):
        logger.info(f"Received message: {body.decode()}")

    channel.basic_consume(queue="coffee_tasks", on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        channel.stop_consuming()
        connection.close()

if __name__ == "__main__":
    main()
