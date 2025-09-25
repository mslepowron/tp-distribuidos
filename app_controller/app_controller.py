import pika
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_controller")

def connect_rabbitmq(max_retries=5, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="rabbitmq",
                    port=5672
                    # Si configuraste usuario/password: credentials=pika.PlainCredentials("admin","admin")
                )
            )
            logger.info("Connected to RabbitMQ")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Attempt {attempt}: Could not connect: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    raise RuntimeError("Failed to connect to RabbitMQ")

def main():
    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue="coffee_tasks")

    for i in range(5):
        message = f"Mensaje {i} desde app_controller"
        print(f"Produciendo: {message}")
        channel.basic_publish(exchange="", routing_key="coffee_tasks", body=message.encode())

    connection.close()

if __name__ == "__main__":
    main()
