import time
import logging
import pika

PORT = 15672

from ..middleware import (
    MessageMiddleware,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareExchange,
)

logger = logging.getLogger("filter")

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._consuming = False

        max_retries = 5
        delay = 3

        for attempt in range(1, max_retries + 1):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=5672,
                        blocked_connection_timeout=30,
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                logger.info(f"Connected to RabbitMQ at {self.host}:{5672}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Attempt {attempt}: Could not connect: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise MessageMiddlewareDisconnectedError(str(e))
    def start_consuming(self, on_message_callback):
        try:
            self._consuming = True
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        if self._consuming:
            self.channel.stop_consuming()
            self._consuming = False

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),  # delivery mode 2 indica que el mensaje es persistente
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e))
