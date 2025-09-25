import time
import logging
import pika

from ..middleware import (
    MessageMiddleware,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)

MAX_RETRIES = 5
DELAY = 5

AMQP_PORT = 5672

logger = logging.getLogger("queue")

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._consuming = False


        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=AMQP_PORT,
                        blocked_connection_timeout=30,
                    )
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                logger.info(f"Connected to RabbitMQ at {self.host}:{5672}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Attempt {attempt}: Could not connect: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"Retrying in {DELAY} seconds...")
                    time.sleep(DELAY
                    )
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
                properties=pika.BasicProperties(delivery_mode=2),  #TODO: Chequear bien este delivery mode de persistencia
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

logger = logging.getLogger("exchange")

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, exchange_type="direct", route_keys=None):
        self.host = host
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.route_keys = route_keys or []
        self.connection = None
        self.channel = None
        self._consuming = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=AMQP_PORT,
                        blocked_connection_timeout=30,
                    )
                )

                self.channel = self.connection.channel()

                self.channel.exchange_declare(exchange=self.exchange_name, 
                                              exchange_type=self.exchange_type, 
                                              durable=True)
                
                for rk in self.route_keys:
                    queue_name = f"{self.exchange_name}_{rk}"
                    self.channel.queue_declare(queue=queue_name, durable=True)
                    self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=rk)
                
                logger.info(f"Connected to RabbitMQ exchange {self.exchange_name} at {self.host}:{5672}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Attempt {attempt}: Could not connect: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"Retrying in {DELAY} seconds...")
                    time.sleep(DELAY)
                else:
                    raise MessageMiddlewareDisconnectedError(str(e))
    
    
    def start_consuming(self, on_message_callback):
        try:
            self._consuming = True
            for rk in self.route_keys:
                queue_name = f"{self.exchange_name}_{rk}"
                self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=on_message_callback,
                    auto_ack=False
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
    
    def send(self, message, route_key = None):
        try:
            routing_key = route_key or (self.route_keys[0] if self.route_keys else "")
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2), #TODO: Chequear bien este delivery mode
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
            for rk in self.route_keys:
                queue_name = f"{self.exchange_name}_{rk}"
                self.channel.queue_delete(queue=queue_name)
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e))