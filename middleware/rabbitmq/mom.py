import time
import logging
import pika
from typing import List, Tuple, Optional

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
INITIAL_DELAY = 1  # segundos (1, 2, 4, 8, 16...)

Binding = Tuple[str, str, str] #Info de (exchange, exchange?type, routing_key)

logger = logging.getLogger("queue")

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name, durable: bool = False, auto_delete: bool = True, exclusive: bool = False,):
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
                self.channel.queue_declare(queue=self.queue_name, durable=durable, auto_delete=auto_delete, exclusive=exclusive)
                logger.info(f"Connected to RabbitMQ at {self.host}:{AMQP_PORT}")
                logger.info(f"Queue declared: {self.queue_name} (durable={durable}, auto_delete={auto_delete}, exclusive={exclusive})")
                break
            except pika.exceptions.AMQPConnectionError as e:
                delay = INITIAL_DELAY * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}: Could not connect to {self.host}:{AMQP_PORT} - {e}")
                if attempt < MAX_RETRIES:
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
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e))

logger = logging.getLogger("exchange")

class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, queue_name, bindings: Optional[List[Binding]] = None, queue_durable: bool = False,
        queue_auto_delete: bool = True, exchange_durable_default: bool = False,
        exchange_auto_delete_default: bool = True,):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._consuming = False

        self._ex_default_durable = exchange_durable_default
        self._ex_default_auto_delete = exchange_auto_delete_default

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
                self.channel.queue_declare(queue=self.queue_name, durable=queue_durable,
                    auto_delete=queue_auto_delete)

                def _on_return(ch, method, properties, body):
                    logger.error(f"UNROUTABLE: exchange={method.exchange} rk={method.routing_key} len={len(body)}")

                self.channel.add_on_return_callback(_on_return)

                if bindings:
                    for ex_name, ex_type, r_key in bindings:
                        self.ensure_exchange(ex_name, ex_type)
                        self.bind_to_exchange(ex_name, ex_type, r_key)

                logger.info(f"Connected to RabbitMQ at {self.host}:{AMQP_PORT} (queue={self.queue_name})")
                break
            except pika.exceptions.AMQPConnectionError as e:
                delay = INITIAL_DELAY * (2 ** (attempt - 1))
                logger.warning(f"Attempt {attempt}: Could not connect to {self.host}:{AMQP_PORT} - {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise MessageMiddlewareDisconnectedError(str(e))

    #asegurarse de ya tener el exchange
    def ensure_exchange(self, exchange_name: str, exchange_type: str = "direct", durable: Optional[bool] = None,
        auto_delete: Optional[bool] = None):
        if durable is None:
            durable = self._ex_default_durable
        if auto_delete is None:
            auto_delete = self._ex_default_auto_delete
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
        )
        logger.info(f"ensure_exchange: name={exchange_name} type={exchange_type} durable={durable} auto_delete={auto_delete}")

    
    def bind_to_exchange(self, exchange_name, exchange_type: str = "direct", routing_key: str = ""):
        """Binding queue to a certain exchange with a given routing key"""
        # self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=False)
        # self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=routing_key)    
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=routing_key)

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
        """publish to default exchange"""
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
    
    def send_to(self, exchange: str, routing_key: str, message):
        """Publish to a specific exchange w/routing key - for direct exchange"""
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                mandatory=True, #Para ver sihay bindings o no
                properties=pika.BasicProperties(delivery_mode=1),
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