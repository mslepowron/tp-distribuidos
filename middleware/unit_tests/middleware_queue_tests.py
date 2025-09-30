import threading
import time
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from conftest import sample_transaction_message

def test_queue_1_to_1(rabbit_host):
    received = []

    def callback(ch, method, properties, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    queue = "test_queue_1to1"
    producer = MessageMiddlewareQueue(rabbit_host, queue)
    consumer = MessageMiddlewareQueue(rabbit_host, queue)

    producer.send(sample_transaction_message())
    consumer.start_consuming(callback)

    assert len(received) == 1
    assert "transaction_id" in received[0]

    producer.close()
    consumer.close()

