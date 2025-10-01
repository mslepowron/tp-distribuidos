import threading
import time
from middleware.rabbitmq import MessageMiddlewareQueue
from config_tests import sample_transaction_message

def test_queue_1_to_1(rabbit_host):
    received = []

    def callback(ch, method, properties, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    queue = "test_queue_1to1"
    producer = MessageMiddlewareQueue(rabbit_host, queue)
    consumer = MessageMiddlewareQueue(rabbit_host, queue)

    t = threading.Thread(target=consumer.start_consuming, args=(callback,))
    t.start()

    time.sleep(0.5)
    producer.send(sample_transaction_message())

    time.sleep(1)
    consumer.stop_consuming()
    t.join()

    assert len(received) == 1
    assert "transaction_id" in received[0]

    producer.close()
    consumer.close()
