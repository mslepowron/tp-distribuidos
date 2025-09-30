import pytest
import json
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from conftest import sample_transaction_message, sample_menu_item_message

import threading

# QUEUE 1-1
def test_queue_1_to_1(rabbit_host):
    received = []

    def callback(ch, method, properties, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    queue = "test_queue_1to1"
    producer = MessageMiddlewareQueue(rabbit_host, queue)
    consumer = MessageMiddlewareQueue(rabbit_host, queue)

    msg = sample_transaction_message()
    producer.send(msg)
    consumer.start_consuming(callback)

    assert len(received) == 1
    assert json.loads(received[0]) == json.loads(msg)

    producer.close()
    consumer.close()

# QUEUE 1-N
def test_queue_1_to_n(rabbit_host):
    received_1 = []
    received_2 = []

    def callback_1(ch, method, properties, body):
        received_1.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()  # lo paramos cuando le llegua un msj

    def callback_2(ch, method, properties, body):
        received_2.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    queue = "test_queue_1toN"
    producer = MessageMiddlewareQueue(rabbit_host, queue)
    consumer1 = MessageMiddlewareQueue(rabbit_host, queue)
    consumer2 = MessageMiddlewareQueue(rabbit_host, queue)

    msgs = [sample_transaction_message(), sample_transaction_message()]
    for m in msgs:
        producer.send(m)

    # Cada consumidor corre en su propio thread
    t1 = threading.Thread(target=consumer1.start_consuming, args=(callback_1,))
    t2 = threading.Thread(target=consumer2.start_consuming, args=(callback_2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    #cada mensaje llega a uno solo de los consumidores
    assert len(received_1) + len(received_2) == len(msgs)
    assert set(received_1 + received_2) == set(msgs)

    producer.close()
    consumer1.close()
    consumer2.close()

# EXCHANGE 1-1
def test_exchange_1_to_1(rabbit_host):
    received = []

    def callback(ch, method, properties, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    queue = "test_ex_1to1"
    exchange_name = "ex1"
    routing_key = "rk1"

    producer = MessageMiddlewareExchange(rabbit_host, queue, bindings=[(exchange_name, "direct", routing_key)])
    consumer = MessageMiddlewareExchange(rabbit_host, queue, bindings=[(exchange_name, "direct", routing_key)])

    msg = sample_menu_item_message()
    producer.send_to(exchange_name, routing_key, msg)
    consumer.start_consuming(callback)

    assert len(received) == 1
    assert json.loads(received[0]) == json.loads(msg)

    producer.close()
    consumer.close()

# -EXCHANGE 1-N
def test_exchange_1_to_n(rabbit_host):
    received_1 = []
    received_2 = []

    def callback_1(ch, method, properties, body):
        received_1.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if len(received_1) >= 2:
            ch.stop_consuming()

    def callback_2(ch, method, properties, body):
        received_2.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if len(received_2) >= 2:
            ch.stop_consuming()

    exchange_name = "test_ex_1_to_N"
    routing_key = "rkN"

    # Cada consumidor tiene su propia cola vinculada al exchange
    consumer1_queue = "ex_1toN_c1"
    consumer2_queue = "ex_1toN_c2"
    producer_queue = "ex_1toN_producer"

    producer = MessageMiddlewareExchange(rabbit_host, producer_queue)
    consumer1 = MessageMiddlewareExchange(rabbit_host, consumer1_queue, bindings=[(exchange_name, "direct", routing_key)])
    consumer2 = MessageMiddlewareExchange(rabbit_host, consumer2_queue, bindings=[(exchange_name, "direct", routing_key)])

    msgs = [sample_menu_item_message(), sample_menu_item_message()]
    for m in msgs:
        producer.send_to(exchange_name, routing_key, m)

    consumer1.start_consuming(callback_1)
    consumer2.start_consuming(callback_2)

    assert len(received_1) == len(msgs)
    assert len(received_2) == len(msgs)
    assert set(received_1) == set(msgs)
    assert set(received_2) == set(msgs)

    producer.close()
    consumer1.close()
    consumer2.close()
