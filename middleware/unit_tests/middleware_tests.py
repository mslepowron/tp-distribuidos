import pytest
import uuid
import logging
import json
import time
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

    # Cada consumidor corre en su propio thread porque start consuming es bloqueante
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

def test_exchange_topic_routing(rabbit_host):
    exchange = "ex_topic_sales"

    q_tx_2024 = "q_tx_2024"
    q_tx_all  = "q_tx_all"

    # bindeamos a q_tx_2024 para que reciba solo claves tipo "transactions.2024.*"
    c2024 = MessageMiddlewareExchange(
        rabbit_host, q_tx_2024,
        bindings=[(exchange, "topic", "transactions.2024.*")]
    )
    # q_tx_all recibe todas las transacciones "transactions.#"
    call = MessageMiddlewareExchange(
        rabbit_host, q_tx_all,
        bindings=[(exchange, "topic", "transactions.#")]
    )

    producer = MessageMiddlewareExchange(rabbit_host, "producer_tmp")
    producer.ensure_exchange(exchange, "topic")

    msgs = [
        ("transactions.2024.10", json.dumps({"y":2024,"m":10})),
        ("transactions.2025.01", json.dumps({"y":2025,"m":1})),
    ]
    for rk, m in msgs:
        producer.send_to(exchange, rk, m)

    rec_2024, rec_all = [], []

    def cb_2024(ch, method, props, body):
        rec_2024.append(body.decode())
        if len(rec_2024) >= 1: # solo el 2024.10
            ch.basic_ack(method.delivery_tag)
            ch.stop_consuming()
        else:
            ch.basic_ack(method.delivery_tag)

    def cb_all(ch, method, props, body):
        rec_all.append(body.decode())
        if len(rec_all) >= 2:
            ch.basic_ack(method.delivery_tag)
            ch.stop_consuming()
        else:
            ch.basic_ack(method.delivery_tag)

    t1 = threading.Thread(target=c2024.start_consuming, args=(cb_2024,))
    t2 = threading.Thread(target=call.start_consuming,  args=(cb_all,))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert rec_2024 == [msgs[0][1]] # solo tiene que llegar 2024.*
    assert set(rec_all) == {msgs[0][1], msgs[1][1]} # llegan todas

    producer.close()
    c2024.close()
    call.close()


def test_exchange_fanout_broadcast(rabbit_host):
    exchange = "ex_fanout_broadcast"
    q1, q2 = "fanout_q1", "fanout_q2"

    c1 = MessageMiddlewareExchange(rabbit_host, q1, bindings=[(exchange, "fanout", "")])
    c2 = MessageMiddlewareExchange(rabbit_host, q2, bindings=[(exchange, "fanout", "")])

    prod = MessageMiddlewareExchange(rabbit_host, "fanout_prod")
    prod.ensure_exchange(exchange, "fanout")

    msgs = [json.dumps({"n": i}) for i in range(3)]
    for m in msgs:
        prod.send_to(exchange, "", m)

    r1, r2 = [], []

    def cb1(ch, method, props, body):
        r1.append(body.decode())
        if len(r1) >= len(msgs):
            ch.basic_ack(method.delivery_tag)
            ch.stop_consuming()
        else:
            ch.basic_ack(method.delivery_tag)

    def cb2(ch, method, props, body):
        r2.append(body.decode())
        if len(r2) >= len(msgs):
            ch.basic_ack(method.delivery_tag)
            ch.stop_consuming()
        else:
            ch.basic_ack(method.delivery_tag)

    t1 = threading.Thread(target=c1.start_consuming, args=(cb1,))
    t2 = threading.Thread(target=c2.start_consuming, args=(cb2,))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert r1 == msgs
    assert r2 == msgs

    prod.close()
    c1.close()
    c2.close()

def test_exchange_multiple_bindings_same_queue(rabbit_host):
    exchange = "ex_direct_multi_bind"
    rk_a, rk_b = "menu", "transactions"
    q = "multi_bind_q"

    consumer = MessageMiddlewareExchange(
        rabbit_host, q,
        bindings=[(exchange, "direct", rk_a), (exchange, "direct", rk_b)]
    )
    producer = MessageMiddlewareExchange(rabbit_host, "multi_bind_prod")
    producer.ensure_exchange(exchange, "direct")

    m1 = json.dumps({"type": "menu_item"})
    m2 = json.dumps({"type": "transaction"})
    producer.send_to(exchange, rk_a, m1)
    producer.send_to(exchange, rk_b, m2)

    received = []

    def cb(ch, method, props, body):
        received.append(body.decode())
        if len(received) >= 2:
            ch.basic_ack(method.delivery_tag)
            ch.stop_consuming()
        else:
            ch.basic_ack(method.delivery_tag)

    t = threading.Thread(target=consumer.start_consuming, args=(cb,))
    t.start(); t.join()

    assert set(received) == {m1, m2}

    producer.close()
    consumer.close()