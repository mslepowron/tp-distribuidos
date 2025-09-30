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

    assert len(received) == 1 #Se fija que le haya llegado 1 mensaje (el unico q se mando)
    assert json.loads(received[0]) == json.loads(msg)#se fija que el msj que llego es igual al enviado

    producer.close()
    consumer.close()


# QUEUE 1-N. Se entrega un msj por consumidor
def test_queue_1_to_n(rabbit_host):
    received_1 = []
    received_2 = []

    def callback_1(ch, method, properties, body):
        received_1.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()  # lo paramos cuando le llega un msj

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

    # Cada consumidor corre en su propio thread porque start_consuming es bloqueante
    t1 = threading.Thread(target=consumer1.start_consuming, args=(callback_1,))
    t2 = threading.Thread(target=consumer2.start_consuming, args=(callback_2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Cada mensaje llega a uno solo de los consumidores en RR
    assert len(received_1) + len(received_2) == len(msgs) #no se perdieron mensajes
    assert set(received_1 + received_2) == set(msgs) # no se duplicaron mensajes y le llego uno a cada uno

    producer.close()
    consumer1.close()
    consumer2.close()


# QUEUE: dos productores, un consumidor (N-1).le tiene que llegar mensajes de los dos
#crea un set de mensajes para cada prod y se los manda a un consumer. El consumer
#los va sacando de su cola. Le tiene que llegar correctamente la cantidad de mensjaes
#mandados por cada prod y que el conj de mensajes sea el que se espera
def test_queue_two_producers_single_consumer(rabbit_host):
    queue = "q_two_prod"

    producer1 = MessageMiddlewareQueue(rabbit_host, queue)
    producer2 = MessageMiddlewareQueue(rabbit_host, queue)

    producer1_amount_msgs_sent= 8
    producer2_amount_msgs_sent = 7
    
    base_tx = json.loads(sample_transaction_message())
    msgs_producer1 = []
    for i in range(producer1_amount_msgs_sent):
        m = base_tx.copy()
        m["transaction_id"] = f"producer1-{i}"
        msgs_producer1.append(json.dumps(m))
    msgs_producer2 = []
    for i in range(producer2_amount_msgs_sent):
        m = base_tx.copy()
        m["transaction_id"] = f"producer2-{i}"
        msgs_producer2.append(json.dumps(m))

    expected = set(msgs_producer1 + msgs_producer2)

    received = []
    consumer = MessageMiddlewareQueue(rabbit_host, queue)

    def callback(ch, method, props, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if len(received) >= len(expected):
            ch.stop_consuming()

    t = threading.Thread(target=consumer.start_consuming, args=(callback,))
    t.start()

    for m in msgs_producer1:
        producer1.send(m)
    for m in msgs_producer2:
        producer2.send(m)

    t.join(timeout=5.0)

    assert set(received) == expected
    assert len(received) == len(expected)

    producer1.close()
    producer2.close()
    consumer.close()


# EXCHANGE 1-1 (routing direct)
# Un solo consumidor espera mensajes del exchange con una routing key especifica
def test_exchange_1_to_1(rabbit_host):
    received = []

    def callback(ch, method, properties, body):
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    queue = "test_ex_1to1"
    exchange_name = "exchange_1to1"
    routing_key = "ex_1_to_1_key"

    producer = MessageMiddlewareExchange(rabbit_host, queue, bindings=[(exchange_name, "direct", routing_key)])
    consumer = MessageMiddlewareExchange(rabbit_host, queue, bindings=[(exchange_name, "direct", routing_key)])

    msg = sample_menu_item_message()
    producer.send_to(exchange_name, routing_key, msg)
    consumer.start_consuming(callback)

    assert len(received) == 1
    assert json.loads(received[0]) == json.loads(msg)

    producer.close()
    consumer.close()


# EXCHANGE 1-N (con routing directo)
#un producer y dos consumers que escuchan con la misma key, le tiene que llegar a los dos
def test_exchange_1_to_n_direct_routing_with_same_key(rabbit_host):
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

#Exchange direct con dos consumers con dos rks diferentes.
def test_exchange_direct_two_keys_two_consumers(rabbit_host):
    exchange = "ex_direct_multi_rks"
    rk_menu = "menu"
    rk_trx   = "transactions"

    q_menu = "q_rk_menu"
    q_tx   = "q_rk_trx"
    producer_q = "q_rk_prod"

    consumer_menu = MessageMiddlewareExchange(
        rabbit_host, q_menu,
        bindings=[(exchange, "direct", rk_menu)]
    )
    consumer_tx = MessageMiddlewareExchange(
        rabbit_host, q_tx,
        bindings=[(exchange, "direct", rk_trx)]
    )

    producer = MessageMiddlewareExchange(rabbit_host, producer_q)
    producer.ensure_exchange(exchange, "direct")

    m_menu = sample_menu_item_message()
    m_tx   = sample_transaction_message()

    producer.send_to(exchange, rk_menu, m_menu)
    producer.send_to(exchange, rk_trx,   m_tx)

    rec_menu, rec_tx = [], []

    def callback_menu(ch, method, props, body):
        rec_menu.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    def callback_tx(ch, method, props, body):
        rec_tx.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    t1 = threading.Thread(target=consumer_menu.start_consuming, args=(callback_menu,))
    t2 = threading.Thread(target=consumer_tx.start_consuming,   args=(callback_tx,))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert len(rec_menu) == 1
    assert len(rec_tx)   == 1
    assert json.loads(rec_menu[0]) == json.loads(m_menu)
    assert json.loads(rec_tx[0])   == json.loads(m_tx)


    producer.close()
    consumer_menu.close()
    consumer_tx.close()

# EXCHANGE con routing por topic
# Usa schema de transactions.csv (created_at 2024 vs 2025)
def test_exchange_topic_routing(rabbit_host):
    exchange = "ex_topic_sales"

    q_tx_2024 = "q_tx_2024" #bindeada con una key que escucha trxs de 2024
    q_tx_all  = "q_tx_all" #bindeado con una key que escucha todas las trxs, sin imp el anio

    # bindeamos a q_tx_2024 para que reciba solo claves tipo "transactions.2024.*"
    consumer_2024 = MessageMiddlewareExchange(
        rabbit_host, q_tx_2024,
        bindings=[(exchange, "topic", "transactions.2024.*")]
    )
    # q_tx_all recibe todas las transacciones "transactions.#"
    consumer_all = MessageMiddlewareExchange(
        rabbit_host, q_tx_all,
        bindings=[(exchange, "topic", "transactions.#")]
    )

    producer = MessageMiddlewareExchange(rabbit_host, "producer_tmp")
    producer.ensure_exchange(exchange, "topic")

    # Mensajes válidos según transactions.csv; cambiamos created_at para diferenciar año
    tx_2024 = json.loads(sample_transaction_message())
    tx_2024["created_at"] = "2024-10-01 11:19:35"
    m1 = json.dumps(tx_2024)

    tx_2025 = json.loads(sample_transaction_message())
    tx_2025["created_at"] = "2025-01-15 09:05:10"
    m2 = json.dumps(tx_2025)

    msgs = [("transactions.2024.10", m1), ("transactions.2025.01", m2)]
    for rk, m in msgs:
        producer.send_to(exchange, rk, m)

    rec_2024, rec_all = [], []

    def callback_2024(ch, method, props, body):
        rec_2024.append(body.decode())
        ch.basic_ack(method.delivery_tag)
        if len(rec_2024) >= 1:  #solo el 2024.10
            ch.stop_consuming()

    def callback_all(ch, method, props, body):
        rec_all.append(body.decode())
        ch.basic_ack(method.delivery_tag)
        if len(rec_all) >= 2:
            ch.stop_consuming()

    t1 = threading.Thread(target=consumer_2024.start_consuming, args=(callback_2024,))
    t2 = threading.Thread(target=consumer_all.start_consuming,  args=(callback_all,))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert rec_2024 == [m1] #solo tiene q llegar el 2024.(cualquier hora)
    assert set(rec_all) == {m1, m2} #llegan todas

    producer.close()
    consumer_2024.close()
    consumer_all.close()


# EXCHANGE con routing por fanout a varias colas
#todas las colas vana  recibir todos los mensajes
def test_exchange_fanout(rabbit_host):
    exchange = "ex_fanout"
    q1, q2 = "fanout_q1", "fanout_q2"

    consumer_1 = MessageMiddlewareExchange(rabbit_host, q1, bindings=[(exchange, "fanout", "")])
    consumer_2 = MessageMiddlewareExchange(rabbit_host, q2, bindings=[(exchange, "fanout", "")])

    prod = MessageMiddlewareExchange(rabbit_host, "fanout_prod")
    prod.ensure_exchange(exchange, "fanout")

    base = json.loads(sample_menu_item_message())
    msgs = []
    for i in range(3):
        m = base.copy()
        m["item_id"] = str(i + 1)
        m["item_name"] = f"Item{i+1}"
        msgs.append(json.dumps(m))

    for m in msgs:
        prod.send_to(exchange, "", m)

    result_cons_1, result_cons_2 = [], []

    def callback_cons_1(ch, method, props, body):
        result_cons_1.append(body.decode())
        ch.basic_ack(method.delivery_tag)
        if len(result_cons_1) >= len(msgs):
            ch.stop_consuming()

    def callback_cons_2(ch, method, props, body):
        result_cons_2.append(body.decode())
        ch.basic_ack(method.delivery_tag)
        if len(result_cons_2) >= len(msgs):
            ch.stop_consuming()

    t1 = threading.Thread(target=consumer_1.start_consuming, args=(callback_cons_1,))
    t2 = threading.Thread(target=consumer_2.start_consuming, args=(callback_cons_2,))
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join()

    assert result_cons_1 == msgs
    assert result_cons_2 == msgs

    prod.close()
    consumer_1.close()
    consumer_2.close()


# EXCHANGE con routing direct: una misma cola bindea a varias rks
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

    m1 = sample_menu_item_message()
    m2 = sample_transaction_message()
    producer.send_to(exchange, rk_a, m1)
    producer.send_to(exchange, rk_b, m2)

    received = []

    def cb(ch, method, props, body):
        received.append(body.decode())
        ch.basic_ack(method.delivery_tag)
        if len(received) >= 2:
            ch.stop_consuming()

    t = threading.Thread(target=consumer.start_consuming, args=(cb,))
    t.start(); t.join()

    assert set(received) == {m1, m2}

    producer.close()
    consumer.close()
