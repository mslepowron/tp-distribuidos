import pika

# import mom from "middleware/rabbitmq"
from middleware.rabbitmq.mom import MessageMiddlewareQueue

def on_message(ch, method, properties, body):
    print(f"Filter recibio: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # confirmamos que el mensaje fue procesado

def main():
    mw = MessageMiddlewareQueue("rabbitmq", "queue1")

    print("Filter conectado a RabbitMQ, esperando mensajes...")
    mw.start_consuming(on_message)

if __name__ == "__main__":
    main()
