#!/usr/bin/env python3
from middleware.mom import MessageMiddlewareQueue

def main():
    # Conectamos al mismo host y cola que escucha el filter
    mw = MessageMiddlewareQueue("rabbitmq", "queue1")

    # Mandamos un mensaje de prueba
    mw.send("Hola filter!")

    print("✅ Mensaje enviado a filter")

    mw.close()

if __name__ == "__main__":
    main()
