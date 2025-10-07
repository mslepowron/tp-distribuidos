import os
from client import Client

def main():

    host = os.getenv("GATEWAY_HOST", "localhost")
    port = int(os.getenv("GATEWAY_PORT", 9000))
    max_batch_size = int(os.getenv("BATCH_SIZE", 50000))

    client = Client(host, port, max_batch_size)
    client.run()

if __name__ == "__main__":
    main()
