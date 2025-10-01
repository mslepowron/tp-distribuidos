import os
import logging
from client.sender import Sender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("client-main")

def main():
    host = os.getenv("GATEWAY_HOST", "localhost")   
    port = int(os.getenv("GATEWAY_PORT", 9000))    
    batch_size = int(os.getenv("BATCH_SIZE", 5))
    input_dir = os.getenv("RAW_DATA_DIR", ".")

    logger.info(f"Iniciando cliente → Enviando a {host}:{port} desde {input_dir}")

    sender = Sender(host, port, batch_size, input_dir)
    try:
        sender.connect()
        sender.send_batches()
        sender.wait_for_reports()
        logger.info("Todos los reportes recibidos, cliente finalizado")
    finally:
        sender.shutdown()

if __name__ == "__main__":
    main()
