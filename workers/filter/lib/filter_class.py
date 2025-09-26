import logging
from threading import Thread
from middleware.rabbitmq.mom import MessageMiddlewareQueue
from communication.protocol.deserialize import deserialize_batch
from communication.protocol.serialize import serialize_row

logger = logging.getLogger("filter")
logging.basicConfig(level=logging.INFO)

class FilterConfig:
    def __init__(self, type_: str, id_: int = 0):
        self.type = type_
        self.id = id_

class Filter:
    def __init__(self, config: FilterConfig, host="rabbitmq"):
        self.config = config
        self.host = host
        self.mw = None
        self.result_mw = None
        self.message_window = set() 

        self.connect()

    def connect(self):
        logger.info("Connecting to RabbitMQ...")
        try:
            self.mw = MessageMiddlewareQueue(host=self.host, queue_name="coffee_tasks")
            self.result_mw = MessageMiddlewareQueue(host=self.host, queue_name="coffee_results")
        except Exception as e:
            logger.fatal(f"Could not connect to RabbitMQ: {e}")
            raise
        logger.info("Connected successfully")

    def start(self):
        logger.info(f"Starting filter of type: {self.config.type}")
        if self.config.type == "filter":
            self.process_filter_amount()
        else:
            logger.fatal(f"Unknown filter type: {self.config.type}")

    def ack(self, msg):
        self.mw.ack(msg)

    def process_filter_amount(self):
        def callback(ch, method, properties, body):
            filtered_rows = []
            try:
                rows = deserialize_batch(body)

                for row in rows:
                    original_amount = float(row.get("original_amount", 0) or 0.0)

                    if original_amount > 75:
                        filtered_rows.append(row)

            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")

            logger.info(f"Imprimo filas")
            for row in filtered_rows:
                logger.info(f"F:{row}")

            # ack del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)

            if filtered_rows:
                try:
                    csv_bytes = serialize_row(filtered_rows)
                    self.result_mw.send(csv_bytes)
                    logger.info(f"Resultado enviado con {len(filtered_rows)} filas")
                except Exception as e:
                    logger.error(f"Error enviando resultado: {e}")

if __name__ == "__main__":
    config = FilterConfig(type_="filter", id_=1)
    filter_worker = Filter(config)
    filter_worker.start()
