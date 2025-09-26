import logging
from uuid import uuid4
from middleware.rabbitmq.mom import MessageMiddlewareQueue, MessageMiddlewareExchange
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header, HeaderError, PayloadError
from communication.protocol.schemas import RAW_SCHEMAS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("filter")

def main():
    try:
        mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name="filters",
            exchange_type="direct",
            route_keys=["filters_year", "filters_hour", "filters_amount"]  # creo que puedo declarar todas pero consumimos solo "year"
        )
        #result_mw = mw #este year filter se lo pasa al hour filter. Publicamos los resultados en el mismo exchange
        
        result_mw = MessageMiddlewareExchange(
            host="rabbitmq",
            exchange_name="results",
            exchange_type="direct",
            route_keys=["coffee_results"]  # el app_controller está bindeado a esto
        )
    except Exception as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        return

    def callback(ch, method, properties, body):
        filtered_rows = []
        try:
            #rows = deserialize_batch(body) comento para probar nuevo protocolo

            header, rows = deserialize_message(body, RAW_SCHEMAS["transactions.raw"])
            logger.info(f"Header recibido: {header.as_dictionary()}")


            for row in rows:
                #original_amount = float(row["original_amount"]) if row["original_amount"] else 0.0
                year = int(row["created_at"].split("-")[0])
                if year in [2024, 2025]:
                # if original_amount > 75:
                    # almaceno fila del batch
                    filtered_rows.append(row)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
        
        logger.info(f"Imprimo filas")
        for row in filtered_rows:
            logger.info(f"F:{row})")

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if filtered_rows:
            try:
                # construir nuevo header
                result_header = Header({
                    "message_type": "DATA",
                    "query_id": header.fields["query_id"],
                    "stage": "FILTERED",
                    "part": "transactions.filtered",
                    "seq": str(uuid4()),
                    "schema": header.fields["schema"],
                    "source": "filter"
                })
                # serializar correctamente
                schema_name = header.fields["schema"]
                schema_fields = RAW_SCHEMAS[schema_name]
                csv_bytes = serialize_message(result_header, filtered_rows, schema_fields)

                result_mw.send(csv_bytes, route_key="coffee_results") #esta es la key que usa el app controller para bindearse con el exchange de results y consumir resultados
                #result_mw.send(csv_bytes, route_key="year") #esto publicaria directamente con routing a la cola del filter hours
                logger.info(f"Resultado enviado con {len(filtered_rows)} filas")
            except Exception as e:
                logger.error(f"Error enviando resultado: {e}")


    # Iniciamos consumo
    try:
        logger.info("Waiting for messages...")
        mw.start_consuming(callback, queues=["filters_year"]) #Esta cola es especifa de este filter. Pero como hay un exchange no se si hay que nombrar la cola
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        mw.stop_consuming()
        try:
            mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexión: {e}")
    except Exception as e:
        logger.error(f"Error durante consumo de mensajes: {e}")
        try:
            mw.close()
        except:
            pass

if __name__ == "__main__":
    main()
