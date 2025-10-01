import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
from communication.protocol.schemas import CLEAN_SCHEMAS

logger = logging.getLogger("top")


class Top:
    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []
        self.top_lenght = 3
        self.top = []

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_top()
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            self.mw.stop_consuming()
            self._close_mw()
        except Exception as e:
            logger.error(f"Error durante consumo de mensajes: {e}")
            self._close_mw()

    def _close_mw(self):
        try:
            self.mw.close()
            self.result_mw.close()
        except Exception as e:
            logger.warning(f"No se pudo cerrar correctamente la conexion: {e}")

    def start_top(self):
        # """Método abstracto, lo redefine cada hijo."""
        raise NotImplementedError

    def define_schema(self, header):
        try:
            schema = header.fields["schema"]
            raw_fieldnames = schema.strip()[1:-1]

            # dividir por coma
            parts = raw_fieldnames.split(",")

            # limpiar cada valor (sacar comillas simples/dobles y espacios extra)
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")


    def _send_rows(self, header, rows, routing_keys, query_id=None):
        if not rows:
            return
        try:
            schema = self.define_schema(header)
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": query_id if query_id is not None else header.fields["query_id"],
                "stage": "Top",
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": schema,
                "source": header.fields["source"],
            })
            payload = serialize_message(out_header, rows, schema)
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  #caso fanout; aca nos aplica para el hours
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, payload)
        except Exception as e:
            logger.error(f"Error enviando resultado: {e}")
    
    def _forward_eof(self, header, stage, routing_keys=None, query_id=None):
        try:
            out_header = Header({
                "message_type": header.fields["message_type"],
                "query_id": query_id if query_id is not None else header.fields["query_id"],
                "stage": stage,
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": header.fields["schema"],
                "source": header.fields["source"],
            })
            eof_payload = serialize_message(out_header, [], header.fields["schema"])
            rks = self.output_rk if routing_keys is None else routing_keys
            if not rks:  # fanout
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                for rk in rks:
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")

# ----------------- SUBCLASES -----------------

class TopSellingItems(Top):
    def start_top(self):
        self.mw.start_consuming(self.callback)

    def update(self, ym, item, count):
        # Caso: aún no llegué a n elementos
        if len(self.top) < self.top_lenght:
            self.top.append((ym, item, count))
            # hago el sort con el numero 2 porque es donde se encuentra el count
            self.top.sort(key=lambda x: x[2], reverse=True)
            return

        # Caso: lista llena, comparo con el minimo (ultimo elemento)
        if count > self.top[-1][1]:
            self.top[-1] = (ym, item, count)
            # hago el sort con el numero 2 porque es donde se encuentra el count
            self.top.sort(key=lambda x: x[2], reverse=True)

    def get_top(self):
        return self.top
    
    def to_csv(self, toped):
        """Convierte el top a formato CSV en un string"""
        result_rows = [
                {"year_month_created_at": ym, "item_name": item, "selling_qty": count}
                for ym, item, count in toped
                ]
        return result_rows

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            logger.info(f"recibo rows")

            if header.fields.get("message_type") == "EOF":
                toped = self.get_top()
                result_rows = self.to_csv(toped)
                header.fields["schema"] = str(["year_month_created_at", "item_name", "selling_qty"])
                header.fields["stage"] = "TopSellingItems"
                logger.info(f"RESULT ROWS: {result_rows}")
                self._send_rows(header, result_rows, self.output_rk, self.output_rk[0])
                self._forward_eof(header, "TopSellingItems", self.output_rk, self.output_rk[0])

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            for row in rows:
                ym = row.get("year_month_created_at")       
                item = row.get("item_name")       
                count = row.get("selling_qty", 1)     
                #logger.info(f"row {item}: {count}")
                if ym is not None and item is not None and count is not None:
                    self.update(ym, item, count)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"TopSellingItems error: {e}")


class TopRevenueGeneratingItems(Top):
    def start_top(self):
        self.mw.start_consuming(self.callback)

    def update(self, ym, item, suma):
        # Caso: aún no llegué a n elementos
        if len(self.top) < self.top_lenght:
            self.top.append((ym, item, suma))
            # hago el sort con el numero 2 porque es donde se encuentra el suma
            self.top.sort(key=lambda x: x[2], reverse=True)
            return

        # Caso: lista llena, comparo con el minimo (ultimo elemento)
        if suma > self.top[-1][1]:
            self.top[-1] = (ym, item, suma)
            # hago el sort con el numero 2 porque es donde se encuentra el suma
            self.top.sort(key=lambda x: x[2], reverse=True)

    def get_top(self):
        return self.top
    
    def to_csv(self, toped):
        """Convierte el top a formato CSV en un string"""
        result_rows = [
                {"year_month_created_at": ym, "item_name": item, "profit_sum": sum}
                for ym, item, sum in toped
                ]
        return result_rows

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            logger.info(f"recibo rows")

            if header.fields.get("message_type") == "EOF":
                toped = self.get_top()
                result_rows = self.to_csv(toped)
                header.fields["schema"] = str(["year_month_created_at", "item_name", "profit_sum"])
                header.fields["stage"] = "TopRevenueGeneratingItems"
                logger.info(f"RESULT ROWS: {result_rows}")
                self._send_rows(header, result_rows, self.output_rk, self.output_rk[0])
                self._forward_eof(header, "TopRevenueGeneratingItems", self.output_rk, self.output_rk[0])

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            for row in rows:
                ym = row.get("year_month_created_at")       
                item = row.get("item_name")       
                sum = row.get("profit_sum", 1)     
                # logger.info(f"row {item}: {sum}")
                if ym is not None and item is not None and sum is not None:
                    self.update(ym, item, sum)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"TopRevenueGeneratingItems error: {e}")



class TopStoreUserPurchases(Top):
    def start_top(self):
        self.mw.start_consuming(self.callback)

    def update(self, ym, item, suma):
        # Caso: aún no llegué a n elementos
        if len(self.top) < self.top_lenght:
            self.top.append((ym, item, suma))
            # hago el sort con el numero 2 porque es donde se encuentra el suma
            logger.info(f"Inserto directo porque no llegue al limite del top")
            self.top.sort(key=lambda x: x[2], reverse=True)
            return

        # Caso: lista llena, comparo con el minimo (ultimo elemento)
        if suma > self.top[-1][1]:
            self.top[-1] = (ym, item, suma)
            logger.info(f"Inserto y ordeno")
            # hago el sort con el numero 2 porque es donde se encuentra el suma
            self.top.sort(key=lambda x: x[2], reverse=True)

    def get_top(self):
        return self.top
    
    def to_csv(self, toped):
        """Convierte el top a formato CSV en un string"""
        result_rows = [
                {"store_name": ym, "user_id": item, "user_purchases": sum}
                for ym, item, sum in toped
                ]
        return result_rows

    def callback(self, ch, method, properties, body):
        try:
            header, rows = deserialize_message(body)
            message_schema = header.fields.get("schema")

            logger.info(f"Schema recibido: {message_schema}")

            # ["store_name", "user_id", "user_purchases"]

            if header.fields.get("message_type") == "EOF":
                logger.info(f"Proceso EOF")
                toped = self.get_top()
                logger.info(f"Obtengo el top {toped}")
                result_rows = self.to_csv(toped)
                logger.info(f"Lo converto a csb {result_rows}")
                header.fields["schema"] = str(["store_name", "user_id", "user_purchases"])
                logger.info(f"cambio schema")
                header.fields["stage"] = "TopStoreUserPurchases"
                logger.info(f"cambio stage")
                logger.info(f"RESULT ROWS: {result_rows}")
                self._send_rows(header, result_rows, self.output_rk)
                self._forward_eof(header, "TopStoreUserPurchases", routing_keys=self.output_rk)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            for row in rows:
                store = row.get("store_name")       
                usr = row.get("user_id")       
                user_purchases = row.get("user_purchases", 1)  
                
                   
                if store is not None and usr is not None and user_purchases is not None:
                    logger.info(f"row {usr}: {user_purchases}")
                    self.update(store, usr, user_purchases)
                else:
                    logger.info(f"Fila invalida")


            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"TopStoreUserPurchases error: {e}")



