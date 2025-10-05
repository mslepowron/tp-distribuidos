import logging
from communication.protocol.deserialize import deserialize_message
from communication.protocol.serialize import serialize_message
from communication.protocol.message import Header
import csv
from pathlib import Path
from typing import Dict, List, Tuple
import heapq
from collections import defaultdict

logger = logging.getLogger("reduce")

class Reduce:
    REQUIRED_STREAMS: Tuple[str, str] = ()

    def __init__(self, mw_in, mw_out, output_exchange: str, output_rks, input_bindings):
        self.mw = mw_in
        self.result_mw = mw_out
        self.output_exchange = output_exchange
        self.input_bindings = input_bindings
        if isinstance(output_rks, str):
            self.output_rk = [output_rks] if output_rks else []
        else:
            self.output_rk = output_rks or []
        self.schema_out = None
        self.accumulator: dict[tuple[str, str], int] = defaultdict(int)

    def start(self):
        try:
            logger.info("Waiting for messages...")
            self.start_reducer()
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

    def start_reducer(self):
        self.mw.start_consuming(self.callback)

    def define_schema(self, header):
        try:
            schema = header.fields["schema"]
            raw_fieldnames = schema.strip()[1:-1]

            # dividir por coma
            parts = raw_fieldnames.split(",")

            # limpiar cada valor (sacar comillas simples/dobles y espacios extra)
            fieldnames = [p.strip().strip("'").strip('"') for p in parts]
            # logger.info(f"DEFINE SCHEMA fieldnames: {fieldnames}")
            return fieldnames
        except KeyError:
            raise KeyError(f"Schema '{raw_fieldnames}' no encontrado en SCHEMAS")

    def _send_rows(self, header, source, rows, routing_keys, query_id=None):
        if not rows:
            return
        try:
            schema = self.define_schema(header)
            out_header = Header({
                "message_type": "DATA",
                "query_id": query_id if query_id is not None else header.fields["query_id"],
                "stage": header.fields.get("stage"),
                "part": header.fields["part"],
                "seq": header.fields["seq"],
                "schema": schema,
                "source": source,
            })
            payload = serialize_message(out_header, rows, schema)

            if not routing_keys:  #caso fanout; 
                self.result_mw.send_to(self.output_exchange, "", payload)
            else:
                for rk in routing_keys:
                    logger.info(f"Reduced rows sent to: {rk}")
                    self.result_mw.send_to(self.output_exchange, rk, payload)
        except Exception as e:
            logger.error(f"ERROR SEND ROWS. HEADER: {out_header}")
            logger.error(f"ERROR SEND ROWS. PAYLOAD: {payload}")
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
            
            if not routing_keys:  # fanout
                self.result_mw.send_to(self.output_exchange, "", eof_payload)
            else:
                for rk in routing_keys:
                    logger.info(f"SEND EOF sent header: {out_header}")
                    self.result_mw.send_to(self.output_exchange, rk, eof_payload)
        except Exception as e:
            logger.error(f"Error reenviando EOF: {e}")
        

# ----------------- SUBCLASES -----------------

class QuantityReducer(Reduce):

    def callback(self, ch, method, properties, body):
        header, rows = deserialize_message(body)

        # Caso EOF
        if header.fields.get("message_type") == "EOF":
            # Emitir todo lo que se acumuló en memoria
            self._aggregate_and_emit(header)
            self._forward_eof(header, "QuantityReducer", self.output_rk)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Acumular las filas
        for row in rows:
            ym = row.get("year_month_created_at")
            item = row.get("item_name")
            if ym and item:
                self.accumulator[(ym, item)] += 1

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _aggregate_and_emit(self, header):
        """Convierte el diccionario en filas y las envía todas."""
        result_rows = []
        for (ym, item), qty in self.accumulator.items():
            result_rows.append({
                "year_month_created_at": ym,
                "item_name": item,
                "selling_qty": qty
            })

        if result_rows:
            header.fields["schema"] = "['year_month_created_at','item_name','selling_qty']"
            header.fields["stage"] = "ReduceQuantity"
            self._send_rows(header, "quantity_reduce", result_rows, self.output_rk)

        self.accumulator.clear()





class ProfitReducer(Reduce):

    def callback(self, ch, method, properties, body):
        header, rows = deserialize_message(body)

        # Caso EOF
        if header.fields.get("message_type") == "EOF":
            # Emitir todo lo que se acumuló en memoria
            self._aggregate_and_emit(header)
            self._forward_eof(header, "ProfitReducer", self.output_rk)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Acumular las filas
        for row in rows:
            ym = row.get("year_month_created_at")
            item = row.get("item_name")
            subtotal = float(row.get("subtotal") or 0.0)
            if ym and item:
                self.accumulator[(ym, item)] += subtotal

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _aggregate_and_emit(self, header):
        """Convierte el diccionario en filas y las envía todas."""
        result_rows = []
        for (ym, item), value in self.accumulator.items():
            result_rows.append({
                "year_month_created_at": ym,
                "item_name": item,
                "profit_sum": value
            })

        if result_rows:
            header.fields["schema"] = "['year_month_created_at','item_name','profit_sum']"
            header.fields["stage"] = "ReduceProfit"
            self._send_rows(header, "profit_reduce", result_rows, self.output_rk)

        self.accumulator.clear()
    
    

class UserPurchasesReducer(Reduce):
    def callback(self, ch, method, properties, body):
        header, rows = deserialize_message(body)

        # Caso EOF
        if header.fields.get("message_type") == "EOF":
            # Emitir todo lo que se acumuló en memoria
            self._aggregate_and_emit(header)
            self._forward_eof(header, "ReduceUsrPurchases", self.output_rk, self.output_rk[0])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Acumular las filas
        for row in rows:
            ym = row.get("year_month_created_at")
            item = row.get("store_name")
            logger.info(f"ROW: {row}")
            if ym and item:
                logger.info("AGREGO ELEMENTO")
                self.accumulator[(ym, item)] += 1

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _aggregate_and_emit(self, header):
        """Convierte el diccionario en filas y las envía todas."""
        result_rows = []
        for (ym, item), user_purchases in self.accumulator.items():
            result_rows.append({
                "store_name": ym,
                "user_id": item,
                "user_purchases": user_purchases
            })

        if result_rows:
            logger.info("HAY DATA PARA ENVIAR")
            logger.info(f"{result_rows}")
            header.fields["schema"] = "['store_name','user_id','user_purchases']"
            header.fields["stage"] = "ReduceUsrPurchases"
            self._send_rows(header, "user_purchases", result_rows, self.output_rk, self.output_rk[0])
        else:
            logger.info("NO HAY DATA PARA ENVIAR")

        self.accumulator.clear()
    
    
class TpvReducer(Reduce):

    def callback(self, ch, method, properties, body):
        header, rows = deserialize_message(body)

        # Caso EOF
        if header.fields.get("message_type") == "EOF":
            # Emitir todo lo que se acumuló en memoria
            self._aggregate_and_emit(header)
            self._forward_eof(header, "ReduceTPV", self.output_rk, self.output_rk[0])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Acumular las filas
        for row in rows:
            ym = row.get("year_half_created_at")
            item = row.get("store_name")
            final_amount = float(row.get("final_amount") or 0.0)
            logger.info(f"ROW: {row}")
            if ym and item:
                logger.info("AGREGO ELEMENTO")
                self.accumulator[(ym, item)] += final_amount

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _aggregate_and_emit(self, header):
        """Convierte el diccionario en filas y las envía todas."""
        result_rows = []
        for (ym, item), tpv in self.accumulator.items():
            result_rows.append({
                "year_half_created_at": ym,
                "item_name": item,
                "tpv": tpv
            })

        if result_rows:
            logger.info("HAY DATA PARA ENVIAR")
            logger.info(f"{result_rows}")
            header.fields["schema"] = "['year_half_created_at','item_name','tpv']"
            header.fields["stage"] = "ReduceTPV"
            self._send_rows(header, "tpv_reduce", result_rows, self.output_rk, self.output_rk[0])
        else:
            logger.info("NO HAY DATA PARA ENVIAR")

        self.accumulator.clear()
    
    