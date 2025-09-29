import pytest
from pathlib import Path
from communication.protocol.message import Header
from communication.protocol.serialize import serialize_message
from communication.protocol.schemas import CLEAN_SCHEMAS
from ..lib.join import MenuJoin

@pytest.fixture
def tmp_storage(tmp_path):
    return tmp_path

def make_msg(schema, rows, source, message_type="DATA"):
    header = Header({
        "message_type": message_type,
        "query_id": "q_month_top_qty",
        "schema": schema,
        "source": source,
        "stage": "test",
        "part": "1",
        "seq": "1",
    })
    payload = serialize_message(header, rows, CLEAN_SCHEMAS[schema])
    return header, payload

def test_join_after_menu_eof(tmp_storage):
    mw_in = mw_out = DummyMW()
    joiner = MenuJoin(mw_in, mw_out, "out", ["rk"], {}, tmp_storage)

    # enviar menu
    menu_rows = [{"item_id": "1", "item_name": "Latte"}]
    h, body = make_msg("menu_items.clean", menu_rows, "menu_items.clean")
    joiner.callback(None, DummyMethod(), None, body)

    # EOF de menu
    h, body = make_msg("menu_items.clean", [], "menu_items.clean", "EOF")
    joiner.callback(None, DummyMethod(), None, body)

    # ahora llega trx
    trx_rows = [{"transaction_id": "t1", "item_id": "1", "subtotal": "100", "created_at": "2024-10-01 12:00:00"}]
    h, body = make_msg("transaction_items.clean", trx_rows, "transaction_items.clean")
    joiner.callback(None, DummyMethod(), None, body)

    assert len(mw_out.sent) > 0
    out_header, out_payload = mw_out.sent[-1]
    assert b"Latte" in out_payload


class DummyMethod:
    delivery_tag = 1

class DummyMW:
    def __init__(self):
        self.sent = []

    def send_to(self, exchange, routing_key, payload):
        # guarda lo enviado en memoria para inspeccionarlo en el test
        self.sent.append((exchange, routing_key, payload))

    def stop_consuming(self):
        pass

    def close(self):
        pass
