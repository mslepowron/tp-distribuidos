import pytest
import json

def sample_transaction_message():
    return json.dumps({
        "transaction_id": "d866383d-dece-43c0-b96b-b01cffc4ac4c",
        "store_id": "7",
        "user_id": "U123",
        "final_amount": "90.0",
        "created_at": "2024-10-01 11:19:35"
    })

def sample_menu_item_message():
    return json.dumps({
        "item_id": "3",
        "item_name": "Latte"
    })

@pytest.fixture
def rabbit_host():
    return "localhost"
