import pytest
import json
import uuid
import datetime

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
    # Cambiar si no corrés en localhost (ej: WSL2 -> "host.docker.internal")
    return "localhost"

transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
d866383d-dece-43c0-b96b-b01cffc4ac4c,7,3,,,9.0,0.0,90.0,2024-10-01 11:19:34
8b2f3704-6b7c-45f9-a1e0-28627ebbe078,9,3,,140170.0,60.0,0.0,78.0,2024-10-01 11:19:35
4dcbca4f-83f9-47aa-89f5-0e4674975198,8,5,,545270.0,16.0,0.0,16.0,2024-10-01 11:19:36
0c4154f8-b84b-4b46-bc02-e907ed4b3dfc,2,5,,8112.0,344.0,0.0,80.0,2024-10-01 11:19:37