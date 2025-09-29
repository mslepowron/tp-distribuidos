from typing import Dict, List

#Defines the columns expected from raw data files sent from the client
RAW_SCHEMAS : Dict[str, List[str]] = {
    "transactions.raw": [
        "transaction_id", "store_id", "payment_method_id", "voucher_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at"
    ],

    "transaction_items.raw": [
        "transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at"
    ],

    "stores.raw": [
        "store_id", "store_name", "street", "postal_code", "city", "state", "latitude", "longitude"
    ],

    "menu.raw": [
        "item_id", "item_name", "category", "price", "is_seasonal", "available_from", "available_to"
    ],
    
    "users.raw": [
        "user_id", "gender", "birthdate", "registered_at"
    ],
}

#defines the columns of data expected from data files that have been cleaned and filtered of null data.
#Only necessary information for processing remains.
CLEAN_SCHEMAS: Dict[str, List[str]] = {
    "transactions.clean": [
        "transaction_id", "store_id", "user_id", "final_amount", "created_at"
    ],

    "transaction_items.clean": [
        "transaction_id", "item_id", "subtotal", "created_at"
    ],

    "stores.clean": [
        "store_id", "store_name"
    ],

    "menu_items.clean": [
        "item_id", "item_name"
    ],

    "users.clean": [
        "user_id", "birthdate"
    ],
}

#All schemas
SCHEMAS: Dict[str, List[str]] = {**RAW_SCHEMAS, **CLEAN_SCHEMAS,}
