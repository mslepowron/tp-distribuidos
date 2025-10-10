import os
import csv
from pathlib import Path
from typing import Iterator, Dict, Union
from datetime import datetime
from communication.protocol.schemas import RAW_SCHEMAS, CLEAN_SCHEMAS
import logging

logger = logging.getLogger("clean_rows")

PREFIX_MAPPING = {
    "transactions": ("transactions.clean", "transactions.raw"),
    "transaction_items": ("transaction_items.clean", "transaction_items.raw"),
    "stores": ("stores.clean", "stores.raw"),
    "menu_items": ("menu_items.clean", "menu.raw"),
    "users": ("users.clean", "users.raw"),
}

def extract_prefix(source: str) -> str:
    """Extrae el prefijo base del nombre del archivo (e.g., 'transactions')."""
    for prefix in PREFIX_MAPPING:
        if source.startswith(prefix):
            return prefix
    raise ValueError(f"No se pudo extraer prefix válido de source: {source}")

def get_schema_columns_by_source(source: str):
    """Obtiene los nombres de las columnas limpias y crudas según el archivo de origen."""
    prefix = extract_prefix(source)
    clean_key, raw_key = PREFIX_MAPPING[prefix]
    clean_cols = CLEAN_SCHEMAS[clean_key]
    raw_cols = RAW_SCHEMAS[raw_key]
    return clean_cols, raw_cols

def clean_rows(source: str, rows: Iterator[Dict]) -> Iterator[Dict]:
    prefix = extract_prefix(source)
    
    # Mapeo de prefijos a sus funciones de limpieza específicas
    cleaning_functions = {
        "transactions": _clean_transactions_row,
        "transaction_items": _clean_transaction_items_row,
        "users": _clean_users_row,
        "menu_items": _clean_menu_items_row,
        "stores": _clean_stores_row,
    }

    cleaner_func = cleaning_functions.get(prefix)

    if not cleaner_func:
        raise ValueError(f"No hay una función de limpieza para el prefijo: {prefix}")

    for row in rows:
        try:
            # Llama a la función de limpieza correspondiente
            cleaned_row = cleaner_func(row)
            if cleaned_row:
                yield cleaned_row
        except (ValueError, TypeError, KeyError) as e:
            # Si una fila no se puede procesar o le falta una columna, se ignora
            continue

# --- Funciones de limpieza específicas por dataset ---

def _clean_transactions_row(row: Dict) -> Union[Dict, None]:
    cleaned = {
        "transaction_id": str(row["transaction_id"]).strip(),
        "store_id": int(row["store_id"]),
        "final_amount": float(row["final_amount"]),
        "created_at": datetime.fromisoformat(row["created_at"].strip()).isoformat(),
    }

    user_id = row.get("user_id")
    cleaned["user_id"] = int(user_id) if user_id and str(user_id).strip() else None

    if not cleaned["transaction_id"]:
        return None
        
    return cleaned

def _clean_transaction_items_row(row: Dict) -> Union[Dict, None]:
    cleaned = {
        "transaction_id": str(row["transaction_id"]).strip(),
        "item_id": int(row["item_id"]),
        "quantity": int(row["quantity"]),
        "unit_price": float(row["unit_price"]),
        "subtotal": float(row["subtotal"]),
        "created_at": datetime.fromisoformat(row["created_at"].strip()).isoformat(),
    }
    if not cleaned["transaction_id"]:
        return None
    return cleaned

def _clean_users_row(row: Dict) -> Union[Dict, None]:
    cleaned = {
        "user_id": int(row["user_id"]),
        "birthdate": datetime.fromisoformat(row["birthdate"].strip()).strftime('%Y-%m-%d'),
        "registered_at": datetime.fromisoformat(row["registered_at"].strip()).isoformat(),
    }
    return cleaned

def _clean_menu_items_row(row: Dict) -> Union[Dict, None]:
    cleaned = {
        "item_id": int(row["item_id"]),
        "item_name": str(row["item_name"]).strip(),
        "category": str(row["category"]).strip(),
        "price": float(row["price"]),
    }
    if not cleaned["item_name"] or not cleaned["category"]:
        return None
    return cleaned

def _clean_stores_row(row: Dict) -> Union[Dict, None]:
    cleaned = {
        "store_id": int(row["store_id"]),
        "store_name": str(row["store_name"]).strip(),
        "city": str(row["city"]).strip(),
        "state": str(row["state"]).strip(),
    }
    if not cleaned["store_name"] or not cleaned["city"] or not cleaned["state"]:
        return None
    return cleaned

def get_routing_key_from_filename(filename: str) -> str:
    """Determina la routing key basada en el prefijo del nombre de archivo."""
    filename = filename.lower()
    for prefix in ["transactions", "transaction_items", "stores", "menu_items", "users"]:
        if filename.startswith(prefix):
            return prefix
    raise ValueError(f"No se pudo determinar routing key para filename: {filename}")