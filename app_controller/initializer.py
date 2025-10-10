import os
import csv
from pathlib import Path
from typing import Iterator, Tuple
from communication.protocol.schemas import RAW_SCHEMAS, CLEAN_SCHEMAS
import logging
logger = logging.getLogger("clean_rows")

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR

PREFIX_MAPPING = {
    "transactions": ("transactions.clean", "transactions.raw"),
    "transaction_items": ("transaction_items.clean", "transaction_items.raw"),
    "stores": ("stores.clean", "stores.raw"),
    "menu_items": ("menu_items.clean", "menu.raw"),
    "users": ("users.clean", "users.raw"),
}

def extract_prefix(source: str) -> str:
    for prefix in PREFIX_MAPPING:
        if source.startswith(prefix):
            return prefix
    raise ValueError(f"No se pudo extraer prefix válido de source: {source}")

def get_schema_columns_by_source(source: str):
    prefix = extract_prefix(source)
    clean_key, raw_key = PREFIX_MAPPING[prefix]
    clean_cols = CLEAN_SCHEMAS[clean_key]
    raw_cols = RAW_SCHEMAS[raw_key]
    return clean_cols, raw_cols

def clean_rows(rows, clean_columns, raw_columns):
    for idx, row in enumerate(rows):
        # Verifica que todas las columnas raw estén presentes
        if not all(col in row for col in raw_columns):
            continue

        cleaned_row = {}
        fila_valida = True
        for col in clean_columns:
            val = row.get(col, None)
            if val is None or val.strip() == "":
                fila_valida = False
                break
            cleaned_row[col] = val.strip()

        if not fila_valida: 
            continue
        yield cleaned_row

def get_routing_key_from_filename(filename: str) -> str:
    filename = filename.lower()
    for prefix in ["transactions", "transaction_items", "stores", "menu_items", "users"]:
        if filename.startswith(prefix):
            return prefix
    raise ValueError(f"No se pudo determinar routing key para filename: {filename}")
