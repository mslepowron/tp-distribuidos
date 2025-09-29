import os
import csv
from pathlib import Path
from typing import Iterator, Tuple
from communication.protocol.schemas import RAW_SCHEMAS, CLEAN_SCHEMAS

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR

PREFIX_MAPPING = {
    "transactions": ("transactions.clean", "transactions.raw"),
    "transaction_items": ("transaction_items.clean", "transaction_items.raw"),
    "stores": ("stores.clean", "stores.raw"),
    "menu_items": ("menu_clean", "menu.raw"),
    "users": ("users.clean", "users.raw"),
}

# Genera tuplas con (routing_key, nombre_archivo, iterador de rows limpios)
def clean_all_files_grouped() -> Iterator[Tuple[str, str, Iterator[dict]]]:
    for csv_path in INPUT_DIR.glob("*.csv"):
        filename = csv_path.name.lower().strip()
        for prefix, (clean_key, raw_key) in PREFIX_MAPPING.items():
            if filename.startswith(prefix):
                clean_columns = CLEAN_SCHEMAS[clean_key]
                raw_columns = RAW_SCHEMAS[raw_key]
                yield prefix, filename, _clean_csv_file(csv_path, clean_columns, raw_columns)
                break

# Limpia un solo archivo y devuelve filas limpias como generador.
def _clean_csv_file(path: Path, clean_columns, raw_columns) -> Iterator[dict]:
    with path.open(newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if not all(col in row for col in raw_columns):
                continue
            cleaned_row = {col: row[col].strip() for col in clean_columns if col in row}
            if any(val == "" for val in cleaned_row.values()):
                continue
            yield cleaned_row
