import os
import csv
from pathlib import Path
from typing import Dict, List
from communication.protocol.schemas import RAW_SCHEMAS, CLEAN_SCHEMAS

BASE_DIR = Path(__file__).resolve().parent.parent 
INPUT_DIR = BASE_DIR
OUTPUT_DIR = Path(os.getenv("STORAGE_DIR", Path(__file__).resolve().parent / "storage"))

# Mapeo de prefijos de archivo a claves de CLEAN y RAW schemas
PREFIX_MAPPING = {
    "transactions": ("transactions.clean", "transactions.raw"),
    "transaction_items": ("transaction_items.clean", "transaction_items.raw"),
    "stores": ("stores.clean", "stores.raw"),
    "menu_items": ("menu_items.clean", "menu.raw"),
    "users": ("users.clean", "users.raw"),
}

def clean_all_files():
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)

    for csv_path in INPUT_DIR.glob("*.csv"):
        filename = csv_path.name.lower().strip()

        for prefix, (clean_key, raw_key) in PREFIX_MAPPING.items():
            if filename.startswith(prefix):
                clean_columns = CLEAN_SCHEMAS[clean_key]
                raw_columns = RAW_SCHEMAS[raw_key]
                output_path = OUTPUT_DIR / csv_path.name  

                clean_single_file(csv_path, output_path, clean_columns, raw_columns)
                break  # ya matcheo un prefijo, no sigue buscando

def clean_single_file(input_path: Path, output_path: Path, clean_columns: List[str], raw_columns: List[str]):
    with input_path.open(newline="", encoding="utf-8") as infile, \
         output_path.open("w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=clean_columns)
        writer.writeheader()

        total_rows = 0
        kept_rows = 0

        for row in reader:
            total_rows += 1

            if not all(col in row for col in raw_columns):
                continue

            cleaned_row = {col: row[col].strip() for col in clean_columns if col in row}
            if any(val == "" for val in cleaned_row.values()):
                continue

            writer.writerow(cleaned_row)
            kept_rows += 1

        print(f"[initializer] {input_path.name}: {kept_rows}/{total_rows} filas válidas.")
