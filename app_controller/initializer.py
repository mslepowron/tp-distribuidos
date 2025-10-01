from typing import List, Dict
from communication.protocol.schemas import RAW_SCHEMAS, CLEAN_SCHEMAS

def initialize_rows(rows: List[Dict[str, str]], source: str) -> List[Dict[str, str]]:

    prefix = source.split(".")[0]
    raw_schema_key = f"{prefix}.raw"
    clean_schema_key = f"{prefix}.clean"

    raw_schema = RAW_SCHEMAS.get(raw_schema_key, [])
    clean_schema = CLEAN_SCHEMAS.get(clean_schema_key, [])

    cleaned = []
    for row in rows:
        if not all(col in row for col in raw_schema):
            continue

        cleaned_row = {
            col: (row[col].strip() if row.get(col) is not None else "")
            for col in clean_schema if col in row
        }

        if any(val == "" for val in cleaned_row.values()):
            continue

        cleaned.append(cleaned_row)

    return cleaned
