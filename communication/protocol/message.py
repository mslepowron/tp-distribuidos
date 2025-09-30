from dataclasses import dataclass
import io
import csv
from typing import List, Tuple, Dict

class ProtocolError(Exception):
    pass

class HeaderError(ProtocolError):
    pass

class PayloadError(ProtocolError):
    pass

VALID_QUERY_IDS = {
    "q_amount_75_tx",
    "q_month_top_qty",
    "q_month_top_rev",
    "q_semester_tpv",
    "q_top3_birthdays"
}

HEADER_KEYS = (
    "message_type",
    "query_id",
    "stage", # etapa de la consulta (ej "filter", "map", etc)
    "part", # nombre del archivo
    "seq", # numero de secuencia del batch
    "schema", #que archivo es
    "source" # de que archivo viene el mensaje
)

MESSAGE_TYPES = {"DATA", "EOF"}

@dataclass
class Header:
    def __init__(self, fields: List[Tuple[str, str]]):
        self.fields = dict(fields)
        self._validate()

    def _validate(self):
        keys = list(self.fields.keys())
        missing = [k for k in HEADER_KEYS if k not in keys]
        if missing:
            raise HeaderError(f"Faltan claves en el header: {missing}")

        if self.fields["query_id"] not in VALID_QUERY_IDS:
            raise HeaderError(f"query_id inválido: {self.fields['query_id']}")


    def as_dictionary(self) -> Dict[str, str]:
        return self.fields

    def encode(self) -> bytes:
        try:
            output = io.StringIO()
            writer = csv.writer(output)
            for key, value in self.fields.items():
                writer.writerow([key, value])
            return output.getvalue().encode("utf-8")
        except Exception as e:
            raise ProtocolError(f"Error al codificar el header: {e}")

    @staticmethod
    def decode(header_bytes: bytes) -> 'Header':
        try:
            input_stream = io.StringIO(header_bytes.decode("utf-8"))
            reader = csv.reader(input_stream)
            fields = [(key, value) for key, value in reader]
            return Header(fields)
        except Exception as e:
            raise ProtocolError(f"Error al decodificar el header: {e}")
