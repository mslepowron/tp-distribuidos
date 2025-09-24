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

HEADER_KEYS = (
    "message_type",
    "query_id",
    "stage",
    "part",
    "seq",
    "schema",
    "source"
)

MESSAGE_TYPES = {"DATA", "EOF"}

# @dataclass
# class Header:
