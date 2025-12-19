# app/email/models.py

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class EmailMeta:
    client_id: Optional[int] = None
    bank_name: Optional[str] = None
    header_row: Optional[int] = None
    password: Optional[str] = None
    sheet_names: Optional[List[str]] = None
