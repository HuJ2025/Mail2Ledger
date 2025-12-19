# app/email/ingest/parser.py

import re
from typing import List, Optional
from app.email.models import EmailMeta


def parse_email_meta(body: str) -> EmailMeta:
    """
    约定 email 正文使用 Key: Value 格式，例如：
      client_id: 123
      bank_name: UBS
      header_row: 1
      password: scinv100
      sheet_names: Trades, Holdings
    """
    b = body or ""

    def find_int(key: str) -> Optional[int]:
        m = re.search(rf"(?im)^\s*{re.escape(key)}\s*:\s*(\d+)\s*$", b)
        return int(m.group(1)) if m else None

    def find_str(key: str) -> Optional[str]:
        m = re.search(rf"(?im)^\s*{re.escape(key)}\s*:\s*(.+?)\s*$", b)
        return m.group(1).strip() if m else None

    def find_list(key: str) -> Optional[List[str]]:
        s = find_str(key)
        if not s:
            return None
        return [x.strip() for x in re.split(r"[,\n]+", s) if x.strip()]

    return EmailMeta(
        client_id=find_int("client_id"),
        bank_name=find_str("bank_name"),
        header_row=find_int("header_row"),
        password=find_str("password"),
        sheet_names=find_list("sheet_names"),
    )
