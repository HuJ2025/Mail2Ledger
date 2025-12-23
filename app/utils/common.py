import re
import os
import json
import pandas as pd
from io import BytesIO
import copy
import math

from openai import OpenAI
from dotenv import load_dotenv
from celery import Celery
from app.services.aws_s3 import upload_to_s3
from urllib.parse import urlparse, urlunparse, quote, urlsplit, urlunsplit
from typing import List, Union, Dict, Any, Iterable
from collections.abc import MutableMapping, Mapping, Sequence
from app.services.reporting.excel_formatter import json_to_xlsx_bytes
from datetime import datetime, date as Date
from pathlib import Path

load_dotenv()
OPENAI_API = os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key=OPENAI_API)

def clean_json_markdown(text):
    """
    1. 移除三個反引號的 markdown 包裝
    2. 移除 JSON 數值裡的千分位逗號，使其能被 json.loads() 正確解析
    """
    # 移除 markdown 三反引號
    text = re.sub(r'^```[a-zA-Z]*\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^```\s*$', '', text, flags=re.MULTILINE)

    # 移除數字之間的逗號（只移除數字之間的逗號，不影響字串/股票名等）
    text = re.sub(r'(?<=\d),(?=\d)', '', text)

    return text.strip()

def generate_excel_report(data: dict, bank_name: str) -> str:
    """
    Build XLSX with new formatter → upload to S3 → return URL.
    The file is named '<bank_name>.xlsx'.
    """
    buf = json_to_xlsx_bytes(data, bank_code=bank_name[:10].upper())  # keep sheets tidy
    filename = f"{bank_name}_{int(datetime.now().timestamp())}.xlsx"                                    # S3 key
    return upload_to_s3(buf, filename)

def safe_url(url):
    # 將路徑部分進行 quote
    parts = list(urlparse(url))
    parts[2] = quote(parts[2])  # path
    return urlunparse(parts)

def encode_path_minimally(url: str) -> str:
    parts = urlsplit(url)
    # 對 path 的每個 segment 做 quote，保留僅有的 URL 保留字元
    segments = [quote(seg, safe="-._~") for seg in parts.path.split('/')]
    encoded_path = "/".join(segments)
    return urlunsplit((parts.scheme, parts.netloc, encoded_path, parts.query, parts.fragment))

def delete_openai_file(file_ids):

    for file_id in file_ids:
        try:
            resp = client.files.delete(file_id)
            print(f"Deleted {file_id}: {resp.deleted}")  # 用 resp.deleted
        except Exception as e:
            print(f"Failed to delete {file_id}: {e}")

def _as_json_str(obj) -> str:
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False)

def _json_safe(obj):
    """Recursively convert to JSON-safe types."""
    if isinstance(obj, (str, int, float)) or obj is None:
        return obj
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return obj.hex()  # fallback
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, Mapping):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, Sequence) and not isinstance(obj, str):
        return [_json_safe(v) for v in obj]
    # last-resort
    return str(obj)

#----------------------------------------------------------------------
# 🟢🟢🟢 FORMAT EXTRACTED JSON - extract_tables_agent 🟢🟢🟢
#----------------------------------------------------------------------

def _dedup_headers(headers: List[Any]) -> List[str]:
    """Ensure unique header names by appending (2), (3), ... when duplicated."""
    out, seen = [], {}
    for h in headers or []:
        base = str(h) if h is not None else ""
        if base not in seen:
            seen[base] = 1
            out.append(base or f"Column {len(out)+1}")
        else:
            seen[base] += 1
            suffix = f" ({seen[base]})"
            out.append((base or f"Column {len(out)+1}") + suffix)
    return out

def to_section_format(data: Dict[str, Any], top_key: str , fill_missing: Any = None) -> Dict[str, Any]:
    """
    Input:
      {
        "tables": [
          {"table_name": "...", "headers": [...], "rows": [[...], ...]},
          ...
        ]
      }
    Output:
      {
        "<top_key>": {
          "<table_name>": [ { "<Col A>": "...", "<Col B>": "..." }, ... ],
          ...
        }
      }
    """
    # print(f"entered to_section_format")
    result: Dict[str, Any] = {top_key: {}}
    tables = data.get("tables") or []

    for i, t in enumerate(tables, 1):
        name = t.get("table_name") or f"Table {i}"
        headers = _dedup_headers(t.get("headers") or [])
        n = len(headers)

        mapped_rows: List[Dict[str, Any]] = []
        for row in (t.get("rows") or []):
            if not isinstance(row, list):
                continue
            # pad/truncate to header length
            padded = row[:n] + [fill_missing] * max(0, n - len(row))
            row_obj = {headers[j]: padded[j] for j in range(n)}
            # store any extra cells (beyond headers)
            if len(row) > n:
                row_obj["raw_extra"] = row[n:]
            mapped_rows.append(row_obj)

        result[top_key][name] = mapped_rows
    # print(f"exited to_section_format", result)
    return result

def replace_nullish_strings(obj: Any, *, extra_nulls: Iterable[str] = ()) -> Any:
    """
    Recursively convert 'null-like' strings to None in dicts/lists/tuples.
    - Defaults: "", "null", "none", "n/a", "na", "nan", "n.a.", "nil", "-", "—", "–"
    - Preserves "0", "0.0", "0%", etc.
    - Also converts float('nan') to None.
    """
    # print(f"entered replace_nullish_strings")
    base_nulls = {"", "null", "none", "n/a", "na", "nan", "n.a.", "nil", "-", "—", "–"}
    nulls = {s.lower() for s in base_nulls} | {str(s).lower() for s in extra_nulls}

    def _is_nullish_str(s: str) -> bool:
        s2 = s.replace("\u00a0", " ").strip().lower()  # normalize NBSP + trim
        return s2 in nulls

    def _coerce(x: Any) -> Any:
        if x is None:
            return None
        if isinstance(x, float):
            return None if math.isnan(x) else x
        if isinstance(x, str):
            return None if _is_nullish_str(x) else x
        if isinstance(x, list):
            return [_coerce(v) for v in x]
        if isinstance(x, tuple):
            return tuple(_coerce(v) for v in x)
        if isinstance(x, dict):
            return {k: _coerce(v) for k, v in x.items()}
        return x
    # print(f"exited replace_nullish_strings")
    return _coerce(obj)

#----------------------------------------------------------------------
# 🔴🔴🔴 END OF FORMAT EXTRACTED JSON - extract_tables_agent 🔴🔴🔴
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# 🟢🟢🟢 ADD DOCUMENT TO DB: Parsers for common formats in table cells 🟢🟢🟢
#----------------------------------------------------------------------

_ccy_amount = re.compile(r"^\s*([A-Z]{3})\s+([-]?\d[\d\s]*\.?\d*)\s*$")

def parse_currency_num(text):
    if not text: return (None, None)
    m = _ccy_amount.match(text.strip())
    if not m: return (None, None)
    ccy, num = m.group(1), float(m.group(2).replace(" ", ""))
    return ccy, num

def parse_percent(text):
    if not text: return None
    t = text.strip()
    if t.endswith("%"):
        try: return float(t[:-1].replace(" ", "")) / 100.0
        except: return None
    try: return float(t)
    except: return None

def parse_date(text):
    if not text: return None
    t = text.strip()
    for fmt in ("%d.%m.%Y","%Y-%m-%d","%d/%m/%Y"):
        try: return datetime.strptime(t, fmt).date()
        except: pass
    return None

#----------------------------------------------------------------------
# 🔴🔴🔴 END OF ADD DOCUMENT TO DB: Parsers for common formats in table cells 🔴🔴🔴
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# 🟢🟢🟢 ADD OVERVIEW TO DB: Parsers for common formats in overview table cells 🟢🟢🟢
#----------------------------------------------------------------------

_CATEGORY_MAP = {
    "cash_equivalents":    "cash_and_equivalents",
    "direct_fixed_income": "direct_fixed_income",
    "fixed_income_funds":  "fixed_income_funds",
    "direct_equities":     "direct_equities",
    "equities_fund":       "equity_funds",
    "alternative_fund":    "alternative_funds",
    "structured_product":  "structured_products",
    "loans":               "loans",
}

# _TICKER_RE = re.compile(r"\bTicker:\s*([^;]+)", re.I)
# _ISIN_RE   = re.compile(r"\bISIN:\s*([A-Z0-9]{10,12})", re.I)

# def _parse_extra(extra: str | None):
#     if not extra:
#         return None, None
#     t = _TICKER_RE.search(extra)
#     i = _ISIN_RE.search(extra)
#     return (t.group(1).strip() if t else None,
#             i.group(1).strip() if i else None)

def build_document_overview_items_from_stock(overview_json: dict, currency: str) -> list[dict]:
    """
    Input = the JSON returned by stock_analysis_process(...) for ONE document:
    {
      "bank": "...",
      "account_number": "...",
      "as_of_date": "YYYY-MM-DD",
      "cash_and_equivalents": { "rows":[...] },
      ...
    }
    Output = list of normalized rows for upsert_document_overview_items(...)
    """
    items: list[dict] = []
    for cat_code, json_key in _CATEGORY_MAP.items():
        block = (overview_json or {}).get(json_key) or {}
        rows  = block.get("rows") or []
        for idx, r in enumerate(rows, start=1):
            # extra = r.get("extra")
            # ticker, isin = _parse_extra(extra)
            items.append({
                "category_code": cat_code,
                "row_index": idx,
                "name": r.get("name"),
                "currency": r.get("currency"),
                "balance_in_currency": r.get("balance_in_currency"),
                "balance_in_usd": r.get("balance_in_usd"),  # use USD balance for now
                "balance_base_currency": r.get("balance_base_currency"),
                "accrued_interest": r.get("accrued_interest"),
                "accrued_interest_base": r.get("accrued_interest_base"),
                "units": r.get("units"),     # can be number | str | None
                "price": r.get("price"),
                "ticker": r.get("ticker"),
                "isin": r.get("isin"),
                "country": r.get("country"),
                "sector": r.get("sector"),
                "base_currency": currency,
            })
    print("Built overview items:", items)
    return items

def normalize_year_month(as_of) -> tuple[int, int]:
    """
    Accepts 'YYYY-MM-DD' str, datetime, or date and returns (year, month).
    """
    if isinstance(as_of, datetime):
        d = as_of.date()
    elif isinstance(as_of, Date):
        d = as_of
    elif isinstance(as_of, str):
        d = datetime.strptime(as_of, "%Y-%m-%d").date()
    else:
        raise TypeError(f"Unsupported as_of_date type: {type(as_of)}")
    return d.year, d.month

#----------------------------------------------------------------------
# 🔴🔴🔴 END OF ADD OVERVIEW TO DB: Parsers for common formats in overview table cells 🔴🔴🔴
#----------------------------------------------------------------------


