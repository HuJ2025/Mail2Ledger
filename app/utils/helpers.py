# app/utils/helpers.py
import json
from typing import Any, Dict, List, Optional
import psycopg2.extras as pg_extras
from app.prompts.extract_agent_prompts import INSERT_COLS, OUTPUT_COLS


def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        s = (s or "").strip()
        i = s.find("{")
        j = s.rfind("}")
        if i != -1 and j != -1 and j > i:
            return json.loads(s[i : j + 1])
        raise


def _is_blank_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def _should_drop_record(rec: dict) -> bool:
    keys = ["description", "custody_account", "trade_date", "amount", "quantity", "amount_sign"]
    return all(_is_blank_value(rec.get(k)) for k in keys)


def _infer_sign_from_raw(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    if s.startswith("(") and s.endswith(")"):
        return -1
    if s.startswith("-") or s.endswith("-"):
        return -1
    try:
        x = float(s.replace(",", ""))
        if x > 0:
            return 1
        if x < 0:
            return -1
    except Exception:
        pass
    return None


def _compute_amount_sign_raw(ttype_raw: str | None, qty_raw, amt_raw, sheet_index: int) -> int | None:
    t = (ttype_raw or "").strip().lower()
    qty_sign = _infer_sign_from_raw(qty_raw)
    amt_sign = _infer_sign_from_raw(amt_raw)

    if sheet_index == 1:
        if qty_sign is not None:
            return -1 if qty_sign > 0 else 1
        return amt_sign

    if t in {"subscription", "redemption"}:
        if qty_sign is not None:
            return -1 if qty_sign > 0 else 1
        return amt_sign

    return amt_sign


def _sanitize_ai_record(obj: Any) -> dict:
    out = {c: None for c in OUTPUT_COLS}

    if not isinstance(obj, dict):
        return out

    for c in OUTPUT_COLS:
        v = obj.get(c, None)
        if isinstance(v, str) and v.strip() == "":
            v = None
        out[c] = v

    return out


def build_insert_sql(schema: str, table: str, cols: list[str]) -> str:
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    vals_sql = ", ".join(f"%({c})s" for c in cols)
    return f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({vals_sql});'


def insert_records_with_gateway(gateway, records, schema="daily", table="statement_txn", cols=None, batch_size=1000):
    if not records:
        return 0
    if cols is None:
        cols = list(records[0].keys())

    sql = build_insert_sql(schema, table, cols)

    try:
        pg_extras.execute_batch(gateway.cursor, sql, records, page_size=batch_size)
        gateway.conn.commit()
        return len(records)
    except Exception:
        gateway.conn.rollback()
        raise


