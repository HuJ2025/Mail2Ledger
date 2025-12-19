# app/agents/excel_2ai_agent.py
import pandas as pd
import os, json

from io import BytesIO

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from app.prompts.extract_agent_prompts import EXCEL_ROW_TO_DB_PROMPT, OUTPUT_COLS
from app.utils.excelchecker import ensure_openpyxl_readable_xlsx, ensure_openpyxl_readable_xlsx_bytes
from app.services.openai_service import get_agent_response_bg
from app.utils.helpers import _safe_json_loads, _should_drop_record, _compute_amount_sign_raw, _sanitize_ai_record

from app.agents.excel_table_detector import detect_tables_from_bytes


# =========================
# 1) Excel -> rows (NOW: via table detector specs)
# =========================

def _make_unique_headers(headers: List[str]) -> List[str]:
    """
    Ensure headers are unique, so dict keys won't collide.
    Example: ["Date","Date","Amount"] -> ["Date","Date_2","Amount"]
    """
    seen = {}
    out = []
    for h in headers:
        key = h
        if key in seen:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
        else:
            seen[key] = 1
            out.append(key)
    return out


def _extract_rows_from_xlsx_bytes_using_specs(
    xlsx_bytes: bytes,
    sheet_names: List[str],
    table_specs_by_sheet: Dict[str, List[Dict[str, Any]]],
    drop_unnamed: bool = True,
    drop_all_blank_rows: bool = True,
    dtype_as_str: bool = True,
) -> List[Dict[str, Any]]:
    """
    Core extractor:
    - reads sheets as raw grid (header=None) to preserve Excel row numbers
    - slices by detector specs (1-based rows)
    - builds row_ctx list
    """
    engine = "openpyxl"
    xls = pd.ExcelFile(BytesIO(xlsx_bytes), engine=engine)

    all_rows: List[Dict[str, Any]] = []
    global_row_idx = 0

    for sheet_idx, sheet_name in enumerate(sheet_names):
        specs = table_specs_by_sheet.get(sheet_name) or []
        if not specs:
            continue

        # IMPORTANT: DO NOT drop blank rows here, or row numbers will shift.
        raw = pd.read_excel(
            xls,
            sheet_name=sheet_name,
            header=None,
            dtype=str if dtype_as_str else None,
            engine=engine,
        ).fillna("")

        n_rows = len(raw)
        if n_rows <= 0:
            continue

        for t_i, spec in enumerate(specs):
            header_row = spec.get("header_row")
            data_start = spec.get("data_start")
            data_end = spec.get("data_end")
            note = (spec.get("note") or "").strip().lower()
            table_name = spec.get("table") or f"table_{t_i+1}"

            if note in {"no record", "no records"}:
                continue

            if header_row is None or data_start is None or data_end is None:
                continue

            try:
                hr = int(header_row) - 1
                ds = int(data_start) - 1
                de = int(data_end) - 1
            except Exception:
                continue

            if hr < 0 or ds < 0 or de < ds:
                continue
            if hr >= n_rows or ds >= n_rows:
                continue
            de = min(de, n_rows - 1)

            # Build headers from header row
            headers_raw = [str(x).strip() for x in list(raw.iloc[hr].values)]

            valid_col_idx: List[int] = []
            headers: List[str] = []
            for ci, h in enumerate(headers_raw):
                if not h:
                    continue
                if drop_unnamed and h.startswith("Unnamed:"):
                    continue
                valid_col_idx.append(ci)
                headers.append(h)

            if not headers:
                continue

            headers = _make_unique_headers(headers)

            # Extract each data row
            for excel_r0 in range(ds, de + 1):
                row_vals = list(raw.iloc[excel_r0].values)
                row_dict: Dict[str, Any] = {}

                for out_i, ci in enumerate(valid_col_idx):
                    h = headers[out_i]
                    v = row_vals[ci] if ci < len(row_vals) else ""
                    if isinstance(v, str):
                        v = v.strip()
                    row_dict[h] = v

                if drop_all_blank_rows:
                    # skip rows where all values are blank
                    if all(str(v).strip() == "" for v in row_dict.values()):
                        continue

                all_rows.append(
                    {
                        "sheet_idx": sheet_idx,
                        "sheet_name": str(sheet_name),
                        "table": table_name,
                        "excel_row": excel_r0 + 1,   # 1-based excel row number (debug)
                        "row_idx": global_row_idx,
                        "row": row_dict,
                    }
                )
                global_row_idx += 1

    return all_rows


def extract_excel_rows(
    file_path: str,
    header_row: int = 0,             # kept for compatibility (ignored now)
    sheet_names=None,                # kept for compatibility (ignored now; detector decides)
    drop_unnamed: bool = True,
    drop_all_blank_rows: bool = True,
    dtype_as_str: bool = True,
    password: str = "scinv100",
):
    """
    Disk Excel -> rows.
    NOW: uses excel_table_detector (multi-table) instead of fixed header_row.
    """
    normalized_path, created_temp = ensure_openpyxl_readable_xlsx(file_path, password=password)

    try:
        # read decrypted/normalized bytes
        with open(normalized_path, "rb") as f:
            xlsx_bytes = f.read()

        # detect table ranges (use original file_path password)
        # note: we already have xlsx bytes, so password isn't needed here
        table_specs = detect_tables_from_bytes(xlsx_bytes, password=None)

        # sheet list from pandas (consistent with detector names)
        engine = "openpyxl"
        xls = pd.ExcelFile(normalized_path, engine=engine)
        sheet_list = list(xls.sheet_names)

        return _extract_rows_from_xlsx_bytes_using_specs(
            xlsx_bytes=xlsx_bytes,
            sheet_names=sheet_list,
            table_specs_by_sheet=table_specs,
            drop_unnamed=drop_unnamed,
            drop_all_blank_rows=drop_all_blank_rows,
            dtype_as_str=dtype_as_str,
        )

    finally:
        if created_temp:
            try:
                os.remove(normalized_path)
            except Exception:
                pass


def extract_excel_rows_from_bytes(
    content_bytes: bytes,
    header_row: int = 0,             # kept for compatibility (ignored now)
    sheet_names=None,                # kept for compatibility (ignored now; detector decides)
    drop_unnamed: bool = True,
    drop_all_blank_rows: bool = True,
    dtype_as_str: bool = True,
    password: str | None = None,
):
    """
    In-memory Excel -> rows.
    NOW: uses excel_table_detector (multi-table) instead of fixed header_row.

    Returns:
      [{"sheet_idx", "sheet_name", "row_idx", "row": {...}, "table", "excel_row"}, ...]
    """
    # 1) detect tables from ORIGINAL bytes (could be encrypted OLE2)
    table_specs = detect_tables_from_bytes(content_bytes, password=password)

    # 2) decrypt/normalize to xlsx zip bytes
    xlsx_bytes = ensure_openpyxl_readable_xlsx_bytes(content_bytes, password=password)

    # 3) get sheet list from workbook (pandas)
    engine = "openpyxl"
    xls = pd.ExcelFile(BytesIO(xlsx_bytes), engine=engine)
    sheet_list = list(xls.sheet_names)

    # 4) extract rows by specs
    return _extract_rows_from_xlsx_bytes_using_specs(
        xlsx_bytes=xlsx_bytes,
        sheet_names=sheet_list,
        table_specs_by_sheet=table_specs,
        drop_unnamed=drop_unnamed,
        drop_all_blank_rows=drop_all_blank_rows,
        dtype_as_str=dtype_as_str,
    )


# =========================
# 2) Step 2: 單 row -> JSON dict
# =========================

def row_dict_to_json_obj(
    row: Dict[str, Any],
    keep_empty: bool = False,
    empty_to_none: bool = True,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, str):
            v = v.strip()

        if empty_to_none and v == "":
            v = None

        if not keep_empty and v is None:
            continue

        out[k] = v
    return out


# =========================
# 3) Step 3: row JSON -> call AI -> DB row JSON (keys MUST match headers)
# =========================

def ai_transform_one_row_to_db_record(
    row_ctx: Dict[str, Any],
    schema: str,
    table: str,
    bank_name: str | None = None,
    file_name: str | None = None,
    client_id: int | None = None,
) -> Optional[Dict[str, Any]]:
    """
    逐行：Excel row_ctx -> row_json -> OpenAI -> DB record(dict)
    - JSON keys: exactly OUTPUT_COLS
    - 不做數值 round/pad（保留原始字串）
    - amount_sign 算完後，把 amount/quantity/gross/net 轉正（只移走負號/括號）
    """
    row_json = row_dict_to_json_obj(row_ctx["row"], keep_empty=False, empty_to_none=True)
    print("row_json:", row_json)

    payload = {
        "target_columns": OUTPUT_COLS,
        "sheet_context": {
            "sheet_index": row_ctx["sheet_idx"],
            "sheet_name": row_ctx["sheet_name"],
        },
        "row": row_json,
    }

    resp = get_agent_response_bg(
        user_input=json.dumps(payload, ensure_ascii=False),
        instructions=EXCEL_ROW_TO_DB_PROMPT,
    )

    print("AI resp:", resp)

    obj = _safe_json_loads(resp)
    rec = _sanitize_ai_record(obj)

    # amount_sign：AI 優先；唔得就用 raw 推斷
    if rec.get("amount_sign") is not None:
        try:
            rec["amount_sign"] = int(str(rec["amount_sign"]).strip())
            if rec["amount_sign"] not in (-1, 1):
                rec["amount_sign"] = None
        except Exception:
            rec["amount_sign"] = None

    if rec.get("amount_sign") is None:
        t_raw = str(
            (rec.get("transaction_type") or row_json.get("Type") or row_json.get("Transaction Type") or "")
        ).strip().lower()
        rec["amount_sign"] = _compute_amount_sign_raw(
            t_raw,
            rec.get("quantity"),
            rec.get("amount"),
            sheet_index=row_ctx["sheet_idx"],
        )

    # ✅ amount_sign 有值後，把 amount/quantity/gross_amount/net_amount 變正（不改小數位）
    if rec.get("amount_sign") in (-1, 1):
        for k in ("amount", "quantity", "gross_amount", "net_amount"):
            v = rec.get(k)
            if v is None:
                continue

            if isinstance(v, (int, float, Decimal)):
                rec[k] = abs(v)
                continue

            s = str(v).strip()
            if s == "":
                rec[k] = None
                continue

            # (123.45) -> 123.45
            if s.startswith("(") and s.endswith(")"):
                s = s[1:-1].strip()
            # -123.45 -> 123.45
            if s.startswith("-"):
                s = s[1:].lstrip()
            # 123.45- -> 123.45
            if s.endswith("-"):
                s = s[:-1].rstrip()

            rec[k] = s if s != "" else None

    # system fields：覆蓋 AI
    now_utc = datetime.now(timezone.utc)
    rec["createdon"] = now_utc
    rec["file_name"] = file_name
    rec["client_id"] = client_id
    rec["bank_name"] = bank_name
    rec["value_date"] = rec.get("trade_date")

    if _should_drop_record(rec):
        return None

    return rec  # full JSON with OUTPUT_COLS keys


# =========================
# Example usage
# =========================
# if __name__ == "__main__":
#     file_path = "MStest.xlsx"

#     rows = extract_excel_rows(file_path, password="")  # header_row ignored now

#     db_records: List[Dict[str, Any]] = []

#     for ctx in rows:
#         rec = ai_transform_one_row_to_db_record(
#             row_ctx=ctx,
#             schema="daily",
#             table="statement_txn",
#             bank_name="UBS",
#             file_name=file_path,
#             client_id=123,
#         )
#         print("record:", rec)
#         if rec:
#             db_records.append(rec)

#     print("records ready:", len(db_records))
