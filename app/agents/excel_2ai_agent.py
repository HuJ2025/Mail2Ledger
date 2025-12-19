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



# =========================
# 1) Excel (encrypted OLE2) -> openpyxl-readable .xlsx
# =========================

def extract_excel_rows(
    file_path: str,
    header_row: int = 0,
    sheet_names=None,
    drop_unnamed: bool = True,
    drop_all_blank_rows: bool = True,
    dtype_as_str: bool = True,
    password: str = "scinv100",
):
    normalized_path, created_temp = ensure_openpyxl_readable_xlsx(file_path, password=password)

    try:
        engine = "openpyxl"
        xls = pd.ExcelFile(normalized_path, engine=engine)

        if sheet_names is None:
            targets = list(enumerate(xls.sheet_names))
        else:
            targets = []
            for s in sheet_names:
                if isinstance(s, int):
                    targets.append((s, xls.sheet_names[s]))
                else:
                    idx = xls.sheet_names.index(s)
                    targets.append((idx, s))

        all_rows = []
        for sheet_idx, sheet_name in targets:
            df = pd.read_excel(
                normalized_path,
                sheet_name=sheet_name,
                header=header_row,
                dtype=str if dtype_as_str else None,
                engine=engine,
            )

            if drop_all_blank_rows:
                df = df.dropna(how="all")

            df.columns = [str(c).strip() for c in df.columns]

            if drop_unnamed:
                df = df[[c for c in df.columns if not str(c).startswith("Unnamed:")]]

            df = df.fillna("")
            for row_idx, row in enumerate(df.to_dict(orient="records")):
                row_clean = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                all_rows.append(
                    {"sheet_idx": sheet_idx, "sheet_name": str(sheet_name), "row_idx": row_idx, "row": row_clean}
                )

        return all_rows

    finally:
        if created_temp:
            try:
                os.remove(normalized_path)
            except Exception:
                pass

def extract_excel_rows_from_bytes(
    content_bytes: bytes,
    header_row: int = 0,
    sheet_names=None,
    drop_unnamed: bool = True,
    drop_all_blank_rows: bool = True,
    dtype_as_str: bool = True,
    password: str | None = None,
):
    """
    In-memory Excel -> rows
    Returns same structure as extract_excel_rows():
      [{"sheet_idx", "sheet_name", "row_idx", "row": {...}}, ...]
    """
    normalized = ensure_openpyxl_readable_xlsx_bytes(content_bytes, password=password)

    engine = "openpyxl"
    bio = BytesIO(normalized)
    xls = pd.ExcelFile(bio, engine=engine)

    if sheet_names is None:
        targets = list(enumerate(xls.sheet_names))
    else:
        targets = []
        for s in sheet_names:
            if isinstance(s, int):
                targets.append((s, xls.sheet_names[s]))
            else:
                idx = xls.sheet_names.index(s)
                targets.append((idx, s))

    all_rows = []
    for sheet_idx, sheet_name in targets:
        df = pd.read_excel(
            xls,  # ✅ 直接用 ExcelFile
            sheet_name=sheet_name,
            header=header_row,
            dtype=str if dtype_as_str else None,
            engine=engine,
        )

        if drop_all_blank_rows:
            df = df.dropna(how="all")

        df.columns = [str(c).strip() for c in df.columns]

        if drop_unnamed:
            df = df[[c for c in df.columns if not str(c).startswith("Unnamed:")]]

        df = df.fillna("")
        for row_idx, row in enumerate(df.to_dict(orient="records")):
            row_clean = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            all_rows.append(
                {"sheet_idx": sheet_idx, "sheet_name": str(sheet_name), "row_idx": row_idx, "row": row_clean}
            )

    return all_rows


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
if __name__ == "__main__":
    file_path = "MStest.xlsx"

    rows = extract_excel_rows(file_path, header_row=0, password="")

    db_records: List[Dict[str, Any]] = []

    for ctx in rows:
        rec = ai_transform_one_row_to_db_record(
            row_ctx=ctx,
            schema="daily",
            table="statement_txn",
            bank_name="UBS",
            file_name=file_path,
            client_id=123,
        )
        print("record:", rec)
        if rec:
            db_records.append(rec)

    print("records ready:", len(db_records))
