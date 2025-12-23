import logging
import psycopg2
import json
from typing import Any, Dict, Literal, Optional
from datetime import date
from decimal import Decimal
from typing import Tuple, Dict, Any, List, Optional
from psycopg2 import sql as psql
from psycopg2.extras import execute_values, Json, RealDictCursor
from psycopg2.extensions import cursor as Cursor
from app.utils.common import parse_currency_num, parse_date, parse_percent
from app.services.aws_db.database import get_conn, put_conn
from app.utils.retry_on_db_failure import retry_on_db_failure

# ------------------------------------------------------------------------------
# 🟢🟢🟢 ADD DOCUMENT (account-centric) 🟢🟢🟢
# ------------------------------------------------------------------------------

STMT_TABLE_SQL = """
  INSERT INTO statement_table (document_id, bank_id, name_raw, category)
  VALUES %s
  RETURNING id
"""

STMT_FIELD_SQL = """
  INSERT INTO statement_field (table_id, ordinal, name_raw, data_type_hint, canonical_field)
  VALUES %s
  RETURNING id
"""

STMT_ROW_SQL = """
  INSERT INTO statement_row (table_id, row_index, source_hash)
  VALUES %s
  RETURNING id
"""

STMT_CELL_SQL = """
  INSERT INTO statement_cell (row_id, field_id, val_text, val_num, val_date, val_percent, val_currency)
  VALUES %s
"""

BANK_UPSERT_SQL = """
  INSERT INTO bank (name)
  VALUES (%s)
  ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
  RETURNING id
"""

CLIENT_BANK_INSERT_SQL = """
  INSERT INTO client_bank (client_id, bank_id)
  VALUES (%s, %s)
  ON CONFLICT DO NOTHING
"""

# 🔑 Upsert key: (client_id, bank_id, account_number)
CLIENT_ACCOUNT_UPSERT_SQL = """
  INSERT INTO client_account
    (client_id, bank_id, custodian, account_name, account_number, investment_strategy, institution)
  VALUES
    (%s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (client_id, bank_id, account_number)
  DO UPDATE SET
    custodian = COALESCE(EXCLUDED.custodian, client_account.custodian),
    account_name = COALESCE(EXCLUDED.account_name, client_account.account_name),
    investment_strategy = COALESCE(EXCLUDED.investment_strategy, client_account.investment_strategy),
    institution = COALESCE(EXCLUDED.institution, client_account.institution)
  RETURNING id
"""

# ⬇️ DOCUMENT: store info
DOC_INSERT_SQL = """
  INSERT INTO document
      (client_id, account_id, pdf_url, excel_url, as_of_date, base_currency)
  VALUES
      (%s, %s, %s, %s, %s, %s)
  RETURNING id
"""

def _infer_hint(h: str) -> str:
    hlow = h.lower()
    if "date" in hlow: return "date"
    if "rate" in hlow or "price" in hlow: return "number"
    if "value" in hlow or "amount" in hlow or "interest" in hlow: return "number"
    if "performance" in hlow or "ytd" in hlow or "%" in hlow: return "percent"
    return "text"

CANONICAL_MAP = {
    # assets
    "liquidity_accounts": {
        "description": "description",
        "market_value": "market_value",
        "as_of_date": "as_of_date",
        "accrued_interest": "accrued_interest",
        "amount": "currency",
    },
    "liquidity_fixed_term_deposits": {
        "Description": "description",
        "Market value": "market_value",
        "Accrued interest": "accrued_interest",
        "Amount": "currency",
    },
    # transactions
    "transaction_list": {
        "trade_date": "trade_date",
        "value_date": "settle_date",
        "booking_text": "description",
        "account": "account_number",           # not stored on document; OK for parsing
        "transaction_price": "price",
        "settlement_amount_account_ccy": "net_amount",
    },
    "details_of_income_and_costs": {
        "date": "trade_date",
        "description": "description",
        "Custody Account": "account_number",   # ditto
        "Currency": "currency",
        "Amount": "net_amount",
    },
}

def _get_or_create_account_id(
    cur: Cursor,
    client_id: int,
    bank_name: Optional[str],
    acc_payload: Dict[str, Any],
) -> Tuple[int, int]:
    """
    Returns (bank_id, account_id).
    Creates bank/client_bank/client_account if necessary.
    acc_payload may contain:
      - account_id (preferred if known)
      - account_number (required to create)
      - account_name, custodian, investment_strategy, institution (optional)
    """
    # 1) Bank upsert
    cur.execute(BANK_UPSERT_SQL, (bank_name or "Unknown Bank",))
    bank_id = cur.fetchone()[0]

    # 2) Ensure client_bank link
    cur.execute(CLIENT_BANK_INSERT_SQL, (client_id, bank_id))

    # 3) Choose or create client_account
    given_account_id = acc_payload.get("account_id")
    if given_account_id:
        cur.execute("SELECT bank_id FROM client_account WHERE id = %s", (given_account_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"client_account id {given_account_id} not found")
        # if bank mismatch, trust the account's actual bank
        if row[0] != bank_id:
            bank_id = row[0]
        return bank_id, int(given_account_id)

    # Create via (client_id, bank_id, account_number)
    account_number = acc_payload.get("account_number")
    if not account_number:
        raise ValueError("Either 'account_id' or 'account_number' is required to create/find client_account")

    cur.execute(
        CLIENT_ACCOUNT_UPSERT_SQL,
        (
            client_id,
            bank_id,
            acc_payload.get("custodian"),
            acc_payload.get("account_name"),
            account_number,
            acc_payload.get("investment_strategy"),
            acc_payload.get("institution"),
        ),
    )
    account_id = cur.fetchone()[0]
    return bank_id, int(account_id)

def load_statement_payloads(
    cur: Cursor,
    document_id: int,
    bank_id: int,
    payload: Any,
    category: str,
) -> List[Tuple[int, str]]:
    """
    Loads the "universal statement layer" from a normalized payload.
    payload can be:
      - {"tables": [ {table_name, headers, rows}, ... ]}
      - [ {table_name, headers, rows}, ... ]
      - None / {}
    """
    results: List[Tuple[int, str]] = []

    # normalize to list of table dicts
    if not payload:
        tables = []
    elif isinstance(payload, dict):
        tables = payload.get("tables") or []
    elif isinstance(payload, list):
        tables = payload
    else:
        tables = []

    for t in tables:
        if not isinstance(t, dict):
            continue

        table_name = t.get("table_name") or "unknown_table"
        headers    = t.get("headers") or []
        rows       = t.get("rows") or []

        # 1) statement_table
        execute_values(
            cur,
            STMT_TABLE_SQL,
            [(document_id, bank_id, table_name, category)],
            template=None,
            page_size=1,
        )
        table_id = cur.fetchone()[0]

        # 2) statement_field
        fields = []
        canon = CANONICAL_MAP.get(table_name, {})
        for i, h in enumerate(headers, start=1):
            fields.append((table_id, i, h, _infer_hint(h), canon.get(h)))
        field_ids: List[int] = []
        if fields:
            execute_values(cur, STMT_FIELD_SQL, fields)
            field_ids = [r[0] for r in cur.fetchall()]

        # 3) rows + 4) cells
        for r_index, rvals in enumerate(rows, start=1):
            execute_values(cur, STMT_ROW_SQL, [(table_id, r_index, None)], page_size=1)
            row_id = cur.fetchone()[0]

            cells = []
            for j in range(len(headers)):
                f_id = field_ids[j] if j < len(field_ids) else None
                raw  = None
                if isinstance(rvals, (list, tuple)) and j < len(rvals):
                    raw = rvals[j]
                text = None if raw is None else str(raw)
                val_num = val_date = val_pct = None
                val_ccy = None

                if f_id:
                    h = headers[j].lower()
                    if "date" in h:
                        val_date = parse_date(text)
                    elif "%" in (text or "") or "performance" in h or "ytd" in h:
                        val_pct = parse_percent(text)
                    elif ("amount" in h or "value" in h or "interest" in h or "price" in h):
                        ccy, num = parse_currency_num(text or "")
                        if ccy:
                            val_ccy = ccy
                            val_num = num
                        else:
                            try:
                                val_num = Decimal((text or "").replace(" ", ""))
                            except Exception:
                                pass

                    cells.append((row_id, f_id, text, val_num, val_date, val_pct, val_ccy))

            if cells:
                execute_values(cur, STMT_CELL_SQL, cells)

        results.append((table_id, table_name))

    return results

@retry_on_db_failure()
def add_document(client_id: int, parsed: Dict[str, Any]) -> Tuple[int, int]:
    """
    Inputs (parsed):
      - bank_name (optional; used for bank upsert and document display)
      - EITHER 'account_id' OR 'account_number'
        (plus optional: account_name, custodian, investment_strategy, institution)
      - pdf_url, excel_report_url, as_of_date
      - assets / transactions payloads (optional) to populate statement_* tables

    Behavior:
      - Never accepts bank_id directly; derives it via (bank_name -> bank_id) and/or client_account.
      - Creates client_account if needed.
      - Inserts document with (client_id, account_id, bankname, pdf_url, excel_url, as_of_date).
      - Parses assets/transactions into statement_table/field/row/cell with bank_id filled from account.
    Returns: (bank_id, document_id)
    """
    conn = get_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            bank_name = parsed.get("bank_name") or "Unknown Bank"

            # Build account payload from parsed
            acc_payload = {
                "account_id": parsed.get("account_id"),
                "account_number": parsed.get("account_number"),
                "account_name": parsed.get("account_name"),
                "custodian": parsed.get("custodian"),
                "investment_strategy": parsed.get("investment_strategy"),
                "institution": parsed.get("institution"),
            }

            # -> bank_id, account_id (create if missing)
            bank_id, account_id = _get_or_create_account_id(cur, client_id, bank_name, acc_payload)

            pdf_url    = parsed.get("pdf_url")
            excel_url  = parsed.get("excel_report_url")
            as_of_date = parsed.get("as_of_date")
            base_currency = parsed.get("base_currency", "USD")

            # Insert the document (account-centric)
            cur.execute(
                DOC_INSERT_SQL,
                (client_id, account_id, pdf_url, excel_url, as_of_date, base_currency),
            )
            doc_id = cur.fetchone()[0]

            # Load universal statement layer
            assets = parsed.get("assets") or {"tables": []}
            txns   = parsed.get("transactions") or {"tables": []}

            load_statement_payloads(cur, doc_id, bank_id, assets, category="asset")
            load_statement_payloads(cur, doc_id, bank_id, txns,   category="transaction")

        conn.commit()
        return bank_id, doc_id, base_currency

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        put_conn(conn)

# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF ADD DOCUMENT 🔴🔴🔴
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 🟢🟢🟢 DELETE DOCUMENT 🟢🟢🟢
# ------------------------------------------------------------------------------
@retry_on_db_failure()
def delete_documents(doc_ids: List[int]) -> None: 
    
    ids = [int(x) for x in doc_ids]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM document
                WHERE id = ANY(%s)
            """, (ids,))
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        raise e
    finally:
        put_conn(conn)

# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF DELETE DOCUMENT 🔴🔴🔴
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 🟢🟢🟢 UPDATE DOCUMENT 🟢🟢🟢
# ------------------------------------------------------------------------------

#this is redundant now you can directly just call the functions created in DB to update from NEXT backend
@retry_on_db_failure()
def update_document_contents(client_id: int, doc_id: int, assets: Any, transactions: Any) -> bool:
    """
    Update the stored `assets` and `transactions` JSON fields for a given document.
    Returns True if a row was updated, False otherwise.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE document
                SET assets = %s,
                    transactions = %s
                WHERE id = %s
                AND client_id = %s
            """, (Json(assets), Json(transactions), doc_id, client_id))
            updated = cur.rowcount > 0
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)

# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF UPDATE DOCUMENT 🔴🔴🔴
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 🟢🟢🟢 GENERATE SUMMARY PER DOC - CELERY TASK 🟢🟢🟢
# ------------------------------------------------------------------------------
@retry_on_db_failure()
def get_documents_with_assets_by_ids(doc_ids: List[int]) -> List[Dict[str, Any]]:
    if not doc_ids:
        return []

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Prefer account_id → client_account → bank; fall back to document.bank_id → bank; then bankname text
            cur.execute("""
                SELECT
                    d.id,
                    b.name AS bank_name,
                    ca.account_number,
                    d.as_of_date
                FROM document d
                LEFT JOIN client_account ca ON ca.id = d.account_id
                LEFT JOIN bank b ON b.id = ca.bank_id
                WHERE d.id = ANY(%s)
                ORDER BY d.as_of_date DESC, d.id DESC
            """, (doc_ids,))
            docs = cur.fetchall()
            if not docs:
                return []

            doc_ids_present = [d["id"] for d in docs]

            # (unchanged) assets-only tables
            cur.execute("""
                SELECT id AS table_id, document_id, name_raw AS table_name, category
                FROM statement_table
                WHERE document_id = ANY(%s) AND category = 'asset'
                ORDER BY id
            """, (doc_ids_present,))
            tables = cur.fetchall()
            if not tables:
                return [{**d, "assets": {"tables": []}} for d in docs]

            table_ids = [t["table_id"] for t in tables]

            cur.execute("""
                SELECT id AS field_id, table_id, name_raw, ordinal
                FROM statement_field
                WHERE table_id = ANY(%s)
                ORDER BY table_id, ordinal
            """, (table_ids,))
            fields = cur.fetchall()

            cur.execute("""
                SELECT id AS row_id, table_id, row_index
                FROM statement_row
                WHERE table_id = ANY(%s)
                ORDER BY table_id, row_index
            """, (table_ids,))
            row_meta = cur.fetchall()
            row_ids = [r["row_id"] for r in row_meta]

            cells_map: Dict[int, Dict[int, Optional[str]]] = {}
            if row_ids:
                cur.execute("""
                    SELECT sr.id AS row_id, sc.field_id, sc.val_text
                    FROM statement_row sr
                    JOIN statement_cell sc ON sc.row_id = sr.id
                    WHERE sr.id = ANY(%s)
                    ORDER BY sr.id, sc.field_id
                """, (row_ids,))
                for rec in cur.fetchall():
                    rid = rec["row_id"]; fid = rec["field_id"]
                    cells_map.setdefault(rid, {})[fid] = rec["val_text"]

            fields_by_table: Dict[int, List[Dict[str, Any]]] = {}
            headers_by_table: Dict[int, List[str]] = {}
            field_ids_by_table: Dict[int, List[int]] = {}
            for f in fields:
                fields_by_table.setdefault(f["table_id"], []).append(f)
            for tid, flist in fields_by_table.items():
                headers_by_table[tid] = [f["name_raw"] for f in flist]
                field_ids_by_table[tid] = [f["field_id"] for f in flist]

            rows_by_table: Dict[int, List[int]] = {}
            for r in row_meta:
                rows_by_table.setdefault(r["table_id"], []).append(r["row_id"])

            tables_by_doc: Dict[int, List[Dict[str, Any]]] = {}
            for t in tables:
                tid = t["table_id"]; did = t["document_id"]
                hdrs = headers_by_table.get(tid, [])
                fids = field_ids_by_table.get(tid, [])
                rids = rows_by_table.get(tid, []) or []

                row_arrays: List[List[Optional[str]]] = []
                for rid in rids:
                    row_arrays.append([(cells_map.get(rid, {})).get(fid) for fid in fids])

                tables_by_doc.setdefault(did, []).append({
                    "table_id": tid,
                    "table_name": t["table_name"],
                    "headers": hdrs,
                    "rows": row_arrays
                })

            out: List[Dict[str, Any]] = []
            for d in docs:
                out.append({
                    "id": d["id"],
                    "bank_name": d["bank_name"],
                    "account_number": d["account_number"],
                    "as_of_date": d["as_of_date"],
                    "assets": {"tables": tables_by_doc.get(d["id"], [])}
                })
            return out
    finally:
        put_conn(conn)

@retry_on_db_failure()
def upsert_document_overview_items(document_id: int, items: list[dict]) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT code, id FROM category")
            code2id = {r[0]: r[1] for r in cur.fetchall()}

            vals = []
            for it in items:
                cat_code = it.get("category_code")
                if cat_code not in code2id:
                    continue  # skip unknown categories safely

                cid = code2id[cat_code]
                units = it.get("units")
                units_num  = units if isinstance(units, (int, float)) else None
                units_text = units if isinstance(units, str) else None

                vals.append((
                    document_id, cid, it.get("row_index", 0),
                    it.get("name"), it.get("currency"),
                    it.get("balance_in_currency"),
                    units_num, units_text,
                    it.get("ticker"), it.get("isin"), it.get("country"), it.get("sector"),
                    it.get("balance_in_usd"), it.get("price"),
                    it.get("balance_base_currency"), it.get("accrued_interest_base"), it.get("base_currency")
                ))

            if not vals:
                return

            execute_values(cur, """
              INSERT INTO document_overview_item
                (document_id, category_id, row_index,
                 name, currency, balance_in_currency,
                 units_num, units_text, ticker, isin, country, sector, balance_in_usd, price, balance_base_currency, accrued_interest_base, base_currency)
              VALUES %s
              ON CONFLICT (document_id, category_id, row_index)
              DO UPDATE SET
                name=EXCLUDED.name,
                currency=EXCLUDED.currency,
                balance_in_currency=EXCLUDED.balance_in_currency,
                units_num=EXCLUDED.units_num,
                units_text=EXCLUDED.units_text,
                ticker=EXCLUDED.ticker,
                isin=EXCLUDED.isin,
                country=EXCLUDED.country,
                sector=EXCLUDED.sector,
                balance_in_usd=EXCLUDED.balance_in_usd,
                price=EXCLUDED.price,
                balance_base_currency=EXCLUDED.balance_base_currency,
                accrued_interest_base=EXCLUDED.accrued_interest_base,
                base_currency=EXCLUDED.base_currency
            """, vals)
        conn.commit()
    finally:
        put_conn(conn)

# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF GENERATE SUMMARY PER DOC - CELERY TASK 🔴🔴🔴
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# 🟢🟢🟢 GENERATE NEWS AND ALERTS - CELERY TASK 🟢🟢🟢
# ------------------------------------------------------------------------------
@retry_on_db_failure()
def get_latest_docs_for_month_full(client_id: int, year: int, month: int) -> List[Dict]:
    """
    Returns a list of (bank, assets, transactions) for ALL documents
    in the client's latest month.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT get_client_latest_month_docs(%s, 'both')", (client_id,))
            row = cur.fetchone()
            arr = row[0] if row else []
    finally:
        put_conn(conn)

    if isinstance(arr, str):
        arr = json.loads(arr)

    out = []
    for item in arr or []:
        out.append((
            item.get("bank"),
            item.get("assets") or {},
            item.get("transactions") or {}
        ))
    return out

# used in generate news and alerts
@retry_on_db_failure()
def get_latest_overview_month(client_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    EXTRACT(YEAR  FROM as_of_date)::int AS yr,
                    EXTRACT(MONTH FROM as_of_date)::int AS mo
                FROM document
                WHERE client_id = %s
                    AND as_of_date IS NOT NULL
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (client_id,))
            return cur.fetchone()
    finally:
        put_conn(conn)

# used in generate news and alerts
@retry_on_db_failure()
def get_latest_overview_by_month(client_id: int, year: int, month: int) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT public.get_month_overview_aggregated(%s, %s, %s)",
                        (client_id, year, month))
            row = cur.fetchone()
            return row[0] if row else {"tableData": []}
    finally:
        put_conn(conn)

# used in generate news and alerts
@retry_on_db_failure()
def update_client_news_alerts(client_id: int, news: str, alerts: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE client
                SET news = %s,
                    alerts = %s,
                    updated_at = now()
                WHERE id = %s
            """, (Json(news), Json(alerts), client_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)

@retry_on_db_failure()
def get_security_name_by_month(client_id: int, year: int, month: int) -> List[str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT doi.name
                FROM document_overview_item AS doi
                JOIN document AS d ON d.id = doi.document_id
                WHERE d.client_id = %s
                  AND EXTRACT(YEAR FROM d.as_of_date) = %s
                  AND EXTRACT(MONTH FROM d.as_of_date) = %s
                ORDER BY doi.name
            """, (client_id, year, month))
            rows = cur.fetchall()
            return [r[0] for r in rows if r[0]]
    finally:
        put_conn(conn)
# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF GENERATE NEWS AND ALERTS - CELERY TASK 🔴🔴🔴
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 🟢🟢🟢 WEBHOOK QUERIES 🟢🟢🟢
# ------------------------------------------------------------------------------

# add client - webhook (NEW DB)
@retry_on_db_failure()
def insert_client(user_id: str, name: str) -> int:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO client (name, user_id)
                VALUES (%s, %s)
                RETURNING id
            """, (name, user_id))
            row = cur.fetchone()
        conn.commit()  # ✅ Make sure this line exists
        return row
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)

# get client - webhook (NEW DB)
@retry_on_db_failure()
def get_clients_by_user(user_id: str) -> List[Dict]:
    SQL = """
    SELECT c.id, c.name
    FROM   client AS c
    WHERE  c.user_id = %s
    ORDER  BY c.created_at DESC
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SQL, (user_id,))
            return cur.fetchall()          # ⇢ [{id, name, pie_chart_data}, …]
    finally:
        put_conn(conn)

# delete client - webhook (NEW DB)
@retry_on_db_failure()
def delete_client_by_id(client_id: int, user_id: str) -> Dict:
    conn = get_conn()
    try:
        conn.autocommit = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                DELETE FROM client
                WHERE id = %s AND user_id = %s
                RETURNING id
            """, (client_id, user_id))
            result = cur.fetchone()
        if result:
            conn.commit()
        else:
            conn.rollback()
        return result
    finally:
        put_conn(conn)

# rename client - webhook (NEW DB)
@retry_on_db_failure()
def rename_client(client_id: int, user_id: str, new_name: str) -> Dict:
    conn = get_conn()
    try:
        conn.autocommit = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:  # ✅ RealDictCursor is key
            cur.execute("""
                UPDATE client
                SET name = %s
                WHERE id = %s AND user_id = %s
                RETURNING id, name
            """, (new_name, client_id, user_id))
            result = cur.fetchone()
        if result:
            conn.commit()
        else:
            conn.rollback()
        return result
    finally:
        put_conn(conn)

# get client documents info- webhook (NEW DB)
@retry_on_db_failure()
def list_documents_by_client(client_id: int) -> List[Dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM public.get_client_documents_info(21);
            """, (client_id,))
            return cur.fetchall()
    finally:
        put_conn(conn)

# get client overview - webhook (OLD DB)
@retry_on_db_failure()
def list_overviews_by_client(client_id: int, month_date: str = None) -> List[Dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT month_date, table_data, pie_chart_data
                FROM overview
                WHERE client_id = %s
            """
            params = [client_id]
            if month_date:
                sql += " AND month_date = %s"
                params.append(month_date)
            sql += " ORDER BY month_date DESC, id DESC"
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        put_conn(conn)

# get client news and alerts - webhook (NEW DB)
@retry_on_db_failure()
def get_news_alerts_by_client(client_id: int) -> Dict:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT news, alerts
                FROM client
                WHERE id = %s
            """, (client_id,))
            return cur.fetchone()
    finally:
        put_conn(conn)

# get document tables - webhook (already have a function was just for testing purposes)
@retry_on_db_failure()
def get_document_tables(
    document_id: int,
    table_name: Optional[str] = None,  # filter by name_raw
    limit: Optional[int] = None,       # max rows per table
    offset: int = 0                    # row offset per table
) -> List[Dict[str, Any]]:
    """
    Reconstructs tables for a document from the universal statement layer.

    Returns: [
      {
        "table_id": int,
        "table_name": str,
        "category": "asset"|"transaction",
        "headers": [str, ...],
        "rows": [[cell_str_or_null, ...], ...]
      },
      ...
    ]
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1) fetch tables for this document (optionally by name)
            if table_name:
                cur.execute(
                    """
                    SELECT id, name_raw, category
                    FROM statement_table
                    WHERE document_id = %s AND name_raw = %s
                    ORDER BY id
                    """,
                    (document_id, table_name,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, name_raw, category
                    FROM statement_table
                    WHERE document_id = %s
                    ORDER BY id
                    """,
                    (document_id,),
                )
            tables = cur.fetchall()

            out: List[Dict[str, Any]] = []

            for t in tables:
                table_id = t["id"]
                name_raw = t["name_raw"]
                category = t["category"]

                # 2) headers in ordinal order
                cur.execute(
                    """
                    SELECT id, name_raw
                    FROM statement_field
                    WHERE table_id = %s
                    ORDER BY ordinal
                    """,
                    (table_id,),
                )
                fields = cur.fetchall()
                field_ids = [f["id"] for f in fields]
                headers   = [f["name_raw"] for f in fields]

                # 3) rows → fetch row ids in index order with pagination
                #    We page at the 'row' level (not cells).
                #    If you expect huge tables, consider adding WHERE row_index between ... for speed.
                if limit is not None:
                    cur.execute(
                        """
                        SELECT id AS row_id
                        FROM statement_row
                        WHERE table_id = %s
                        ORDER BY row_index
                        LIMIT %s OFFSET %s
                        """,
                        (table_id, int(limit), int(offset)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id AS row_id
                        FROM statement_row
                        WHERE table_id = %s
                        ORDER BY row_index
                        """,
                        (table_id,),
                    )
                row_id_rows = cur.fetchall()
                if not row_id_rows:
                    out.append({
                        "table_id": table_id,
                        "table_name": name_raw,
                        "category": category,
                        "headers": headers,
                        "rows": []
                    })
                    continue

                row_ids = [r["row_id"] for r in row_id_rows]

                # 4) get cells for just these row_ids
                cur.execute(
                    """
                    SELECT sr.id AS row_id, sc.field_id, sc.val_text
                    FROM statement_row sr
                    JOIN statement_cell sc ON sc.row_id = sr.id
                    WHERE sr.id = ANY(%s)
                    ORDER BY sr.id, sc.field_id
                    """,
                    (row_ids,),
                )
                rows_map: Dict[int, Dict[int, Optional[str]]] = {}
                for rec in cur.fetchall():
                    rid = rec["row_id"]
                    fid = rec["field_id"]
                    rows_map.setdefault(rid, {})[fid] = rec["val_text"]

                # 5) build ordered row arrays (by field_ids)
                row_arrays: List[List[Optional[str]]] = []
                for rid in row_ids:
                    row_arrays.append([rows_map.get(rid, {}).get(fid) for fid in field_ids])

                out.append({
                    "table_id": table_id,
                    "table_name": name_raw,
                    "category": category,
                    "headers": headers,
                    "rows": row_arrays
                })

            return out
    finally:
        put_conn(conn)

# get native totals grouped - webhook (NEW DB)
@retry_on_db_failure()
def get_native_totals_grouped_rows(client_id: int, year: int, month: int) -> List[Dict[str, Any]]:
    """
    SELECT wrapper: returns compact native totals per (bank, category, currency).
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT bank_id, bank, category, currency, total_native
                FROM public.get_native_totals_grouped(%s,%s,%s)
            """, (client_id, year, month))
            return cur.fetchall() or []
    finally:
        put_conn(conn)

# get native totals grouped by account - webhook (NEW DB)
@retry_on_db_failure()
def get_native_totals_grouped_by_account_rows(client_id: int, year: int, month: int) -> List[Dict[str, Any]]:
    """
    SELECT wrapper: returns compact native totals per (bank, account_number, category, currency).
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT bank_id, bank, account_number, category, currency, total_native
                FROM public.get_native_totals_grouped_by_account(%s,%s,%s)
            """, (client_id, year, month))
            return cur.fetchall() or []
    finally:
        put_conn(conn)


def get_month_overview_aggregated_json(
    client_id: int,
    year: int,
    month: int,
) -> Dict[str, Any]:
    """
    Calls SQL function public.get_month_overview_aggregated and returns a Python dict.
    Shape: {"tableData": [ { bank, account_number, as_of_date, <category arrays> }, ... ]}
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.get_month_overview_aggregated(%s,%s,%s)",
                (client_id, year, month),
            )
            row = cur.fetchone()
            if not row:
                return {"tableData": []}

            payload = row[0]
            # psycopg2 may return jsonb as str or dict depending on conn settings
            if isinstance(payload, str):
                try:
                    return json.loads(payload)
                except Exception:
                    logging.exception("Failed to json.loads SQL jsonb result")
                    return {"tableData": []}
            elif isinstance(payload, dict):
                return payload
            else:
                # fallback
                try:
                    return json.loads(str(payload))
                except Exception:
                    logging.exception("Failed to coerce SQL result to JSON")
                    return {"tableData": []}
    finally:
        put_conn(conn)
        
# ------------------------------------------------------------------------------
# 🔴🔴🔴 END OF WEBHOOK QUERIES 🔴🔴🔴
# ------------------------------------------------------------------------------


#----------------------------------------------------------------------
# 🟢🟢🟢 ADD pdd TO DB 🟢🟢🟢
#----------------------------------------------------------------------

def upsert_security_pdd(
    security_type: "Literal['equity','fixed_income','fund','structured_product']",
    pdd_json: Dict[str, Any],
    isin: Optional[str] = None,
) -> None:
    SECURITY_TYPE_TO_PRODUCT_CLASS = {
        "equity": "EQUITY",
        "fixed_income": "BOND",
        "fund": "FUND",
        "structured_product": "STRUCTURED",
    }

    if security_type not in SECURITY_TYPE_TO_PRODUCT_CLASS:
        raise ValueError(f"Unsupported security_type: {security_type}")

    product_class = SECURITY_TYPE_TO_PRODUCT_CLASS[security_type]

    resolved_isin = (isin or pdd_json.get("isin") or "").strip().upper()

    # 只對 equity 讀 ticker；其他類型當作沒有 ticker
    resolved_ticker = None
    if security_type == "equity":
        resolved_ticker = (pdd_json.get("ticker") or "").strip().upper() or None

    # ✅ 規則：有 isin 或有 ticker 才繼續；兩個都沒有就不存
    if not resolved_isin and not resolved_ticker:
        return

    # ✅ 沒 isin 但有 ticker：用合成 isin 當主鍵，才能沿用你現有 ON CONFLICT(isin)
    if not resolved_isin and resolved_ticker:
        resolved_isin = f"TICKER:{resolved_ticker}"

    pdd_as_of = pdd_json.get("asOf") or date.today().isoformat()
    risk_rating = pdd_json.get("riskRating")

    sql = """
    INSERT INTO pdd.security_pdd (
      isin, ticker, product_class, pdd_as_of, risk_rating, pdd_json, last_refreshed_at
    )
    VALUES (%s, %s, %s, %s::date, %s, %s::jsonb, now())
    ON CONFLICT (isin)
    DO UPDATE SET
      ticker            = COALESCE(EXCLUDED.ticker, pdd.security_pdd.ticker),
      product_class     = EXCLUDED.product_class,
      pdd_as_of         = EXCLUDED.pdd_as_of,
      risk_rating       = EXCLUDED.risk_rating,
      pdd_json          = EXCLUDED.pdd_json,
      last_refreshed_at = now();
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    resolved_isin,
                    resolved_ticker,
                    product_class,
                    pdd_as_of,
                    risk_rating,
                    json.dumps(pdd_json, ensure_ascii=False),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)

#----------------------------------------------------------------------
# 🔴🔴🔴 END OF ADD pdd TO DB 🔴🔴🔴
#----------------------------------------------------------------------