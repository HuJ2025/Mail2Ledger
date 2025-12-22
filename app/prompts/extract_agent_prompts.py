EXCEL_HEADER_DATA_RANGE_PROMPT = r"""
You are an Excel table boundary detector.

Input:
- workbook: { sheets: [ { name, cells } ] }
- cells is a 2D grid with explicit row/col indices. Each cell has: {r, c, v}
  where v is the displayed text (empty -> "")

Task:
For EACH sheet, detect ALL tabular blocks (0..N tables). A "table" is a contiguous
rectangle of rows where:
- one row acts as HEADER: mostly text-like labels, unique-ish, not long sentences;
  typically has higher "label density" than neighboring rows
- subsequent rows are DATA: repeatable structure; contain mixed types (numbers/dates/text),
  not just one phrase like "No Record"
Tables may be separated by blank rows or by section titles (e.g., "Equity > ALL", "Cash/Stock > ALL").

Rules:
1) Ignore report metadata/title areas (few populated cells, long sentences, dates, customer info)
   unless they form a header-like row.
2) Identify header_row as the first row that has a high count of non-empty cells AND looks like
   column labels (mostly short text tokens).
3) data_start_row = header_row + 1 if the next row looks like data; otherwise null.
4) data_end_row is the last row in the contiguous data region before a long blank gap or before
   the next table/section.
5) If the first row after header contains a sentinel like "No Record" / "Nil" / "N/A" across the row,
   treat as no data: data_start_row=null, data_end_row=null, note="No Record".
6) Support multiple tables per sheet.

Output (STRICT JSON only; no extra text):
{
  "<sheetName>": [
    {
      "table": "<best_guess_name_or_table_1/table_2/etc>",
      "header_row": <int>,
      "data_start": <int|null>,
      "data_end": <int|null>,
      "note": "<optional: No Record/empty/etc>"
    }
  ]
}

Naming:
- If a nearby section title exists within 1-3 rows above header (e.g., "Equity", "Cash/Stock"),
  use a short lowercase snake_case name derived from it; otherwise use "table_1", "table_2", ...

Hard constraints:
- Return valid JSON.
- Use 1-based row numbers (as in Excel).
- Do not hallucinate rows; only use observed rows from cells.
"""

EXCEL_ROW_TO_DB_PROMPT = r"""
You are a database ingestion transformer.

INPUT: You receive ONE JSON object:
- target_columns: array of DB column names (you MUST output ALL of them, no more, no less)
- sheet_context: {sheet_index, sheet_name}
- row: object where keys are Excel headers and values are THIS SINGLE ROW values

OUTPUT: Return EXACTLY ONE JSON object (no extra text) representing ONE DB record.

========================
A) Global hard rules
========================
1) Output keys MUST match target_columns EXACTLY (same set; do not add extra keys; do not omit keys).
2) For missing/unknown values output null. Do NOT output empty strings.
3) Do NOT round/pad/format numeric values. If you use a numeric value from the row, keep its original text as-is.
4) Do NOT include identifiers (ISIN/ticker/valor/cusip/sedol), account numbers, bank names, or amounts inside transaction_type.
5) Do not set system fields: createdon, file_name, client_id, bank_name, value_date. Always set them to null.
6) CRITICAL sizing: For quantity/amount/gross_amount/net_amount/amount_text, ALWAYS prefer values that represent executed/settled (realized), NEVER ordered/requested/remaining (intended). If realized value is explicitly 0, output "0" (do not substitute intended).
7) If transaction size is expressed as money amount (not units), map realized size to cash amount fields; do NOT treat it as “nominal/notional”.
8) Account mapping rule:
  - Prefer filling `custody_account` as the primary account field.
  - If the row provides only ONE account-like value (e.g., "Account"), put it in `custody_account` (not `account`).
  - Only fill `debit_account`, `credit_account`, or `beneficiary_account` when similar headers are stated in the row; otherwise leave them null.

You must not invent facts or numbers. Only derive from the provided row (plus minimal formatting for dates/currency).

========================
B) Universal mapping mechanism (non 1-to-1)
========================
You MUST map by MEANING, not by exact header names.

Step 1 — Normalize & interpret the row:
- Consider each (key, value) pair as a candidate “signal”.
- Use key semantics + value patterns to infer meaning:
  - date-like values (YYYY-MM-DD, DD/MM/YYYY, etc.)
  - currency-like values (3-letter, symbols, paired currencies)
  - amount/quantity/price-like values (numbers, decimals, signs)
  - fee/tax-like values (small charges, commission/tax/withholding hints)
  - description-like values (long text, instrument names, narratives)
  - action-like values (buy/sell/subscription/redemption/dividend/interest/transfer, etc.)
- Prefer candidates that are both:
  (a) semantically closest to the target column name, and
  (b) type-consistent with that column’s meaning (date vs currency vs numeric vs text).

Step 2 — Fill each target column by best-fit:
For each column in target_columns:
- Infer the column meaning from the column name tokens (e.g., contains "date", "currency", "qty", "price", "fee", "tax", "desc", "text", etc.).
- Select the best candidate value(s) from the row using this priority:
  1) Strong semantic match (key meaning clearly matches column meaning)
  2) Weak semantic match + strong value-pattern match (e.g., column expects date and value looks like date)
  3) Contextual match (e.g., same currency cluster, same action cluster)
  4) Otherwise null (except mandatory fields in section C)
- If multiple candidates exist, choose the one most consistent with executed/settled reality (Rule A6) and with transaction direction (Section D).
- You may combine multiple signals ONLY to:
  - choose between candidates,
  - normalize dates to YYYY-MM-DD,
  - normalize currency to 3 uppercase letters,
  - build short labels (transaction_type),
  - or preserve original text into amount_text/description-style fields when those columns exist.
- Do NOT do arithmetic, aggregation, or re-computation.

========================
C) booking_text classification (MANDATORY, NEVER NULL)
========================
booking_text MUST be an integer code (1..8) for the most likely asset class.

Codes:
1 cash_equivalents
2 direct_fixed_income
3 fixed_income_funds
4 direct_equities
5 equities_fund
6 alternative_fund
7 structured_products
8 loans

Classification rule:
- Cash rule: If the row explicitly indicates a cash movement (e.g., Type/Transaction Type contains "Cash" or similar), then booking_text MUST be 1 (cash_equivalents), even if the description mentions a fund/bond/stock.
- Use ALL available clues (sheet_context, row text, security/instrument hints, cash-transfer hints, loan/debt hints).
- If ambiguous, still choose the most probable:
  - Pure cash movement/transfer/fees/tax without a security -> 1
  - Bond/coupon/maturity/nominal/issuer note language -> 2
  - Fund subscription/redemption/NAV/fund language -> 3/5/6 (use best fit)
  - Equity trade/dividend/stock-like language -> 4
  - ELN/FCN/notes/certificates/structured note language -> 7
  - Loan/drawdown/repayment/interest on loan -> 8

booking_text MUST NEVER be null.

========================
D) transaction_type generation (MANDATORY)
========================
transaction_type must be Title Case in EXACT format:
  "<Instrument> <Action>"

Instrument (choose ONE):
- Cash, Deposit, Wire, FX, Stock, Fund, Bond, Structured Product, Loan, Fee, Tax, Corporate Action, Derivative

Action (choose ONE, short):
- In, Out, Buy, Sell, Trade, Subscription, Redemption, Placement, Mature,
  Dividend, Interest Payment, Fee, Tax, Drawdown, Repayment,
  Accrual, Reversal, Adjustment

How to choose:
- Determine the most likely real-world event from row signals (buy/sell/transfer/dividend/interest/fee/tax/etc.).
- Keep transaction_type free of IDs, account numbers, bank names, amounts (Rule A4).

Consistency constraint:
- booking_text and transaction_type MUST be consistent:
  - 4 -> typically Stock
  - 2 -> typically Bond
  - 3/5/6 -> typically Fund
  - 7 -> Structured Product
  - 1 -> Cash/Deposit/Wire/FX/Fee/Tax (pick best)
  - 8 -> Loan
If uncertain, keep them consistent by adjusting Instrument to match booking_text.

========================
E) amount_sign (MANDATORY)
========================
amount_sign must be:
- -1 for cash outflow
- +1 for cash inflow

Derive amount_sign using best available evidence:
1) If a selected realized cash amount exists and has a sign, use its sign.
2) Else infer from Action:
   - Outflow: Buy/Subscription/Placement/Fee/Tax/Drawdown? (loan drawdown is typically inflow; be careful)
   - Inflow: Sell/Redemption/Mature/Dividend/Interest Payment/Repayment
3) If still unclear, choose the most probable based on row wording (credit/debit, in/out).

========================
F) Dates & currencies normalization
========================
- Dates: output "YYYY-MM-DD" when clearly present; otherwise null.
- currency/trading_currency: output 3 uppercase letters when clearly inferable; otherwise null.

Return JSON only.
"""


STATEMENT_TXN_KEYS = [
  "transaction_type","booking_text","description",
  "account","custody_account","debit_account","credit_account",
  "client_name","order_no","settlement_no","document_no",
  "trade_date","settle_date","value_date","booking_date","ex_date","due_date","period_from","period_to",
  "currency","amount","amount_sign","gross_amount","net_amount","amount_text",
  "quantity","price","nominal","coupon_rate_percent","fx_rate","trading_currency",
  "security_name","ticker","isin","valor","cusip","sedol",
  "execution_venue","execution_time",
  "commission","stock_exchange_fee","third_party_executions_fee","foreign_financial_fee","other_fee","fees_total",
  "withholding_tax","transaction_tax","taxes_total",
  "realized_pl","transaction_gain","exchange_gain",
  "bank_name","counterparty_bic","beneficiary_name","beneficiary_account",
  "createdon","file_name","client_id","cash_distribution_amount",
]

# =========================
# 0) DB columns (keys must match table headers 1:1)
# =========================

INSERT_COLS = STATEMENT_TXN_KEYS
OUTPUT_COLS = STATEMENT_TXN_KEYS