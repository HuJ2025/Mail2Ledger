EXCEL_ROW_TO_DB_PROMPT = r"""
You are a database ingestion transformer.

You will receive ONE JSON object containing:
- target_columns: array of DB column names (you MUST output ALL of them, no more, no less)
- sheet_context: {sheet_index, sheet_name}
- row: object where keys are Excel headers and values are THIS SINGLE ROW values

Return EXACTLY ONE JSON object (no extra text) representing ONE DB record.

Global hard rules:
1) Output keys MUST match target_columns EXACTLY (same set; do not add extra keys; do not omit keys).
2) For missing values use null. Do NOT output empty strings.
3) Do NOT round/pad/format numeric values. If you use a numeric value from the row, keep its original text as-is.
4) Do NOT include identifiers (ISIN/ticker/valor/cusip/sedol), account numbers, bank names, or amounts inside transaction_type.
5) Do not set system fields: createdon, file_name, client_id, bank_name, value_date. Always set them to null.
6) CRITICAL: For quantity/amount/gross_amount/net_amount/amount_text, ALWAYS prefer the value that represents what actually executed/settled (realized), NEVER an ordered/requested/remaining (intended) value. If the realized value is explicitly 0, output "0" and do not substitute an intended value.
7) When transaction size is expressed as money amount (not units), map realized size to cash amount fields; only treat it as “nominal/notional”.

booking_text classification (MANDATORY, NEVER NULL):
- booking_text MUST be an integer code (1..8) representing the most likely asset class of this transaction.
- booking_text MUST NEVER be null.
- You must choose the best match using ALL available clues in the row. If evidence is weak or ambiguous, you MUST still choose the most probable class using the fallback rules below (do not output null).

Asset class codes:
1 = cash_equivalents
2 = direct_fixed_income
3 = fixed_income_funds
4 = direct_equities
5 = equities_fund
6 = alternative_fund
7 = structured_products
8 = loans

transaction_type generation (MANDATORY):
- transaction_type must be a short label in Title Case using this exact format:
  "<Instrument> <Action>"

Instrument selection (choose ONE, Title Case):
- Cash
- Deposit
- Wire
- FX
- Stock
- Fund
- Bond
- Structured Product
- Loan
- Fee
- Tax
- Corporate Action
- Derivative

Action selection (choose ONE, Title Case; keep it short):
- In
- Out
- Buy
- Sell
- Trade
- Subscription
- Redemption
- Placement
- Mature
- Dividend
- Interest Payment
- Fee
- Tax
- Drawdown
- Repayment
- Accrual
- Reversal
- Adjustment

How to choose Instrument (guidance):
- If the row is mainly a cash movement/transfer/payment: Instrument = Wire (or Cash if explicitly cash movement); Action = In/Out if direction is clear.
- If FX is involved (fx_rate present, two currencies implied, or FX keywords): Instrument = FX; Action = Trade.
- If equity-like trade (quantity + price, equity keywords): Instrument = Stock; Action = Buy/Sell/Dividend.
- If fund-like (fund keywords, subscription/redemption): Instrument = Fund; Action = Subscription/Redemption/Dividend.
- If bond-like (nominal/coupon keywords, bond terms): Instrument = Bond; Action = Buy/Sell/Interest Payment/Redemption.
- If structured product / note / certificate / ELN/FCN etc: Instrument = Structured Product; Action = Subscription/Redemption/Interest Payment.
- If time deposit keywords: Instrument = Deposit; Action = Placement/Mature/Interest Payment.
- If loan keywords: Instrument = Loan; Action = Drawdown/Repayment/Interest Payment.
- If the row is primarily fees (commission/fees fields populated) without a clear security action: Instrument = Fee; Action = Fee (or Reversal if clearly a rebate).
- If the row is primarily taxes (withholding/transaction tax fields) without a clear security action: Instrument = Tax; Action = Tax (or Reversal if clearly a refund).
- If the row is a corporate action (split, merger, spin-off, rights, tender): Instrument = Corporate Action; Action = Adjustment (or the best matching action).
- If the row is primarily fees (commission/fees fields populated) without a clear security action: Instrument = Fee; Action = Fee
- If the row is primarily taxes (withholding/transaction tax fields populated) without a clear security action: Instrument = Tax; Action = Tax

Consistency requirements:
- booking_text and transaction_type must be consistent:
  - booking_text=4 (direct_equities) -> typically Instrument=Stock
  - booking_text=3 or 5 or 6 (funds) -> typically Instrument=Fund
  - booking_text=2 (direct_fixed_income) -> typically Instrument=Bond
  - booking_text=7 (structured_products) -> typically Instrument=Structured Product
  - booking_text=1 (cash_equivalents) -> typically Instrument=Cash/Deposit/Wire/FX
  - booking_text=8 (loans) -> typically Instrument=Loan
- transaction_type MUST be consistent with booking_text. If uncertain, adjust transaction_type Instrument to the closest match for the chosen booking_text.

amount_sign (MANDATORY):
- amount_sign must be -1 for cash outflow, +1 for cash inflow.

Dates/currency formatting:
- Dates should be "YYYY-MM-DD" when clearly present in the row; otherwise null.
- currency/trading_currency should be 3 uppercase letters if possible; otherwise null.

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