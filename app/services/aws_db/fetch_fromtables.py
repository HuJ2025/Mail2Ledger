from pathlib import Path
import pandas as pd
from app.services.aws_postgresql import AWSPostgresGateway

gateway = AWSPostgresGateway()

def fetch_trade_fromdb(account: str, isin: str):
    sql = "SELECT * FROM daily.get_latest_units_balance_by_isin_account(%s, %s);"
    df = gateway.query_df(sql, params=(isin, account))

    if df.empty:
        return None, None

    u = df.iat[0, 0]  # units
    b = df.iat[0, 1]  # balance
    units   = None if pd.isna(u) else float(u)
    balance = None if pd.isna(b) else float(b)
    return units, balance

