# -*- coding: utf-8 -*-
"""
Resolve ISIN via InvestPy (Investing.com) using an FMP-normalized ticker.
Examples: 9988.HK, 603019.SH, AAPL

Deps: pip install investpy==1.0.8 pandas
Debug: export INVESTPY_DEBUG=1
"""

from __future__ import annotations
import os, re
from typing import Optional, Dict, List, Tuple, Any

import pandas as pd
import investpy  # type: ignore

# =============== Debug ===============
INVESTPY_DEBUG = os.getenv("INVESTPY_DEBUG", "0") == "1"
def _dbg(*a): 
    if INVESTPY_DEBUG: 
        print("[investpy]", *a)

# =============== Basics ===============
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
def is_valid_isin(s: Optional[str]) -> bool:
    return bool(s and ISIN_RE.match(s.strip().upper()))

SUFFIX_TO_COUNTRY = {
    ".HK":"hong kong", ".SH":"china", ".SZ":"china", ".L":"united kingdom",
    ".DE":"germany", ".F":"germany", ".PA":"france", ".AS":"netherlands",
    ".BR":"belgium", ".MI":"italy", ".MC":"spain", ".SW":"switzerland",
    ".AX":"australia", ".NZ":"new zealand", ".T":"japan", ".KS":"south korea",
    ".KQ":"south korea", ".TW":"taiwan", ".TO":"canada", ".V":"canada",
    ".CN":"canada", "":"united states"
}

ALLOWED_PREFIXES: Dict[str, List[str]] = {
    ".SH":["CNE"], ".SZ":["CNE"],
    ".HK":["CNE","KYG","BMG","HK"],
    ".L":["GB"], ".DE":["DE"], ".F":["DE"], ".PA":["FR"], ".AS":["NL"], ".BR":["BE"],
    ".MI":["IT"], ".MC":["ES"], ".SW":["CH"], ".AX":["AU"], ".NZ":["NZ"], ".T":["JP"],
    ".KS":["KR"], ".KQ":["KR"], ".TW":["TW"], ".TO":["CA"], ".V":["CA"], ".CN":["CA"],
    "":["US"]
}

def symbol_suffix(sym: str) -> str:
    u = sym.upper()
    for s in (".HK",".SH",".SZ",".L",".DE",".F",".PA",".AS",".BR",".MI",".MC",
              ".SW",".AX",".NZ",".T",".KS",".KQ",".TW",".TO",".V",".CN"):
        if u.endswith(s): return s
    return ""

def prefix_ok_for_symbol(sym: str, isin: str) -> bool:
    if not is_valid_isin(isin): return False
    allowed = ALLOWED_PREFIXES.get(symbol_suffix(sym), [])
    return True if not allowed else any(isin.upper().startswith(p) for p in allowed)

# In-memory cache (swap to DB if needed)
_cache: Dict[str, Optional[str]] = {}
def get_cached(s: str) -> Optional[str]: return _cache.get(s)
def put_cached(s: str, v: Optional[str]) -> None: _cache[s] = v

# =============== Helpers ===============
def fmp_symbol_to_investpy_query(sym: str) -> Tuple[str, str, List[str]]:
    """Return (country, primary_symbol, aliases)."""
    sym = (sym or "").strip().upper()
    suf = symbol_suffix(sym)
    country = SUFFIX_TO_COUNTRY.get(suf, "united states")
    base = sym[:-len(suf)] if suf else sym

    if suf == ".HK" and base.isdigit():
        primary = str(int(base))
        pad4, pad5 = primary.zfill(4), primary.zfill(5)
        aliases = [base, primary, pad4, pad5]  # include zero-padded variants
    elif suf in (".SH",".SZ") and base.isdigit() and len(base) == 6:
        primary, aliases = base, [base]
    else:
        primary, aliases = base, []
    return country, primary, aliases

_country_list_cache: Dict[str, Optional[pd.DataFrame]] = {}
def _load_country_stocks(country: str) -> Optional[pd.DataFrame]:
    if country in _country_list_cache: return _country_list_cache[country]
    try:
        _dbg(f"get_stocks country={country!r}")
        df = investpy.stocks.get_stocks(country=country)
        _country_list_cache[country] = df if isinstance(df, pd.DataFrame) and not df.empty else None
    except Exception as e:
        _dbg(f"get_stocks error: {e}")
        _country_list_cache[country] = None
    return _country_list_cache[country]

def _try_isin_via_stock_information(symbol_text: str, country: str) -> Optional[str]:
    try:
        _dbg(f"get_stock_information stock={symbol_text!r}, country={country!r}")
        df = investpy.stocks.get_stock_information(stock=symbol_text, country=country)
        if isinstance(df, pd.DataFrame) and not df.empty:
            for col in ("isin","ISIN"):
                if col in df.columns:
                    val = str(df.iloc[0][col]).strip().upper()
                    return val if is_valid_isin(val) else None
    except Exception as e:
        _dbg(f"get_stock_information error: {e}")
    return None

def _try_isin_via_get_stocks(country: str, symbol_text: str, name_hint: Optional[str]=None) -> Optional[str]:
    df = _load_country_stocks(country)
    if df is None or df.empty: return None
    cols = {c.lower(): c for c in df.columns}
    sym_col, isin_col = cols.get("symbol"), cols.get("isin")
    name_col = cols.get("name") or cols.get("full_name") or cols.get("company")
    if sym_col and isin_col:
        raw = symbol_text.upper()
        num = ''.join(ch for ch in raw if ch.isdigit())
        cands = {raw}
        if num: cands |= {num, num.zfill(4), num.zfill(5)}
        hit = df[df[sym_col].astype(str).str.upper().isin(cands)]
        if not hit.empty:
            val = str(hit.iloc[0][isin_col]).strip().upper()
            return val if is_valid_isin(val) else None
    if name_col and isin_col:
        target = (name_hint or symbol_text or "").upper()
        if target:
            hit = df[df[name_col].astype(str).str.upper().str.contains(target, na=False)]
            if symbol_suffix(symbol_text) == ".HK" and not hit.empty:
                hit = hit[hit[isin_col].astype(str).str.upper()
                         .str.startswith(tuple(ALLOWED_PREFIXES[".HK"]))]
            if not hit.empty:
                val = str(hit.iloc[0][isin_col]).strip().upper()
                return val if is_valid_isin(val) else None
    return None

def _try_quote(candidate: str, country: str, name_hint: Optional[str]=None) -> Optional[str]:
    try:
        _dbg(f"search_quotes text={candidate!r}, country={country!r}")
        res = investpy.search_quotes(text=candidate, products=['stocks'], countries=[country])
        objs = res if isinstance(res, list) else [res]
        if not objs:
            return (_try_isin_via_stock_information(candidate, country)
                    or _try_isin_via_get_stocks(country, candidate, name_hint))
        for q in objs:
            try:
                info = q.retrieve_information()
                isin = (info or {}).get("isin")
                if isin: return isin
                qsym = getattr(q, "symbol", None) or candidate
                return (_try_isin_via_stock_information(qsym, country)
                        or _try_isin_via_get_stocks(country, qsym, name_hint))
            except Exception:
                continue
    except Exception:
        return (_try_isin_via_stock_information(candidate, country)
                or _try_isin_via_get_stocks(country, candidate, name_hint))
    return None

# =============== Public: one symbol ===============
def fetch_isin_via_investpy(symbol: str, company_name_hint: Optional[str]=None) -> Optional[str]:
    if not symbol or not symbol.strip(): return None
    symbol = symbol.strip().upper()

    cached = get_cached(symbol)
    if cached is not None: return cached

    country, primary, aliases = fmp_symbol_to_investpy_query(symbol)
    for cand in [primary, *aliases, symbol, (company_name_hint or "")]:
        if not cand: continue
        isin = _try_quote(cand, country, company_name_hint)
        if isin and is_valid_isin(isin) and prefix_ok_for_symbol(symbol, isin):
            put_cached(symbol, isin.strip().upper()); return _cache[symbol]
    put_cached(symbol, None)
    return None

# =============== Public: batch fill ===============
def fill_isin_with_investpy(items: List[Dict[str, Any]],
                            symbol_field: str = "ticker",
                            isin_field: str = "isin",
                            name_field: str = "name") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    report = {"filled": [], "failed": [], "skipped": []}
    for i, row in enumerate(items):
        cur = row.get(isin_field)
        if is_valid_isin(cur):
            report["skipped"].append({"row_index": i, "reason": "already_has_isin"})
            continue
        sym = (row.get(symbol_field) or "").strip().upper()
        if not sym:
            report["skipped"].append({"row_index": i, "reason": "no_symbol"})
            continue
        hit = fetch_isin_via_investpy(sym, row.get(name_field))
        if is_valid_isin(hit):
            row[isin_field] = hit
            report["filled"].append({"row_index": i, "symbol": sym, "isin": hit})
        else:
            report["failed"].append({"row_index": i, "symbol": sym, "reason": "not_found"})
    return items, report
