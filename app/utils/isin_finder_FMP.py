import os, re, time, requests
from typing import Optional, Dict, List, Tuple, Any

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"
HEADERS = {"Accept": "application/json"}

# ———————————————————————————————————
# 1) 小工具：HTTP、校验、缓存钩子
# ———————————————————————————————————
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

def is_valid_isin(s: Optional[str]) -> bool:
    return bool(s and ISIN_RE.match(s.strip().upper()))

def _get_json(url: str, params: Dict[str, Any], max_retries: int = 3) -> Any:
    """GET with retry/backoff; 返回 JSON（失败返回 None）。"""
    if FMP_API_KEY:
        params = {**params, "apikey": FMP_API_KEY}
    backoff = 0.8
    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                time.sleep(backoff); backoff = min(backoff * 2, 6.4); continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            time.sleep(backoff); backoff = min(backoff * 2, 6.4)
    return None

# —— 可选：你若已有 DB 缓存，把这两个函数改成查/写 DB 即可 ——
_local_cache: Dict[str, Optional[str]] = {}  # FMP symbol -> ISIN or None
def get_cached_isin(symbol: str) -> Optional[str]:
    return _local_cache.get(symbol)

def put_cached_isin(symbol: str, isin: Optional[str]) -> None:
    _local_cache[symbol] = isin

# 可选：强制覆盖（少数已知需要矫正的标的）
OVERRIDE_ISIN: Dict[str, str] = {
    # 例如：
    # "9988.HK": "KYG017191142",
    # "2202.HK": "CNE1000003X6",
    # "981.HK":  "KYG8020E1060",
}

# ———————————————————————————————————
# 2) 核心：FMP profile 取 ISIN（单只）
# ———————————————————————————————————
def fetch_isin_from_fmp_profile(symbol: str) -> Optional[str]:
    """
    输入：FMP 正规 symbol（例如 9988.HK / 603019.SH / AAPL）
    输出：ISIN（若拿不到则 None）
    """
    if not symbol or not symbol.strip():
        return None

    # 覆盖优先（保证关键持仓总是正确）
    if symbol in OVERRIDE_ISIN:
        return OVERRIDE_ISIN[symbol]

    # 本地缓存
    hit = get_cached_isin(symbol)
    if hit is not None:  # 命中 None 也表示查过但没有
        return hit

    data = _get_json(f"{FMP_BASE}/profile", {"symbol": symbol})
    print(f"[FMP] fetch profile for {symbol}: got {data}")
    isin = None
    if isinstance(data, list) and data:
        maybe = data[0].get("isin")
        if is_valid_isin(maybe):
            isin = maybe.strip().upper()

    # 写缓存（即便 None 也缓存，避免短时间重复请求）
    put_cached_isin(symbol, isin)
    return isin

# ———————————————————————————————————
# 3) 批量：就地补齐 items 列表里的 ISIN
# ———————————————————————————————————
def fill_isin_from_fmp(items: List[Dict[str, Any]],
                       symbol_field: str = "ticker",
                       isin_field: str = "isin",
                       dry_run: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    假设 items[*][symbol_field] 已是 FMP 标准 symbol。
    仅当 isin 为空/无效时才尝试补齐。
    返回：(items, report)
      - report: {"filled": [...], "skipped": [...], "failed": [...], "stats": {...}}
    """
    report = {
        "filled": [],   # 列表元素形如 {"symbol": "...", "isin": "...", "row_index": i}
        "skipped": [],  # 已有有效 ISIN 或无 symbol
        "failed": [],   # 请求后仍无 ISIN
        "stats": {"requests": 0, "cache_hits": 0}
    }

    for i, row in enumerate(items):
        cur_isin = row.get(isin_field)
        if is_valid_isin(cur_isin):
            report["skipped"].append({"row_index": i, "reason": "already_has_isin"})
            continue

        symbol = (row.get(symbol_field) or "").strip().upper()
        if not symbol:
            report["skipped"].append({"row_index": i, "reason": "no_symbol"})
            continue

        # 先查缓存
        cached = get_cached_isin(symbol)
        if cached is not None:
            report["stats"]["cache_hits"] += 1
            if is_valid_isin(cached):
                if not dry_run:
                    row[isin_field] = cached
                report["filled"].append({"row_index": i, "symbol": symbol, "isin": cached, "source": "cache"})
            else:
                report["failed"].append({"row_index": i, "symbol": symbol, "reason": "cached_none"})
            continue

        # 调 FMP
        report["stats"]["requests"] += 1
        isin = fetch_isin_from_fmp_profile(symbol)
        if is_valid_isin(isin):
            if not dry_run:
                row[isin_field] = isin
            report["filled"].append({"row_index": i, "symbol": symbol, "isin": isin, "source": "fmp"})
        else:
            # 标记失败；你可以在外层根据 symbol 后缀决定用交易所清单兜底
            report["failed"].append({"row_index": i, "symbol": symbol, "reason": "not_found_or_invalid"})

    return items, report

# ———————————————————————————————————
# 4) 可选：一个简单的后置兜底（示例）
#     - 对 .HK/.SH/.SZ 你可以接入各交易所免费清单，或你的 security_master 表
# ———————————————————————————————————
def fallback_fill_from_exchange_lists(items: List[Dict[str, Any]]) -> None:
    """
    示例：根据 symbol 后缀决定是否走本地交易所清单（此处留空；按你的实现替换即可）。
    e.g., 9988.HK -> 查 HKEX 清单；603019.SH -> 查上交所清单。
    找到则 row["isin"] = 命中的值。
    """
    pass
