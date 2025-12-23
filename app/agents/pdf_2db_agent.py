import logging
import json
from typing import Any, Dict, List, Optional, Tuple

from app.agents.extract_tables_agent import run_assets_vs_txn_workflow, stock_analysis_process
from app.services.storage_service import (
    add_document,
    get_documents_with_assets_by_ids,
    upsert_document_overview_items,
)
from app.utils.common import normalize_year_month, build_document_overview_items_from_stock, to_section_format
from app.utils.isin_finder_IPY import fill_isin_with_investpy
from app.utils.isin_finder_FMP import fill_isin_from_fmp


def _build_and_upsert_overview_for_doc(*, doc_id: int, currency: str = "USD") -> Dict[str, Any]:
    """
    同步生成 overview（等同你原本 generate_summary_for_doc 做嘅嘢）
    """
    docs = get_documents_with_assets_by_ids([doc_id])
    if not docs:
        raise ValueError(f"Doc not found or has no assets: doc_id={doc_id}")

    doc = docs[0]
    as_of_date = doc.get("as_of_date")
    as_of_date_str = (
        as_of_date.strftime("%Y-%m-%d")
        if hasattr(as_of_date, "strftime")
        else str(as_of_date)
    )

    json_input = {
        "bank_name": doc.get("bank_name"),
        "account_number": doc.get("account_number"),
        "as_of_date": as_of_date_str,
        "assets": to_section_format(doc.get("assets"), "Assets"),
    }

    overview_json = stock_analysis_process(json_input, currency)
    rows = build_document_overview_items_from_stock(overview_json, currency)

    # ISIN enrichment（你現有流程）
    rows, _ = fill_isin_with_investpy(rows)
    rows, _ = fill_isin_from_fmp(rows)

    # upsert overview items
    upsert_document_overview_items(doc_id, rows)

    y, m = normalize_year_month(as_of_date)
    return {
        "doc_id": doc_id,
        "ym": [y, m],
        "overview_rows": len(rows),
        # 你想要就打開（會很大）
        # "rows": rows,
        # "overview_json": overview_json,
    }


def ingest_then_update_overview(
    *,
    urls: List[str],
    client_id: int,
    currency: str = "USD",
) -> Dict[str, Any]:
    """
    ✅ 單一入口：先 ingest（upload+add_document），再更新 overview（build+upsert）
    完全唔用 webhook/celery/redis/chunk

    Return:
      {
        "state": "SUCCESS" | "PARTIAL_SUCCESS" | "FAILURE",
        "total": N,
        "success": [
          {"url": ..., "doc_id": ..., "ym":[y,m], "overview_rows": ...},
          ...
        ],
        "failed": [
          {"url": ..., "error": "..."},
          ...
        ],
        "unique_months": [[y,m], ...]
      }
    """
    success: List[Dict[str, Any]] = []
    failed: List[Dict[str, str]] = []
    months: set[Tuple[int, int]] = set()

    for url in urls:
        try:
            # 1) ingest (parse PDF)
            parsed = run_assets_vs_txn_workflow(url)

            # 2) store to DB
            _, doc_id, base_currency = add_document(client_id, parsed)

            # 3) build + upsert overview
            overview_res = _build_and_upsert_overview_for_doc(doc_id=doc_id, currency=base_currency)

            success.append({"url": url, **overview_res})
            y, m = overview_res["ym"]
            months.add((y, m))
            bank_name = parsed.get("bank_name")

        except Exception as e:
            logging.exception("❌ ingest_then_update_overview failed | url=%s", url)
            failed.append({"url": url, "error": str(e)})

    unique_months = [[y, m] for (y, m) in sorted(months)]
    state = (
        "SUCCESS" if not failed
        else "PARTIAL_SUCCESS" if success
        else "FAILURE"
    )

    return {
        "state": state,
        "total": len(urls),
        "success": success,
        "failed": failed,
        "bank_name": bank_name,
        "unique_months": unique_months,
    }
