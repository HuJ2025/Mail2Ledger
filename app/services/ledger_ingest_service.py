# app/services/ledger_ingest_service.py

from __future__ import annotations

import os
import logging
import configparser
from typing import Dict, Any, List, Optional, Tuple

import psycopg2

from app.services.aws_postgresql import AWSPostgresGateway
from app.agents.excel_2ai_agent import extract_excel_rows_from_bytes, ai_transform_one_row_to_db_record
from app.utils.helpers import insert_records_with_gateway
from app.prompts.extract_agent_prompts import OUTPUT_COLS

logger = logging.getLogger(__name__)


def _strip_inline_comment(s: str | None) -> str:
    if not s:
        return ""
    return s.split(";", 1)[0].strip()


def _parse_csv_list(s: str | None) -> Optional[List[str]]:
    s = _strip_inline_comment(s)
    if not s:
        return None
    parts = [x.strip() for x in s.split(",") if x.strip()]
    return parts or None


class LedgerIngestService:
    """
    Uses config (INI) for defaults:
      [MAIL2LEDGER]
      DEFAULT_HEADER_ROW=1
      DEFAULT_SHEET_NAMES=
      DEFAULT_PASSWORD=

      [MAIL2LEDGER_BANK_UBS]
      HEADER_ROW=1
      SHEET_NAMES=Trades,Holdings
      PASSWORD=scinv100
    """

    def __init__(
        self,
        schema: str = "daily",
        table: str = "statement_txn",
        batch_size: int = 200,
        db_page_size: int = 50,
        db_retry: int = 1,
        # ✅ pass the SAME config object from pipeline to avoid reading env twice
        config: Optional[configparser.ConfigParser] = None,
        env_path: Optional[str] = None,
    ):
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.db_page_size = db_page_size
        self.db_retry = db_retry

        # config is preferred (read once in pipeline)
        if config is not None:
            self._config = config
            self.env_path = env_path or "<passed-config>"
        else:
            # fallback (still works if someone uses service directly)
            self.env_path = (
                env_path
                or os.getenv("MAIL2LEDGER_ENV_PATH")
                or "/Users/HuJ/Documents/Mail2Ledger/.env"
            )
            cfg = configparser.ConfigParser()
            cfg.read(self.env_path)
            self._config = cfg

        base = self._config["MAIL2LEDGER"] if self._config.has_section("MAIL2LEDGER") else None

        self.default_header_row = base.getint("DEFAULT_HEADER_ROW", fallback=0) if base else 0
        self.default_sheet_names = _parse_csv_list(base.get("DEFAULT_SHEET_NAMES", fallback="")) if base else None
        self.default_password = _strip_inline_comment(base.get("DEFAULT_PASSWORD", fallback="")) if base else ""

    def _resolve_bank_defaults(self, bank_name: Optional[str]) -> Tuple[int, Optional[List[str]], str]:
        header_row = int(self.default_header_row)
        sheet_names = self.default_sheet_names
        password = self.default_password

        if not bank_name:
            return header_row, sheet_names, password

        sec = f"MAIL2LEDGER_BANK_{bank_name.strip().upper()}"
        if not self._config.has_section(sec):
            return header_row, sheet_names, password

        header_row = self._config.getint(sec, "HEADER_ROW", fallback=header_row)
        sn = self._config.get(sec, "SHEET_NAMES", fallback="")
        pw = self._config.get(sec, "PASSWORD", fallback="")

        sheet_names = _parse_csv_list(sn) or sheet_names
        password = _strip_inline_comment(pw) or password

        return header_row, sheet_names, password

    def _insert_batch(self, records: List[dict]) -> int:
        if not records:
            return 0

        attempts = 1 + max(0, int(self.db_retry))
        last_err: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            gw = AWSPostgresGateway()
            try:
                n = insert_records_with_gateway(
                    gw,
                    records,
                    schema=self.schema,
                    table=self.table,
                    cols=OUTPUT_COLS,
                    batch_size=self.db_page_size,
                )
                return int(n or 0)

            except psycopg2.OperationalError as e:
                last_err = e
                logger.warning(f"[db] OperationalError on insert (attempt {attempt}/{attempts}): {e}")
                if attempt == attempts:
                    raise
            finally:
                try:
                    gw.close_connection()
                except Exception:
                    pass

        if last_err:
            raise last_err
        return 0

    def ingest_email_job(self, job: Dict[str, Any]) -> int:
        meta = job.get("meta", {}) or {}

        client_id = meta.get("client_id")
        bank_name = meta.get("bank_name")

        bank_header_row, bank_sheet_names, bank_password = self._resolve_bank_defaults(bank_name)

        header_row = meta.get("header_row")
        header_row = int(header_row) if header_row is not None else bank_header_row

        sheet_names = meta.get("sheet_names")
        sheet_names = sheet_names if sheet_names not in (None, "", []) else bank_sheet_names

        password = meta.get("password")
        password = str(password).strip() if password not in (None, "") else bank_password

        inserted_total = 0

        for att in job.get("attachments", []) or []:
            filename = att.get("filename")
            content_bytes = att.get("content_bytes")

            if not filename or not content_bytes:
                continue

            rows = extract_excel_rows_from_bytes(
                content_bytes=content_bytes,
                header_row=header_row,
                sheet_names=sheet_names,
                password=password,
            )

            batch: List[dict] = []
            for ctx in rows:
                rec = ai_transform_one_row_to_db_record(
                    row_ctx=ctx,
                    schema=self.schema,
                    table=self.table,
                    bank_name=bank_name,
                    file_name=filename,
                    client_id=client_id,
                )
                if rec:
                    batch.append(rec)

                if len(batch) >= self.batch_size:
                    inserted_total += self._insert_batch(batch)
                    batch.clear()

            if batch:
                inserted_total += self._insert_batch(batch)
                batch.clear()

        return inserted_total
