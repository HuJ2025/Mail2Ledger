# app/email/ingest/pipeline.py

from __future__ import annotations

import os
import re
import logging
import configparser
from typing import Any, Dict, List, Optional

from app.email.gmail.client import _load_gmail_service
from app.email.gmail.reader import (
    search_message_ids,
    get_message,
    get_header,
    extract_body_text,
)
from app.email.gmail.actions import mark_message_as_read
from app.email.ingest.parser import parse_email_meta
from app.email.gmail.attachments import download_excel_attachments_in_memory
from app.email.gmail.labels import resolve_label_ids
from app.email.gmail.sender import send_email_text

from app.services.ledger_ingest_service import LedgerIngestService

logger = logging.getLogger(__name__)


# ------------------------
# Small helpers
# ------------------------

def _strip(s: str | None) -> str:
    if not s:
        return ""
    return s.split(";", 1)[0].strip()


def _get_bool(section: Optional[configparser.SectionProxy], key: str, fallback: bool = False) -> bool:
    if section is None:
        return fallback
    v = _strip(section.get(key, fallback=str(fallback))).lower()
    return v in {"1", "true", "yes", "y", "on"}


def _get_int(section: Optional[configparser.SectionProxy], key: str, fallback: int) -> int:
    if section is None:
        return fallback
    v = _strip(section.get(key, fallback=str(fallback)))
    try:
        return int(v)
    except Exception:
        return fallback


def _load_cfg(env_path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(env_path)
    return cfg


_EMAIL_RE = re.compile(r"[\w\.\-\+]+@[\w\.\-]+\.\w+")


def _extract_email(from_header: str | None) -> Optional[str]:
    """
    Parse "Name <a@b.com>" -> "a@b.com"
    """
    if not from_header:
        return None
    m = _EMAIL_RE.search(from_header)
    return m.group(0) if m else None


def _iter_label_entries(cfg: configparser.ConfigParser) -> List[Dict[str, Any]]:
    """
    Read all [MAIL2LEDGER_LABEL_*] sections:
      - LABEL_NAME (required)
      - CLIENT_ID  (required, int)
      - DEFAULT_BANK (optional, 3-char code)
      - QUERY (optional, overrides GMAIL_QUERY_BASE for this label)
    """
    out: List[Dict[str, Any]] = []
    for sec in cfg.sections():
        if not sec.startswith("MAIL2LEDGER_LABEL_"):
            continue

        s = cfg[sec]
        label_name = _strip(s.get("LABEL_NAME", fallback=""))
        client_id = _strip(s.get("CLIENT_ID", fallback=""))
        default_bank = _strip(s.get("DEFAULT_BANK", fallback=""))
        query_override = _strip(s.get("QUERY", fallback=""))

        if not label_name or not client_id.isdigit():
            continue

        out.append(
            {
                "section": sec,
                "label_name": label_name,
                "client_id": int(client_id),
                "default_bank": default_bank or None,
                "query": query_override or None,
            }
        )
    return out


# ------------------------
# Main entry: run once
# ------------------------

def run_once(
    *,
    env_path: Optional[str] = None,
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
) -> Dict[str, int]:
    """
    Real-time (polling) style runner:
    - For each label entry:
        search unread in that label
        set client_id / default bank if missing
        ingest -> mark read only if inserted>0
        on fail -> alert email to configured ALERT_TO
        on success -> receipt email to sender
    """
    env_path = env_path or os.getenv("MAIL2LEDGER_ENV_PATH") or "/Users/HuJ/Documents/Mail2Ledger/.env"
    cfg = _load_cfg(env_path)

    if not cfg.has_section("MAIL2LEDGER"):
        raise RuntimeError(f"Missing [MAIL2LEDGER] section in env: {env_path}")

    base = cfg["MAIL2LEDGER"]
    notify = cfg["MAIL2LEDGER_NOTIFY"] if cfg.has_section("MAIL2LEDGER_NOTIFY") else None

    # Base email scan config
    base_query = _strip(base.get("GMAIL_QUERY_BASE", fallback="is:unread has:attachment filename:xlsx"))
    max_results = _get_int(base, "MAX_RESULTS", 50)
    allow_xls = _get_bool(base, "ALLOW_XLS", False)

    # Ingest config (used by LedgerIngestService)
    schema = _strip(base.get("SCHEMA", fallback="daily"))
    table = _strip(base.get("TABLE", fallback="statement_txn"))
    batch_size = _get_int(base, "BATCH_SIZE", 200)
    db_page_size = _get_int(base, "DB_PAGE_SIZE", 50)
    db_retry = _get_int(base, "DB_RETRY", 1)

    # Notify config
    send_alerts = _get_bool(notify, "SEND_ALERTS", True)
    alert_to = _strip(notify.get("ALERT_TO", fallback="")) if notify else ""
    send_receipt = _get_bool(notify, "SEND_RECEIPT", True)
    receipt_prefix = _strip(notify.get("RECEIPT_SUBJECT_PREFIX", fallback="[Mail2Ledger]")) if notify else "[Mail2Ledger]"

    label_entries = _iter_label_entries(cfg)
    if not label_entries:
        raise RuntimeError("No [MAIL2LEDGER_LABEL_*] sections found. Add at least one label entry.")

    # Gmail service (one per run)
    service = _load_gmail_service(credentials_path=credentials_path, token_path=token_path)

    # Ledger ingest service (shares same cfg object; no re-read)
    ingest_service = LedgerIngestService(
        schema=schema,
        table=table,
        batch_size=batch_size,
        db_page_size=db_page_size,
        db_retry=db_retry,
        config=cfg,
        env_path=env_path,
    )

    processed_message_ids: set[str] = set()

    emails_processed = 0
    rows_inserted = 0
    emails_failed = 0
    emails_skipped = 0

    for entry in label_entries:
        label_name = entry["label_name"]
        client_id = entry["client_id"]
        default_bank = entry["default_bank"]
        label_query = entry["query"] or base_query

        # resolve label id
        lids = resolve_label_ids(service, [label_name])
        if not lids:
            logger.warning(f"[label] not found: {label_name} (skip)")
            continue
        label_id = lids[0]

        ids = search_message_ids(
            service,
            query=label_query,
            max_results=max_results,
            label_ids=[label_id],
        )

        # Process oldest -> newest
        for mid in reversed(ids):
            if mid in processed_message_ids:
                continue

            subject = None
            sender = None
            date = None

            try:
                msg = get_message(service, mid)

                subject = get_header(msg, "Subject")
                sender = get_header(msg, "From")
                date = get_header(msg, "Date")

                body_text = extract_body_text(msg)
                meta = parse_email_meta(body_text).__dict__

                # label-level client_id override if missing
                if meta.get("client_id") is None:
                    meta["client_id"] = client_id

                # label-level default bank if missing
                if (meta.get("bank_name") is None or str(meta.get("bank_name")).strip() == "") and default_bank:
                    meta["bank_name"] = default_bank

                attachments = download_excel_attachments_in_memory(service, msg, allow_xls=allow_xls)

                job: Dict[str, Any] = {
                    "message_id": mid,
                    "subject": subject,
                    "from": sender,
                    "date": date,
                    "meta": meta,
                    "attachments": attachments,
                    "body": body_text,
                }

                if not attachments:
                    emails_skipped += 1
                    logger.warning(f"[email] skip(no attachments) label={label_name} message_id={mid} subject={subject}")
                    processed_message_ids.add(mid)
                    continue

                inserted = ingest_service.ingest_email_job(job)

                # inserted==0 => treat as failed (do NOT mark read)
                if not inserted or inserted <= 0:
                    emails_failed += 1
                    err = "Ingest returned 0 rows (likely header_row/sheet_names/bank_name mismatch)."
                    logger.error(
                        f"[email] {err} label={label_name} message_id={mid} subject={subject}"
                    )

                    # ✅ ALERT (immediate)
                    if send_alerts and alert_to:
                        body = (
                            "Mail2Ledger FAILED\n"
                            f"label={label_name}\n"
                            f"client_id={meta.get('client_id')}\n"
                            f"bank={meta.get('bank_name')}\n"
                            f"message_id={mid}\n"
                            f"from={sender}\n"
                            f"subject={subject}\n"
                            f"error={err}\n"
                            f"attachments={[a.get('filename') for a in attachments]}\n"
                        )
                        try:
                            send_email_text(
                                alert_to,
                                f"[ALERT] Ingest failed ({label_name})",
                                body,
                                credentials_path=credentials_path,
                                token_path=token_path,
                            )
                        except Exception as mail_err:
                            logger.exception(f"[alert-email] failed to send alert: {mail_err}")

                    # do NOT mark read; do NOT add to processed set (so it retries next run)
                    continue

                # ✅ Success: mark read first (so we don't reprocess if receipt fails)
                rows_inserted += int(inserted)
                mark_message_as_read(service, mid)

                emails_processed += 1
                processed_message_ids.add(mid)

                logger.info(
                    f"[email] ok label={label_name} message_id={mid} client_id={meta.get('client_id')} "
                    f"bank={meta.get('bank_name')} inserted_rows={inserted}"
                )

                # ✅ Receipt email to sender
                if send_receipt:
                    to_sender = _extract_email(sender)
                    if to_sender:
                        receipt_subject = f"{receipt_prefix} Processed: {subject or ''}".strip()
                        receipt_body = (
                            "Your file(s) have been processed successfully.\n\n"
                            f"label={label_name}\n"
                            f"client_id={meta.get('client_id')}\n"
                            f"bank={meta.get('bank_name')}\n"
                            f"message_id={mid}\n"
                            f"inserted_rows={inserted}\n"
                            f"attachments={[a.get('filename') for a in attachments]}\n"
                        )
                        try:
                            send_email_text(
                                to_sender,
                                receipt_subject,
                                receipt_body,
                                credentials_path=credentials_path,
                                token_path=token_path,
                            )
                        except Exception as mail_err:
                            # Receipt failing shouldn't block processing; optionally alert yourself
                            logger.exception(f"[receipt-email] failed to send receipt to {to_sender}: {mail_err}")
                            if send_alerts and alert_to:
                                try:
                                    send_email_text(
                                        alert_to,
                                        f"[ALERT] Receipt send failed ({label_name})",
                                        f"Receipt failed\nmessage_id={mid}\nto={to_sender}\nsubject={subject}\nerror={mail_err}\n",
                                        credentials_path=credentials_path,
                                        token_path=token_path,
                                    )
                                except Exception:
                                    pass

            except Exception as e:
                emails_failed += 1
                err = str(e)

                logger.exception(f"[email] failed label={label_name} message_id={mid}: {e}")

                # ✅ ALERT (immediate)
                if send_alerts and alert_to:
                    body = (
                        "Mail2Ledger FAILED\n"
                        f"label={label_name}\n"
                        f"client_id={client_id}\n"
                        f"default_bank={default_bank}\n"
                        f"message_id={mid}\n"
                        f"from={sender}\n"
                        f"subject={subject}\n"
                        f"error={err}\n"
                    )
                    try:
                        send_email_text(
                            alert_to,
                            f"[ALERT] Ingest failed ({label_name})",
                            body,
                            credentials_path=credentials_path,
                            token_path=token_path,
                        )
                    except Exception as mail_err:
                        logger.exception(f"[alert-email] failed to send alert: {mail_err}")

                # do NOT mark read; do NOT add to processed set
                continue

    return {
        "emails_processed": emails_processed,
        "rows_inserted": rows_inserted,
        "emails_failed": emails_failed,
        "emails_skipped": emails_skipped,
    }
