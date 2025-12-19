# app/agents/gmail_ingest.py
# 兼容层：把旧 import 指向新实现

from app.email.gmail.client import _load_gmail_service  # 兼容旧名
from app.email.ingest.pipeline import fetch_email_jobs, run_once
from app.email.gmail.actions import mark_message_as_read

__all__ = [
    "_load_gmail_service",
    "fetch_email_jobs",
    "run_once",
    "mark_message_as_read",
]
