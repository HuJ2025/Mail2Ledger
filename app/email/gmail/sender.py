# app/email/gmail/sender.py

import base64
from email.mime.text import MIMEText
from typing import Optional

from app.email.gmail.client import _load_gmail_service


def send_email_text(
    to_email: str,
    subject: str,
    body_text: str,
    *,
    from_name: Optional[str] = None,
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
) -> str:
    """
    Send a plain-text email using Gmail API.
    Returns sent message id.
    """
    service = _load_gmail_service(credentials_path=credentials_path, token_path=token_path)

    msg = MIMEText(body_text, "plain", "utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject
    if from_name:
        # Gmail will still use the authenticated account as the sender; this is display name hint
        msg["From"] = from_name

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return result["id"]


def send_error_text(
    to_email: str,
    subject: str,
    body_text: str,
    *,
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
) -> str:
    service = _load_gmail_service(credentials_path=credentials_path, token_path=token_path)

    msg = MIMEText(body_text, "plain", "utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return result["id"]