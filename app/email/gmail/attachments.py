# app/email/gmail/attachments.py

import base64
import hashlib
from typing import Any, Dict, List
from app.email.gmail.reader import walk_parts

def download_excel_attachments_in_memory(
    service,
    msg: Dict[str, Any],
    user_id: str = "me",
    allow_xls: bool = False,  # ✅ 默认只支持 xlsx
) -> List[Dict[str, Any]]:
    """
    Return:
      [{
        "filename": "...xlsx",
        "content_bytes": b"...",
        "sha256": "...",
        "size": 12345
      }]
    """
    payload = msg.get("payload", {})
    parts = payload.get("parts", [])
    out: List[Dict[str, Any]] = []

    for p in walk_parts(parts):
        filename = p.get("filename") or ""
        if not filename:
            continue

        lower = filename.lower()
        ok = lower.endswith(".xlsx") or (allow_xls and lower.endswith(".xls"))
        if not ok:
            continue

        body = p.get("body", {})
        att_id = body.get("attachmentId")
        data = body.get("data")

        if att_id:
            att = (
                service.users()
                .messages()
                .attachments()
                .get(userId=user_id, messageId=msg["id"], id=att_id)
                .execute()
            )
            data = att.get("data")

        if not data:
            continue

        file_bytes = base64.urlsafe_b64decode(data.encode("utf-8"))
        sha256 = hashlib.sha256(file_bytes).hexdigest()

        out.append({
            "filename": filename,
            "content_bytes": file_bytes,
            "sha256": sha256,
            "size": len(file_bytes),
        })

    return out
