# app/email/gmail/reader.py

import base64
import re
from typing import Any, Dict, List, Optional


def search_message_ids(
    service,
    query: str,
    user_id: str = "me",
    max_results: int = 20,
    label_ids: Optional[List[str]] = None,   # ✅ 新增
) -> List[str]:
    ids: List[str] = []
    page_token = None
    while True:
        resp = service.users().messages().list(
            userId=user_id,
            q=query,
            maxResults=min(max_results, 500),
            pageToken=page_token,
            labelIds=label_ids,              # ✅ 新增：Gmail API 原生筛 label
        ).execute()

        msgs = resp.get("messages", [])
        ids.extend([m["id"] for m in msgs])

        if len(ids) >= max_results:
            return ids[:max_results]

        page_token = resp.get("nextPageToken")
        if not page_token:
            return ids


def get_message(service, message_id: str, user_id: str = "me") -> Dict[str, Any]:
    return service.users().messages().get(userId=user_id, id=message_id, format="full").execute()


def get_header(msg: Dict[str, Any], name: str) -> Optional[str]:
    headers = msg.get("payload", {}).get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value")
    return None


def walk_parts(parts: List[Dict[str, Any]]):
    for p in parts or []:
        yield p
        if p.get("parts"):
            yield from walk_parts(p["parts"])


def extract_body_text(msg: Dict[str, Any]) -> str:
    """
    尽量抽取 text/plain；没有就退回 text/html（粗略去 tag）
    """
    payload = msg.get("payload", {})
    parts = payload.get("parts")

    texts = []
    htmls = []

    if parts:
        for p in walk_parts(parts):
            mime = p.get("mimeType")
            body = p.get("body", {})
            data = body.get("data")
            if not data:
                continue

            raw = base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="ignore")
            if mime == "text/plain":
                texts.append(raw)
            elif mime == "text/html":
                htmls.append(raw)
    else:
        body = payload.get("body", {})
        data = body.get("data")
        if data:
            raw = base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="ignore")
            if payload.get("mimeType") == "text/plain":
                texts.append(raw)
            else:
                htmls.append(raw)

    if texts:
        return "\n".join(texts).strip()

    if htmls:
        html = "\n".join(htmls)
        html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        html = re.sub(r"</p\s*>", "\n", html, flags=re.I)
        html = re.sub(r"<[^>]+>", "", html)
        return html.strip()

    return ""
