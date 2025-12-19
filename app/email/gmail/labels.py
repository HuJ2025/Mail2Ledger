# app/email/gmail/labels.py

from __future__ import annotations
from typing import Dict, List, Optional


def list_labels(service, user_id: str = "me") -> List[dict]:
    resp = service.users().labels().list(userId=user_id).execute()
    return resp.get("labels", []) or []


def get_label_id(service, label_name: str, user_id: str = "me") -> Optional[str]:
    """
    label_name 是 Gmail UI 里看到的名称（例如：Mail2Ledger/ToProcess）
    返回 labelId（例如：Label_123 或系统的 INBOX 等）
    """
    for lab in list_labels(service, user_id=user_id):
        if lab.get("name") == label_name:
            return lab.get("id")
    return None


def resolve_label_ids(service, label_names: List[str], user_id: str = "me") -> List[str]:
    """
    多个 label names -> 多个 label ids
    找不到就直接跳过（你也可以改成 raise）
    """
    name_to_id: Dict[str, str] = {}
    for lab in list_labels(service, user_id=user_id):
        name = lab.get("name")
        lid = lab.get("id")
        if name and lid:
            name_to_id[name] = lid

    out: List[str] = []
    for n in label_names:
        lid = name_to_id.get(n)
        if lid:
            out.append(lid)
    return out
