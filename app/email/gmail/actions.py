# app/email/gmail/actions.py

def mark_message_as_read(service, message_id: str, user_id: str = "me") -> None:
    """
    标记为已读：移除 UNREAD label
    """
    service.users().messages().modify(
        userId=user_id,
        id=message_id,
        body={"removeLabelIds": ["UNREAD"], "addLabelIds": []},
    ).execute()
