# app/email/gmail/client.py

import os
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ✅ 可标记已读 / 加 label
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


def _load_gmail_service(
    credentials_path: str = "credentials.json",
    token_path: str = "token.json",
):
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


# ✅ 对外暴露一个“非私有名”，但保留旧 _load_ 兼容你旧代码
load_gmail_service = _load_gmail_service
