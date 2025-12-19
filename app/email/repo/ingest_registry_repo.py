# app/email/repo/ingest_registry_repo.py

from typing import Optional

def log_processed_file(
    gw,
    *,
    client_id: int,
    bank_name: str,
    account_canonical: Optional[str],
    label_name: Optional[str],
    message_id: Optional[str],
    email_from: Optional[str],
    email_subject: Optional[str],
    email_date_raw: Optional[str],
    attachment_name: str,
    attachment_sha256: Optional[str],
    schema_name: str = "daily",
    table_name: str = "statement_txn",
) -> None:
    sql = """
    INSERT INTO daily.ingest_file_registry
      (client_id, bank_name, account_canonical, label_name, message_id,
       email_from, email_subject, email_date_raw,
       attachment_name, attachment_sha256, schema_name, table_name)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (message_id, attachment_name) DO NOTHING;
    """
    gw.execute_update(sql, (
        client_id, bank_name, account_canonical, label_name, message_id,
        email_from, email_subject, email_date_raw,
        attachment_name, attachment_sha256, schema_name, table_name
    ))
