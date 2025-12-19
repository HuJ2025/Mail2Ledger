# app/repo/ingest_registry_repo.py

from typing import Optional


def log_ingest_file_registry(
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
    """
    Insert ONE record into daily.ingest_file_registry.

    De-dupe strategy (no need UNIQUE constraint):
    - if message_id exists: de-dupe by (message_id, attachment_name)
    - else if sha256 exists: de-dupe by (client_id, attachment_sha256)
    - else: insert as-is
    """
    if not attachment_name:
        return

    params = dict(
        client_id=client_id,
        bank_name=bank_name,
        account_canonical=account_canonical,
        label_name=label_name,
        message_id=message_id,
        email_from=email_from,
        email_subject=email_subject,
        email_date_raw=email_date_raw,
        attachment_name=attachment_name,
        attachment_sha256=attachment_sha256,
        schema_name=schema_name,
        table_name=table_name,
    )

    if message_id:
        sql = """
        INSERT INTO daily.ingest_file_registry
          (client_id, bank_name, account_canonical, label_name, message_id,
           email_from, email_subject, email_date_raw,
           attachment_name, attachment_sha256, schema_name, table_name)
        SELECT
          %(client_id)s, %(bank_name)s, %(account_canonical)s, %(label_name)s, %(message_id)s,
          %(email_from)s, %(email_subject)s, %(email_date_raw)s,
          %(attachment_name)s, %(attachment_sha256)s, %(schema_name)s, %(table_name)s
        WHERE NOT EXISTS (
          SELECT 1 FROM daily.ingest_file_registry
          WHERE message_id = %(message_id)s
            AND attachment_name = %(attachment_name)s
        );
        """
        gw.execute_update(sql, params)
        return

    if attachment_sha256:
        sql = """
        INSERT INTO daily.ingest_file_registry
          (client_id, bank_name, account_canonical, label_name, message_id,
           email_from, email_subject, email_date_raw,
           attachment_name, attachment_sha256, schema_name, table_name)
        SELECT
          %(client_id)s, %(bank_name)s, %(account_canonical)s, %(label_name)s, %(message_id)s,
          %(email_from)s, %(email_subject)s, %(email_date_raw)s,
          %(attachment_name)s, %(attachment_sha256)s, %(schema_name)s, %(table_name)s
        WHERE NOT EXISTS (
          SELECT 1 FROM daily.ingest_file_registry
          WHERE client_id = %(client_id)s
            AND attachment_sha256 = %(attachment_sha256)s
        );
        """
        gw.execute_update(sql, params)
        return

    # fallback: no de-dupe keys available
    sql = """
    INSERT INTO daily.ingest_file_registry
      (client_id, bank_name, account_canonical, label_name, message_id,
       email_from, email_subject, email_date_raw,
       attachment_name, attachment_sha256, schema_name, table_name)
    VALUES
      (%(client_id)s, %(bank_name)s, %(account_canonical)s, %(label_name)s, %(message_id)s,
       %(email_from)s, %(email_subject)s, %(email_date_raw)s,
       %(attachment_name)s, %(attachment_sha256)s, %(schema_name)s, %(table_name)s);
    """
    gw.execute_update(sql, params)
