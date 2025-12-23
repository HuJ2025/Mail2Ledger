# app/utils/retry_on_db_failure.py
import time
import logging
import psycopg2
from functools import wraps

RETRYABLE_DB_ERRORS = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
)

TRANSIENT_ERROR_PATTERNS = [
    "SSL SYSCALL error",
    "SSL error: decryption failed",
    "server closed the connection unexpectedly",
    "bad record mac",
    "connection already closed",
    "could not receive data from server",
    "ssl syscall error: eof detected",
]

def is_retryable_db_error(e: Exception) -> bool:
    return isinstance(e, RETRYABLE_DB_ERRORS) and any(msg in str(e) for msg in TRANSIENT_ERROR_PATTERNS)

def retry_on_db_failure(max_retries=5, delay_seconds=2):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if is_retryable_db_error(e):
                        logging.warning(f"⚠️ Retryable DB error on attempt {attempt}: {e}")
                        if attempt == max_retries:
                            logging.error("❌ Exhausted retries for DB operation")
                            raise
                        time.sleep(delay_seconds * attempt)
                    else:
                        raise
        return wrapped
    return decorator
