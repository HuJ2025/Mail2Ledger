# app/services/db.py
from psycopg2.pool import ThreadedConnectionPool
import os
from dotenv import load_dotenv
import configparser

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional
from langchain_community.utilities import SQLDatabase

load_dotenv()
_cfg = configparser.ConfigParser()
_cfg.read(os.getenv("DOTENV_PATH", ".env"))
aws_section = _cfg["AWS_DB"] if "AWS_DB" in _cfg else {}

# helper to fetch var with optional fallback key
_get = lambda key, default=None: os.getenv(key) or aws_section.get(key, default)

DB_HOST = _get("HOST", "localhost")
DB_PORT = _get("PORT", "5432")
DB_NAME = _get("DATABASE")
DB_USER = aws_section.get("USER") or os.getenv("DB_USER") or os.getenv("USER", "postgres")
DB_PASS = aws_section.get("PASSWORD") or os.getenv("DB_PASSWORD") or os.getenv("PASSWORD", "")
DB_SSL  = _get("SSL_MODE", "disable")        # 本地默认用 disable，而不是 require

_pool = ThreadedConnectionPool(
    minconn=2,
    maxconn=20,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
)

def get_conn():
    """Checkout a connection; caller **must** put it back."""
    return _pool.getconn()


def put_conn(conn):
    _pool.putconn(conn)

# ---- central URI helper ----
def get_db_uri() -> str:
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSL}"

# ---- singleton SQLAlchemy engine ----
_engine: Optional[Engine] = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_db_uri(),
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            future=True,
        )
    return _engine

# ---- singleton LangChain SQLDatabase (built on the engine) ----
_sql_db: Optional[SQLDatabase] = None

def get_sql_database() -> SQLDatabase:
    global _sql_db
    if _sql_db is None:
        # SQLDatabase can be constructed with an existing SQLAlchemy engine
        _sql_db = SQLDatabase(engine=get_engine())
    return _sql_db
