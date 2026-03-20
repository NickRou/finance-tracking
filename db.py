from __future__ import annotations

import os
from pathlib import Path

from sqlcipher3 import dbapi2 as sqlite


def _escaped_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _db_path() -> Path:
    raw_path = os.getenv("FINANCE_DB_PATH", "data/finance.db")
    path = Path(raw_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_connection() -> sqlite.Connection:
    key = os.getenv("FINANCE_DB_KEY")
    if not key:
        raise RuntimeError("FINANCE_DB_KEY is not set")

    conn = sqlite.connect(_db_path())
    conn.execute(f"PRAGMA key = '{_escaped_sql_literal(key)}'")
    conn.execute("PRAGMA cipher_page_size = 4096")
    conn.execute("PRAGMA kdf_iter = 256000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("SELECT count(*) FROM sqlite_master")
    return conn


def initialize_database() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_on TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                category TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
