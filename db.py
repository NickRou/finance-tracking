from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

from sqlcipher3 import dbapi2 as sqlite


def _escaped_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _db_path() -> Path:
    raw_path = os.getenv("FINANCE_DB_PATH", "data/finance.db")
    path = Path(raw_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_connection() -> Any:
    key = os.getenv("FINANCE_DB_KEY")
    if not key:
        raise RuntimeError("FINANCE_DB_KEY is not set")

    connect = getattr(sqlite, "connect")
    conn = connect(_db_path())
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
                institution TEXT NOT NULL,
                occurred_on TEXT NOT NULL,
                posted_on TEXT,
                amount_cents INTEGER NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '',
                category_raw TEXT,
                external_id TEXT NOT NULL DEFAULT '',
                source_file TEXT,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _migrate_transactions_table(conn)
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_dedupe
            ON transactions (institution, occurred_on, amount_cents, description, external_id)
            """
        )


def _table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _add_missing_columns(
    conn: Any,
    existing_columns: set[str],
    definitions: Iterable[tuple[str, str]],
) -> None:
    for name, ddl in definitions:
        if name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE transactions ADD COLUMN {ddl}")


def _migrate_transactions_table(conn: Any) -> None:
    columns = _table_columns(conn, "transactions")
    missing = [
        ("institution", "institution TEXT NOT NULL DEFAULT 'unknown'"),
        ("posted_on", "posted_on TEXT"),
        ("description", "description TEXT NOT NULL DEFAULT ''"),
        ("category", "category TEXT NOT NULL DEFAULT ''"),
        ("category_raw", "category_raw TEXT"),
        ("external_id", "external_id TEXT NOT NULL DEFAULT ''"),
        ("source_file", "source_file TEXT"),
        ("imported_at", "imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ]
    _add_missing_columns(conn, columns, missing)

    columns = _table_columns(conn, "transactions")
    if "category" in columns and "category_raw" in columns:
        conn.execute(
            "UPDATE transactions SET category_raw = category WHERE category_raw IS NULL OR category_raw = ''"
        )
    if "category" in columns and "category_raw" in columns:
        conn.execute(
            "UPDATE transactions SET category = category_raw WHERE category = ''"
        )
