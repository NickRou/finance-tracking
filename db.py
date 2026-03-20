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
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                institution TEXT NOT NULL,
                account_type TEXT NOT NULL CHECK (account_type IN ('credit_card', 'savings_account')),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, institution)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                institution TEXT NOT NULL,
                occurred_on TEXT NOT NULL,
                posted_on TEXT,
                amount_cents INTEGER NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '',
                category_raw TEXT,
                external_id TEXT NOT NULL DEFAULT '',
                source_file TEXT,
                import_batch_id INTEGER,
                source_row_number INTEGER,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """
        )
        _migrate_transactions_table(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS import_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                institution TEXT NOT NULL,
                source_file TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, institution, file_hash),
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
            """
        )
        conn.execute("DROP INDEX IF EXISTS idx_transactions_dedupe")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_import_row
            ON transactions (import_batch_id, source_row_number)
            WHERE import_batch_id IS NOT NULL AND source_row_number IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statement_anchors (
                account_id INTEGER PRIMARY KEY,
                anchor_date TEXT NOT NULL,
                anchor_balance_cents INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
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
        ("account_id", "account_id INTEGER"),
        ("institution", "institution TEXT NOT NULL DEFAULT 'unknown'"),
        ("posted_on", "posted_on TEXT"),
        ("description", "description TEXT NOT NULL DEFAULT ''"),
        ("category", "category TEXT NOT NULL DEFAULT ''"),
        ("category_raw", "category_raw TEXT"),
        ("external_id", "external_id TEXT NOT NULL DEFAULT ''"),
        ("source_file", "source_file TEXT"),
        ("import_batch_id", "import_batch_id INTEGER"),
        ("source_row_number", "source_row_number INTEGER"),
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


def upsert_statement_anchor(
    *, account_id: int, anchor_date: str, anchor_balance_cents: int
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO statement_anchors (account_id, anchor_date, anchor_balance_cents)
            VALUES (?, ?, ?)
            ON CONFLICT(account_id)
            DO UPDATE SET
                anchor_date = excluded.anchor_date,
                anchor_balance_cents = excluded.anchor_balance_cents,
                updated_at = CURRENT_TIMESTAMP
            """,
            (account_id, anchor_date, anchor_balance_cents),
        )


def create_account(*, name: str, institution: str, account_type: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO accounts (name, institution, account_type)
            VALUES (?, ?, ?)
            """,
            (name, institution, account_type),
        )


def list_accounts() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, institution, account_type
            FROM accounts
            ORDER BY institution ASC, name ASC
            """
        ).fetchall()
    return [
        {
            "id": int(row[0]),
            "name": str(row[1]),
            "institution": str(row[2]),
            "account_type": str(row[3]),
        }
        for row in rows
    ]
