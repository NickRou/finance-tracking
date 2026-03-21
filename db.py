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
                account_type TEXT NOT NULL CHECK (account_type IN ('credit_card', 'savings_account', 'investment_account')),
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS investment_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                asset_type TEXT NOT NULL CHECK (
                    asset_type IN ('cash', 'stock_etf', 'crypto')
                ),
                valuation_method TEXT NOT NULL DEFAULT 'market' CHECK (
                    valuation_method IN ('market', 'manual')
                ),
                symbol TEXT,
                name TEXT NOT NULL,
                quantity REAL,
                cost_basis_total_cents INTEGER,
                manual_market_value_cents INTEGER,
                cash_balance_cents INTEGER,
                currency TEXT NOT NULL DEFAULT 'USD',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(account_id) REFERENCES accounts(id),
                CHECK (
                    (
                        asset_type = 'cash'
                        AND valuation_method = 'manual'
                        AND symbol IS NULL
                        AND quantity IS NULL
                        AND cost_basis_total_cents IS NULL
                        AND manual_market_value_cents IS NULL
                        AND cash_balance_cents IS NOT NULL
                    )
                    OR
                    (
                        asset_type != 'cash'
                        AND valuation_method = 'market'
                        AND symbol IS NOT NULL
                        AND quantity IS NOT NULL
                        AND quantity > 0
                        AND cost_basis_total_cents IS NOT NULL
                        AND cost_basis_total_cents > 0
                        AND manual_market_value_cents IS NULL
                        AND cash_balance_cents IS NULL
                    )
                    OR
                    (
                        asset_type != 'cash'
                        AND valuation_method = 'manual'
                        AND symbol IS NULL
                        AND quantity IS NULL
                        AND cost_basis_total_cents IS NOT NULL
                        AND cost_basis_total_cents > 0
                        AND manual_market_value_cents IS NOT NULL
                        AND manual_market_value_cents >= 0
                        AND cash_balance_cents IS NULL
                    )
                )
            )
            """
        )
        _migrate_investment_holdings_table(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS investment_holding_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                holding_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                snapshot_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL CHECK (
                    event_type IN ('add', 'manual_update', 'delete', 'backfill')
                ),
                asset_type TEXT NOT NULL,
                valuation_method TEXT NOT NULL,
                symbol TEXT,
                name TEXT NOT NULL,
                quantity REAL,
                cost_basis_total_cents INTEGER,
                manual_market_value_cents INTEGER,
                cash_balance_cents INTEGER,
                market_value_cents INTEGER,
                currency TEXT NOT NULL DEFAULT 'USD'
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_holding_snapshots_holding_ts
            ON investment_holding_snapshots (holding_id, snapshot_ts)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_holding_snapshots_account_ts
            ON investment_holding_snapshots (account_id, snapshot_ts)
            """
        )
        _backfill_investment_holding_snapshots(conn)


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


def _migrate_investment_holdings_table(conn: Any) -> None:
    columns = _table_columns(conn, "investment_holdings")
    required = {"valuation_method", "manual_market_value_cents"}
    if required.issubset(columns):
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS investment_holdings_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL CHECK (
                asset_type IN ('cash', 'stock_etf', 'crypto')
            ),
            valuation_method TEXT NOT NULL DEFAULT 'market' CHECK (
                valuation_method IN ('market', 'manual')
            ),
            symbol TEXT,
            name TEXT NOT NULL,
            quantity REAL,
            cost_basis_total_cents INTEGER,
            manual_market_value_cents INTEGER,
            cash_balance_cents INTEGER,
            currency TEXT NOT NULL DEFAULT 'USD',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id),
            CHECK (
                (
                    asset_type = 'cash'
                    AND valuation_method = 'manual'
                    AND symbol IS NULL
                    AND quantity IS NULL
                    AND cost_basis_total_cents IS NULL
                    AND manual_market_value_cents IS NULL
                    AND cash_balance_cents IS NOT NULL
                )
                OR
                (
                    asset_type != 'cash'
                    AND valuation_method = 'market'
                    AND symbol IS NOT NULL
                    AND quantity IS NOT NULL
                    AND quantity > 0
                    AND cost_basis_total_cents IS NOT NULL
                    AND cost_basis_total_cents > 0
                    AND manual_market_value_cents IS NULL
                    AND cash_balance_cents IS NULL
                )
                OR
                (
                    asset_type != 'cash'
                    AND valuation_method = 'manual'
                    AND symbol IS NULL
                    AND quantity IS NULL
                    AND cost_basis_total_cents IS NOT NULL
                    AND cost_basis_total_cents > 0
                    AND manual_market_value_cents IS NOT NULL
                    AND manual_market_value_cents >= 0
                    AND cash_balance_cents IS NULL
                )
            )
        )
        """
    )

    conn.execute(
        """
        INSERT INTO investment_holdings_v2 (
            id,
            account_id,
            asset_type,
            valuation_method,
            symbol,
            name,
            quantity,
            cost_basis_total_cents,
            manual_market_value_cents,
            cash_balance_cents,
            currency,
            created_at,
            updated_at
        )
        SELECT
            id,
            account_id,
            asset_type,
            CASE WHEN asset_type = 'cash' THEN 'manual' ELSE 'market' END,
            symbol,
            name,
            quantity,
            cost_basis_total_cents,
            NULL,
            cash_balance_cents,
            currency,
            created_at,
            updated_at
        FROM investment_holdings
        """
    )
    conn.execute("DROP TABLE investment_holdings")
    conn.execute("ALTER TABLE investment_holdings_v2 RENAME TO investment_holdings")


def _snapshot_market_value_cents_from_row(row: dict[str, Any]) -> int | None:
    asset_type = str(row.get("asset_type") or "")
    valuation_method = str(row.get("valuation_method") or "")
    if asset_type == "cash":
        value = row.get("cash_balance_cents")
        return int(value) if value is not None else 0
    if valuation_method == "manual":
        value = row.get("manual_market_value_cents")
        return int(value) if value is not None else 0
    return None


def _backfill_investment_holding_snapshots(conn: Any) -> None:
    snapshot_count = int(
        conn.execute("SELECT COUNT(*) FROM investment_holding_snapshots").fetchone()[0]
    )
    if snapshot_count > 0:
        return

    rows = conn.execute(
        """
        SELECT
            id,
            account_id,
            asset_type,
            valuation_method,
            symbol,
            name,
            quantity,
            cost_basis_total_cents,
            manual_market_value_cents,
            cash_balance_cents,
            currency
        FROM investment_holdings
        """
    ).fetchall()
    if not rows:
        return

    for row in rows:
        row_data: dict[str, Any] = {
            "holding_id": int(row[0]),
            "account_id": int(row[1]),
            "asset_type": str(row[2]),
            "valuation_method": str(row[3]),
            "symbol": str(row[4]) if row[4] is not None else None,
            "name": str(row[5]),
            "quantity": float(row[6]) if row[6] is not None else None,
            "cost_basis_total_cents": int(row[7]) if row[7] is not None else None,
            "manual_market_value_cents": int(row[8]) if row[8] is not None else None,
            "cash_balance_cents": int(row[9]) if row[9] is not None else None,
            "currency": str(row[10]),
        }
        create_investment_holding_snapshot(
            holding_id=row_data["holding_id"],
            account_id=row_data["account_id"],
            event_type="backfill",
            asset_type=row_data["asset_type"],
            valuation_method=row_data["valuation_method"],
            symbol=row_data["symbol"],
            name=row_data["name"],
            quantity=row_data["quantity"],
            cost_basis_total_cents=row_data["cost_basis_total_cents"],
            manual_market_value_cents=row_data["manual_market_value_cents"],
            cash_balance_cents=row_data["cash_balance_cents"],
            market_value_cents=_snapshot_market_value_cents_from_row(row_data),
            currency=row_data["currency"],
            conn=conn,
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


def list_transaction_accounts() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, institution, account_type
            FROM accounts
            WHERE account_type IN ('credit_card', 'savings_account')
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


def list_investment_accounts() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, institution, account_type
            FROM accounts
            WHERE account_type IN ('investment_account', 'savings_account')
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


def list_investment_holdings() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                h.id,
                h.account_id,
                a.name AS account_name,
                a.institution,
                h.asset_type,
                h.valuation_method,
                h.symbol,
                h.name,
                h.quantity,
                h.cost_basis_total_cents,
                h.manual_market_value_cents,
                h.cash_balance_cents,
                h.currency
            FROM investment_holdings h
            INNER JOIN accounts a ON a.id = h.account_id
            ORDER BY a.institution ASC, a.name ASC, h.asset_type ASC, h.name ASC
            """
        ).fetchall()

    return [
        {
            "id": int(row[0]),
            "account_id": int(row[1]),
            "account_name": str(row[2]),
            "institution": str(row[3]),
            "asset_type": str(row[4]),
            "valuation_method": str(row[5]),
            "symbol": str(row[6]) if row[6] is not None else None,
            "name": str(row[7]),
            "quantity": float(row[8]) if row[8] is not None else None,
            "cost_basis_total_cents": int(row[9]) if row[9] is not None else None,
            "manual_market_value_cents": int(row[10]) if row[10] is not None else None,
            "cash_balance_cents": int(row[11]) if row[11] is not None else None,
            "currency": str(row[12]),
        }
        for row in rows
    ]


def create_investment_holding(
    *,
    account_id: int,
    asset_type: str,
    valuation_method: str,
    symbol: str | None,
    name: str,
    quantity: float | None,
    cost_basis_total_cents: int | None,
    manual_market_value_cents: int | None,
    cash_balance_cents: int | None,
    currency: str = "USD",
) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO investment_holdings (
                account_id,
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                currency,
            ),
        )
        return int(cursor.lastrowid)


def update_investment_holding(
    *,
    holding_id: int,
    asset_type: str,
    valuation_method: str,
    symbol: str | None,
    name: str,
    quantity: float | None,
    cost_basis_total_cents: int | None,
    manual_market_value_cents: int | None,
    cash_balance_cents: int | None,
) -> int:
    with get_connection() as conn:
        before = conn.total_changes
        conn.execute(
            """
            UPDATE investment_holdings
            SET
                asset_type = ?,
                valuation_method = ?,
                symbol = ?,
                name = ?,
                quantity = ?,
                cost_basis_total_cents = ?,
                manual_market_value_cents = ?,
                cash_balance_cents = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                holding_id,
            ),
        )
        return conn.total_changes - before


def get_investment_holding_by_id(holding_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                id,
                account_id,
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                currency
            FROM investment_holdings
            WHERE id = ?
            """,
            (holding_id,),
        ).fetchone()

    if row is None:
        return None

    return {
        "id": int(row[0]),
        "account_id": int(row[1]),
        "asset_type": str(row[2]),
        "valuation_method": str(row[3]),
        "symbol": str(row[4]) if row[4] is not None else None,
        "name": str(row[5]),
        "quantity": float(row[6]) if row[6] is not None else None,
        "cost_basis_total_cents": int(row[7]) if row[7] is not None else None,
        "manual_market_value_cents": int(row[8]) if row[8] is not None else None,
        "cash_balance_cents": int(row[9]) if row[9] is not None else None,
        "currency": str(row[10]),
    }


def create_investment_holding_snapshot(
    *,
    holding_id: int,
    account_id: int,
    event_type: str,
    asset_type: str,
    valuation_method: str,
    symbol: str | None,
    name: str,
    quantity: float | None,
    cost_basis_total_cents: int | None,
    manual_market_value_cents: int | None,
    cash_balance_cents: int | None,
    market_value_cents: int | None,
    currency: str = "USD",
    conn: Any | None = None,
) -> None:
    params = (
        holding_id,
        account_id,
        event_type,
        asset_type,
        valuation_method,
        symbol,
        name,
        quantity,
        cost_basis_total_cents,
        manual_market_value_cents,
        cash_balance_cents,
        market_value_cents,
        currency,
    )

    if conn is not None:
        conn.execute(
            """
            INSERT INTO investment_holding_snapshots (
                holding_id,
                account_id,
                event_type,
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                market_value_cents,
                currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        return

    with get_connection() as snapshot_conn:
        snapshot_conn.execute(
            """
            INSERT INTO investment_holding_snapshots (
                holding_id,
                account_id,
                event_type,
                asset_type,
                valuation_method,
                symbol,
                name,
                quantity,
                cost_basis_total_cents,
                manual_market_value_cents,
                cash_balance_cents,
                market_value_cents,
                currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )


def delete_investment_holdings(holding_ids: list[int]) -> int:
    if not holding_ids:
        return 0

    placeholders = ",".join(["?"] * len(holding_ids))
    with get_connection() as conn:
        before = conn.total_changes
        conn.execute(
            f"DELETE FROM investment_holdings WHERE id IN ({placeholders})",
            tuple(holding_ids),
        )
        return conn.total_changes - before
