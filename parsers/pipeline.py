from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

from db import get_connection

from .registry import get_adapter


@dataclass(frozen=True)
class ImportSummary:
    parsed: int
    inserted: int
    duplicates: int
    invalid: int
    skipped_existing_file: bool = False


def import_csv(
    *,
    institution: str,
    account_id: int,
    file_path: str,
    source_filename: str | None = None,
) -> ImportSummary:
    adapter = get_adapter(institution)
    path = Path(file_path)
    source_file = source_filename or str(path.name)
    file_hash = hashlib.sha256(path.read_bytes()).hexdigest()

    parsed = 0
    inserted = 0
    invalid = 0

    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM import_batches WHERE account_id = ? AND institution = ? AND file_hash = ?",
            (account_id, institution, file_hash),
        ).fetchone()
        if existing is not None:
            return ImportSummary(
                parsed=0,
                inserted=0,
                duplicates=0,
                invalid=0,
                skipped_existing_file=True,
            )

        conn.execute(
            """
            INSERT INTO import_batches (account_id, institution, source_file, file_hash)
            VALUES (?, ?, ?, ?)
            """,
            (account_id, institution, source_file, file_hash),
        )
        import_batch_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            dialect = _detect_dialect(handle)
            rows = _iter_rows(adapter=adapter, handle=handle, dialect=dialect)
            for source_row_number, row in enumerate(rows, start=1):
                parsed += 1
                try:
                    record = adapter.parse_row(row, source_file)
                except ValueError:
                    invalid += 1
                    continue

                conn.execute(
                    """
                    INSERT INTO transactions (
                        account_id,
                        institution,
                        occurred_on,
                        posted_on,
                        amount_cents,
                        description,
                        category,
                        category_raw,
                        external_id,
                        source_file,
                        import_batch_id,
                        source_row_number
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        record.institution,
                        record.occurred_on,
                        record.posted_on,
                        record.amount_cents,
                        record.description,
                        record.category_raw or "",
                        record.category_raw,
                        record.external_id,
                        record.source_file,
                        import_batch_id,
                        source_row_number,
                    ),
                )
                inserted += 1

    duplicates = parsed - inserted - invalid
    return ImportSummary(
        parsed=parsed,
        inserted=inserted,
        duplicates=duplicates,
        invalid=invalid,
        skipped_existing_file=False,
    )


def _detect_dialect(handle: Any) -> Any:
    sample = handle.read(2048)
    handle.seek(0)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        return csv.excel


def _iter_rows(adapter: Any, handle: Any, dialect: csv.Dialect) -> Any:
    has_header = getattr(adapter, "has_header", True)
    if has_header:
        return csv.DictReader(handle, dialect=dialect)

    columns = getattr(adapter, "headerless_columns", ())
    if not columns:
        raise ValueError(
            f"adapter {adapter.__class__.__name__} missing headerless_columns"
        )

    reader = csv.reader(handle, dialect=dialect)

    def _mapped_rows() -> Any:
        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue
            padded = list(row) + [""] * max(0, len(columns) - len(row))
            mapped = {columns[idx]: padded[idx] for idx in range(len(columns))}
            yield mapped

    return _mapped_rows()
