from __future__ import annotations

import csv
from dataclasses import dataclass
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


def import_csv(*, institution: str, file_path: str) -> ImportSummary:
    adapter = get_adapter(institution)
    source_file = str(Path(file_path).name)

    parsed = 0
    inserted = 0
    invalid = 0

    with Path(file_path).open("r", encoding="utf-8-sig", newline="") as handle:
        dialect = _detect_dialect(handle)
        rows = _iter_rows(adapter=adapter, handle=handle, dialect=dialect)
        with get_connection() as conn:
            for row in rows:
                parsed += 1
                try:
                    record = adapter.parse_row(row, source_file)
                except ValueError:
                    invalid += 1
                    continue

                before_changes = conn.total_changes
                conn.execute(
                    """
                    INSERT OR IGNORE INTO transactions (
                        institution,
                        occurred_on,
                        posted_on,
                        amount_cents,
                        description,
                        category,
                        category_raw,
                        external_id,
                        source_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.institution,
                        record.occurred_on,
                        record.posted_on,
                        record.amount_cents,
                        record.description,
                        record.category_raw or "",
                        record.category_raw,
                        record.external_id,
                        record.source_file,
                    ),
                )
                inserted += int(conn.total_changes > before_changes)

    duplicates = parsed - inserted - invalid
    return ImportSummary(
        parsed=parsed, inserted=inserted, duplicates=duplicates, invalid=invalid
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
