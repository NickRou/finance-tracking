from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

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
        reader = csv.DictReader(handle)
        with get_connection() as conn:
            for row in reader:
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
