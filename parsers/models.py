from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TransactionRecord:
    institution: str
    occurred_on: str
    posted_on: str | None
    description: str
    category_raw: str | None
    amount_cents: int
    external_id: str
    source_file: str
