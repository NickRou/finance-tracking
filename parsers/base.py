from __future__ import annotations

from typing import Protocol

from .models import TransactionRecord


class InstitutionAdapter(Protocol):
    institution: str

    def parse_row(self, row: dict[str, str], source_file: str) -> TransactionRecord: ...
