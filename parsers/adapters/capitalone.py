from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib

from ..models import TransactionRecord


class CapitalOneAdapter:
    institution = "capitalone"

    def parse_row(self, row: dict[str, str], source_file: str) -> TransactionRecord:
        occurred_on = _parse_date(row.get("Transaction Date", ""))
        posted_on = _parse_date_optional(row.get("Posted Date", ""))
        description = _clean(row.get("Description", ""))
        category_raw = _clean_optional(row.get("Category", ""))
        debit = _parse_money(row.get("Debit", ""))
        credit = _parse_money(row.get("Credit", ""))
        amount_cents = credit - debit
        external_id = _stable_external_id(
            self.institution,
            occurred_on,
            posted_on or "",
            description,
            str(amount_cents),
            category_raw or "",
        )

        return TransactionRecord(
            institution=self.institution,
            occurred_on=occurred_on,
            posted_on=posted_on,
            description=description,
            category_raw=category_raw,
            amount_cents=amount_cents,
            external_id=external_id,
            source_file=source_file,
        )


def _clean(value: str) -> str:
    cleaned = " ".join(value.split()).strip()
    if not cleaned:
        raise ValueError("missing description")
    return cleaned


def _clean_optional(value: str) -> str | None:
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _parse_date(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("missing transaction date")
    return datetime.strptime(value, "%m/%d/%Y").date().isoformat()


def _parse_date_optional(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    return datetime.strptime(value, "%m/%d/%Y").date().isoformat()


def _parse_money(value: str) -> int:
    raw = value.strip()
    if not raw:
        return 0

    normalized = raw.replace("$", "").replace(",", "")
    negative = normalized.startswith("(") and normalized.endswith(")")
    if negative:
        normalized = normalized[1:-1]

    try:
        cents = int((Decimal(normalized) * 100).quantize(Decimal("1")))
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount: {value!r}") from exc

    return -cents if negative else cents


def _stable_external_id(*parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return digest
