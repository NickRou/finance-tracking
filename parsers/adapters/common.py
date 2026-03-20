from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib


def clean(value: str) -> str:
    cleaned = " ".join(value.split()).strip()
    if not cleaned:
        raise ValueError("missing required text value")
    return cleaned


def clean_optional(value: str) -> str | None:
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def parse_date(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("missing transaction date")

    formats = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d")
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value!r}")


def parse_date_optional(value: str) -> str | None:
    if not value.strip():
        return None
    return parse_date(value)


def parse_money(value: str) -> int:
    raw = value.strip()
    if not raw:
        return 0

    normalized = raw.replace("$", "").replace(",", "")
    negative = normalized.startswith("(") and normalized.endswith(")")
    if negative:
        normalized = normalized[1:-1]

    try:
        cents = int(
            (Decimal(normalized) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount: {value!r}") from exc

    return -cents if negative else cents


def stable_external_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
