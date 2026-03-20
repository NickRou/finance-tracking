from __future__ import annotations

from ..models import TransactionRecord
from .common import (
    clean,
    clean_optional,
    parse_date,
    parse_date_optional,
    parse_money,
    stable_external_id,
)


class DiscoverAdapter:
    institution = "discover"
    has_header = True

    def parse_row(self, row: dict[str, str], source_file: str) -> TransactionRecord:
        occurred_on = parse_date(row.get("Trans. Date", ""))
        posted_on = parse_date_optional(row.get("Post Date", ""))
        description = clean(row.get("Description", ""))
        category_raw = clean_optional(row.get("Category", ""))
        amount_cents = -parse_money(row.get("Amount", ""))

        external_id = stable_external_id(
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
