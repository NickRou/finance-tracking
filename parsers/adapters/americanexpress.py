from __future__ import annotations

from ..models import TransactionRecord
from .common import clean, parse_date, parse_money, stable_external_id


class AmericanExpressAdapter:
    institution = "americanexpress"
    has_header = False
    headerless_columns = ("date", "description", "amount")

    def parse_row(self, row: dict[str, str], source_file: str) -> TransactionRecord:
        occurred_on = parse_date(row.get("date", ""))
        description = clean(row.get("description", ""))
        amount_cents = parse_money(row.get("amount", ""))

        external_id = stable_external_id(
            self.institution,
            occurred_on,
            description,
            str(amount_cents),
        )

        return TransactionRecord(
            institution=self.institution,
            occurred_on=occurred_on,
            posted_on=None,
            description=description,
            category_raw=None,
            amount_cents=amount_cents,
            external_id=external_id,
            source_file=source_file,
        )
