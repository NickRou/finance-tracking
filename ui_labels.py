from __future__ import annotations


_INSTITUTION_LABELS: dict[str, str] = {
    "americanexpress": "American Express",
    "capitalone": "Capital One",
    "charles_schwab": "Charles Schwab",
    "chase": "Chase",
    "coinbase": "Coinbase",
    "discover": "Discover",
    "fidelity": "Fidelity",
}

_ACCOUNT_TYPE_LABELS: dict[str, str] = {
    "credit_card": "Credit Card",
    "savings_account": "Savings Account",
    "investment_account": "Investment Account",
}

_ASSET_TYPE_LABELS: dict[str, str] = {
    "cash": "Cash",
    "stock_etf": "Stock / ETF",
    "crypto": "Crypto",
    "bond_fund": "Bond Fund",
    "other": "Other",
}


def _snake_to_title(value: str) -> str:
    return value.replace("_", " ").strip().title()


def format_institution(value: str) -> str:
    key = value.strip().lower()
    if not key:
        return ""
    return _INSTITUTION_LABELS.get(key, _snake_to_title(key))


def format_account_type(value: str) -> str:
    key = value.strip().lower()
    if not key:
        return ""
    return _ACCOUNT_TYPE_LABELS.get(key, _snake_to_title(key))


def format_asset_type(value: str) -> str:
    key = value.strip().lower()
    if not key:
        return ""
    return _ASSET_TYPE_LABELS.get(key, _snake_to_title(key))
