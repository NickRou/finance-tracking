# Local Python Finance Dashboard

## Encrypted database setup (SQLCipher)

1. Copy `.env.example` to `.env`.
2. Set `FINANCE_DB_KEY` to a long random secret.
3. Run the app with `uv run app.py`.

The app initializes an encrypted SQLCipher database at `data/finance.db` by default.
Change `FINANCE_DB_PATH` in `.env` if you want a different location.

## CSV import

Import Capital One CSV files using the generic parser pipeline plus institution adapter:

```bash
uv run python -m parsers.import_csv --institution capitalone --file /path/to/capitalone.csv
```

Capital One headers expected by the adapter:

`Transaction Date, Posted Date, Card No., Description, Category, Debit, Credit`
