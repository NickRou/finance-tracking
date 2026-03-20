# Local Python Finance Dashboard

## Encrypted database setup (SQLCipher)

1. Copy `.env.example` to `.env`.
2. Set `FINANCE_DB_KEY` to a long random secret.
3. Run the app with `uv run app.py`.

The app initializes an encrypted SQLCipher database at `data/finance.db` by default.
Change `FINANCE_DB_PATH` in `.env` if you want a different location.

## CSV import

Import institution files using the generic parser pipeline plus adapters:

```bash
uv run python -m parsers.import_csv --institution capitalone --file /path/to/capitalone.csv
uv run python -m parsers.import_csv --institution chase --file /path/to/chase.csv
uv run python -m parsers.import_csv --institution discover --file /path/to/discover.csv
uv run python -m parsers.import_csv --institution americanexpress --file /path/to/amex.csv
```

Supported formats:

- Capital One headers: `Transaction Date, Posted Date, Card No., Description, Category, Debit, Credit`
- Chase headers: `Transaction Date, Post Date, Description, Category, Type, Amount, Memo`
- Discover headers: `Trans. Date, Post Date, Description, Amount, Category`
- American Express: no header row, each row is `date,description,amount` (tab or comma delimited)

## Dash dashboard workflow

Run the app:

```bash
uv run app.py
```

In the dashboard:

1. Upload one or more files (CSV only).
2. Tag each file with its institution in the file table.
3. Click `Import Tagged Files`.
4. Review the institution overview and recent transactions tables.
