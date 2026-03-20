# Local Python Finance Dashboard

## Encrypted database setup (SQLCipher)

1. Copy `.env.example` to `.env`.
2. Set `FINANCE_DB_KEY` to a long random secret.
3. Run the app with `uv run app.py`.

The app initializes an encrypted SQLCipher database at `data/finance.db` by default.
Change `FINANCE_DB_PATH` in `.env` if you want a different location.
