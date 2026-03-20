from __future__ import annotations

import argparse

from dotenv import load_dotenv

from db import initialize_database

from .pipeline import import_csv


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import transactions from institution CSV exports"
    )
    parser.add_argument(
        "--institution", required=True, help="Institution key, e.g. capitalone"
    )
    parser.add_argument(
        "--file", required=True, dest="file_path", help="Path to CSV export file"
    )
    args = parser.parse_args()

    load_dotenv(dotenv_path=".env")
    initialize_database()

    summary = import_csv(institution=args.institution, file_path=args.file_path)
    print(f"parsed={summary.parsed}")
    print(f"inserted={summary.inserted}")
    print(f"duplicates={summary.duplicates}")
    print(f"invalid={summary.invalid}")


if __name__ == "__main__":
    main()
