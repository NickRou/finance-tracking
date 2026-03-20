from __future__ import annotations

import base64
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import tempfile
from typing import Any

from dash import (
    Input,
    Output,
    State,
    callback,
    dash_table,
    dcc,
    html,
    no_update,
    register_page,
)

from db import get_connection, list_accounts, upsert_statement_anchor
from parsers.pipeline import ImportSummary, import_csv


register_page(__name__, path="/transactions", title="Transactions")


def _format_money(cents: int | None) -> str:
    value = 0 if cents is None else cents
    return f"${value / 100:,.2f}"


def _parse_dollars_to_cents(value: str | int | float | None) -> int:
    if value is None:
        raise ValueError("Statement balance is required.")
    raw = str(value).strip().replace("$", "").replace(",", "")
    if not raw:
        raise ValueError("Statement balance is required.")
    try:
        amount = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("Statement balance is invalid.") from exc
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _accounts_by_id() -> dict[int, dict[str, str | int]]:
    return {int(account["id"]): account for account in list_accounts()}


def _account_dropdown_options() -> list[dict[str, str | int]]:
    options: list[dict[str, str | int]] = []
    for account in list_accounts():
        options.append(
            {
                "label": f"{account['name']} ({account['institution']})",
                "value": int(account["id"]),
            }
        )
    return options


def _fetch_overview() -> tuple[dict[str, str], list[dict[str, str]]]:
    with get_connection() as conn:
        totals_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_transactions,
                COALESCE(SUM(amount_cents), 0) AS net_cents,
                COALESCE(SUM(CASE WHEN amount_cents < 0 THEN -amount_cents ELSE 0 END), 0) AS debit_cents,
                COALESCE(SUM(CASE WHEN amount_cents > 0 THEN amount_cents ELSE 0 END), 0) AS credit_cents
            FROM transactions
            """
        ).fetchone()
        rows = conn.execute(
            """
            SELECT
                a.id,
                a.name,
                a.institution,
                a.account_type,
                COUNT(t.id) AS transaction_count,
                COALESCE(SUM(t.amount_cents), 0) AS net_cents,
                COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN -t.amount_cents ELSE 0 END), 0) AS debit_cents,
                COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS credit_cents,
                MAX(t.occurred_on) AS latest_transaction_date,
                s.anchor_date,
                s.anchor_balance_cents,
                COALESCE(SUM(CASE WHEN s.anchor_date IS NOT NULL AND COALESCE(t.posted_on, t.occurred_on) > s.anchor_date THEN t.amount_cents ELSE 0 END), 0) AS post_anchor_net_cents
            FROM accounts a
            LEFT JOIN transactions t ON t.account_id = a.id
            LEFT JOIN statement_anchors s ON s.account_id = a.id
            GROUP BY a.id, a.name, a.institution, a.account_type, s.anchor_date, s.anchor_balance_cents
            ORDER BY a.institution ASC, a.name ASC
            """
        ).fetchall()

    totals = {
        "total_transactions": str(totals_row[0]),
        "net": _format_money(int(totals_row[1])),
        "debits": _format_money(int(totals_row[2])),
        "credits": _format_money(int(totals_row[3])),
    }
    overview = [
        {
            "account_id": int(row[0]),
            "account": str(row[1]),
            "institution": str(row[2]),
            "account_type": str(row[3]),
            "transaction_count": int(row[4]),
            "net": _format_money(int(row[5])),
            "debits": _format_money(int(row[6])),
            "credits": _format_money(int(row[7])),
            "latest_transaction_date": str(row[8] or "-"),
            "anchor_date": str(row[9] or "-"),
            "anchor_balance": _format_money(int(row[10]))
            if row[10] is not None
            else "-",
            "estimated_current_balance": (
                _format_money(int(row[10]) - int(row[11]))
                if row[10] is not None
                else "-"
            ),
        }
        for row in rows
    ]
    return totals, overview


def _fetch_transactions(account_filter: str) -> list[dict[str, str]]:
    with get_connection() as conn:
        if account_filter == "all":
            rows = conn.execute(
                """
                SELECT occurred_on, posted_on, institution, description, category_raw, amount_cents
                FROM transactions
                ORDER BY occurred_on DESC, id DESC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT occurred_on, posted_on, institution, description, category_raw, amount_cents
                FROM transactions
                WHERE account_id = ?
                ORDER BY occurred_on DESC, id DESC
                """,
                (int(account_filter),),
            ).fetchall()

    return [
        {
            "occurred_on": str(row[0]),
            "posted_on": str(row[1] or "-"),
            "institution": str(row[2]),
            "description": str(row[3]),
            "category": str(row[4] or ""),
            "amount": _format_money(int(row[5])),
        }
        for row in rows
    ]


def _file_rows_for_table(
    uploaded_files: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not uploaded_files:
        return []
    return [
        {
            "id": row["file_id"],
            "filename": row["filename"],
            "account_id": row.get("account_id"),
        }
        for row in uploaded_files
    ]


def _next_file_id(existing_rows: list[dict[str, Any]]) -> int:
    max_id = 0
    for row in existing_rows:
        raw_id = str(row.get("file_id", ""))
        if raw_id.startswith("file-") and raw_id[5:].isdigit():
            max_id = max(max_id, int(raw_id[5:]))
    return max_id + 1


def _card_style() -> dict[str, str]:
    return {
        "minWidth": "170px",
        "padding": "12px",
        "border": "1px solid #cfcfcf",
        "borderRadius": "8px",
        "backgroundColor": "#f8f8f8",
    }


def layout() -> html.Div:
    account_options = _account_dropdown_options()
    default_account = int(account_options[0]["value"]) if account_options else None

    return html.Div(
        [
            html.H2("Transactions"),
            html.P(
                "Upload CSV files, tag each file by account, and import into your encrypted database."
            ),
            html.H3("Import Files"),
            dcc.Upload(
                id="tx-upload-files",
                children=html.Div(
                    ["Drag and drop CSV files or ", html.Button("Select Files")]
                ),
                multiple=True,
                style={
                    "width": "100%",
                    "padding": "16px",
                    "border": "1px dashed #7a7a7a",
                    "borderRadius": "10px",
                    "marginBottom": "12px",
                },
            ),
            dash_table.DataTable(
                id="tx-file-tag-table",
                columns=[
                    {"name": "File", "id": "filename", "editable": False},
                    {"name": "Account", "id": "account_id", "presentation": "dropdown"},
                ],
                data=[],
                editable=True,
                row_selectable="multi",
                selected_rows=[],
                dropdown={"account_id": {"options": account_options}},
                style_table={
                    "marginBottom": "12px",
                    "overflowX": "auto",
                    "overflowY": "auto",
                    "maxHeight": "300px",
                },
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            html.Button(
                "Remove Selected Files",
                id="tx-remove-selected-files",
                n_clicks=0,
                disabled=True,
            ),
            html.Button("Import Tagged Files", id="tx-import-files", n_clicks=0),
            html.Div(id="tx-upload-message", style={"marginTop": "10px"}),
            html.Div(
                id="tx-import-message",
                style={"marginTop": "10px", "marginBottom": "20px"},
            ),
            html.H3("Statement Anchor"),
            html.Div(
                [
                    dcc.Dropdown(
                        id="tx-anchor-account",
                        options=account_options,
                        value=default_account,
                        clearable=False,
                        style={"minWidth": "220px"},
                    ),
                    dcc.Input(
                        id="tx-anchor-balance",
                        type="number",
                        placeholder="Statement balance",
                        step="0.01",
                        style={"minWidth": "220px"},
                    ),
                    dcc.DatePickerSingle(
                        id="tx-anchor-date", placeholder="Statement date"
                    ),
                    html.Button("Save Anchor", id="tx-save-anchor", n_clicks=0),
                ],
                style={
                    "display": "flex",
                    "gap": "10px",
                    "alignItems": "center",
                    "marginBottom": "8px",
                },
            ),
            html.Div(id="tx-anchor-message", style={"marginBottom": "16px"}),
            html.H3("Overview"),
            html.Div(
                id="tx-kpi-cards",
                style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
            ),
            html.H4("By Account"),
            dash_table.DataTable(
                id="tx-overview-table",
                columns=[
                    {"name": "Account", "id": "account"},
                    {"name": "Institution", "id": "institution"},
                    {"name": "Type", "id": "account_type"},
                    {"name": "Transactions", "id": "transaction_count"},
                    {"name": "Net", "id": "net"},
                    {"name": "Debits", "id": "debits"},
                    {"name": "Credits", "id": "credits"},
                    {"name": "Latest", "id": "latest_transaction_date"},
                    {"name": "Anchor Date", "id": "anchor_date"},
                    {"name": "Anchor Balance", "id": "anchor_balance"},
                    {"name": "Est Current", "id": "estimated_current_balance"},
                ],
                data=[],
                style_table={"overflowX": "auto", "marginBottom": "20px"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            html.H4("Recent Transactions"),
            dcc.Dropdown(
                id="tx-transactions-account-filter",
                options=[{"label": "All", "value": "all"}] + account_options,
                value="all",
                clearable=False,
                style={"maxWidth": "300px", "marginBottom": "10px"},
            ),
            dash_table.DataTable(
                id="tx-recent-transactions",
                columns=[
                    {"name": "Occurred On", "id": "occurred_on"},
                    {"name": "Posted On", "id": "posted_on"},
                    {"name": "Institution", "id": "institution"},
                    {"name": "Description", "id": "description"},
                    {"name": "Category", "id": "category"},
                    {"name": "Amount", "id": "amount"},
                ],
                data=[],
                page_size=10,
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            dcc.Store(id="tx-uploaded-files-store", data=[]),
            dcc.Store(id="tx-refresh-token", data=0),
        ]
    )


@callback(
    Output("tx-uploaded-files-store", "data"),
    Output("tx-file-tag-table", "data"),
    Output("tx-upload-message", "children"),
    Input("tx-upload-files", "contents"),
    State("tx-upload-files", "filename"),
    State("tx-uploaded-files-store", "data"),
)
def handle_uploads(
    uploaded_contents: list[str] | None,
    uploaded_filenames: list[str] | None,
    stored_files: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | object, str]:
    current = list(stored_files or [])
    if not uploaded_contents or not uploaded_filenames:
        return current, no_update, ""

    account_options = _account_dropdown_options()
    default_account_id = int(account_options[0]["value"]) if account_options else None
    next_id = _next_file_id(current)

    new_rows: list[dict[str, Any]] = []
    for content, filename in zip(uploaded_contents, uploaded_filenames, strict=True):
        new_rows.append(
            {
                "file_id": f"file-{next_id}",
                "filename": filename,
                "content": content,
                "account_id": default_account_id,
            }
        )
        next_id += 1

    merged = current + new_rows
    return (
        merged,
        _file_rows_for_table(merged),
        f"Added {len(new_rows)} file(s). Tag each file with the right account.",
    )


@callback(
    Output("tx-remove-selected-files", "disabled"),
    Input("tx-uploaded-files-store", "data"),
    Input("tx-file-tag-table", "selected_rows"),
)
def toggle_remove_selected_button(
    uploaded_files: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
) -> bool:
    if not uploaded_files:
        return True
    return not bool(selected_rows)


@callback(
    Output("tx-import-files", "disabled"),
    Input("tx-uploaded-files-store", "data"),
    Input("tx-file-tag-table", "data"),
)
def toggle_import_button(
    uploaded_files: list[dict[str, Any]] | None,
    table_data: list[dict[str, Any]] | None,
) -> bool:
    if not uploaded_files:
        return True

    account_map = _accounts_by_id()
    rows_by_id = {str(row.get("id", "")): row for row in (table_data or [])}

    for file_row in uploaded_files:
        file_id = str(file_row.get("file_id", ""))
        table_row = rows_by_id.get(file_id, {})
        raw_account_id = table_row.get("account_id", file_row.get("account_id"))

        if not str(file_row.get("filename", "")).lower().endswith(".csv"):
            return True
        if raw_account_id is None or str(raw_account_id) == "":
            return True
        try:
            account_id = int(raw_account_id)
        except TypeError, ValueError:
            return True
        if account_id not in account_map:
            return True

    return False


@callback(
    Output("tx-uploaded-files-store", "data", allow_duplicate=True),
    Output("tx-file-tag-table", "data", allow_duplicate=True),
    Output("tx-file-tag-table", "selected_rows"),
    Output("tx-upload-message", "children", allow_duplicate=True),
    Input("tx-remove-selected-files", "n_clicks"),
    State("tx-uploaded-files-store", "data"),
    State("tx-file-tag-table", "selected_rows"),
    prevent_initial_call=True,
)
def remove_selected_files(
    n_clicks: int,
    uploaded_files: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
) -> tuple[
    list[dict[str, Any]] | object, list[dict[str, Any]] | object, list[int], str
]:
    if not n_clicks:
        return no_update, no_update, [], ""

    current = list(uploaded_files or [])
    if not current:
        return no_update, no_update, [], "No files to remove."

    selected = set(selected_rows or [])
    if not selected:
        return no_update, no_update, [], "Select file rows to remove."

    kept = [row for idx, row in enumerate(current) if idx not in selected]
    removed_count = len(current) - len(kept)
    return (
        kept,
        _file_rows_for_table(kept),
        [],
        f"Removed {removed_count} file(s) from the import queue.",
    )


@callback(
    Output("tx-import-message", "children"),
    Output("tx-refresh-token", "data"),
    Output("tx-uploaded-files-store", "data", allow_duplicate=True),
    Output("tx-file-tag-table", "data", allow_duplicate=True),
    Input("tx-import-files", "n_clicks"),
    State("tx-uploaded-files-store", "data"),
    State("tx-file-tag-table", "data"),
    State("tx-refresh-token", "data"),
    prevent_initial_call=True,
)
def import_uploaded_files(
    n_clicks: int,
    uploaded_files: list[dict[str, Any]] | None,
    table_data: list[dict[str, Any]] | None,
    refresh_token: int,
) -> tuple[str, int, list[dict[str, Any]] | object, list[dict[str, Any]] | object]:
    if not n_clicks:
        return "", refresh_token, no_update, no_update
    if not uploaded_files:
        return (
            "No files to import. Upload CSV files first.",
            refresh_token,
            no_update,
            no_update,
        )

    rows_by_id = {str(row.get("id", "")): row for row in (table_data or [])}
    account_map = _accounts_by_id()

    total = ImportSummary(parsed=0, inserted=0, duplicates=0, invalid=0)
    skipped: list[str] = []
    skipped_existing: list[str] = []
    failed: list[str] = []
    remaining_rows: list[dict[str, Any]] = []

    for row in uploaded_files:
        file_id = str(row.get("file_id", ""))
        filename = str(row.get("filename", ""))
        table_row = rows_by_id.get(file_id, {})
        raw_account_id = table_row.get("account_id", row.get("account_id"))
        content = str(row.get("content", ""))
        temp_path: Path | None = None

        if not filename.lower().endswith(".csv"):
            skipped.append(filename)
            remaining_rows.append({**row, "account_id": raw_account_id})
            continue
        if raw_account_id is None or str(raw_account_id) == "":
            failed.append(f"{filename}: select a valid account")
            remaining_rows.append({**row, "account_id": raw_account_id})
            continue
        try:
            account_id = int(raw_account_id)
        except TypeError, ValueError:
            failed.append(f"{filename}: select a valid account")
            remaining_rows.append({**row, "account_id": raw_account_id})
            continue

        account = account_map.get(account_id)
        if account is None:
            failed.append(f"{filename}: account does not exist")
            remaining_rows.append({**row, "account_id": account_id})
            continue

        institution = str(account["institution"])

        try:
            _meta, encoded = content.split(",", 1)
            decoded = base64.b64decode(encoded)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as handle:
                handle.write(decoded)
                temp_path = Path(handle.name)

            result = import_csv(
                institution=institution,
                account_id=account_id,
                file_path=str(temp_path),
                source_filename=filename,
            )
            if result.skipped_existing_file:
                skipped_existing.append(filename)
                continue

            total = ImportSummary(
                parsed=total.parsed + result.parsed,
                inserted=total.inserted + result.inserted,
                duplicates=total.duplicates + result.duplicates,
                invalid=total.invalid + result.invalid,
            )
        except Exception as exc:
            failed.append(f"{filename}: {exc}")
            remaining_rows.append({**row, "account_id": account_id})
        finally:
            if temp_path and temp_path.exists():
                temp_path.unlink()

    message = (
        f"Imported files: parsed={total.parsed}, inserted={total.inserted}, "
        f"duplicates={total.duplicates}, invalid={total.invalid}."
    )
    if skipped:
        message += f" Skipped non-CSV files: {', '.join(skipped)}."
    if skipped_existing:
        message += f" Already imported (same file hash): {', '.join(skipped_existing)}."
    if failed:
        message += f" Failed: {'; '.join(failed)}."

    return (
        message,
        refresh_token + 1,
        remaining_rows,
        _file_rows_for_table(remaining_rows),
    )


@callback(
    Output("tx-anchor-message", "children"),
    Output("tx-refresh-token", "data", allow_duplicate=True),
    Input("tx-save-anchor", "n_clicks"),
    State("tx-anchor-account", "value"),
    State("tx-anchor-balance", "value"),
    State("tx-anchor-date", "date"),
    State("tx-refresh-token", "data"),
    prevent_initial_call=True,
)
def save_statement_anchor(
    n_clicks: int,
    account_id: int | None,
    anchor_balance: str | int | float | None,
    anchor_date: str | None,
    refresh_token: int,
) -> tuple[str, int]:
    if not n_clicks:
        return "", refresh_token

    accounts = _accounts_by_id()
    if account_id is None or int(account_id) not in accounts:
        return "Choose a valid account before saving.", refresh_token
    if not anchor_date:
        return "Choose the statement date for this anchor.", refresh_token

    try:
        anchor_balance_cents = _parse_dollars_to_cents(anchor_balance)
    except ValueError as exc:
        return str(exc), refresh_token

    upsert_statement_anchor(
        account_id=int(account_id),
        anchor_date=anchor_date,
        anchor_balance_cents=anchor_balance_cents,
    )
    account = accounts[int(account_id)]
    message = (
        f"Saved anchor for {account['name']}: {_format_money(anchor_balance_cents)} "
        f"as of {anchor_date}."
    )
    return message, refresh_token + 1


@callback(
    Output("tx-kpi-cards", "children"),
    Output("tx-overview-table", "data"),
    Input("tx-refresh-token", "data"),
)
def refresh_overview(
    _refresh_token: int,
) -> tuple[list[html.Div], list[dict[str, str]]]:
    totals, overview_rows = _fetch_overview()
    cards = [
        html.Div(
            [html.Div("Transactions"), html.Strong(totals["total_transactions"])],
            style=_card_style(),
        ),
        html.Div([html.Div("Net"), html.Strong(totals["net"])], style=_card_style()),
        html.Div(
            [html.Div("Debits"), html.Strong(totals["debits"])], style=_card_style()
        ),
        html.Div(
            [html.Div("Credits"), html.Strong(totals["credits"])], style=_card_style()
        ),
    ]
    return cards, overview_rows


@callback(
    Output("tx-recent-transactions", "data"),
    Input("tx-refresh-token", "data"),
    Input("tx-transactions-account-filter", "value"),
)
def refresh_transactions_table(
    _refresh_token: int,
    account_filter: str | None,
) -> list[dict[str, str]]:
    selected = account_filter or "all"
    return _fetch_transactions(selected)
