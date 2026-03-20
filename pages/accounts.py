from __future__ import annotations

from typing import Any

from dash import Input, Output, State, callback, dash_table, dcc, html, register_page

from db import create_account, get_connection, list_accounts
from parsers.registry import list_institutions


register_page(__name__, path="/accounts", title="Accounts")

INSTITUTIONS = sorted(
    set(list_institutions() + ["fidelity", "charles_schwab", "coinbase"])
)
ACCOUNT_TYPES = ["credit_card", "savings_account", "investment_account"]


def _accounts_rows() -> list[dict[str, Any]]:
    return list_accounts()


def layout() -> html.Div:
    return html.Div(
        [
            html.H2("Accounts"),
            html.Div(
                [
                    dcc.Input(
                        id="accounts-new-name",
                        type="text",
                        placeholder="Account name",
                        style={"minWidth": "220px"},
                    ),
                    dcc.Dropdown(
                        id="accounts-new-institution",
                        options=[
                            {"label": value, "value": value} for value in INSTITUTIONS
                        ],
                        value=INSTITUTIONS[0] if INSTITUTIONS else None,
                        clearable=False,
                        style={"minWidth": "200px"},
                    ),
                    dcc.Dropdown(
                        id="accounts-new-type",
                        options=[
                            {"label": value, "value": value} for value in ACCOUNT_TYPES
                        ],
                        value="credit_card",
                        clearable=False,
                        style={"minWidth": "200px"},
                    ),
                    html.Button("Add Account", id="accounts-add-button", n_clicks=0),
                ],
                style={
                    "display": "flex",
                    "gap": "10px",
                    "alignItems": "center",
                    "marginBottom": "8px",
                },
            ),
            html.Div(id="accounts-message", style={"marginBottom": "10px"}),
            dash_table.DataTable(
                id="accounts-table",
                columns=[
                    {"name": "Account", "id": "name"},
                    {"name": "Institution", "id": "institution"},
                    {"name": "Type", "id": "account_type"},
                ],
                data=_accounts_rows(),
                row_selectable="multi",
                selected_rows=[],
                style_table={"overflowX": "auto", "marginBottom": "12px"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            html.Button(
                "Remove Selected Accounts",
                id="accounts-remove-button",
                n_clicks=0,
                disabled=True,
            ),
            html.Div(
                "Permanently deletes selected account(s) and associated imported data.",
                style={"fontSize": "12px", "color": "#666", "marginTop": "6px"},
            ),
            html.Div(id="accounts-delete-message", style={"marginTop": "6px"}),
        ],
        className="page page-accounts",
    )


@callback(
    Output("accounts-message", "children"),
    Output("accounts-table", "data"),
    Output("accounts-table", "selected_rows"),
    Input("accounts-add-button", "n_clicks"),
    State("accounts-new-name", "value"),
    State("accounts-new-institution", "value"),
    State("accounts-new-type", "value"),
    prevent_initial_call=True,
)
def add_account(
    n_clicks: int,
    name: str | None,
    institution: str | None,
    account_type: str | None,
) -> tuple[str, list[dict[str, Any]], list[int]]:
    if not n_clicks:
        return "", _accounts_rows(), []

    normalized_name = (name or "").strip()
    if not normalized_name:
        return "Account name is required.", _accounts_rows(), []
    if institution not in INSTITUTIONS:
        return "Choose a valid institution.", _accounts_rows(), []
    if account_type not in ACCOUNT_TYPES:
        return "Choose a valid account type.", _accounts_rows(), []

    try:
        create_account(
            name=normalized_name,
            institution=institution,
            account_type=account_type,
        )
    except Exception as exc:
        return f"Could not add account: {exc}", _accounts_rows(), []

    message = f"Added account {normalized_name} ({institution}, {account_type})."
    return message, _accounts_rows(), []


@callback(
    Output("accounts-remove-button", "disabled"),
    Input("accounts-table", "data"),
    Input("accounts-table", "selected_rows"),
)
def toggle_remove_accounts_button(
    accounts_data: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
) -> bool:
    if not accounts_data:
        return True
    return not bool(selected_rows)


@callback(
    Output("accounts-delete-message", "children"),
    Output("accounts-table", "data", allow_duplicate=True),
    Output("accounts-table", "selected_rows", allow_duplicate=True),
    Input("accounts-remove-button", "n_clicks"),
    State("accounts-table", "data"),
    State("accounts-table", "selected_rows"),
    prevent_initial_call=True,
)
def remove_selected_accounts(
    n_clicks: int,
    accounts_data: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
) -> tuple[str, list[dict[str, Any]], list[int]]:
    if not n_clicks:
        return "", _accounts_rows(), []
    if not accounts_data:
        return "No accounts to remove.", _accounts_rows(), []
    if not selected_rows:
        return "Select account rows to remove.", _accounts_rows(), []

    selected_ids: list[int] = []
    for idx in selected_rows:
        if idx < 0 or idx >= len(accounts_data):
            continue
        raw_id = accounts_data[idx].get("id")
        if raw_id is None or str(raw_id) == "":
            continue
        try:
            selected_ids.append(int(raw_id))
        except TypeError, ValueError:
            continue

    if not selected_ids:
        return "No valid accounts selected.", _accounts_rows(), []

    tx_count = 0
    batch_count = 0
    anchor_count = 0
    deleted_accounts = 0

    with get_connection() as conn:
        for account_id in selected_ids:
            tx_count += int(
                conn.execute(
                    "SELECT COUNT(*) FROM transactions WHERE account_id = ?",
                    (account_id,),
                ).fetchone()[0]
            )
            batch_count += int(
                conn.execute(
                    "SELECT COUNT(*) FROM import_batches WHERE account_id = ?",
                    (account_id,),
                ).fetchone()[0]
            )
            anchor_count += int(
                conn.execute(
                    "SELECT COUNT(*) FROM statement_anchors WHERE account_id = ?",
                    (account_id,),
                ).fetchone()[0]
            )

            conn.execute("DELETE FROM transactions WHERE account_id = ?", (account_id,))
            conn.execute(
                "DELETE FROM import_batches WHERE account_id = ?", (account_id,)
            )
            conn.execute(
                "DELETE FROM statement_anchors WHERE account_id = ?", (account_id,)
            )
            result = conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            deleted_accounts += int(result.rowcount > 0)

    message = (
        f"Removed {deleted_accounts} account(s), {tx_count} transaction(s), "
        f"{batch_count} import batch(es), and {anchor_count} anchor(s)."
    )
    return message, _accounts_rows(), []
