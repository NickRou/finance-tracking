from __future__ import annotations

from typing import Any

from dash import Input, Output, State, callback, dash_table, dcc, html, register_page

from db import create_account, get_connection, list_accounts
from parsers.registry import list_institutions
from ui_labels import format_account_type, format_institution


register_page(__name__, path="/accounts", title="Accounts")

INSTITUTIONS = sorted(
    set(list_institutions() + ["fidelity", "charles_schwab", "coinbase"])
)
ACCOUNT_TYPES = ["credit_card", "savings_account", "investment_account"]


def _all_accounts() -> list[dict[str, Any]]:
    return list_accounts()


def _section_rows(account_type: str) -> list[dict[str, Any]]:
    rows = [
        row for row in _all_accounts() if str(row.get("account_type")) == account_type
    ]
    return [
        {
            **row,
            "institution": format_institution(str(row.get("institution", ""))),
            "account_type": format_account_type(str(row.get("account_type", ""))),
        }
        for row in rows
    ]


def _section_header(title: str, account_type: str) -> html.H3:
    return html.H3(f"{title} ({len(_section_rows(account_type))})")


def _section_table(table_id: str, rows: list[dict[str, Any]]) -> dash_table.DataTable:
    return dash_table.DataTable(
        id=table_id,
        columns=[
            {"name": "Account", "id": "name"},
            {"name": "Institution", "id": "institution"},
        ],
        data=rows,
        row_selectable="multi",
        selected_rows=[],
        style_table={"overflowX": "auto", "marginBottom": "14px", "width": "100%"},
        style_cell={
            "textAlign": "left",
            "padding": "8px",
            "minWidth": "0",
            "maxWidth": "0",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "whiteSpace": "nowrap",
        },
        style_cell_conditional=[
            {
                "if": {"column_id": "name"},
                "minWidth": "300px",
                "width": "50%",
                "maxWidth": "50%",
            },
            {
                "if": {"column_id": "institution"},
                "minWidth": "300px",
                "width": "50%",
                "maxWidth": "50%",
            },
        ],
    )


def layout() -> html.Div:
    cash_rows = _section_rows("savings_account")
    credit_rows = _section_rows("credit_card")
    investment_rows = _section_rows("investment_account")

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
                            {"label": format_institution(value), "value": value}
                            for value in INSTITUTIONS
                        ],
                        value=INSTITUTIONS[0] if INSTITUTIONS else None,
                        clearable=False,
                        style={"minWidth": "220px"},
                    ),
                    dcc.Dropdown(
                        id="accounts-new-type",
                        options=[
                            {"label": format_account_type(value), "value": value}
                            for value in ACCOUNT_TYPES
                        ],
                        value="credit_card",
                        clearable=False,
                        style={"minWidth": "220px"},
                    ),
                    html.Button(
                        "+ Add Account",
                        id="accounts-add-button",
                        n_clicks=0,
                        disabled=True,
                    ),
                    html.Button(
                        "- Remove Account(s)",
                        id="accounts-remove-button",
                        n_clicks=0,
                        disabled=True,
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "10px",
                    "alignItems": "center",
                    "flexWrap": "wrap",
                    "marginBottom": "10px",
                },
            ),
            html.Div(id="accounts-message", style={"marginBottom": "8px"}),
            html.Div(id="accounts-delete-message", style={"marginBottom": "14px"}),
            html.Div(
                "Removing accounts permanently deletes associated transactions, imports, anchors, and investment holdings.",
                style={"fontSize": "12px", "color": "#666", "marginBottom": "14px"},
            ),
            _section_header("Cash", "savings_account"),
            _section_table("accounts-cash-table", cash_rows),
            _section_header("Credit Cards", "credit_card"),
            _section_table("accounts-credit-table", credit_rows),
            _section_header("Investments", "investment_account"),
            _section_table("accounts-investment-table", investment_rows),
        ],
        className="page page-accounts",
    )


@callback(
    Output("accounts-add-button", "disabled"),
    Input("accounts-new-name", "value"),
)
def toggle_add_account_button(name: str | None) -> bool:
    return not bool((name or "").strip())


@callback(
    Output("accounts-message", "children"),
    Output("accounts-cash-table", "data"),
    Output("accounts-credit-table", "data"),
    Output("accounts-investment-table", "data"),
    Output("accounts-cash-table", "selected_rows"),
    Output("accounts-credit-table", "selected_rows"),
    Output("accounts-investment-table", "selected_rows"),
    Output("accounts-new-name", "value"),
    Output("accounts-new-institution", "value"),
    Output("accounts-new-type", "value"),
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
) -> tuple[
    str,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[int],
    list[int],
    list[int],
    str | None,
    str | None,
    str | None,
]:
    if not n_clicks:
        return (
            "",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
            name,
            institution,
            account_type,
        )

    normalized_name = (name or "").strip()
    if not normalized_name:
        return (
            "Account name is required.",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
            name,
            institution,
            account_type,
        )
    if institution not in INSTITUTIONS:
        return (
            "Choose a valid institution.",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
            name,
            institution,
            account_type,
        )
    if account_type not in ACCOUNT_TYPES:
        return (
            "Choose a valid account type.",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
            name,
            institution,
            account_type,
        )

    try:
        create_account(
            name=normalized_name,
            institution=institution,
            account_type=account_type,
        )
        message = (
            f"Added account {normalized_name} "
            f"({format_institution(institution)}, {format_account_type(account_type)})."
        )
    except Exception as exc:
        message = f"Could not add account: {exc}"

    return (
        message,
        _section_rows("savings_account"),
        _section_rows("credit_card"),
        _section_rows("investment_account"),
        [],
        [],
        [],
        "",
        INSTITUTIONS[0] if INSTITUTIONS else None,
        "credit_card",
    )


@callback(
    Output("accounts-remove-button", "disabled"),
    Input("accounts-cash-table", "selected_rows"),
    Input("accounts-credit-table", "selected_rows"),
    Input("accounts-investment-table", "selected_rows"),
)
def toggle_remove_button(
    cash_selected: list[int] | None,
    credit_selected: list[int] | None,
    investment_selected: list[int] | None,
) -> bool:
    return not bool(cash_selected or credit_selected or investment_selected)


def _selected_ids(
    rows: list[dict[str, Any]] | None,
    selected: list[int] | None,
) -> list[int]:
    if not rows or not selected:
        return []
    ids: list[int] = []
    for idx in selected:
        if idx < 0 or idx >= len(rows):
            continue
        raw_id = rows[idx].get("id")
        if raw_id is None:
            continue
        try:
            ids.append(int(raw_id))
        except TypeError, ValueError:
            continue
    return ids


@callback(
    Output("accounts-delete-message", "children"),
    Output("accounts-cash-table", "data", allow_duplicate=True),
    Output("accounts-credit-table", "data", allow_duplicate=True),
    Output("accounts-investment-table", "data", allow_duplicate=True),
    Output("accounts-cash-table", "selected_rows", allow_duplicate=True),
    Output("accounts-credit-table", "selected_rows", allow_duplicate=True),
    Output("accounts-investment-table", "selected_rows", allow_duplicate=True),
    Input("accounts-remove-button", "n_clicks"),
    State("accounts-cash-table", "data"),
    State("accounts-credit-table", "data"),
    State("accounts-investment-table", "data"),
    State("accounts-cash-table", "selected_rows"),
    State("accounts-credit-table", "selected_rows"),
    State("accounts-investment-table", "selected_rows"),
    prevent_initial_call=True,
)
def remove_selected_accounts(
    n_clicks: int,
    cash_rows: list[dict[str, Any]] | None,
    credit_rows: list[dict[str, Any]] | None,
    investment_rows: list[dict[str, Any]] | None,
    cash_selected: list[int] | None,
    credit_selected: list[int] | None,
    investment_selected: list[int] | None,
) -> tuple[
    str,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[int],
    list[int],
    list[int],
]:
    if not n_clicks:
        return (
            "",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
        )

    selected_ids = sorted(
        set(
            _selected_ids(cash_rows, cash_selected)
            + _selected_ids(credit_rows, credit_selected)
            + _selected_ids(investment_rows, investment_selected)
        )
    )

    if not selected_ids:
        return (
            "Select at least one account.",
            _section_rows("savings_account"),
            _section_rows("credit_card"),
            _section_rows("investment_account"),
            [],
            [],
            [],
        )

    tx_count = 0
    batch_count = 0
    anchor_count = 0
    holdings_count = 0
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
            holdings_count += int(
                conn.execute(
                    "SELECT COUNT(*) FROM investment_holdings WHERE account_id = ?",
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
            conn.execute(
                "DELETE FROM investment_holdings WHERE account_id = ?", (account_id,)
            )
            result = conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            deleted_accounts += int(result.rowcount > 0)

    message = (
        f"Removed {deleted_accounts} account(s), {tx_count} transaction(s), "
        f"{batch_count} import batch(es), {anchor_count} anchor(s), and {holdings_count} holding(s)."
    )

    return (
        message,
        _section_rows("savings_account"),
        _section_rows("credit_card"),
        _section_rows("investment_account"),
        [],
        [],
        [],
    )
