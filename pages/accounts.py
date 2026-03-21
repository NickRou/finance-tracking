from __future__ import annotations

from typing import Any, cast

from dash import (
    Input,
    Output,
    State,
    callback,
    ctx,
    dash_table,
    dcc,
    html,
    no_update,
    register_page,
)

from db import create_account, get_connection, list_accounts
from parsers.registry import list_institutions
from ui_labels import format_account_type, format_institution


register_page(__name__, path="/accounts", title="Accounts")

INSTITUTIONS = sorted(
    set(list_institutions() + ["fidelity", "charles_schwab", "coinbase"])
)
ACCOUNT_TYPES = ["credit_card", "savings_account", "investment_account"]


def _modal_overlay_style(is_open: bool) -> dict[str, str]:
    if is_open:
        return {
            "display": "flex",
            "visibility": "visible",
            "opacity": "1",
            "pointerEvents": "auto",
        }
    return {
        "display": "flex",
        "visibility": "hidden",
        "opacity": "0",
        "pointerEvents": "none",
    }


def _all_accounts() -> list[dict[str, Any]]:
    return list_accounts()


def _rows_for_type(account_type: str) -> list[dict[str, Any]]:
    rows = [
        row for row in _all_accounts() if str(row.get("account_type")) == account_type
    ]
    return [
        {
            **row,
            "institution": format_institution(str(row.get("institution", ""))),
            "account_type": format_account_type(str(row.get("account_type", ""))),
            "action": "![Delete](/assets/svgs/trash-2.svg)",
        }
        for row in rows
    ]


def _all_section_rows() -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]
]:
    return (
        _rows_for_type("savings_account"),
        _rows_for_type("credit_card"),
        _rows_for_type("investment_account"),
    )


def _section_headers() -> tuple[str, str, str]:
    cash_rows, credit_rows, investment_rows = _all_section_rows()
    return (
        f"Cash ({len(cash_rows)})",
        f"Credit Cards ({len(credit_rows)})",
        f"Investments ({len(investment_rows)})",
    )


def _section_table(table_id: str, rows: list[dict[str, Any]]) -> dash_table.DataTable:
    return dash_table.DataTable(
        id=table_id,
        columns=[
            {"name": "Account", "id": "name"},
            {"name": "Institution", "id": "institution"},
            {"name": "", "id": "action", "presentation": "markdown"},
        ],
        data=cast(Any, rows),
        cell_selectable=True,
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
        style_cell_conditional=cast(
            Any,
            [
                {
                    "if": {"column_id": "name"},
                    "minWidth": "300px",
                    "width": "48%",
                    "maxWidth": "48%",
                },
                {
                    "if": {"column_id": "institution"},
                    "minWidth": "300px",
                    "width": "48%",
                    "maxWidth": "48%",
                },
                {
                    "if": {"column_id": "action"},
                    "minWidth": "40px",
                    "width": "40px",
                    "maxWidth": "40px",
                    "textAlign": "center",
                    "cursor": "pointer",
                },
            ],
        ),
    )


def _delete_account_cascade(account_id: int) -> tuple[int, int, int, int, int]:
    tx_count = 0
    batch_count = 0
    anchor_count = 0
    holdings_count = 0
    deleted_accounts = 0

    with get_connection() as conn:
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
        conn.execute("DELETE FROM import_batches WHERE account_id = ?", (account_id,))
        conn.execute(
            "DELETE FROM statement_anchors WHERE account_id = ?", (account_id,)
        )
        conn.execute(
            "DELETE FROM investment_holdings WHERE account_id = ?", (account_id,)
        )
        result = conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        deleted_accounts += int(result.rowcount > 0)

    return deleted_accounts, tx_count, batch_count, anchor_count, holdings_count


def layout() -> html.Div:
    cash_rows, credit_rows, investment_rows = _all_section_rows()
    cash_header, credit_header, investment_header = _section_headers()

    return html.Div(
        [
            html.Div(
                [
                    html.H2("Accounts", style={"marginBottom": "0"}),
                    html.Button(
                        "+ Add Account", id="accounts-open-add-modal", n_clicks=0
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "8px",
                },
            ),
            html.P(
                "Manage cash, credit card, and investment accounts used across the app."
            ),
            html.Div(id="accounts-delete-message", style={"marginBottom": "10px"}),
            dcc.ConfirmDialog(
                id="accounts-delete-confirm", message="", displayed=False
            ),
            html.Div(
                id="accounts-add-modal-overlay",
                className="accounts-add-modal-overlay",
                style=_modal_overlay_style(False),
                children=[
                    html.Div(
                        className="accounts-add-modal",
                        children=[
                            html.Div(
                                [
                                    html.H3("Add Account", style={"margin": "0"}),
                                    html.Button(
                                        "Close",
                                        id="accounts-close-add-modal",
                                        n_clicks=0,
                                    ),
                                ],
                                className="accounts-add-modal-header",
                            ),
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
                                            {
                                                "label": format_institution(value),
                                                "value": value,
                                            }
                                            for value in INSTITUTIONS
                                        ],
                                        value=INSTITUTIONS[0] if INSTITUTIONS else None,
                                        clearable=False,
                                        style={"minWidth": "220px"},
                                    ),
                                    dcc.Dropdown(
                                        id="accounts-new-type",
                                        options=[
                                            {
                                                "label": format_account_type(value),
                                                "value": value,
                                            }
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
                                ],
                                style={
                                    "display": "flex",
                                    "gap": "10px",
                                    "alignItems": "center",
                                    "flexWrap": "wrap",
                                    "marginBottom": "10px",
                                },
                            ),
                            html.Div(
                                id="accounts-message", style={"marginBottom": "6px"}
                            ),
                        ],
                    )
                ],
            ),
            html.H3(cash_header, id="accounts-cash-header"),
            _section_table("accounts-cash-table", cash_rows),
            html.H3(credit_header, id="accounts-credit-header"),
            _section_table("accounts-credit-table", credit_rows),
            html.H3(investment_header, id="accounts-investment-header"),
            _section_table("accounts-investment-table", investment_rows),
            dcc.Store(id="accounts-pending-delete", data=None),
            dcc.Store(id="accounts-add-modal-open", data=False),
        ],
        className="page page-accounts",
    )


@callback(
    Output("accounts-add-modal-open", "data"),
    Output("accounts-add-modal-overlay", "style"),
    Input("accounts-open-add-modal", "n_clicks"),
    Input("accounts-close-add-modal", "n_clicks"),
    State("accounts-add-modal-open", "data"),
    prevent_initial_call=True,
)
def toggle_add_modal(
    open_clicks: int,
    close_clicks: int,
    is_open: bool,
) -> tuple[bool, dict[str, str]]:
    trigger = ctx.triggered_id
    if trigger == "accounts-open-add-modal":
        next_state = True
    elif trigger == "accounts-close-add-modal":
        next_state = False
    else:
        next_state = bool(is_open)
    return next_state, _modal_overlay_style(next_state)


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
    Output("accounts-cash-header", "children"),
    Output("accounts-credit-header", "children"),
    Output("accounts-investment-header", "children"),
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
    str,
    str,
    str,
    str | None,
    str | None,
    str | None,
]:
    cash_rows, credit_rows, investment_rows = _all_section_rows()
    cash_header, credit_header, investment_header = _section_headers()

    if not n_clicks:
        return (
            "",
            cash_rows,
            credit_rows,
            investment_rows,
            cash_header,
            credit_header,
            investment_header,
            name,
            institution,
            account_type,
        )

    normalized_name = (name or "").strip()
    if not normalized_name:
        return (
            "Account name is required.",
            cash_rows,
            credit_rows,
            investment_rows,
            cash_header,
            credit_header,
            investment_header,
            name,
            institution,
            account_type,
        )
    if institution not in INSTITUTIONS:
        return (
            "Choose a valid institution.",
            cash_rows,
            credit_rows,
            investment_rows,
            cash_header,
            credit_header,
            investment_header,
            name,
            institution,
            account_type,
        )
    if account_type not in ACCOUNT_TYPES:
        return (
            "Choose a valid account type.",
            cash_rows,
            credit_rows,
            investment_rows,
            cash_header,
            credit_header,
            investment_header,
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
            cash_rows,
            credit_rows,
            investment_rows,
            cash_header,
            credit_header,
            investment_header,
            name,
            institution,
            account_type,
        )

    cash_rows, credit_rows, investment_rows = _all_section_rows()
    cash_header, credit_header, investment_header = _section_headers()
    return (
        message,
        cash_rows,
        credit_rows,
        investment_rows,
        cash_header,
        credit_header,
        investment_header,
        "",
        INSTITUTIONS[0] if INSTITUTIONS else None,
        "credit_card",
    )


@callback(
    Output("accounts-delete-confirm", "message"),
    Output("accounts-delete-confirm", "displayed"),
    Output("accounts-pending-delete", "data"),
    Output("accounts-cash-table", "active_cell"),
    Output("accounts-credit-table", "active_cell"),
    Output("accounts-investment-table", "active_cell"),
    Input("accounts-cash-table", "active_cell"),
    Input("accounts-credit-table", "active_cell"),
    Input("accounts-investment-table", "active_cell"),
    State("accounts-cash-table", "data"),
    State("accounts-credit-table", "data"),
    State("accounts-investment-table", "data"),
    prevent_initial_call=True,
)
def prompt_delete_account_from_action(
    cash_active: dict[str, Any] | None,
    credit_active: dict[str, Any] | None,
    investment_active: dict[str, Any] | None,
    cash_rows: list[dict[str, Any]] | None,
    credit_rows: list[dict[str, Any]] | None,
    investment_rows: list[dict[str, Any]] | None,
) -> tuple[str | Any, bool | Any, dict[str, Any] | None | Any, None, None, None]:
    trigger = ctx.triggered_id
    active_map = {
        "accounts-cash-table": (cash_active, cash_rows or []),
        "accounts-credit-table": (credit_active, credit_rows or []),
        "accounts-investment-table": (investment_active, investment_rows or []),
    }
    active_cell, rows = active_map.get(str(trigger), (None, []))

    if not active_cell or active_cell.get("column_id") != "action":
        return no_update, no_update, no_update, None, None, None

    raw_row_index = active_cell.get("row", -1)
    try:
        row_index = int(raw_row_index)
    except TypeError, ValueError:
        return no_update, no_update, no_update, None, None, None
    if row_index < 0 or row_index >= len(rows):
        return no_update, no_update, no_update, None, None, None

    raw_id = rows[row_index].get("id")
    account_name = str(rows[row_index].get("name") or "this account")
    if raw_id is None:
        return no_update, no_update, no_update, None, None, None
    try:
        account_id = int(raw_id)
    except TypeError, ValueError:
        return no_update, no_update, no_update, None, None, None

    confirm_message = (
        f"Delete {account_name}? This will also remove associated transactions, "
        "import batches, statement anchors, and holdings."
    )
    return (
        confirm_message,
        True,
        {"account_id": account_id, "account_name": account_name},
        None,
        None,
        None,
    )


@callback(
    Output("accounts-delete-message", "children"),
    Output("accounts-cash-table", "data", allow_duplicate=True),
    Output("accounts-credit-table", "data", allow_duplicate=True),
    Output("accounts-investment-table", "data", allow_duplicate=True),
    Output("accounts-cash-header", "children", allow_duplicate=True),
    Output("accounts-credit-header", "children", allow_duplicate=True),
    Output("accounts-investment-header", "children", allow_duplicate=True),
    Output("accounts-delete-confirm", "displayed", allow_duplicate=True),
    Output("accounts-pending-delete", "data", allow_duplicate=True),
    Input("accounts-delete-confirm", "submit_n_clicks"),
    Input("accounts-delete-confirm", "cancel_n_clicks"),
    State("accounts-pending-delete", "data"),
    prevent_initial_call=True,
)
def delete_account_after_confirmation(
    submit_clicks: int,
    cancel_clicks: int,
    pending_delete: dict[str, Any] | None,
) -> tuple[
    str | Any,
    list[dict[str, Any]] | Any,
    list[dict[str, Any]] | Any,
    list[dict[str, Any]] | Any,
    str | Any,
    str | Any,
    str | Any,
    bool,
    None,
]:
    _ = submit_clicks, cancel_clicks
    trigger_prop = ctx.triggered[0]["prop_id"].split(".")[-1] if ctx.triggered else ""
    if trigger_prop == "cancel_n_clicks":
        return (
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            False,
            None,
        )

    if trigger_prop != "submit_n_clicks":
        return (
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            False,
            None,
        )

    raw_id = (pending_delete or {}).get("account_id")
    if raw_id is None:
        return (
            "Could not delete account.",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            False,
            None,
        )
    try:
        account_id = int(raw_id)
    except TypeError, ValueError:
        return (
            "Could not delete account.",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            False,
            None,
        )

    deleted_accounts, tx_count, batch_count, anchor_count, holdings_count = (
        _delete_account_cascade(account_id)
    )

    cash_new, credit_new, investment_new = _all_section_rows()
    cash_header, credit_header, investment_header = _section_headers()
    message = (
        f"Removed {deleted_accounts} account(s), {tx_count} transaction(s), "
        f"{batch_count} import batch(es), {anchor_count} anchor(s), and {holdings_count} holding(s)."
    )

    return (
        message,
        cash_new,
        credit_new,
        investment_new,
        cash_header,
        credit_header,
        investment_header,
        False,
        None,
    )
