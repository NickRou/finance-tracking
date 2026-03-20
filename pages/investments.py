from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
import plotly.graph_objects as go
import yfinance as yf

from db import (
    create_investment_holding,
    delete_investment_holdings,
    list_investment_accounts,
    list_investment_holdings,
)
from ui_labels import format_asset_type, format_institution


register_page(__name__, path="/investments", title="Investments")

ASSET_TYPES = ["cash", "stock_etf", "crypto", "bond_fund", "other"]


def _format_money(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _parse_dollars_to_cents(value: str | int | float | None) -> int:
    if value is None:
        raise ValueError("Value is required.")
    raw = str(value).strip().replace("$", "").replace(",", "")
    if not raw:
        raise ValueError("Value is required.")
    try:
        amount = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("Invalid dollar value.") from exc
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _investment_account_options() -> list[dict[str, str | int]]:
    options: list[dict[str, str | int]] = []
    for account in list_investment_accounts():
        options.append(
            {
                "label": (
                    f"{account['name']} "
                    f"({format_institution(str(account['institution']))})"
                ),
                "value": int(account["id"]),
            }
        )
    return options


def _asset_type_options() -> list[dict[str, str]]:
    return [
        {"label": format_asset_type(value), "value": value} for value in ASSET_TYPES
    ]


def _fetch_symbol_prices(symbols: list[str]) -> tuple[dict[str, float], dict[str, str]]:
    price_map: dict[str, float] = {}
    error_map: dict[str, str] = {}

    for symbol in symbols:
        try:
            history = yf.Ticker(symbol).history(period="6mo", interval="1d")
            if history.empty or "Close" not in history:
                error_map[symbol] = "No price history"
                continue

            close_series = history["Close"].dropna()
            if close_series.empty:
                error_map[symbol] = "No close prices"
                continue

            price_map[symbol] = float(close_series.iloc[-1])
        except Exception as exc:
            error_map[symbol] = str(exc)

    return price_map, error_map


def _build_account_distribution_chart(
    by_account: dict[int, dict[str, Any]],
) -> go.Figure:
    labels: list[str] = []
    values: list[float] = []
    for row in by_account.values():
        market_value = float(row["market_value"])
        if market_value <= 0:
            continue
        labels.append(str(row["account"]))
        values.append(market_value)

    fig = go.Figure()
    if values:
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=values,
                hole=0.45,
                sort=False,
                textinfo="label+percent",
            )
        )
    else:
        fig.add_annotation(
            text="No market history available yet.",
            showarrow=False,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
        )

    fig.update_layout(
        title="Investment Allocation by Account",
        margin={"l": 30, "r": 20, "t": 50, "b": 30},
        height=360,
    )
    return fig


def _build_dashboard_data() -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[html.Div], go.Figure
]:
    holdings = list_investment_holdings()

    symbols = sorted(
        {
            str(row["symbol"]).upper().strip()
            for row in holdings
            if row["asset_type"] != "cash" and row["symbol"]
        }
    )
    price_map, error_map = _fetch_symbol_prices(symbols)

    table_rows: list[dict[str, Any]] = []
    by_account: dict[int, dict[str, Any]] = {}

    total_market = 0.0
    total_cost_basis = 0.0

    for row in holdings:
        holding_id = int(row["id"])
        account_id = int(row["account_id"])
        account_name = str(row["account_name"])
        institution = str(row["institution"])
        asset_type = str(row["asset_type"])
        symbol = str(row["symbol"] or "")
        name = str(row["name"])

        latest_price: float | None = None
        cost_basis = 0.0
        market_value = 0.0
        unrealized: float | None = None
        qty_display = "-"
        price_display = "-"
        note = ""

        if asset_type == "cash":
            market_value = float(row["cash_balance_cents"] or 0) / 100
            cost_basis = market_value
            unrealized = 0.0
        else:
            quantity = float(row["quantity"] or 0)
            qty_display = f"{quantity:,.6f}".rstrip("0").rstrip(".")
            cost_basis = float(row["cost_basis_total_cents"] or 0) / 100
            symbol_key = symbol.upper()
            latest_price = price_map.get(symbol_key)
            if latest_price is not None:
                market_value = quantity * latest_price
                unrealized = market_value - cost_basis
                price_display = _format_money(latest_price)
            else:
                note = error_map.get(symbol_key, "Price unavailable")

        total_market += market_value
        total_cost_basis += cost_basis

        agg = by_account.setdefault(
            account_id,
            {
                "account": account_name,
                "institution": institution,
                "holdings_count": 0,
                "market_value": 0.0,
                "cost_basis": 0.0,
                "cash_value": 0.0,
            },
        )
        agg["holdings_count"] += 1
        agg["market_value"] += market_value
        agg["cost_basis"] += cost_basis
        if asset_type == "cash":
            agg["cash_value"] += market_value

        table_rows.append(
            {
                "id": holding_id,
                "account": account_name,
                "institution": format_institution(institution),
                "asset_type": format_asset_type(asset_type),
                "symbol": symbol or "-",
                "name": name,
                "quantity": qty_display,
                "cost_basis": _format_money(cost_basis),
                "latest_price": price_display,
                "market_value": _format_money(market_value),
                "unrealized_pl": _format_money(unrealized),
                "note": note,
            }
        )

    account_rows: list[dict[str, Any]] = []
    for row in by_account.values():
        cash_pct = (
            (row["cash_value"] / row["market_value"] * 100)
            if row["market_value"]
            else 0.0
        )
        account_rows.append(
            {
                "account": row["account"],
                "institution": format_institution(str(row["institution"])),
                "holdings_count": row["holdings_count"],
                "market_value": _format_money(row["market_value"]),
                "cost_basis": _format_money(row["cost_basis"]),
                "unrealized_pl": _format_money(row["market_value"] - row["cost_basis"]),
                "cash_pct": f"{cash_pct:.1f}%",
            }
        )

    cash_total = sum(float(r.get("cash_value", 0.0)) for r in by_account.values())
    invested_total = max(total_market - cash_total, 0.0)
    cards = [
        html.Div(
            [html.Div("Market Value"), html.Strong(_format_money(total_market))],
            style=_card_style(),
        ),
        html.Div(
            [html.Div("Cost Basis"), html.Strong(_format_money(total_cost_basis))],
            style=_card_style(),
        ),
        html.Div(
            [
                html.Div("Unrealized P/L"),
                html.Strong(_format_money(total_market - total_cost_basis)),
            ],
            style=_card_style(),
        ),
        html.Div(
            [
                html.Div("Cash / Invested"),
                html.Strong(
                    f"{_format_money(cash_total)} / {_format_money(invested_total)}"
                ),
            ],
            style=_card_style(),
        ),
    ]

    figure = _build_account_distribution_chart(by_account)
    return table_rows, account_rows, cards, figure


def _card_style() -> dict[str, str]:
    return {
        "minWidth": "190px",
        "padding": "12px",
        "border": "1px solid #cfcfcf",
        "borderRadius": "8px",
        "backgroundColor": "#f8f8f8",
    }


def layout() -> html.Div:
    account_options = _investment_account_options()
    default_account = int(account_options[0]["value"]) if account_options else None
    default_asset_type = "stock_etf"

    return html.Div(
        [
            html.H2("Investments"),
            html.Div(
                [
                    dcc.Dropdown(
                        id="inv-account",
                        options=account_options,
                        value=default_account,
                        clearable=False,
                        placeholder="Select investment account",
                        style={"minWidth": "230px"},
                    ),
                    dcc.Dropdown(
                        id="inv-asset-type",
                        options=_asset_type_options(),
                        value=default_asset_type,
                        clearable=False,
                        style={"minWidth": "170px"},
                    ),
                    dcc.Input(
                        id="inv-symbol",
                        type="text",
                        placeholder="Symbol (e.g. VTI, BTC-USD)",
                        style={"minWidth": "220px"},
                    ),
                    dcc.Input(
                        id="inv-name",
                        type="text",
                        placeholder="Holding name",
                        style={"minWidth": "220px"},
                    ),
                    dcc.Input(
                        id="inv-quantity",
                        type="number",
                        step="0.000001",
                        placeholder="Quantity",
                        style={"minWidth": "140px"},
                    ),
                    dcc.Input(
                        id="inv-cost-basis",
                        type="number",
                        step="0.01",
                        placeholder="Cost basis total",
                        style={"minWidth": "170px"},
                    ),
                    dcc.Input(
                        id="inv-cash-balance",
                        type="number",
                        step="0.01",
                        placeholder="Cash balance",
                        style={"minWidth": "170px"},
                    ),
                    html.Button("Add Holding", id="inv-add-holding", n_clicks=0),
                    html.Button("Refresh Prices", id="inv-refresh-prices", n_clicks=0),
                ],
                style={
                    "display": "flex",
                    "gap": "8px",
                    "flexWrap": "wrap",
                    "marginBottom": "10px",
                },
            ),
            html.Div(id="inv-form-message", style={"marginBottom": "12px"}),
            dash_table.DataTable(
                id="inv-holdings-table",
                columns=[
                    {"name": "Account", "id": "account"},
                    {"name": "Institution", "id": "institution"},
                    {"name": "Type", "id": "asset_type"},
                    {"name": "Symbol", "id": "symbol"},
                    {"name": "Name", "id": "name"},
                    {"name": "Quantity", "id": "quantity"},
                    {"name": "Cost Basis", "id": "cost_basis"},
                    {"name": "Latest Price", "id": "latest_price"},
                    {"name": "Market Value", "id": "market_value"},
                    {"name": "Unrealized P/L", "id": "unrealized_pl"},
                    {"name": "Note", "id": "note"},
                ],
                data=[],
                row_selectable="multi",
                selected_rows=[],
                style_table={"overflowX": "auto", "marginBottom": "12px"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            html.Button(
                "Remove Selected Holdings",
                id="inv-remove-holdings",
                n_clicks=0,
                disabled=True,
            ),
            html.Div(
                id="inv-remove-message",
                style={"marginTop": "8px", "marginBottom": "14px"},
            ),
            html.Div(
                id="inv-kpi-cards",
                style={
                    "display": "flex",
                    "gap": "12px",
                    "flexWrap": "wrap",
                    "marginBottom": "14px",
                },
            ),
            html.H3("By Account"),
            dash_table.DataTable(
                id="inv-account-overview",
                columns=[
                    {"name": "Account", "id": "account"},
                    {"name": "Institution", "id": "institution"},
                    {"name": "Holdings", "id": "holdings_count"},
                    {"name": "Market Value", "id": "market_value"},
                    {"name": "Cost Basis", "id": "cost_basis"},
                    {"name": "Unrealized P/L", "id": "unrealized_pl"},
                    {"name": "Cash %", "id": "cash_pct"},
                ],
                data=[],
                style_table={"overflowX": "auto", "marginBottom": "16px"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            dcc.Graph(id="inv-portfolio-chart", figure=go.Figure()),
            dcc.Store(id="inv-refresh-token", data=0),
        ],
        className="page page-investments",
    )


@callback(
    Output("inv-remove-holdings", "disabled"),
    Input("inv-holdings-table", "data"),
    Input("inv-holdings-table", "selected_rows"),
)
def toggle_remove_holdings(
    table_data: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
) -> bool:
    if not table_data:
        return True
    return not bool(selected_rows)


@callback(
    Output("inv-form-message", "children"),
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Input("inv-add-holding", "n_clicks"),
    State("inv-account", "value"),
    State("inv-asset-type", "value"),
    State("inv-symbol", "value"),
    State("inv-name", "value"),
    State("inv-quantity", "value"),
    State("inv-cost-basis", "value"),
    State("inv-cash-balance", "value"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def add_holding(
    n_clicks: int,
    account_id: int | None,
    asset_type: str | None,
    symbol: str | None,
    name: str | None,
    quantity: str | int | float | None,
    cost_basis_total: str | int | float | None,
    cash_balance: str | int | float | None,
    refresh_token: int,
) -> tuple[str, int]:
    if not n_clicks:
        return "", refresh_token

    account_ids = {int(a["id"]) for a in list_investment_accounts()}
    if account_id is None or int(account_id) not in account_ids:
        return "Choose a valid investment account.", refresh_token
    if asset_type not in ASSET_TYPES:
        return "Choose a valid asset type.", refresh_token

    normalized_name = (name or "").strip()
    if not normalized_name:
        return "Holding name is required.", refresh_token

    normalized_symbol = (symbol or "").strip().upper() or None

    try:
        if asset_type == "cash":
            cash_cents = _parse_dollars_to_cents(cash_balance)
            create_investment_holding(
                account_id=int(account_id),
                asset_type=asset_type,
                symbol=None,
                name=normalized_name,
                quantity=None,
                cost_basis_total_cents=None,
                cash_balance_cents=cash_cents,
            )
        else:
            if not normalized_symbol:
                return "Symbol is required for non-cash holdings.", refresh_token
            if asset_type == "crypto" and "-" not in normalized_symbol:
                return "For crypto use Yahoo symbols like BTC-USD.", refresh_token
            if quantity is None or float(quantity) <= 0:
                return "Quantity must be greater than zero.", refresh_token
            cost_basis_cents = _parse_dollars_to_cents(cost_basis_total)
            create_investment_holding(
                account_id=int(account_id),
                asset_type=asset_type,
                symbol=normalized_symbol,
                name=normalized_name,
                quantity=float(quantity),
                cost_basis_total_cents=cost_basis_cents,
                cash_balance_cents=None,
            )
    except ValueError as exc:
        return str(exc), refresh_token
    except Exception as exc:
        return f"Could not add holding: {exc}", refresh_token

    return "Holding added.", refresh_token + 1


@callback(
    Output("inv-remove-message", "children"),
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Output("inv-holdings-table", "selected_rows"),
    Input("inv-remove-holdings", "n_clicks"),
    State("inv-holdings-table", "data"),
    State("inv-holdings-table", "selected_rows"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def remove_holdings(
    n_clicks: int,
    table_data: list[dict[str, Any]] | None,
    selected_rows: list[int] | None,
    refresh_token: int,
) -> tuple[str, int, list[int]]:
    if not n_clicks:
        return "", refresh_token, []
    if not table_data or not selected_rows:
        return "Select holdings to remove.", refresh_token, []

    selected_ids: list[int] = []
    for idx in selected_rows:
        if idx < 0 or idx >= len(table_data):
            continue
        raw_id = table_data[idx].get("id")
        if raw_id is None:
            continue
        try:
            selected_ids.append(int(raw_id))
        except TypeError, ValueError:
            continue

    removed = delete_investment_holdings(selected_ids)
    return f"Removed {removed} holding(s).", refresh_token + 1, []


@callback(
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Input("inv-refresh-prices", "n_clicks"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def refresh_prices(n_clicks: int, refresh_token: int) -> int:
    if not n_clicks:
        return refresh_token
    return refresh_token + 1


@callback(
    Output("inv-account", "options"),
    Output("inv-account", "value"),
    Output("inv-holdings-table", "data"),
    Output("inv-account-overview", "data"),
    Output("inv-kpi-cards", "children"),
    Output("inv-portfolio-chart", "figure"),
    Input("inv-refresh-token", "data"),
)
def refresh_dashboard_data(
    _refresh_token: int,
) -> tuple[
    list[dict[str, str | int]],
    int | None,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[html.Div],
    go.Figure,
]:
    account_options = _investment_account_options()
    default_account = int(account_options[0]["value"]) if account_options else None
    holdings_rows, account_rows, cards, figure = _build_dashboard_data()
    return (
        account_options,
        default_account,
        holdings_rows,
        account_rows,
        cards,
        figure,
    )
