from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import time
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
import plotly.graph_objects as go
import yfinance as yf

from db import (
    create_investment_holding,
    delete_investment_holdings,
    list_investment_accounts,
    list_investment_holdings,
)
from ui_labels import (
    format_asset_type,
    format_institution,
    format_money as format_money_display,
)


register_page(__name__, path="/investments", title="Investments")

ASSET_TYPES = ["cash", "stock_etf", "crypto"]
VALUATION_METHODS = ["market", "manual"]
CACHE_TTL_SECONDS = 15 * 60


def _cache_is_fresh(cache_data: dict[str, Any] | None) -> bool:
    if not cache_data:
        return False
    fetched_at = cache_data.get("fetched_at")
    if not isinstance(fetched_at, (int, float)):
        return False
    return (time.time() - float(fetched_at)) <= CACHE_TTL_SECONDS


def _build_cache_payload(
    holdings_rows: list[dict[str, Any]],
    account_rows: list[dict[str, Any]],
    figure: go.Figure,
) -> dict[str, Any]:
    return {
        "fetched_at": time.time(),
        "holdings_rows": holdings_rows,
        "account_rows": account_rows,
        "figure": figure.to_plotly_json(),
    }


def _format_money(value: float | None) -> str:
    return format_money_display(value)


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


def _valuation_method_options() -> list[dict[str, str]]:
    return [
        {"label": "Yahoo Market Price", "value": "market"},
        {"label": "Manual Current Balance", "value": "manual"},
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
    list[dict[str, Any]], list[dict[str, Any]], go.Figure
]:
    holdings = list_investment_holdings()

    symbols = sorted(
        {
            str(row["symbol"]).upper().strip()
            for row in holdings
            if (
                row["asset_type"] != "cash"
                and row["valuation_method"] == "market"
                and row["symbol"]
            )
        }
    )
    price_map, error_map = _fetch_symbol_prices(symbols)

    table_rows: list[dict[str, Any]] = []
    by_account: dict[int, dict[str, Any]] = {}

    for row in holdings:
        holding_id = int(row["id"])
        account_id = int(row["account_id"])
        account_name = str(row["account_name"])
        institution = str(row["institution"])
        asset_type = str(row["asset_type"])
        valuation_method = str(row.get("valuation_method") or "market")
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
            note = "Cash"
        elif valuation_method == "manual":
            market_value = float(row.get("manual_market_value_cents") or 0) / 100
            cost_basis = float(row["cost_basis_total_cents"] or 0) / 100
            unrealized = market_value - cost_basis
            note = "Manual value"
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
                "action": "![Delete](/assets/svgs/trash-2.svg)",
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

    figure = _build_account_distribution_chart(by_account)
    return table_rows, account_rows, figure


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


def layout() -> html.Div:
    account_options = _investment_account_options()
    default_account = int(account_options[0]["value"]) if account_options else None
    default_asset_type = "stock_etf"
    default_valuation_method = "market"

    return html.Div(
        [
            html.Div(
                [
                    html.H2("Investments", style={"marginBottom": "0"}),
                    html.Div(
                        [
                            html.Button(
                                "Refresh Prices",
                                id="inv-refresh-prices",
                                n_clicks=0,
                            ),
                            html.Button(
                                "Add Holding",
                                id="inv-open-add-modal",
                                n_clicks=0,
                            ),
                        ],
                        style={
                            "display": "flex",
                            "gap": "10px",
                            "alignItems": "center",
                        },
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
                "Track holdings, allocation, and performance across your investment accounts."
            ),
            html.Div(
                id="inv-add-modal-overlay",
                className="inv-add-modal-overlay",
                style=_modal_overlay_style(False),
                children=[
                    html.Div(
                        className="inv-add-modal",
                        children=[
                            html.Div(
                                [
                                    html.H3("Add Holding", style={"margin": "0"}),
                                    html.Button(
                                        "Close",
                                        id="inv-close-add-modal",
                                        n_clicks=0,
                                    ),
                                ],
                                className="inv-add-modal-header",
                            ),
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
                                    dcc.Dropdown(
                                        id="inv-valuation-method",
                                        options=_valuation_method_options(),
                                        value=default_valuation_method,
                                        clearable=False,
                                        style={"minWidth": "220px"},
                                    ),
                                ],
                                style={
                                    "display": "flex",
                                    "gap": "8px",
                                    "flexWrap": "wrap",
                                    "marginBottom": "8px",
                                },
                            ),
                            html.Div(
                                [
                                    dcc.Input(
                                        id="inv-name",
                                        type="text",
                                        placeholder="Holding name",
                                        style={"width": "100%"},
                                    )
                                ],
                                style={"marginBottom": "8px"},
                            ),
                            html.Div(
                                id="inv-symbol-wrap",
                                children=[
                                    dcc.Input(
                                        id="inv-symbol",
                                        type="text",
                                        placeholder="Symbol (e.g. VTI, BTC-USD)",
                                        style={"width": "100%"},
                                    )
                                ],
                                style={"marginBottom": "8px"},
                            ),
                            html.Div(
                                id="inv-quantity-wrap",
                                children=[
                                    dcc.Input(
                                        id="inv-quantity",
                                        type="number",
                                        step="0.000001",
                                        placeholder="Quantity",
                                        style={"width": "100%"},
                                    )
                                ],
                                style={"marginBottom": "8px"},
                            ),
                            html.Div(
                                id="inv-cost-basis-wrap",
                                children=[
                                    dcc.Input(
                                        id="inv-cost-basis",
                                        type="number",
                                        step="0.01",
                                        placeholder="Cost basis total",
                                        style={"width": "100%"},
                                    )
                                ],
                                style={"marginBottom": "8px"},
                            ),
                            html.Div(
                                id="inv-manual-market-value-wrap",
                                children=[
                                    dcc.Input(
                                        id="inv-manual-market-value",
                                        type="number",
                                        step="0.01",
                                        placeholder="Current balance",
                                        style={"width": "100%"},
                                        disabled=True,
                                    )
                                ],
                                style={"display": "none", "marginBottom": "8px"},
                            ),
                            html.Div(
                                id="inv-cash-balance-wrap",
                                children=[
                                    dcc.Input(
                                        id="inv-cash-balance",
                                        type="number",
                                        step="0.01",
                                        placeholder="Cash balance",
                                        style={"width": "100%"},
                                    )
                                ],
                                style={"display": "none", "marginBottom": "8px"},
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        "Add Holding",
                                        id="inv-add-holding",
                                        n_clicks=0,
                                    )
                                ],
                                style={
                                    "display": "flex",
                                    "justifyContent": "flex-end",
                                    "marginBottom": "10px",
                                },
                            ),
                            html.Div(
                                id="inv-form-message",
                                style={"marginBottom": "8px"},
                            ),
                        ],
                    )
                ],
            ),
            dcc.Loading(
                type="default",
                children=[
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
                            {
                                "name": "",
                                "id": "action",
                                "presentation": "markdown",
                            },
                        ],
                        data=[],
                        cell_selectable=True,
                        style_table={"overflowX": "auto", "marginBottom": "12px"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_cell_conditional=cast(
                            Any,
                            [
                                {
                                    "if": {"column_id": "action"},
                                    "minWidth": "40px",
                                    "width": "40px",
                                    "maxWidth": "40px",
                                    "textAlign": "center",
                                    "cursor": "pointer",
                                }
                            ],
                        ),
                    ),
                    html.Div(
                        id="inv-remove-message",
                        style={"marginBottom": "14px"},
                    ),
                    dcc.ConfirmDialog(
                        id="inv-delete-confirm", message="", displayed=False
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
                ],
            ),
            dcc.Store(id="inv-pending-delete", data=None),
            dcc.Store(id="inv-refresh-token", data=0),
            dcc.Store(id="inv-add-modal-open", data=False),
        ],
        className="page page-investments",
    )


@callback(
    Output("inv-add-modal-open", "data"),
    Output("inv-add-modal-overlay", "style"),
    Output("inv-remove-message", "children", allow_duplicate=True),
    Input("inv-open-add-modal", "n_clicks"),
    Input("inv-close-add-modal", "n_clicks"),
    State("inv-add-modal-open", "data"),
    prevent_initial_call=True,
)
def toggle_add_modal(
    open_clicks: int,
    close_clicks: int,
    is_open: bool,
) -> tuple[bool, dict[str, str], str]:
    trigger = ctx.triggered_id
    if trigger == "inv-open-add-modal":
        next_state = True
    elif trigger == "inv-close-add-modal":
        next_state = False
    else:
        next_state = bool(is_open)
    return next_state, _modal_overlay_style(next_state), ""


@callback(
    Output("inv-symbol", "disabled"),
    Output("inv-quantity", "disabled"),
    Output("inv-cost-basis", "disabled"),
    Output("inv-manual-market-value", "disabled"),
    Output("inv-cash-balance", "disabled"),
    Output("inv-symbol-wrap", "style"),
    Output("inv-quantity-wrap", "style"),
    Output("inv-cost-basis-wrap", "style"),
    Output("inv-manual-market-value-wrap", "style"),
    Output("inv-cash-balance-wrap", "style"),
    Input("inv-asset-type", "value"),
    Input("inv-valuation-method", "value"),
)
def toggle_holding_form_fields(
    asset_type: str | None,
    valuation_method: str | None,
) -> tuple[
    bool,
    bool,
    bool,
    bool,
    bool,
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    show = {"display": "block", "marginBottom": "8px"}
    hide = {"display": "none", "marginBottom": "8px"}
    is_cash = asset_type == "cash"
    is_manual = valuation_method == "manual"

    if is_cash:
        return True, True, True, True, False, hide, hide, hide, hide, show
    if is_manual:
        return True, True, False, False, True, hide, hide, show, show, hide
    return False, False, False, True, True, show, show, show, hide, hide


@callback(
    Output("inv-form-message", "children"),
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Output("inv-dashboard-cache", "data", allow_duplicate=True),
    Output("inv-remove-message", "children", allow_duplicate=True),
    Output("inv-add-modal-open", "data", allow_duplicate=True),
    Output("inv-add-modal-overlay", "style", allow_duplicate=True),
    Output("inv-asset-type", "value"),
    Output("inv-valuation-method", "value"),
    Output("inv-symbol", "value"),
    Output("inv-name", "value"),
    Output("inv-quantity", "value"),
    Output("inv-cost-basis", "value"),
    Output("inv-manual-market-value", "value"),
    Output("inv-cash-balance", "value"),
    Input("inv-add-holding", "n_clicks"),
    State("inv-account", "value"),
    State("inv-asset-type", "value"),
    State("inv-valuation-method", "value"),
    State("inv-symbol", "value"),
    State("inv-name", "value"),
    State("inv-quantity", "value"),
    State("inv-cost-basis", "value"),
    State("inv-manual-market-value", "value"),
    State("inv-cash-balance", "value"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def add_holding(
    n_clicks: int,
    account_id: int | None,
    asset_type: str | None,
    valuation_method: str | None,
    symbol: str | None,
    name: str | None,
    quantity: str | int | float | None,
    cost_basis_total: str | int | float | None,
    manual_market_value: str | int | float | None,
    cash_balance: str | int | float | None,
    refresh_token: int,
) -> tuple[Any, ...]:
    if not n_clicks:
        return (
            "",
            refresh_token,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    account_ids = {int(a["id"]) for a in list_investment_accounts()}
    if account_id is None or int(account_id) not in account_ids:
        return (
            "Choose a valid investment account.",
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )
    if asset_type not in ASSET_TYPES:
        return (
            "Choose a valid asset type.",
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )
    if valuation_method not in VALUATION_METHODS:
        return (
            "Choose a valid valuation method.",
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    normalized_name = (name or "").strip()
    if not normalized_name:
        return (
            "Holding name is required.",
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    normalized_symbol = (symbol or "").strip().upper() or None

    def _missing(value: str | int | float | None) -> bool:
        return value is None or str(value).strip() == ""

    try:
        if asset_type == "cash":
            if _missing(cash_balance):
                return (
                    "Cash balance is required for cash holdings.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            cash_cents = _parse_dollars_to_cents(cash_balance)
            create_investment_holding(
                account_id=int(account_id),
                asset_type=asset_type,
                valuation_method="manual",
                symbol=None,
                name=normalized_name,
                quantity=None,
                cost_basis_total_cents=None,
                manual_market_value_cents=None,
                cash_balance_cents=cash_cents,
            )
        elif valuation_method == "manual":
            if _missing(cost_basis_total):
                return (
                    "Cost basis total is required for manual holdings.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            if _missing(manual_market_value):
                return (
                    "Current balance is required for manual holdings.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            cost_basis_cents = _parse_dollars_to_cents(cost_basis_total)
            manual_market_value_cents = _parse_dollars_to_cents(manual_market_value)
            create_investment_holding(
                account_id=int(account_id),
                asset_type=asset_type,
                valuation_method="manual",
                symbol=None,
                name=normalized_name,
                quantity=None,
                cost_basis_total_cents=cost_basis_cents,
                manual_market_value_cents=manual_market_value_cents,
                cash_balance_cents=None,
            )
        else:
            if not normalized_symbol:
                return (
                    "Symbol is required for non-cash holdings.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            if asset_type == "crypto" and "-" not in normalized_symbol:
                return (
                    "For crypto use Yahoo symbols like BTC-USD.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            if quantity is None or float(quantity) <= 0:
                return (
                    "Quantity must be greater than zero.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            if _missing(cost_basis_total):
                return (
                    "Cost basis total is required for Yahoo-priced holdings.",
                    refresh_token,
                    no_update,
                    "",
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            cost_basis_cents = _parse_dollars_to_cents(cost_basis_total)
            create_investment_holding(
                account_id=int(account_id),
                asset_type=asset_type,
                valuation_method="market",
                symbol=normalized_symbol,
                name=normalized_name,
                quantity=float(quantity),
                cost_basis_total_cents=cost_basis_cents,
                manual_market_value_cents=None,
                cash_balance_cents=None,
            )
    except ValueError as exc:
        return (
            str(exc),
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )
    except Exception as exc:
        return (
            f"Could not add holding: {exc}",
            refresh_token,
            no_update,
            "",
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
        )

    return (
        "Holding added.",
        refresh_token + 1,
        None,
        "",
        False,
        _modal_overlay_style(False),
        "stock_etf",
        "market",
        "",
        "",
        None,
        None,
        None,
        None,
    )


@callback(
    Output("inv-delete-confirm", "message"),
    Output("inv-delete-confirm", "displayed"),
    Output("inv-pending-delete", "data"),
    Output("inv-holdings-table", "active_cell"),
    Input("inv-holdings-table", "active_cell"),
    State("inv-holdings-table", "data"),
    prevent_initial_call=True,
)
def prompt_delete_holding_from_action(
    active_cell: dict[str, Any] | None,
    table_data: list[dict[str, Any]] | None,
) -> tuple[str | Any, bool | Any, dict[str, Any] | None | Any, None]:
    rows = table_data or []
    if not active_cell or active_cell.get("column_id") != "action":
        return no_update, no_update, no_update, None

    raw_row_index = active_cell.get("row", -1)
    try:
        row_index = int(raw_row_index)
    except TypeError, ValueError:
        return no_update, no_update, no_update, None

    if row_index < 0 or row_index >= len(rows):
        return no_update, no_update, no_update, None

    selected_row = rows[row_index]
    raw_id = selected_row.get("id")
    if raw_id is None:
        return no_update, no_update, no_update, None

    try:
        holding_id = int(raw_id)
    except TypeError, ValueError:
        return no_update, no_update, no_update, None

    holding_name = str(selected_row.get("name") or "this holding")
    symbol = str(selected_row.get("symbol") or "").strip()
    descriptor = (
        f"{holding_name} ({symbol})" if symbol and symbol != "-" else holding_name
    )

    return (
        f"Delete {descriptor}?",
        True,
        {"holding_id": holding_id},
        None,
    )


@callback(
    Output("inv-remove-message", "children"),
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Output("inv-delete-confirm", "displayed", allow_duplicate=True),
    Output("inv-pending-delete", "data", allow_duplicate=True),
    Output("inv-dashboard-cache", "data", allow_duplicate=True),
    Input("inv-delete-confirm", "submit_n_clicks"),
    Input("inv-delete-confirm", "cancel_n_clicks"),
    State("inv-pending-delete", "data"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def delete_holding_after_confirmation(
    submit_clicks: int,
    cancel_clicks: int,
    pending_delete: dict[str, Any] | None,
    refresh_token: int,
) -> tuple[str | Any, int | Any, bool, None, dict[str, Any] | None | Any]:
    _ = submit_clicks, cancel_clicks
    trigger_prop = ctx.triggered[0]["prop_id"].split(".")[-1] if ctx.triggered else ""

    if trigger_prop == "cancel_n_clicks":
        return no_update, no_update, False, None, no_update
    if trigger_prop != "submit_n_clicks":
        return no_update, no_update, False, None, no_update

    raw_id = (pending_delete or {}).get("holding_id")
    if raw_id is None:
        return no_update, no_update, False, None, no_update

    try:
        holding_id = int(raw_id)
    except TypeError, ValueError:
        return "Could not delete holding.", no_update, False, None, no_update

    removed = delete_investment_holdings([holding_id])
    return f"Removed {removed} holding(s).", refresh_token + 1, False, None, None


@callback(
    Output("inv-refresh-token", "data", allow_duplicate=True),
    Output("inv-dashboard-cache", "data", allow_duplicate=True),
    Output("inv-remove-message", "children", allow_duplicate=True),
    Input("inv-refresh-prices", "n_clicks"),
    State("inv-refresh-token", "data"),
    prevent_initial_call=True,
)
def refresh_prices(
    n_clicks: int,
    refresh_token: int,
) -> tuple[int, None | Any, str | Any]:
    if not n_clicks:
        return refresh_token, no_update, no_update
    return refresh_token + 1, None, ""


@callback(
    Output("inv-account", "options"),
    Output("inv-account", "value"),
    Output("inv-holdings-table", "data"),
    Output("inv-account-overview", "data"),
    Output("inv-portfolio-chart", "figure"),
    Output("inv-dashboard-cache", "data"),
    Input("inv-refresh-token", "data"),
    State("inv-dashboard-cache", "data"),
)
def refresh_dashboard_data(
    _refresh_token: int,
    cache_data: dict[str, Any] | None,
) -> tuple[
    list[dict[str, str | int]],
    int | None,
    list[dict[str, Any]],
    list[dict[str, Any]],
    go.Figure,
    dict[str, Any] | Any,
]:
    account_options = _investment_account_options()
    default_account = int(account_options[0]["value"]) if account_options else None

    if _cache_is_fresh(cache_data):
        cache_dict = cast(dict[str, Any], cache_data)
        cached_holdings = cache_dict.get("holdings_rows")
        cached_accounts = cache_dict.get("account_rows")
        cached_figure = cache_dict.get("figure")
        if (
            isinstance(cached_holdings, list)
            and isinstance(cached_accounts, list)
            and cached_figure is not None
        ):
            return (
                account_options,
                default_account,
                cached_holdings,
                cached_accounts,
                cast(Any, cached_figure),
                no_update,
            )

    holdings_rows, account_rows, figure = _build_dashboard_data()
    cache_payload = _build_cache_payload(holdings_rows, account_rows, figure)
    return (
        account_options,
        default_account,
        holdings_rows,
        account_rows,
        figure,
        cache_payload,
    )
