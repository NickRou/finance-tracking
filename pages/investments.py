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
    create_investment_holding_snapshot,
    delete_investment_holdings,
    get_investment_holding_by_id,
    list_investment_accounts,
    list_investment_holdings,
    update_investment_holding,
)
from ui_labels import (
    format_asset_type,
    format_account_type,
    format_institution,
    format_money as format_money_display,
)


register_page(__name__, path="/investments", title="Investments")

ASSET_TYPES = ["cash", "stock_etf", "crypto"]
VALUATION_METHODS = ["market", "manual"]
CACHE_TTL_SECONDS = 15 * 60
CACHE_VERSION = 4
DONUT_DOMAIN_X = [0.08, 0.78]


def _cache_is_fresh(cache_data: dict[str, Any] | None) -> bool:
    if not cache_data:
        return False
    version = cache_data.get("version")
    try:
        parsed_version = int(version or 0)
    except TypeError, ValueError:
        return False
    if parsed_version != CACHE_VERSION:
        return False
    fetched_at = cache_data.get("fetched_at")
    if not isinstance(fetched_at, (int, float)):
        return False
    return (time.time() - float(fetched_at)) <= CACHE_TTL_SECONDS


def _build_cache_payload(
    holdings_rows: list[dict[str, Any]],
    account_rows: list[dict[str, Any]],
    figure: go.Figure,
    allocation_rows: list[dict[str, Any]],
    total_market_value_cents: int,
) -> dict[str, Any]:
    return {
        "version": CACHE_VERSION,
        "fetched_at": time.time(),
        "holdings_rows": holdings_rows,
        "account_rows": account_rows,
        "figure": figure.to_plotly_json(),
        "allocation_rows": allocation_rows,
        "total_market_value_cents": total_market_value_cents,
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
                    f"({format_institution(str(account['institution']))}, "
                    f"{format_account_type(str(account['account_type']))})"
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
                showlegend=True,
                domain={"x": DONUT_DOMAIN_X, "y": [0.0, 1.0]},
                hovertemplate="%{label}<br>%{value:$,.2f} (%{percent})<extra></extra>",
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
        legend={
            "orientation": "v",
            "x": 1.02,
            "xanchor": "left",
            "y": 1.0,
            "yanchor": "top",
        },
    )
    return fig


def _build_account_holdings_chart(
    allocation_rows: list[dict[str, Any]],
    account_id: int | None,
) -> go.Figure:
    fig = go.Figure()
    if account_id is None:
        fig.add_annotation(
            text="Select an investment account to view allocation.",
            showarrow=False,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
        )
        fig.update_layout(
            title="Investment Allocation within Account",
            margin={"l": 30, "r": 20, "t": 50, "b": 30},
            height=360,
        )
        return fig

    rows = [
        row
        for row in allocation_rows
        if int(row.get("account_id", -1)) == int(account_id)
        and float(row.get("market_value", 0.0)) > 0
    ]
    labels = [str(row.get("holding_label") or "Unknown") for row in rows]
    values = [float(row.get("market_value") or 0.0) for row in rows]

    if values:
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=values,
                hole=0.45,
                sort=False,
                textinfo="label+percent",
                showlegend=True,
                domain={"x": DONUT_DOMAIN_X, "y": [0.0, 1.0]},
                hovertemplate="%{label}<br>%{value:$,.2f} (%{percent})<extra></extra>",
            )
        )
    else:
        fig.add_annotation(
            text="No positive market value holdings for this account.",
            showarrow=False,
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
        )

    fig.update_layout(
        title="Investment Allocation within Account",
        margin={"l": 30, "r": 20, "t": 50, "b": 30},
        height=360,
        legend={
            "orientation": "v",
            "x": 1.02,
            "xanchor": "left",
            "y": 1.0,
            "yanchor": "top",
        },
    )
    return fig


def _build_dashboard_data() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    go.Figure,
    list[dict[str, Any]],
    int,
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
    allocation_rows: list[dict[str, Any]] = []
    by_account: dict[int, dict[str, Any]] = {}
    total_market_value_cents = 0

    for row in holdings:
        holding_id = int(row["id"])
        account_id = int(row["account_id"])
        account_name = str(row["account_name"])
        institution = str(row["institution"])
        asset_type = str(row["asset_type"])
        valuation_method = str(row.get("valuation_method") or "market")
        symbol = str(row["symbol"] or "").strip().upper()
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
        total_market_value_cents += int(round(market_value * 100))

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
                "action": (
                    f"[![Edit](/assets/svgs/pencil.svg)](#inv-edit-{holding_id}) "
                    f"[![Delete](/assets/svgs/trash-2.svg)](#inv-delete-{holding_id})"
                ),
            }
        )

        if asset_type == "cash":
            holding_label = "Cash"
        elif valuation_method == "market" and symbol:
            holding_label = symbol
        else:
            holding_label = name

        allocation_rows.append(
            {
                "account_id": account_id,
                "account_name": account_name,
                "holding_label": holding_label,
                "market_value": market_value,
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
    return table_rows, account_rows, figure, allocation_rows, total_market_value_cents


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
                "Track holdings, allocation, and performance across your investment accounts.",
                style={"margin": "0 0 10px 0"},
            ),
            html.Div(
                [
                    html.Span(
                        "Total Market Value",
                        style={
                            "fontSize": "0.8rem",
                            "fontWeight": "700",
                            "textTransform": "uppercase",
                            "letterSpacing": "0.04em",
                            "color": "#7f7768",
                        },
                    ),
                    html.Div(
                        id="inv-total-market-value",
                        children="$0.00",
                        style={
                            "fontSize": "1.25rem",
                            "fontWeight": "700",
                            "lineHeight": "1.15",
                        },
                    ),
                ],
                style={
                    "display": "inline-flex",
                    "flexDirection": "column",
                    "alignItems": "flex-start",
                    "gap": "2px",
                    "padding": "8px 12px",
                    "border": "1px solid #d7d1c6",
                    "borderRadius": "10px",
                    "backgroundColor": "#f8f4eb",
                    "minWidth": "220px",
                    "marginBottom": "10px",
                },
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
                                    html.H3(
                                        "Add Holding",
                                        id="inv-modal-title",
                                        style={"margin": "0"},
                                    ),
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
                            {"name": "Quantity", "id": "quantity"},
                            {"name": "Cost Basis", "id": "cost_basis"},
                            {"name": "Latest Price", "id": "latest_price"},
                            {"name": "Market Value", "id": "market_value"},
                            {"name": "Unrealized P/L", "id": "unrealized_pl"},
                            {
                                "name": "",
                                "id": "action",
                                "presentation": "markdown",
                            },
                        ],
                        data=[],
                        cell_selectable=False,
                        markdown_options={"link_target": "_self"},
                        style_table={"overflowX": "auto", "marginBottom": "12px"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_cell_conditional=cast(
                            Any,
                            [
                                {
                                    "if": {"column_id": "action"},
                                    "minWidth": "64px",
                                    "width": "64px",
                                    "maxWidth": "64px",
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
                    dcc.Tabs(
                        id="inv-chart-view",
                        value="by_account",
                        parent_className="inv-chart-tabs",
                        className="inv-chart-tabs-inner",
                        children=[
                            dcc.Tab(
                                label="Allocation by Account",
                                value="by_account",
                                className="inv-chart-tab",
                                selected_className="inv-chart-tab-selected",
                            ),
                            dcc.Tab(
                                label="Allocation within Account",
                                value="within_account",
                                className="inv-chart-tab",
                                selected_className="inv-chart-tab-selected",
                            ),
                        ],
                    ),
                    html.Div(
                        id="inv-chart-account-wrap",
                        children=[
                            dcc.Dropdown(
                                id="inv-chart-account",
                                options=[],
                                value=None,
                                placeholder="Select account",
                                clearable=False,
                                style={"maxWidth": "460px", "marginTop": "10px"},
                            )
                        ],
                        style={"display": "none"},
                    ),
                    dcc.Graph(id="inv-portfolio-chart", figure=go.Figure()),
                ],
            ),
            dcc.Store(id="inv-pending-delete", data=None),
            dcc.Store(id="inv-refresh-token", data=0),
            dcc.Store(id="inv-add-modal-open", data=False),
            dcc.Store(id="inv-form-mode", data="add"),
            dcc.Store(id="inv-edit-holding-id", data=None),
            dcc.Location(id="inv-action-location", refresh=False),
        ],
        className="page page-investments",
    )


@callback(
    Output("inv-add-modal-open", "data"),
    Output("inv-add-modal-overlay", "style"),
    Output("inv-remove-message", "children", allow_duplicate=True),
    Output("inv-form-mode", "data", allow_duplicate=True),
    Output("inv-edit-holding-id", "data", allow_duplicate=True),
    Input("inv-open-add-modal", "n_clicks"),
    Input("inv-close-add-modal", "n_clicks"),
    State("inv-add-modal-open", "data"),
    prevent_initial_call=True,
)
def toggle_add_modal(
    open_clicks: int,
    close_clicks: int,
    is_open: bool,
) -> tuple[bool, dict[str, str], str, str | Any, None | Any]:
    trigger = ctx.triggered_id
    if trigger == "inv-open-add-modal":
        next_state = True
    elif trigger == "inv-close-add-modal":
        next_state = False
    else:
        next_state = bool(is_open)
    if next_state:
        return next_state, _modal_overlay_style(next_state), "", "add", None
    return next_state, _modal_overlay_style(next_state), "", no_update, no_update


@callback(
    Output("inv-add-holding", "children"),
    Input("inv-form-mode", "data"),
)
def update_submit_label(form_mode: str | None) -> str:
    if form_mode == "edit":
        return "Save Changes"
    return "Add Holding"


@callback(
    Output("inv-account", "disabled"),
    Input("inv-form-mode", "data"),
)
def lock_account_on_edit(form_mode: str | None) -> bool:
    return form_mode == "edit"


@callback(
    Output("inv-modal-title", "children"),
    Input("inv-form-mode", "data"),
    Input("inv-name", "value"),
)
def update_modal_title(form_mode: str | None, holding_name: str | None) -> str:
    if form_mode != "edit":
        return "Add Holding"
    name = (holding_name or "").strip()
    if name:
        return f"Edit Holding: {name}"
    return "Edit Holding"


@callback(
    Output("inv-form-mode", "data", allow_duplicate=True),
    Output("inv-edit-holding-id", "data", allow_duplicate=True),
    Input("inv-add-modal-open", "data"),
    prevent_initial_call=True,
)
def reset_form_mode_on_modal_close(is_open: bool) -> tuple[str | Any, None | Any]:
    if is_open:
        return no_update, no_update
    return "add", None


@callback(
    Output("inv-asset-type", "disabled"),
    Output("inv-valuation-method", "disabled"),
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
    Input("inv-account", "value"),
    Input("inv-asset-type", "value"),
    Input("inv-valuation-method", "value"),
)
def toggle_holding_form_fields(
    account_id: int | None,
    asset_type: str | None,
    valuation_method: str | None,
) -> tuple[
    bool,
    bool,
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
    account_type_by_id = {
        int(row["id"]): str(row.get("account_type", ""))
        for row in list_investment_accounts()
    }
    is_savings_selected = account_id is not None and (
        account_type_by_id.get(int(account_id)) == "savings_account"
    )
    is_cash = asset_type == "cash" or is_savings_selected
    is_manual = valuation_method == "manual" or is_savings_selected
    lock_asset_controls = is_savings_selected

    if is_cash:
        return (
            lock_asset_controls,
            lock_asset_controls,
            True,
            True,
            True,
            True,
            False,
            hide,
            hide,
            hide,
            hide,
            show,
        )
    if is_manual:
        return (
            lock_asset_controls,
            lock_asset_controls,
            True,
            True,
            False,
            False,
            True,
            hide,
            hide,
            show,
            show,
            hide,
        )
    return (
        lock_asset_controls,
        lock_asset_controls,
        False,
        False,
        False,
        True,
        True,
        show,
        show,
        show,
        hide,
        hide,
    )


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
    State("inv-form-mode", "data"),
    State("inv-edit-holding-id", "data"),
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
    form_mode: str | None,
    edit_holding_id: int | None,
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

    account_map = {int(a["id"]): a for a in list_investment_accounts()}
    account_ids = set(account_map.keys())
    is_edit = form_mode == "edit"

    target_account_id: int | None = None
    if is_edit:
        if edit_holding_id is None:
            return (
                "Select a holding to edit.",
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
        existing = get_investment_holding_by_id(int(edit_holding_id))
        if existing is None:
            return (
                "Selected holding no longer exists.",
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
        target_account_id = int(existing["account_id"])
    else:
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
        target_account_id = int(account_id)

    if target_account_id is None:
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

    resolved_account_id = int(target_account_id)

    selected_account = account_map.get(resolved_account_id, {})
    selected_account_type = str(selected_account.get("account_type") or "")

    effective_asset_type = asset_type
    effective_valuation_method = valuation_method
    if selected_account_type == "savings_account":
        effective_asset_type = "cash"
        effective_valuation_method = "manual"

    if effective_asset_type not in ASSET_TYPES:
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
    if effective_valuation_method not in VALUATION_METHODS:
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
        target_holding_id: int | None = None
        if effective_asset_type == "cash":
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
            if is_edit and edit_holding_id is not None:
                updated = update_investment_holding(
                    holding_id=int(edit_holding_id),
                    asset_type=effective_asset_type,
                    valuation_method="manual",
                    symbol=None,
                    name=normalized_name,
                    quantity=None,
                    cost_basis_total_cents=None,
                    manual_market_value_cents=None,
                    cash_balance_cents=cash_cents,
                )
                if updated <= 0:
                    return (
                        "Could not update holding.",
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
                target_holding_id = int(edit_holding_id)
            else:
                target_holding_id = create_investment_holding(
                    account_id=resolved_account_id,
                    asset_type=effective_asset_type,
                    valuation_method="manual",
                    symbol=None,
                    name=normalized_name,
                    quantity=None,
                    cost_basis_total_cents=None,
                    manual_market_value_cents=None,
                    cash_balance_cents=cash_cents,
                )
            create_investment_holding_snapshot(
                holding_id=target_holding_id,
                account_id=resolved_account_id,
                event_type="manual_update" if is_edit else "add",
                asset_type=effective_asset_type,
                valuation_method="manual",
                symbol=None,
                name=normalized_name,
                quantity=None,
                cost_basis_total_cents=None,
                manual_market_value_cents=None,
                cash_balance_cents=cash_cents,
                market_value_cents=cash_cents,
            )
        elif effective_valuation_method == "manual":
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
            if is_edit and edit_holding_id is not None:
                updated = update_investment_holding(
                    holding_id=int(edit_holding_id),
                    asset_type=effective_asset_type,
                    valuation_method="manual",
                    symbol=None,
                    name=normalized_name,
                    quantity=None,
                    cost_basis_total_cents=cost_basis_cents,
                    manual_market_value_cents=manual_market_value_cents,
                    cash_balance_cents=None,
                )
                if updated <= 0:
                    return (
                        "Could not update holding.",
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
                target_holding_id = int(edit_holding_id)
            else:
                target_holding_id = create_investment_holding(
                    account_id=resolved_account_id,
                    asset_type=effective_asset_type,
                    valuation_method="manual",
                    symbol=None,
                    name=normalized_name,
                    quantity=None,
                    cost_basis_total_cents=cost_basis_cents,
                    manual_market_value_cents=manual_market_value_cents,
                    cash_balance_cents=None,
                )
            create_investment_holding_snapshot(
                holding_id=target_holding_id,
                account_id=resolved_account_id,
                event_type="manual_update" if is_edit else "add",
                asset_type=effective_asset_type,
                valuation_method="manual",
                symbol=None,
                name=normalized_name,
                quantity=None,
                cost_basis_total_cents=cost_basis_cents,
                manual_market_value_cents=manual_market_value_cents,
                cash_balance_cents=None,
                market_value_cents=manual_market_value_cents,
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
            if effective_asset_type == "crypto" and "-" not in normalized_symbol:
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
            if is_edit and edit_holding_id is not None:
                updated = update_investment_holding(
                    holding_id=int(edit_holding_id),
                    asset_type=effective_asset_type,
                    valuation_method="market",
                    symbol=normalized_symbol,
                    name=normalized_name,
                    quantity=float(quantity),
                    cost_basis_total_cents=cost_basis_cents,
                    manual_market_value_cents=None,
                    cash_balance_cents=None,
                )
                if updated <= 0:
                    return (
                        "Could not update holding.",
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
                target_holding_id = int(edit_holding_id)
            else:
                target_holding_id = create_investment_holding(
                    account_id=resolved_account_id,
                    asset_type=effective_asset_type,
                    valuation_method="market",
                    symbol=normalized_symbol,
                    name=normalized_name,
                    quantity=float(quantity),
                    cost_basis_total_cents=cost_basis_cents,
                    manual_market_value_cents=None,
                    cash_balance_cents=None,
                )
            create_investment_holding_snapshot(
                holding_id=target_holding_id,
                account_id=resolved_account_id,
                event_type="manual_update" if is_edit else "add",
                asset_type=effective_asset_type,
                valuation_method="market",
                symbol=normalized_symbol,
                name=normalized_name,
                quantity=float(quantity),
                cost_basis_total_cents=cost_basis_cents,
                manual_market_value_cents=None,
                cash_balance_cents=None,
                market_value_cents=None,
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
        "Holding updated." if is_edit else "Holding added.",
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
    Output("inv-add-modal-open", "data", allow_duplicate=True),
    Output("inv-add-modal-overlay", "style", allow_duplicate=True),
    Output("inv-form-mode", "data", allow_duplicate=True),
    Output("inv-edit-holding-id", "data", allow_duplicate=True),
    Output("inv-account", "value", allow_duplicate=True),
    Output("inv-asset-type", "value", allow_duplicate=True),
    Output("inv-valuation-method", "value", allow_duplicate=True),
    Output("inv-symbol", "value", allow_duplicate=True),
    Output("inv-name", "value", allow_duplicate=True),
    Output("inv-quantity", "value", allow_duplicate=True),
    Output("inv-cost-basis", "value", allow_duplicate=True),
    Output("inv-manual-market-value", "value", allow_duplicate=True),
    Output("inv-cash-balance", "value", allow_duplicate=True),
    Output("inv-form-message", "children", allow_duplicate=True),
    Output("inv-remove-message", "children", allow_duplicate=True),
    Output("inv-action-location", "hash", allow_duplicate=True),
    Input("inv-action-location", "hash"),
    prevent_initial_call=True,
)
def handle_holding_action_from_hash(action_hash: str | None) -> tuple[Any, ...]:
    noop = (
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
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        no_update,
        "",
    )
    if not action_hash:
        return noop

    raw = action_hash.lstrip("#")
    if raw.startswith("inv-delete-"):
        raw_id = raw.removeprefix("inv-delete-")
        try:
            holding_id = int(raw_id)
        except TypeError, ValueError:
            return noop

        holding = get_investment_holding_by_id(holding_id)
        if holding is None:
            return noop

        name = str(holding.get("name") or "this holding")
        symbol = str(holding.get("symbol") or "").strip()
        descriptor = f"{name} ({symbol})" if symbol else name
        return (
            f"Delete {descriptor}?",
            True,
            {"holding_id": holding_id},
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
            no_update,
            no_update,
            no_update,
            "",
        )

    if raw.startswith("inv-edit-"):
        raw_id = raw.removeprefix("inv-edit-")
        try:
            holding_id = int(raw_id)
        except TypeError, ValueError:
            return noop

        holding = get_investment_holding_by_id(holding_id)
        if holding is None:
            return noop

        cost_basis_cents = holding.get("cost_basis_total_cents")
        manual_cents = holding.get("manual_market_value_cents")
        cash_cents = holding.get("cash_balance_cents")
        quantity = holding.get("quantity")

        return (
            no_update,
            False,
            None,
            True,
            _modal_overlay_style(True),
            "edit",
            holding_id,
            int(holding["account_id"]),
            str(holding.get("asset_type") or "stock_etf"),
            str(holding.get("valuation_method") or "market"),
            str(holding.get("symbol") or ""),
            str(holding.get("name") or ""),
            float(quantity) if quantity is not None else None,
            float(int(cost_basis_cents) / 100)
            if cost_basis_cents is not None
            else None,
            float(int(manual_cents) / 100) if manual_cents is not None else None,
            float(int(cash_cents) / 100) if cash_cents is not None else None,
            "",
            "",
            "",
        )

    return noop


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

    holding = get_investment_holding_by_id(holding_id)
    if holding is not None:
        asset_type = str(holding.get("asset_type") or "")
        valuation_method = str(holding.get("valuation_method") or "")
        if asset_type == "cash":
            market_value_cents = int(holding.get("cash_balance_cents") or 0)
        elif valuation_method == "manual":
            market_value_cents = int(holding.get("manual_market_value_cents") or 0)
        else:
            market_value_cents = None

        create_investment_holding_snapshot(
            holding_id=holding_id,
            account_id=int(holding["account_id"]),
            event_type="delete",
            asset_type=asset_type,
            valuation_method=valuation_method,
            symbol=(str(holding.get("symbol")) if holding.get("symbol") else None),
            name=str(holding.get("name") or ""),
            quantity=(
                float(holding["quantity"])
                if holding.get("quantity") is not None
                else None
            ),
            cost_basis_total_cents=(
                int(holding["cost_basis_total_cents"])
                if holding.get("cost_basis_total_cents") is not None
                else None
            ),
            manual_market_value_cents=(
                int(holding["manual_market_value_cents"])
                if holding.get("manual_market_value_cents") is not None
                else None
            ),
            cash_balance_cents=(
                int(holding["cash_balance_cents"])
                if holding.get("cash_balance_cents") is not None
                else None
            ),
            market_value_cents=market_value_cents,
            currency=str(holding.get("currency") or "USD"),
        )

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
    Output("inv-total-market-value", "children"),
    Output("inv-holdings-table", "data"),
    Output("inv-account-overview", "data"),
    Output("inv-chart-account", "options"),
    Output("inv-chart-account", "value"),
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
    str,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, str | int]],
    int | None,
    dict[str, Any] | Any,
]:
    account_options = _investment_account_options()
    default_account = int(account_options[0]["value"]) if account_options else None

    if _cache_is_fresh(cache_data):
        cache_dict = cast(dict[str, Any], cache_data)
        cached_holdings = cache_dict.get("holdings_rows")
        cached_accounts = cache_dict.get("account_rows")
        cached_figure = cache_dict.get("figure")
        cached_allocation = cache_dict.get("allocation_rows")
        cached_total_market_value_cents = cache_dict.get("total_market_value_cents")
        if (
            isinstance(cached_holdings, list)
            and isinstance(cached_accounts, list)
            and cached_figure is not None
            and isinstance(cached_allocation, list)
        ):
            total_market_value_cents = int(cached_total_market_value_cents or 0)
            return (
                account_options,
                default_account,
                _format_money(total_market_value_cents / 100),
                cached_holdings,
                cached_accounts,
                account_options,
                default_account,
                no_update,
            )

    (
        holdings_rows,
        account_rows,
        figure,
        allocation_rows,
        total_market_value_cents,
    ) = _build_dashboard_data()
    cache_payload = _build_cache_payload(
        holdings_rows,
        account_rows,
        figure,
        allocation_rows,
        total_market_value_cents,
    )
    return (
        account_options,
        default_account,
        _format_money(total_market_value_cents / 100),
        holdings_rows,
        account_rows,
        account_options,
        default_account,
        cache_payload,
    )


@callback(
    Output("inv-chart-account-wrap", "style"),
    Input("inv-chart-view", "value"),
)
def toggle_chart_account_selector(view: str | None) -> dict[str, str]:
    if view == "within_account":
        return {"display": "block"}
    return {"display": "none"}


@callback(
    Output("inv-portfolio-chart", "figure"),
    Input("inv-chart-view", "value"),
    Input("inv-chart-account", "value"),
    Input("inv-dashboard-cache", "data"),
)
def update_allocation_chart(
    view: str | None,
    chart_account_id: int | None,
    cache_data: dict[str, Any] | None,
) -> go.Figure:
    cache_dict = cast(dict[str, Any], cache_data or {})

    if view == "within_account":
        allocation_rows_raw = cache_dict.get("allocation_rows")
        allocation_rows = (
            allocation_rows_raw if isinstance(allocation_rows_raw, list) else []
        )
        return _build_account_holdings_chart(allocation_rows, chart_account_id)

    figure_json = cache_dict.get("figure")
    if figure_json is not None:
        return go.Figure(cast(Any, figure_json))
    return go.Figure()
