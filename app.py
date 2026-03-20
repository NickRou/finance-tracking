from __future__ import annotations

from dash import Dash, dcc, html, page_container
from dotenv import load_dotenv

from db import initialize_database


load_dotenv(dotenv_path=".env")
initialize_database()

app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True)
app.title = "Finance Tracking"

app.layout = html.Div(
    [
        html.H1("Finance Tracking Dashboard"),
        html.Div(
            [
                dcc.Link("Transactions", href="/transactions"),
                dcc.Link("Accounts", href="/accounts"),
                dcc.Link("Investments", href="/investments"),
            ],
            style={"display": "flex", "gap": "14px", "marginBottom": "16px"},
        ),
        page_container,
    ],
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "16px"},
)


if __name__ == "__main__":
    app.run(debug=True)
