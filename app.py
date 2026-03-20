from __future__ import annotations

from dash import Dash, dcc, html, page_container
from dotenv import load_dotenv

from db import initialize_database


load_dotenv(dotenv_path=".env")
initialize_database()

app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True)
app.title = "Local Finance Tracking"

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src="/assets/gifs/coffee-gif.gif",
                            alt="Coffee",
                            className="title-gif",
                        ),
                        html.H1("Local Finance Tracking", className="app-title"),
                    ],
                    className="title-wrap",
                ),
                html.Div(
                    [
                        dcc.Link(
                            "Transactions", href="/transactions", className="nav-link"
                        ),
                        dcc.Link("Accounts", href="/accounts", className="nav-link"),
                        dcc.Link(
                            "Investments", href="/investments", className="nav-link"
                        ),
                    ],
                    className="nav-links",
                ),
            ],
            className="top-nav",
        ),
        html.Div(page_container, className="page-container"),
    ],
    className="app-shell",
)


if __name__ == "__main__":
    app.run(debug=True)
