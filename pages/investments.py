from __future__ import annotations

from dash import html, register_page


register_page(__name__, path="/investments", title="Investments")


layout = html.Div(
    [
        html.H2("Investments"),
        html.P("Investments dashboard coming soon."),
    ]
)
