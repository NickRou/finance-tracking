from __future__ import annotations

from dash import dcc, register_page


register_page(__name__, path="/")


layout = dcc.Location(pathname="/transactions", id="home-redirect")
