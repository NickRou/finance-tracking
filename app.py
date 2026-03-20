from __future__ import annotations

import base64
from pathlib import Path
import tempfile

from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from dotenv import load_dotenv

from db import get_connection, initialize_database
from parsers.pipeline import ImportSummary, import_csv
from parsers.registry import list_institutions


load_dotenv(dotenv_path=".env")
initialize_database()

app = Dash()
INSTITUTIONS = list_institutions()


def _format_money(cents: int | None) -> str:
    value = 0 if cents is None else cents
    return f"${value / 100:,.2f}"


def _fetch_overview() -> tuple[
    dict[str, str], list[dict[str, str]], list[dict[str, str]]
]:
    with get_connection() as conn:
        totals_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_transactions,
                COALESCE(SUM(amount_cents), 0) AS net_cents,
                COALESCE(SUM(CASE WHEN amount_cents < 0 THEN -amount_cents ELSE 0 END), 0) AS debit_cents,
                COALESCE(SUM(CASE WHEN amount_cents > 0 THEN amount_cents ELSE 0 END), 0) AS credit_cents
            FROM transactions
            """
        ).fetchone()
        institution_rows = conn.execute(
            """
            SELECT
                institution,
                COUNT(*) AS transaction_count,
                COALESCE(SUM(amount_cents), 0) AS net_cents,
                COALESCE(SUM(CASE WHEN amount_cents < 0 THEN -amount_cents ELSE 0 END), 0) AS debit_cents,
                COALESCE(SUM(CASE WHEN amount_cents > 0 THEN amount_cents ELSE 0 END), 0) AS credit_cents,
                MAX(occurred_on) AS latest_transaction_date
            FROM transactions
            GROUP BY institution
            ORDER BY institution ASC
            """
        ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT occurred_on, institution, description, category_raw, amount_cents, source_file
            FROM transactions
            ORDER BY occurred_on DESC, id DESC
            LIMIT 25
            """
        ).fetchall()

    totals = {
        "total_transactions": str(totals_row[0]),
        "net": _format_money(int(totals_row[1])),
        "debits": _format_money(int(totals_row[2])),
        "credits": _format_money(int(totals_row[3])),
    }

    institution_data = [
        {
            "institution": str(row[0]),
            "transaction_count": int(row[1]),
            "net": _format_money(int(row[2])),
            "debits": _format_money(int(row[3])),
            "credits": _format_money(int(row[4])),
            "latest_transaction_date": str(row[5] or "-"),
        }
        for row in institution_rows
    ]
    recent_data = [
        {
            "occurred_on": str(row[0]),
            "institution": str(row[1]),
            "description": str(row[2]),
            "category": str(row[3] or ""),
            "amount": _format_money(int(row[4])),
            "source_file": str(row[5] or ""),
        }
        for row in recent_rows
    ]
    return totals, institution_data, recent_data


def _file_rows_for_table(
    uploaded_files: list[dict[str, str]] | None,
) -> list[dict[str, str]]:
    if not uploaded_files:
        return []
    return [
        {
            "file_id": row["file_id"],
            "filename": row["filename"],
            "institution": row["institution"],
        }
        for row in uploaded_files
    ]


def _next_file_id(existing_rows: list[dict[str, str]]) -> int:
    max_id = 0
    for row in existing_rows:
        raw_id = str(row.get("file_id", ""))
        if raw_id.startswith("file-") and raw_id[5:].isdigit():
            max_id = max(max_id, int(raw_id[5:]))
    return max_id + 1


app.layout = html.Div(
    [
        html.H1("Finance Tracking Dashboard"),
        html.P(
            "Upload CSV files, tag each file by institution, and import into your encrypted database."
        ),
        html.H2("Import Files"),
        dcc.Upload(
            id="upload-files",
            children=html.Div(
                ["Drag and drop CSV files or ", html.Button("Select Files")]
            ),
            multiple=True,
            style={
                "width": "100%",
                "padding": "16px",
                "border": "1px dashed #7a7a7a",
                "borderRadius": "10px",
                "marginBottom": "12px",
            },
        ),
        dash_table.DataTable(
            id="file-tag-table",
            columns=[
                {"name": "File ID", "id": "file_id", "editable": False},
                {"name": "File", "id": "filename", "editable": False},
                {
                    "name": "Institution",
                    "id": "institution",
                    "presentation": "dropdown",
                },
            ],
            data=[],
            editable=True,
            dropdown={
                "institution": {
                    "options": [
                        {"label": value, "value": value} for value in INSTITUTIONS
                    ]
                }
            },
            hidden_columns=["file_id"],
            style_table={"marginBottom": "12px", "overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px"},
        ),
        html.Button("Import Tagged Files", id="import-files", n_clicks=0),
        html.Div(id="upload-message", style={"marginTop": "10px"}),
        html.Div(
            id="import-message", style={"marginTop": "10px", "marginBottom": "20px"}
        ),
        html.H2("Overview"),
        html.Div(
            id="kpi-cards", style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}
        ),
        html.H3("By Institution"),
        dash_table.DataTable(
            id="institution-overview",
            columns=[
                {"name": "Institution", "id": "institution"},
                {"name": "Transactions", "id": "transaction_count"},
                {"name": "Net", "id": "net"},
                {"name": "Debits", "id": "debits"},
                {"name": "Credits", "id": "credits"},
                {"name": "Latest", "id": "latest_transaction_date"},
            ],
            data=[],
            style_table={"overflowX": "auto", "marginBottom": "20px"},
            style_cell={"textAlign": "left", "padding": "8px"},
        ),
        html.H3("Recent Transactions"),
        dash_table.DataTable(
            id="recent-transactions",
            columns=[
                {"name": "Date", "id": "occurred_on"},
                {"name": "Institution", "id": "institution"},
                {"name": "Description", "id": "description"},
                {"name": "Category", "id": "category"},
                {"name": "Amount", "id": "amount"},
                {"name": "Source File", "id": "source_file"},
            ],
            data=[],
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px"},
        ),
        dcc.Store(id="uploaded-files-store", data=[]),
        dcc.Store(id="refresh-token", data=0),
    ],
    style={"maxWidth": "1100px", "margin": "0 auto", "padding": "16px"},
)


@callback(
    Output("uploaded-files-store", "data"),
    Output("file-tag-table", "data"),
    Output("upload-message", "children"),
    Input("upload-files", "contents"),
    State("upload-files", "filename"),
    State("uploaded-files-store", "data"),
)
def handle_uploads(
    uploaded_contents: list[str] | None,
    uploaded_filenames: list[str] | None,
    stored_files: list[dict[str, str]] | None,
) -> tuple[list[dict[str, str]], list[dict[str, str]] | object, str]:
    current = list(stored_files or [])
    if not uploaded_contents or not uploaded_filenames:
        return current, no_update, ""

    default_institution = INSTITUTIONS[0] if INSTITUTIONS else "capitalone"
    next_id = _next_file_id(current)
    new_rows: list[dict[str, str]] = []
    for content, filename in zip(uploaded_contents, uploaded_filenames, strict=True):
        new_rows.append(
            {
                "file_id": f"file-{next_id}",
                "filename": filename,
                "content": content,
                "institution": default_institution,
            }
        )
        next_id += 1

    merged = current + new_rows
    return (
        merged,
        _file_rows_for_table(merged),
        f"Added {len(new_rows)} file(s). Tag each file with the right institution.",
    )


@callback(
    Output("import-message", "children"),
    Output("refresh-token", "data"),
    Input("import-files", "n_clicks"),
    State("uploaded-files-store", "data"),
    State("file-tag-table", "data"),
    State("refresh-token", "data"),
)
def import_uploaded_files(
    n_clicks: int,
    uploaded_files: list[dict[str, str]] | None,
    table_data: list[dict[str, str]] | None,
    refresh_token: int,
) -> tuple[str, int]:
    if not n_clicks:
        return "", refresh_token
    if not uploaded_files:
        return "No files to import. Upload CSV files first.", refresh_token

    institution_by_id = {
        str(row.get("file_id", "")): str(row.get("institution", ""))
        for row in (table_data or [])
    }

    total = ImportSummary(parsed=0, inserted=0, duplicates=0, invalid=0)
    skipped: list[str] = []
    failed: list[str] = []

    for row in uploaded_files:
        file_id = str(row.get("file_id", ""))
        filename = row.get("filename", "")
        institution = institution_by_id.get(file_id, str(row.get("institution", "")))
        content = row.get("content", "")
        temp_path: Path | None = None
        if not filename.lower().endswith(".csv"):
            skipped.append(filename)
            continue
        if institution not in INSTITUTIONS:
            failed.append(f"{filename}: invalid institution tag")
            continue

        try:
            _meta, encoded = content.split(",", 1)
            decoded = base64.b64decode(encoded)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as handle:
                handle.write(decoded)
                temp_path = Path(handle.name)

            result = import_csv(institution=institution, file_path=str(temp_path))
            total = ImportSummary(
                parsed=total.parsed + result.parsed,
                inserted=total.inserted + result.inserted,
                duplicates=total.duplicates + result.duplicates,
                invalid=total.invalid + result.invalid,
            )
        except Exception as exc:
            failed.append(f"{filename}: {exc}")
        finally:
            if temp_path and temp_path.exists():
                temp_path.unlink()

    message = (
        f"Imported files: parsed={total.parsed}, inserted={total.inserted}, "
        f"duplicates={total.duplicates}, invalid={total.invalid}."
    )
    if skipped:
        message += f" Skipped non-CSV files: {', '.join(skipped)}."
    if failed:
        message += f" Failed: {'; '.join(failed)}."

    return message, refresh_token + 1


@callback(
    Output("kpi-cards", "children"),
    Output("institution-overview", "data"),
    Output("recent-transactions", "data"),
    Input("refresh-token", "data"),
)
def refresh_overview(
    _refresh_token: int,
) -> tuple[list[html.Div], list[dict[str, str]], list[dict[str, str]]]:
    totals, institution_data, recent_data = _fetch_overview()
    cards = [
        html.Div(
            [html.Div("Transactions"), html.Strong(totals["total_transactions"])],
            style=_card_style(),
        ),
        html.Div([html.Div("Net"), html.Strong(totals["net"])], style=_card_style()),
        html.Div(
            [html.Div("Debits"), html.Strong(totals["debits"])], style=_card_style()
        ),
        html.Div(
            [html.Div("Credits"), html.Strong(totals["credits"])], style=_card_style()
        ),
    ]
    return cards, institution_data, recent_data


def _card_style() -> dict[str, str]:
    return {
        "minWidth": "170px",
        "padding": "12px",
        "border": "1px solid #cfcfcf",
        "borderRadius": "8px",
        "backgroundColor": "#f8f8f8",
    }


if __name__ == "__main__":
    app.run(debug=True)
