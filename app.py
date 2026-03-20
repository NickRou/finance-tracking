from dash import Dash, html
from dotenv import load_dotenv

from db import initialize_database


load_dotenv(dotenv_path=".env")
initialize_database()

app = Dash()

app.layout = [html.Div(children="Hello World")]

if __name__ == "__main__":
    app.run(debug=True)
