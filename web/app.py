from flask import Flask, render_template
import pandas as pd
import pyodbc
from config import AZURE_SQL_CONNECTION_STRING

app = Flask(__name__)


def fetch_data():
    """Fetch data from Azure SQL using pyodbc and return it as a dictionary."""
    try:
        #  Connect to Azure SQL Database using pyodbc
        conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
        print("Connected to Azure SQL Database.")

        #  Fetch data into a DataFrame
        query = "SELECT * FROM WaterData"
        df = pd.read_sql(query, conn)

        #  Close the connection
        conn.close()

        #  Convert to dictionary for rendering
        return df.to_dict(orient="records")

    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

@app.route("/")
def index():
    """Route to display the data on the web page."""
    data = fetch_data()
    return render_template("index.html", data=data)

if __name__ == "__main__":
    app.run(debug=True)
