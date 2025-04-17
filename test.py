import pyodbc
from config import AZURE_SQL_CONNECTION_STRING

try:
    conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
    cursor = conn.cursor()

    # ✅ Check if the table contains any data
    cursor.execute("SELECT COUNT(*) FROM WaterData")
    count = cursor.fetchone()[0]
    print(f"Total rows in WaterData: {count}")

    # ✅ Fetch and preview some rows
    cursor.execute("SELECT TOP 5 * FROM WaterData")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.close()

except Exception as e:
    print(f"Error: {e}")