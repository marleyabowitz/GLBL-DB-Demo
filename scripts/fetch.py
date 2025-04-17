import pandas as pd
import pyodbc
from config import AZURE_SQL_CONNECTION_STRING


#  Connect to Azure SQL Database using pyodbc
try:
    conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
    cursor = conn.cursor()
    print("Connected to Azure SQL Database.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(1)

#  Fetch data into a Pandas DataFrame
query = "SELECT * FROM WaterData"

try:
    # Use pandas to read data from the database into a DataFrame
    df = pd.read_sql(query, conn)
    print("Data fetched successfully!")

    #  Preview the first few rows
    print(df.head())

except Exception as e:
    print(f"Error fetching data: {e}")

finally:
    #  Close the connection
    conn.close()
    print("Connection closed.")
