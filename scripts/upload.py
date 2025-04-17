import pyodbc
import pandas as pd
import time
from config import AZURE_SQL_CONNECTION_STRING
import numpy as np

BATCH_SIZE = 1000         # Number of rows to insert in each batch
MAX_RETRIES = 5            # Number of connection retries


file_path = "../data/water_potability.csv"
df = pd.read_csv(file_path)

#Round floats to 4 decimal places and replace NaN with None
data_tuples = [
    tuple(
        round(val, 4) if isinstance(val, (float, np.float64)) else val
        for val in row
    ) for row in df.replace({np.nan: None}).itertuples(index=False, name=None)
]

#Function to establish connection with retries
def connect_with_retries():
    """Establish connection to Azure SQL with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING, autocommit=False)
            cursor = conn.cursor()
            print(f"Connected to Azure SQL Database (Attempt {attempt + 1}/{MAX_RETRIES})")
            return conn, cursor
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            time.sleep(5)  # Wait before retrying
    print("Failed to connect after multiple attempts.")
    exit(1)

# Create table with DECIMAL(18,4) types
create_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WaterData' AND xtype='U')
CREATE TABLE WaterData (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ph DECIMAL(18,4) NULL,
    Hardness DECIMAL(18,4) NULL,
    Solids DECIMAL(18,4) NULL,
    Chloramines DECIMAL(18,4) NULL,
    Sulfate DECIMAL(18,4) NULL,
    Conductivity DECIMAL(18,4) NULL,
    Organic_carbon DECIMAL(18,4) NULL,
    Trihalomethanes DECIMAL(18,4) NULL,
    Turbidity DECIMAL(18,4) NULL,
    Potability INT NULL
)
"""

#  Connect and create table
conn, cursor = connect_with_retries()
try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created or already exists.")
except Exception as e:
    print(f"Error creating table: {e}")
    conn.close()
    exit(1)

#  Batch insert function with retries
def batch_insert(rows, conn, cursor):
    """Insert batch of rows with retry logic, returning the connection and cursor in case of reconnection."""
    for attempt in range(MAX_RETRIES):
        try:
            insert_query = """
            INSERT INTO WaterData (ph, Hardness, Solids, Chloramines, Sulfate, Conductivity, Organic_carbon, Trihalomethanes, Turbidity, Potability)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # Batch insert
            cursor.executemany(insert_query, rows)
            conn.commit()

            print(f"Batch inserted successfully (attempt {attempt + 1})")
            return conn, cursor  # Return the current connection and cursor
        except Exception as e:
            print(f"Batch insert failed (Attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(5)  # Wait before retrying

            # Close current connection and reconnect
            try:
                conn.close()
            except Exception as close_error:
                print(f"Error closing connection: {close_error}")

            conn, cursor = connect_with_retries()  # Reconnect
    return conn, cursor

#Batch processing
total_rows = len(data_tuples)
inserted_rows = 0
failed_batches = 0

# Insert in batches
for i in range(0, total_rows, BATCH_SIZE):
    batch = data_tuples[i:i + BATCH_SIZE]

    # Use the returned connection and cursor after each batch insert
    conn, cursor = batch_insert(batch, conn, cursor)

    inserted_rows += len(batch)
    print(f"Inserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")

#  Final output
print(f"Data upload completed! Inserted: {inserted_rows}, Failed batches: {failed_batches}")
conn.close()
print("Connection closed.")
