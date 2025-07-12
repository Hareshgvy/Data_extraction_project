import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor

# 🌐 Full PostgreSQL connection URL (Change this to your setup)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "PythonProject",
    "user": "postgres",
    "password": "Sabagvy07@"
}

def extract(conn_url: str, query: str) -> list:
    """Extracts data from PostgreSQL using the connection URL."""
    with psycopg2.connect(conn_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

def transform(data: list) -> pd.DataFrame:
    """Transforms the extracted data into a pandas DataFrame."""
    return pd.DataFrame(data)

def load(df: pd.DataFrame):
    """Loads the DataFrame (prints or saves it)."""
    print(df)

# 🔁 Run the ETL process
if __name__ == "__main__":
    # 🧾 Example SQL query — modify as needed
    query = "SELECT * FROM film LIMIT 10;"
    
    # 🏗️ ETL pipeline
    data = extract(DB_CONFIG, query)
    df = transform(data)
    load(df)
