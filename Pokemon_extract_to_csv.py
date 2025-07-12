import requests
import pandas as pd
import psycopg2

base_url = "https://pokeapi.co/api/v2/"
endpoint = "pokemon/ditto"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "PythonProject",
    "user": "postgres",
    "password": "Sabagvy07@"
}

def extract(base_url, endpoint) -> dict:
    try:
        response = requests.get(f"{base_url}{endpoint}", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: Received status code {response.status_code}")
    except Exception as e:
        print(f"Request failed: {e}")
    return {}

def transform(data) -> pd.DataFrame:
    try:
        name = data.get("name", "unknown")
        abilities = data.get('abilities', [])
        flat_data = [{
            'pokemon_name': name,
            'ability_name': item['ability']['name'],
            'is_hidden': item['is_hidden'],
            'slot': item['slot']
        } for item in abilities]
        return pd.DataFrame(flat_data)
    except (KeyError, TypeError) as e:
        print(f"Data transformation error: {e}")
        return pd.DataFrame()

def load_to_db(df):
    try:
        if df.empty:
            print("DataFrame is empty. No data to insert.")
            return

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS pokemon_abilities (
            id SERIAL PRIMARY KEY,
            pokemon_name VARCHAR(50),
            ability_name VARCHAR(100),
            is_hidden BOOLEAN,
            slot INT,
            UNIQUE (pokemon_name, ability_name, slot)
        );
        """
        cursor.execute(create_table_query)

        # Insert data using executemany
        insert_query = """
        INSERT INTO pokemon_abilities (pokemon_name, ability_name, is_hidden, slot)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (pokemon_name, ability_name, slot) DO NOTHING;
        """
        values = list(df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, values)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully loaded into the database.")

    except Exception as e:
        print(f"Database error: {e}")

# Run ETL process
data = extract(base_url, endpoint)
df = transform(data)
load_to_db(df)
