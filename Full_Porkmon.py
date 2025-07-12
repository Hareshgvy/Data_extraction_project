import requests
import pandas as pd
import psycopg2
from time import sleep

# PostgreSQL config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "PythonProject",
    "user": "postgres",
    "password": "Sabagvy07@"
}

BASE_URL = "https://pokeapi.co/api/v2/"

# Step 1: Get all Pokémon URLs
def get_all_pokemon_urls():
    try:
        url = f"{BASE_URL}pokemon?limit=100000"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return [pokemon['url'] for pokemon in data['results']]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Pokémon URLs: {e}")
        return []

# Step 2: Extract Pokémon data
def extract_pokemon_data(pokemon_url):
    try:
        response = requests.get(pokemon_url)
        response.raise_for_status()
        data = response.json()
        return {
            "name": data["name"],
            "base_experience": data.get("base_experience"),
            "height": data["height"],
            "weight": data["weight"],
            "types": ','.join([t["type"]["name"] for t in data["types"]]),
            "abilities": ','.join([a["ability"]["name"] for a in data["abilities"]]),
            "sprite": data["sprites"]["front_default"]
        }
    except Exception as e:
        print(f"Failed to fetch data from {pokemon_url}: {e}")
        return None

# Step 3: Insert data into PostgreSQL
def insert_into_db(data_list):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pokemon_data (
                id SERIAL PRIMARY KEY,
                name TEXT,
                base_experience INT,
                height INT,
                weight INT,
                types TEXT,
                abilities TEXT,
                sprite TEXT
            );
        """)

        insert_query = """
            INSERT INTO pokemon_data (name, base_experience, height, weight, types, abilities, sprite)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """

        for row in data_list:
            cursor.execute(insert_query, (
                row["name"],
                row["base_experience"],
                row["height"],
                row["weight"],
                row["types"],
                row["abilities"],
                row["sprite"]
            ))

        conn.commit()
        print("Data successfully inserted into PostgreSQL.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Step 4: Main logic
def main():
    all_pokemon_urls = get_all_pokemon_urls()
    pokemon_data_list = []

    for url in all_pokemon_urls:
        data = extract_pokemon_data(url)
        if data:
            pokemon_data_list.append(data)
            sleep(0.1)

    if pokemon_data_list:
        # Save to CSV
        df = pd.DataFrame(pokemon_data_list)
        df.to_csv("all_pokemon_data.csv", index=False)
        print("Saved to CSV.")

        # Save to PostgreSQL
        insert_into_db(pokemon_data_list)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
