import psycopg2
import mysql.connector
import pandas as pd

# PostgreSQL config
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "PythonProject",
    "user": "postgres",
    "password": "Sabagvy07@"
}

# MySQL config
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': 'PythonProject',
    'user': 'root',
    'password': 'Sabagvy07@'
}


def fetch_data_from_postgres():
    """Fetch data from PostgreSQL."""
    query = """
    SELECT 
        p.payment_id,
        p.amount,
        p.payment_date,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email
    FROM 
        payment p
    JOIN 
        customer c ON p.customer_id = c.customer_id;
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Fetched data from PostgreSQL")
        return df
    except Exception as e:
        print("❌ Error fetching data from PostgreSQL:", e)
        return None


def transform_data(df):
    """Transform data: combine names, format date."""
    try:
        df['full_name'] = df['first_name'].str.strip() + ' ' + df['last_name'].str.strip()
        df['payment_date'] = pd.to_datetime(df['payment_date']).dt.strftime('%m/%d/%Y')
        df = df[['payment_id', 'amount', 'payment_date', 'customer_id', 'full_name', 'email']]
        print("✅ Transformed data")
        return df
    except Exception as e:
        print("❌ Error transforming data:", e)
        return None


def load_data_to_mysql(df):
    """Create MySQL table and load transformed data into it."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Create table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS payment_customer (
            payment_id INT,
            amount DECIMAL(10,2),
            payment_date DATETIME,
            customer_id INT,
            full_name VARCHAR(200),
            email VARCHAR(255)
        );
        """
        cursor.execute(create_table_sql)

        # Insert data
        insert_sql = """
        INSERT INTO payment_customer 
        (payment_id, amount, payment_date, customer_id, full_name, email)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(insert_sql, data)
        conn.commit()

        print(f"✅ Inserted {cursor.rowcount} rows into MySQL")

        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error loading data to MySQL:", e)


def main():
    df = fetch_data_from_postgres()

    if df is None or df.empty:
        print("❌ No data fetched from PostgreSQL.")
        return

    transformed_df = transform_data(df)
    if transformed_df is None:
        print("❌ Transformation failed.")
        return

    load_data_to_mysql(transformed_df)

if __name__ == "__main__":
    main()
