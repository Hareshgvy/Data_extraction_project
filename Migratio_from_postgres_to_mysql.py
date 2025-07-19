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
    'database': 'pythonmigration',
    'user': 'root',
    'password': 'Sabagvy07@'
}


def fetch_data_from_postgres():
    """Fetch data from PostgreSQL without using read_sql."""
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
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        print("✅ Fetched data from PostgreSQL")
        return df
    except Exception as e:
        print("❌ Error fetching data from PostgreSQL:", e)
        return None


def transform_data(df):
    """Transform names and format date for MySQL."""
    try:
        # Format full name: capitalize first letter, rest lowercase
        df['full_name'] = (
            df['first_name'].str.strip().str.title() + ' ' +
            df['last_name'].str.strip().str.title()
        )

        # Format payment_date to YY-MM-DD (MySQL DATE format)
        df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
        df['payment_date'] = df['payment_date'].dt.strftime('%d-%m-%y')

        # Select final columns
        df = df[['payment_id', 'amount', 'payment_date', 'customer_id', 'full_name', 'email']]
        print("Transformed data")
        print(df.head())  # Debug preview
        return df
    except Exception as e:
        print("❌ Error transforming data:", e)
        return None


def load_data_to_mysql(df):
    """Drop + recreate MySQL table and load transformed data."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Drop and recreate the table
        cursor.execute("DROP TABLE IF EXISTS payment_customer")
        create_table_sql = """
        CREATE TABLE payment_customer (
            payment_id INT,
            amount DECIMAL(10,2),
            payment_date DATE,
            customer_id INT,
            full_name VARCHAR(200),
            email VARCHAR(255)
        )
        """
        cursor.execute(create_table_sql)

        # Prepare and insert data
        insert_sql = """
        INSERT INTO payment_customer 
        (payment_id, amount, payment_date, customer_id, full_name, email)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, data)
        conn.commit()  # ✅ Commit is essential

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
    if transformed_df is None or transformed_df.empty:
        print("❌ Transformation failed or resulted in empty data.")
        return

    load_data_to_mysql(transformed_df)


if __name__ == "__main__":
    main()
