import psycopg2
from psycopg2 import sql, OperationalError
from config import db_config


def init_db():
    try:
        conn = psycopg2.connect(
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
            dbname=db_config.DB_NAME,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        cursor = conn.cursor()
        with open('schema.sql', 'r') as f:
            cursor.execute(f.read())
        conn.commit()
        cursor.close()
        conn.close()
        print("Database schema created successfully.")
    except OperationalError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == '__main__':
    init_db()
