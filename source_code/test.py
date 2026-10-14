import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

required = {
    "DB_PASSWORD": DB_PASSWORD,
    "DB_USER": DB_USER,
    "DB_NAME": DB_NAME,
    "DB_HOST": DB_HOST,
}

missing = [key for key, value in required.items() if not value]

if missing:
    raise RuntimeError(
        f"Missing environment variables: {', '.join(missing)}"
    )

try:
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    ) as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO insurance_src.customer_master
                (
                    customer_id,
                    customer_name
                )
                VALUES (%s, %s)
                ON CONFLICT (customer_id)
                DO NOTHING
                RETURNING customer_id
                """,
                (900001, "TEST CUSTOMER")
            )

            result = cursor.fetchone()

            if result:
                print(
                    f"SUCCESS: Customer {result[0]} inserted"
                )
            else:
                print(
                    "SKIPPED: Customer ID already exists"
                )

except psycopg2.Error as e:
    print("ERROR: Database operation failed")
    raise