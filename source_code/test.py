import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="etl_quality_test_project",
        user="postgres",
        password="senthil"
    )

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO insurance_src.customer_master
        (
            customer_name
        )
        VALUES
        (
            'TEST CUSTOMER'
        )
    """)

    conn.commit()

    print("SUCCESS: 1 row inserted")

    cursor.close()
    conn.close()

except Exception as e:
    print("ERROR:")
    print(e)