
import os
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv


# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

fake = Faker("en_IN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

STATES = {
    "Karnataka": ["Bangalore", "Mysore", "Hubli", "Mangalore"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai"],
    "Telangana": ["Hyderabad", "Warangal"],
    "Maharashtra": ["Mumbai", "Pune", "Nagpur"],
    "Delhi": ["New Delhi"]
}


def get_db_config():
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise ValueError("DB_PASSWORD is missing")

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv(
            "DB_NAME", "etl_quality_test_project"
        ),
        "user": os.getenv("DB_USER", "postgres"),
        "password": password,
        "connect_timeout": 10
    }


def get_timestamp_for_date(load_date):
    return load_date + timedelta(
        seconds=random.randint(0, 86399)
    )


def generate_customer(load_date):
    state = random.choice(list(STATES))
    city = random.choice(STATES[state])
    gender = random.choice(["Male", "Female"])

    first_name = (
        fake.first_name_male()
        if gender == "Male"
        else fake.first_name_female()
    )

    last_name = fake.last_name()
    customer_name = f"{first_name} {last_name}"

    email = (
        f"{first_name.lower()}."
        f"{last_name.lower()}."
        f"{random.randint(100, 99999)}@gmail.com"
    )

    mobile_number = (
        random.choice("9876")
        + "".join(
            str(random.randint(0, 9))
            for _ in range(9)
        )
    )

    created_ts = get_timestamp_for_date(load_date)

    return (
        customer_name,
        gender,
        fake.date_of_birth(
            minimum_age=18,
            maximum_age=75
        ),
        email,
        mobile_number,
        fake.street_address(),
        f"Flat {random.randint(1, 999)}, Block {random.choice(['A', 'B', 'C', 'D'])}",
        city,
        state,
        "India",
        fake.postcode(),
        random.choices(
            ["ACTIVE", "INACTIVE"],
            weights=[90, 10],
            k=1
        )[0],
        created_ts,
        created_ts
    )


INSERT_SQL = """
INSERT INTO insurance_src.customer_master
(
    customer_name,
    gender,
    date_of_birth,
    email,
    mobile_number,
    address_line1,
    address_line2,
    city,
    state,
    country,
    postal_code,
    customer_status,
    created_at,
    updated_at
)
VALUES %s
"""


def insert_records(business_date, record_count):
    if record_count <= 0:
        raise ValueError("Record count must be positive")

    load_date = datetime.strptime(
        business_date, "%Y-%m-%d"
    )

    batch_size = int(os.getenv("BATCH_SIZE", "1000"))

    if batch_size <= 0:
        raise ValueError("BATCH_SIZE must be positive")

    logger.info(
        "Starting customer generation: date=%s count=%s",
        business_date,
        record_count
    )

    conn = None
    total_inserted = 0

    try:
        conn = psycopg2.connect(**get_db_config())

        with conn.cursor() as cursor:
            for start in range(0, record_count, batch_size):
                current_batch_size = min(
                    batch_size,
                    record_count - start
                )

                batch = [
                    generate_customer(load_date)
                    for _ in range(current_batch_size)
                ]

                execute_values(
                    cursor,
                    INSERT_SQL,
                    batch,
                    page_size=batch_size
                )

                total_inserted += len(batch)

                logger.info(
                    "Prepared %s/%s records",
                    total_inserted,
                    record_count
                )

        # Commit all generated records together.
        conn.commit()

        logger.info(
            "SUCCESS: Committed %s customer records for %s",
            total_inserted,
            business_date
        )

        return total_inserted

    except Exception:
        if conn is not None:
            conn.rollback()

        logger.exception(
            "Customer data generation failed"
        )
        raise

    finally:
        if conn is not None:
            conn.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        default=os.getenv(
            "BUSINESS_DATE", "2026-10-01"
        )
    )

    parser.add_argument(
        "--records",
        type=int,
        default=int(
            os.getenv("CUSTOMER_RECORD_COUNT", "10000")
        )
    )

    args = parser.parse_args()

    insert_records(args.date, args.records)


if __name__ == "__main__":
    main()
