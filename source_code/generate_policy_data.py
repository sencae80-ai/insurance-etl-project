
import os
import random
import argparse
import logging
from datetime import datetime, timedelta, date
from decimal import Decimal
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


POLICY_TYPES = {
    "AUTO": (100000, 1500000, 3000, 25000),
    "HEALTH": (200000, 5000000, 5000, 60000),
    "LIFE": (500000, 10000000, 8000, 100000),
    "HOME": (500000, 10000000, 4000, 50000)
}

POLICY_STATUSES = [
    "ACTIVE",
    "EXPIRED",
    "CANCELLED",
    "LAPSED"
]

PAYMENT_FREQUENCIES = [
    "MONTHLY",
    "QUARTERLY",
    "HALF_YEARLY",
    "YEARLY"
]

PAYMENT_MODES = [
    "UPI",
    "CARD",
    "NET_BANKING",
    "NACH"
]

RELATIONSHIPS = [
    "SPOUSE",
    "FATHER",
    "MOTHER",
    "SON",
    "DAUGHTER"
]


# =====================================================
# DATABASE CONFIGURATION
# =====================================================

def get_db_config():
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise ValueError("DB_PASSWORD is missing in .env")

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv(
            "DB_NAME",
            "etl_quality_test_project"
        ),
        "user": os.getenv("DB_USER", "postgres"),
        "password": password,
        "connect_timeout": 10
    }


# =====================================================
# CUSTOMER IDS
# =====================================================

def get_customer_ids(cursor):
    cursor.execute("""
        SELECT customer_id
        FROM insurance_src.customer_master
        ORDER BY customer_id
    """)

    customer_ids = [
        row[0] for row in cursor.fetchall()
    ]

    if not customer_ids:
        raise ValueError(
            "No customers found in insurance_src.customer_master"
        )

    logger.info(
        "Retrieved %s customer IDs",
        len(customer_ids)
    )

    return customer_ids


# =====================================================
# POLICY DATE GENERATION
# =====================================================

def generate_policy_dates(business_date, status):
    if status == "EXPIRED":
        end_date = business_date - timedelta(
            days=random.randint(1, 365)
        )
        start_date = end_date - timedelta(
            days=random.choice([365, 730, 1095])
        )

    elif status == "ACTIVE":
        start_date = business_date - timedelta(
            days=random.randint(0, 180)
        )
        end_date = business_date + timedelta(
            days=random.randint(30, 1095)
        )

    elif status == "CANCELLED":
        start_date = business_date - timedelta(
            days=random.randint(30, 365)
        )
        end_date = business_date + timedelta(
            days=random.randint(30, 730)
        )

    else:  # LAPSED
        end_date = business_date - timedelta(
            days=random.randint(1, 180)
        )
        start_date = end_date - timedelta(days=365)

    return start_date, end_date


def random_timestamp(business_date):
    return datetime.combine(
        business_date,
        datetime.min.time()
    ) + timedelta(
        seconds=random.randint(0, 86399)
    )


# =====================================================
# POLICY GENERATION
# =====================================================

def generate_policy(
    customer_id,
    business_date,
    policy_number
):
    policy_type = random.choice(
        list(POLICY_TYPES.keys())
    )

    min_coverage, max_coverage, min_premium, max_premium = (
        POLICY_TYPES[policy_type]
    )

    policy_status = random.choices(
        POLICY_STATUSES,
        weights=[70, 15, 10, 5],
        k=1
    )[0]

    start_date, end_date = generate_policy_dates(
        business_date,
        policy_status
    )

    sum_assured = Decimal(
        random.randint(min_coverage, max_coverage)
    ).quantize(Decimal("0.01"))

    premium_amount = Decimal(
        random.randint(min_premium, max_premium)
    ).quantize(Decimal("0.01"))

    created_at = random_timestamp(business_date)

    return (
        policy_number,
        customer_id,
        policy_type,
        policy_status,
        start_date,
        end_date,
        sum_assured,
        premium_amount,
        random.choice(PAYMENT_FREQUENCIES),
        random.choice(PAYMENT_MODES),
        f"AGT{random.randint(1000, 9999)}",
        fake.name(),
        random.choice(RELATIONSHIPS),
        created_at,
        created_at
    )


# =====================================================
# INSERT SQL
# =====================================================

INSERT_SQL = """
INSERT INTO insurance_src.policy_master
(
    policy_number,
    customer_id,
    policy_type,
    policy_status,
    policy_start_date,
    policy_end_date,
    sum_assured,
    premium_amount,
    payment_frequency,
    payment_mode,
    agent_code,
    nominee_name,
    nominee_relationship,
    created_at,
    updated_at
)
VALUES %s
ON CONFLICT (policy_number) DO NOTHING
"""


# =====================================================
# MAIN LOAD FUNCTION
# =====================================================

def insert_records(
    business_date,
    record_count,
    batch_tag="INITIAL"
):
    if record_count <= 0:
        raise ValueError(
            "Record count must be greater than zero"
        )

    load_date = date.fromisoformat(business_date)

    batch_size = int(
        os.getenv("BATCH_SIZE", "1000")
    )

    if batch_size <= 0:
        raise ValueError(
            "BATCH_SIZE must be greater than zero"
        )

    if not batch_tag.isalnum():
        raise ValueError(
            "Batch tag must contain only letters and numbers"
        )

    if len(batch_tag) > 17:
        raise ValueError(
            "Batch tag must not exceed 17 characters"
        )

    logger.info(
        "Starting policy generation: date=%s records=%s tag=%s",
        business_date,
        record_count,
        batch_tag
    )

    conn = None
    inserted_count = 0

    try:
        conn = psycopg2.connect(**get_db_config())

        with conn.cursor() as cursor:
            customer_ids = get_customer_ids(cursor)

            for start in range(0, record_count, batch_size):
                current_size = min(
                    batch_size,
                    record_count - start
                )

                batch = []

                for offset in range(current_size):
                    sequence = start + offset + 1

                    policy_number = (
                                        f"POL-{load_date.strftime('%Y%m%d')}-"
                                        f"{batch_tag.upper()}-"
                                        f"{sequence:08d}"
                                    )

                    customer_id = random.choice(
                        customer_ids
                    )

                    batch.append(
                        generate_policy(
                            customer_id,
                            load_date,
                            policy_number
                        )
                    )

                execute_values(
                    cursor,
                    INSERT_SQL,
                    batch,
                    page_size=batch_size
                )

                logger.info(
                    "Processed %s/%s policy records",
                    start + current_size,
                    record_count
                )

        conn.commit()

        logger.info(
            "SUCCESS: Processed %s policy records for %s",
            record_count,
            business_date
        )

        return record_count

    except Exception:
        if conn is not None:
            conn.rollback()

        logger.exception(
            "Policy data generation failed"
        )
        raise

    finally:
        if conn is not None:
            conn.close()


# =====================================================
# ENTRY POINT
# =====================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        default=os.getenv(
            "BUSINESS_DATE", "2026-10-01"
        ),
        help="Business date YYYY-MM-DD"
    )

    parser.add_argument(
        "--records",
        type=int,
        default=int(
            os.getenv("POLICY_RECORD_COUNT", "10000")
        ),
        help="Number of policies to generate"
    )

    parser.add_argument(
        "--batch-tag",
        default=os.getenv(
            "POLICY_BATCH_TAG", "INITIAL"
        ),
        help="Unique alphanumeric batch identifier"
    )

    args = parser.parse_args()

    insert_records(
        args.date,
        args.records,
        args.batch_tag
    )


if __name__ == "__main__":
    main()
