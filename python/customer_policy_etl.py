"""Python-transformed full reconciliation of customer and policy data.

Run from repository root: python python/customer_policy_etl.py --date 2026-10-02
Requires DATABASE_URL in .env or environment. No transformation SQL file is used.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import logging
import os
from pathlib import Path
import uuid

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

LOG = logging.getLogger("customer_policy_etl")
STATUSES = {"ACTIVE", "EXPIRED", "CANCELLED", "LAPSED"}
MONEY = Decimal("0.01")
COLUMNS = [
    "customer_id", "customer_name", "gender", "age", "email", "mobile_number",
    "city", "state", "customer_status", "total_policies", "active_policies",
    "expired_policies", "cancelled_policies", "lapsed_policies",
    "total_premium", "average_premium", "total_sum_assured",
    "first_policy_start_date", "latest_policy_end_date", "customer_category",
    "policy_holding_status", "source_last_updated_at", "business_date", "etl_run_id",
]
# Columns that determine whether an existing row should be updated.
COMPARE = [c for c in COLUMNS if c not in ("customer_id", "etl_run_id")]


def normalized(value, mode="plain"):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return {"upper": str.upper, "lower": str.lower, "title": str.title,
            "plain": lambda x: x}[mode](s)


def years_old(dob, business_date):
    if dob is None or dob > business_date:
        return None
    return business_date.year - dob.year - ((business_date.month, business_date.day) < (dob.month, dob.day))


def money(value):
    return Decimal(value if value is not None else 0).quantize(MONEY, rounding=ROUND_HALF_UP)


def latest(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def transform(customers, policies, business_date, run_id):
    """Perform all business transformations in Python, not SQL."""
    aggregates = defaultdict(list)
    for p in policies:
        status = normalized(p["policy_status"], "upper")
        if status not in STATUSES:
            raise ValueError(f"Invalid policy status for customer {p['customer_id']}: {status!r}")
        aggregates[p["customer_id"]].append((p, status))

    output = []
    seen = set()
    for c in customers:
        cid = c["customer_id"]
        if cid in seen:
            raise ValueError(f"Duplicate customer_id in source: {cid}")
        seen.add(cid)
        items = aggregates.get(cid, [])
        counts = {status: sum(s == status for _, s in items) for status in STATUSES}
        total = len(items)
        if total != sum(counts.values()):
            raise ValueError(f"Policy reconciliation failed for customer {cid}")
        premiums = [Decimal(p["premium_amount"]) for p, _ in items if p["premium_amount"] is not None]
        assured = [Decimal(p["sum_assured"]) for p, _ in items if p["sum_assured"] is not None]
        starts = [p["policy_start_date"] for p, _ in items if p["policy_start_date"] is not None]
        ends = [p["policy_end_date"] for p, _ in items if p["policy_end_date"] is not None]
        updated = None
        for p, _ in items:
            updated = latest(updated, p["updated_at"])
        row = {
            "customer_id": cid,
            "customer_name": normalized(c["customer_name"], "title"),
            "gender": normalized(c["gender"], "upper"),
            "age": years_old(c["date_of_birth"], business_date),
            "email": normalized(c["email"], "lower"),
            "mobile_number": normalized(c["mobile_number"]),
            "city": normalized(c["city"], "title"),
            "state": normalized(c["state"], "title"),
            "customer_status": normalized(c["customer_status"], "upper") or "UNKNOWN",
            "total_policies": total,
            "active_policies": counts["ACTIVE"],
            "expired_policies": counts["EXPIRED"],
            "cancelled_policies": counts["CANCELLED"],
            "lapsed_policies": counts["LAPSED"],
            "total_premium": money(sum(premiums, Decimal(0))),
            "average_premium": money(sum(premiums, Decimal(0)) / len(premiums)) if premiums else money(0),
            "total_sum_assured": money(sum(assured, Decimal(0))),
            "first_policy_start_date": min(starts) if starts else None,
            "latest_policy_end_date": max(ends) if ends else None,
            "customer_category": "NO_POLICY" if total == 0 else "PLATINUM" if total >= 5 else "GOLD" if total >= 3 else "STANDARD",
            "policy_holding_status": "NO_POLICY" if total == 0 else "ACTIVE_HOLDER" if counts["ACTIVE"] else "INACTIVE_HOLDER",
            "source_last_updated_at": latest(c["updated_at"], updated),
            "business_date": business_date,
            "etl_run_id": run_id,
        }
        output.append(row)
    return output


def ensure_target(conn):
    # Table DDL is database administration, not business transformation.
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS insurance_tgt")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS insurance_tgt.customer_policy_summary (
                customer_id BIGINT PRIMARY KEY,
                customer_name VARCHAR(200), gender VARCHAR(20), age INTEGER,
                email VARCHAR(255), mobile_number VARCHAR(20), city VARCHAR(100),
                state VARCHAR(100), customer_status VARCHAR(20),
                total_policies INTEGER, active_policies INTEGER, expired_policies INTEGER,
                cancelled_policies INTEGER, lapsed_policies INTEGER,
                total_premium NUMERIC(18,2), average_premium NUMERIC(18,2),
                total_sum_assured NUMERIC(20,2), first_policy_start_date DATE,
                latest_policy_end_date DATE, customer_category VARCHAR(30),
                policy_holding_status VARCHAR(30), source_last_updated_at TIMESTAMP,
                business_date DATE, etl_run_id UUID,
                etl_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)


def extract(conn):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT customer_id, customer_name, gender, date_of_birth, email,
                       mobile_number, city, state, customer_status, updated_at
                       FROM insurance_src.customer_master""")
        customers = cur.fetchall()
        cur.execute("""SELECT customer_id, policy_status, premium_amount, sum_assured,
                       policy_start_date, policy_end_date, updated_at
                       FROM insurance_src.policy_master""")
        policies = cur.fetchall()
    return customers, policies


def persist(conn, rows):
    # Use parameterized SQL solely to persist Python-computed values.
    names = ", ".join(COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "customer_id")
    assignments += ", etl_updated_at = CURRENT_TIMESTAMP"
    left = ", ".join(f"t.{c}" for c in COMPARE)
    right = ", ".join(f"EXCLUDED.{c}" for c in COMPARE)
    statement = (f"INSERT INTO insurance_tgt.customer_policy_summary AS t ({names}) "
                 f"VALUES ({placeholders}) ON CONFLICT (customer_id) DO UPDATE SET {assignments} "
                 f"WHERE ({left}) IS DISTINCT FROM ({right})")
    with conn.cursor() as cur:
        if rows:
            cur.executemany(statement, [tuple(row[c] for c in COLUMNS) for row in rows])
        # Full reconciliation, including the zero-source-row case.
        ids = [row["customer_id"] for row in rows]
        cur.execute("DELETE FROM insurance_tgt.customer_policy_summary WHERE NOT (customer_id = ANY(%s))", (ids,))
        deleted = cur.rowcount
    return deleted


def run(business_date, database_url):
    run_id = uuid.uuid4()
    with psycopg.connect(database_url, autocommit=False) as conn:
        try:
            # Transaction ensures inserts/updates/deletions are atomic.
            ensure_target(conn)
            customers, policies = extract(conn)
            LOG.info("Extracted customers=%d policies=%d", len(customers), len(policies))
            rows = transform(customers, policies, business_date, run_id)
            deleted = persist(conn, rows)
            conn.commit()
            LOG.info("Committed run_id=%s customer_rows=%d deleted=%d", run_id, len(rows), deleted)
        except Exception:
            conn.rollback()
            LOG.exception("ETL failed; PostgreSQL transaction rolled back")
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        parser.error("DATABASE_URL is required in environment or .env")
    run(args.date, url)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    main()
