"""Transactional PostgreSQL customer-policy full reconciliation ETL runner.

Run from the project root:
    python source_code/customer_policy_etl.py --date 2026-10-02
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
SQL_PATH = ROOT / "sql" / "customer_policy_transformation.sql"
LOG = logging.getLogger("customer_policy_etl")


def get_db_config() -> dict:
    load_dotenv(ROOT / ".env")
    required = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing .env settings: {', '.join(missing)}")
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "connect_timeout": 10,
        "application_name": "customer_policy_etl",
    }



def prepare_sql(path: Path, business_date: date) -> str:
    """Prepare SQL using the requested business date."""

    if not path.is_file():
        raise FileNotFoundError(
            f"Transformation SQL not found: {path}"
        )

    script = path.read_text(encoding="utf-8-sig")

    # Match the actual SQL parameter declaration.
    # Allow optional whitespace, comments and trailing comma.
    pattern = (
        r"(?im)^([ \t]*)DATE\s*"
        r"'[0-9]{4}-[0-9]{2}-[0-9]{2}'"
        r"([ \t]+AS[ \t]+business_date\b)"
    )

    replacement = (
        lambda match:
        f"{match.group(1)}DATE '{business_date.isoformat()}'"
        f"{match.group(2)}"
    )

    script, count = re.subn(
        pattern,
        replacement,
        script
    )

    LOG.info(
        "Business date parameter matches: %s",
        count
    )

    if count != 1:
        raise ValueError(
            "Business date declaration not found uniquely. "
            f"Found {count} matches in {path}. "
            "Check the SQL parameter section."
        )

    # Remove outer transaction statements.
    # Python will handle COMMIT and ROLLBACK.
    script, begin_count = re.subn(
        r"(?im)^[ \t]*BEGIN[ \t]*;",
        "",
        script,
        count=1
    )

    script, commit_count = re.subn(
        r"(?im)^[ \t]*COMMIT[ \t]*;[ \t]*$",
        "",
        script,
        count=1
    )

    if begin_count != 1 or commit_count != 1:
        raise ValueError(
            "Expected outer BEGIN; and COMMIT; "
            "statements in transformation SQL"
        )

    return script



def fetch_metrics(cur) -> dict:
    cur.execute("""
        SELECT
          (SELECT COUNT(*) FROM insurance_src.customer_master),
          (SELECT COUNT(*) FROM insurance_src.policy_master),
          (SELECT COUNT(*) FROM insurance_tgt.customer_policy_summary),
          (SELECT COALESCE(SUM(total_policies),0)
             FROM insurance_tgt.customer_policy_summary),
          (SELECT COALESCE(SUM(premium_amount),0)
             FROM insurance_src.policy_master),
          (SELECT COALESCE(SUM(total_premium),0)
             FROM insurance_tgt.customer_policy_summary),
          (SELECT COUNT(*) FROM insurance_tgt.customer_policy_summary
             WHERE total_policies <> active_policies + expired_policies
                                   + cancelled_policies + lapsed_policies)
    """)
    row = cur.fetchone()
    keys = (
        "source_customers", "source_policies", "target_customers",
        "target_policies", "source_premium", "target_premium", "invalid_status_summaries",
    )
    return dict(zip(keys, row))


def validate(metrics: dict) -> None:
    checks = {
        "customer counts": metrics["source_customers"] == metrics["target_customers"],
        "policy counts": metrics["source_policies"] == metrics["target_policies"],
        "premium totals": Decimal(metrics["source_premium"]) == Decimal(metrics["target_premium"]),
        "policy status counts": metrics["invalid_status_summaries"] == 0,
    }
    for name, passed in checks.items():
        LOG.info("%s: %s", name, "PASS" if passed else "FAIL")
    if not all(checks.values()):
        raise RuntimeError("Source-to-target validation failed; transaction will be rolled back")


def run_etl(business_date: date) -> dict:
    script = prepare_sql(SQL_PATH, business_date)
    execution_id = str(uuid4())
    LOG.info("Starting ETL execution_id=%s business_date=%s", execution_id, business_date)
    conn = psycopg2.connect(**get_db_config())
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Prevent two ETL runs from modifying the same target concurrently.
            cur.execute("SELECT pg_advisory_xact_lock(71002, 1)")
            LOG.info("Executing SQL transformation")
            cur.execute(script)
            metrics = fetch_metrics(cur)
            for name, value in metrics.items():
                LOG.info("%s=%s", name, value)
            validate(metrics)
        conn.commit()
        LOG.info("ETL SUCCESS: committed execution_id=%s", execution_id)
        return metrics
    except Exception:
        conn.rollback()
        LOG.exception("ETL FAILED: rolled back execution_id=%s", execution_id)
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run customer-policy PostgreSQL ETL")
    parser.add_argument("--date", default=os.getenv("BUSINESS_DATE", date.today().isoformat()),
                        help="Business date in YYYY-MM-DD format")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        business_date = date.fromisoformat(args.date)
        run_etl(business_date)
        return 0
    except (ValueError, OSError, psycopg2.Error, RuntimeError):
        LOG.exception("ETL execution stopped")
        return 1


if __name__ == "__main__":
    sys.exit(main())

