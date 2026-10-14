"""Transactional PostgreSQL customer-policy SQL reconciliation runner.

Run from repository root:
    python source_code/customer_policy_etl.py --date 2026-10-14

The SQL file must have exactly one DATE 'YYYY-MM-DD' AS business_date
expression, and exactly one outer BEGIN; and COMMIT; statement.
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
LOG = logging.getLogger("customer_policy_etl_sql")
LOCK_KEY = 48291017  # Must match python/customer_policy_etl.py


def get_db_config() -> dict:
    load_dotenv(ROOT / ".env")
    required = ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing database settings: {', '.join(missing)}")
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "connect_timeout": 10,
        "application_name": "customer_policy_sql_etl",
        "options": "-c timezone=UTC -c statement_timeout=300000 -c lock_timeout=30000",
    }


def prepare_sql(path: Path, business_date: date) -> str:
    """Replace the SQL's single date literal; leave transaction ownership to Python.

    This is compatible with the existing SQL declaration style, not a generic
    SQL templating engine. Parameterize the SQL itself in a later coordinated change.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Transformation SQL not found: {path}")
    script = path.read_text(encoding="utf-8-sig")
    pattern = (
        r"(?im)^(?P<prefix>[ \t]*)DATE[ \t]+"
        r"'[0-9]{4}-[0-9]{2}-[0-9]{2}'"
        r"(?P<suffix>[ \t]+AS[ \t]+business_date\b)"
    )
    script, date_count = re.subn(
        pattern,
        lambda m: f"{m.group('prefix')}DATE '{business_date.isoformat()}'{m.group('suffix')}",
        script,
    )
    if date_count != 1:
        raise ValueError(
            f"Expected exactly one DATE literal AS business_date in {path}; "
            f"found {date_count}. Review the SQL parameter section."
        )

    # Only standalone outer transaction statements are supported here.
    begin_pattern = r"(?im)^[ \t]*BEGIN[ \t]*;[ \t]*(?:--[^\n]*)?$"
    commit_pattern = r"(?im)^[ \t]*COMMIT[ \t]*;[ \t]*(?:--[^\n]*)?$"
    if len(re.findall(begin_pattern, script)) != 1 or len(re.findall(commit_pattern, script)) != 1:
        raise ValueError("Expected exactly one standalone BEGIN; and COMMIT; in SQL")
    script = re.sub(begin_pattern, "", script)
    script = re.sub(commit_pattern, "", script)
    # Guard against additional standalone transaction control in the script.
    if re.search(r"(?im)^[ \t]*(?:BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)[ \t]*(?:;|\b)", script):
        raise ValueError("Unexpected transaction control in transformation SQL")
    return script


def source_counts(cur) -> tuple[int, int]:
    cur.execute("SELECT COUNT(*) FROM insurance_src.customer_master")
    customers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM insurance_src.policy_master")
    policies = cur.fetchone()[0]
    return customers, policies


def validate_source(cur, min_source_customers: int) -> None:
    customers, policies = source_counts(cur)
    LOG.info("Source counts customers=%s policies=%s", customers, policies)
    if customers < min_source_customers:
        raise RuntimeError(
            f"Source completeness gate failed: customers={customers}, "
            f"minimum={min_source_customers}"
        )
    # A source control manifest is still required to detect partial upstream loads.


def validate_deletions(cur, max_delete_count: int, max_delete_fraction: float) -> None:
    cur.execute("SELECT COUNT(*) FROM insurance_tgt.customer_policy_summary")
    existing = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*)
        FROM insurance_tgt.customer_policy_summary AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM insurance_src.customer_master AS s
            WHERE s.customer_id = t.customer_id
        )
    """)
    potential = cur.fetchone()[0]
    fraction = potential / existing if existing else 0.0
    if potential > max_delete_count or fraction > max_delete_fraction:
        raise RuntimeError(
            f"Deletion safety gate failed: potential={potential}, existing={existing}, "
            f"fraction={fraction:.6f}, max_count={max_delete_count}, "
            f"max_fraction={max_delete_fraction}"
        )
    LOG.info("Deletion gate PASS potential=%s existing=%s", potential, existing)


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
    keys = (
        "source_customers", "source_policies", "target_customers",
        "target_policies", "source_premium", "target_premium",
        "invalid_status_summaries",
    )
    return dict(zip(keys, cur.fetchone()))


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
        raise RuntimeError("Source-to-target validation failed; rolling back")


def run_etl(
    business_date: date,
    *,
    min_source_customers: int = 1,
    max_delete_count: int = 0,
    max_delete_fraction: float = 0.0,
) -> dict:
    if min_source_customers < 1:
        raise ValueError("min_source_customers must be >= 1")
    if max_delete_count < 0 or not 0 <= max_delete_fraction <= 1:
        raise ValueError("Invalid deletion safety thresholds")
    script = prepare_sql(SQL_PATH, business_date)
    execution_id = str(uuid4())
    LOG.info("Starting execution_id=%s business_date=%s", execution_id, business_date)
    conn = psycopg2.connect(**get_db_config())
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
            validate_source(cur, min_source_customers)
            validate_deletions(cur, max_delete_count, max_delete_fraction)
            LOG.info("Executing SQL transformation")
            cur.execute(script)
            metrics = fetch_metrics(cur)
            for name, value in metrics.items():
                LOG.info("%s=%s", name, value)
            validate(metrics)
        conn.commit()
        LOG.info("ETL SUCCESS execution_id=%s", execution_id)
        return metrics
    except Exception:
        conn.rollback()
        LOG.exception("ETL FAILED and rolled back execution_id=%s", execution_id)
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run customer-policy PostgreSQL SQL ETL")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--min-source-customers", type=int, default=1)
    parser.add_argument("--max-delete-count", type=int, default=0)
    parser.add_argument("--max-delete-fraction", type=float, default=0.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        run_etl(
            args.date,
            min_source_customers=args.min_source_customers,
            max_delete_count=args.max_delete_count,
            max_delete_fraction=args.max_delete_fraction,
        )
        return 0
    except (ValueError, OSError, psycopg2.Error, RuntimeError):
        LOG.exception("ETL execution stopped")
        return 1


if __name__ == "__main__":
    sys.exit(main())
