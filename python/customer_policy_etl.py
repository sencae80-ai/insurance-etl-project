"""Python-based customer/policy full reconciliation.

Run from repository root:
    python python/customer_policy_etl.py --date 2026-10-02

Requires DATABASE_URL. Database schema must be deployed separately.
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
MAX_PREMIUM = Decimal("9999999999999999.99")
MAX_COVERAGE = Decimal("999999999999999999.99")
TARGET = "insurance_tgt.customer_policy_summary"
COLUMNS = [
    "customer_id", "customer_name", "gender", "age", "email", "mobile_number",
    "city", "state", "customer_status", "total_policies", "active_policies",
    "expired_policies", "cancelled_policies", "lapsed_policies",
    "total_premium", "average_premium", "total_sum_assured",
    "first_policy_start_date", "latest_policy_end_date", "customer_category",
    "policy_holding_status", "source_last_updated_at", "business_date", "etl_run_id",
]
COMPARE = [c for c in COLUMNS if c not in ("customer_id", "etl_run_id")]

# PostgreSQL format_type() values; (type, required_not_null, requires_default)
TARGET_SCHEMA = {
    "customer_id": ("bigint", True, False),
    "customer_name": ("character varying(200)", False, False),
    "gender": ("character varying(20)", False, False),
    "age": ("integer", False, False),
    "email": ("character varying(255)", False, False),
    "mobile_number": ("character varying(20)", False, False),
    "city": ("character varying(100)", False, False),
    "state": ("character varying(100)", False, False),
    "customer_status": ("character varying(20)", False, False),
    "total_policies": ("integer", True, True),
    "active_policies": ("integer", True, True),
    "expired_policies": ("integer", True, True),
    "cancelled_policies": ("integer", True, True),
    "lapsed_policies": ("integer", True, True),
    "total_premium": ("numeric(18,2)", True, True),
    "average_premium": ("numeric(18,2)", True, True),
    "total_sum_assured": ("numeric(20,2)", True, True),
    "first_policy_start_date": ("date", False, False),
    "latest_policy_end_date": ("date", False, False),
    "customer_category": ("character varying(30)", False, False),
    "policy_holding_status": ("character varying(30)", False, False),
    "source_last_updated_at": ("timestamp with time zone", False, False),
    "business_date": ("date", True, False),
    "etl_created_at": ("timestamp with time zone", True, True),
    "etl_updated_at": ("timestamp with time zone", False, True),
    "etl_run_id": ("uuid", True, False),
}
SOURCE_COLUMNS = {
    "insurance_src.customer_master": {
        "customer_id": "bigint", "customer_name": "character varying(200)",
        "gender": "character varying(20)", "date_of_birth": "date",
        "email": "character varying(255)", "mobile_number": "character varying(20)",
        "city": "character varying(100)", "state": "character varying(100)",
        "customer_status": "character varying(20)",
        "updated_at": "timestamp with time zone",
    },
    "insurance_src.policy_master": {
        "customer_id": "bigint", "policy_status": "character varying(30)",
        "premium_amount": "numeric(18,2)", "sum_assured": "numeric(20,2)",
        "policy_start_date": "date", "policy_end_date": "date",
        "updated_at": "timestamp with time zone",
    },
}
REQUIRED_CHECKS = {
    "chk_total_policies", "chk_policy_reconciliation",
    "chk_customer_category", "chk_policy_holding_status",
}
REQUIRED_INDEXES = {
    "idx_customer_policy_business_date", "idx_customer_policy_status",
}


def initcap_postgres(value):
    """Approximate PostgreSQL INITCAP for ASCII letters and digits.

    Non-ASCII and locale-specific behavior must be parity-tested against PostgreSQL.
    """
    return _initcap_ascii(value)


def _initcap_ascii(value):
    result = []
    word_start = True
    for ch in value:
        if ch.isascii() and ch.isalpha():
            result.append(ch.upper() if word_start else ch.lower())
            word_start = False
        elif ch.isascii() and ch.isdigit():
            result.append(ch)
            word_start = False
        else:
            result.append(ch)
            word_start = True
    return "".join(result)


def normalized(value, mode="plain"):
    if value is None:
        return None
    s = str(value).strip(" ")
    if not s:
        return None
    return {"upper": str.upper, "lower": str.lower, "title": initcap_postgres,
            "plain": lambda x: x}[mode](s)


def years_old(dob, business_date):
    if dob is None or dob > business_date:
        return None
    return business_date.year - dob.year - (
        (business_date.month, business_date.day) < (dob.month, dob.day)
    )


def money(value):
    return Decimal(value if value is not None else 0).quantize(
        MONEY, rounding=ROUND_HALF_UP
    )


def latest(a, b):
    if a is None:
        return b
    if b is None:
        return a
    if (a.tzinfo is None) != (b.tzinfo is None):
        raise ValueError("Mixed timezone-aware and naive source timestamps")
    return max(a, b)


def transform(customers, policies, business_date, run_id):
    """Business transformations remain in Python, not SQL."""
    aggregates = defaultdict(list)
    for p in policies:
        status = normalized(p["policy_status"], "upper")
        if status not in STATUSES:
            raise ValueError(
                f"Invalid policy status for customer {p['customer_id']}: {status!r}"
            )
        aggregates[p["customer_id"]].append((p, status))

    # Fail rather than silently dropping orphan policies when the FK is missing.
    customer_ids = {c["customer_id"] for c in customers}
    orphan_ids = set(aggregates) - customer_ids
    if orphan_ids:
        raise ValueError(f"Orphan policies reference missing customers: {len(orphan_ids)}")
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
        premiums = [Decimal(p["premium_amount"]) for p, _ in items
                    if p["premium_amount"] is not None]
        assured = [Decimal(p["sum_assured"]) for p, _ in items
                   if p["sum_assured"] is not None]
        starts = [p["policy_start_date"] for p, _ in items
                  if p["policy_start_date"] is not None]
        ends = [p["policy_end_date"] for p, _ in items
                if p["policy_end_date"] is not None]
        updated = None
        for p, _ in items:
            updated = latest(updated, p["updated_at"])
        total_premium = money(sum(premiums, Decimal(0)))
        total_coverage = money(sum(assured, Decimal(0)))
        if abs(total_premium) > MAX_PREMIUM or abs(total_coverage) > MAX_COVERAGE:
            raise OverflowError(f"Aggregate exceeds target NUMERIC precision for customer {cid}")
        average_premium = (
            money(sum(premiums, Decimal(0)) / len(premiums))
            if premiums else money(0)
        )
        if abs(average_premium) > MAX_PREMIUM:
            raise OverflowError(
                f"Average premium exceeds NUMERIC(18,2) for customer {cid}"
            )
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
            "total_premium": total_premium,
            "average_premium": average_premium,
            "total_sum_assured": total_coverage,
            "first_policy_start_date": min(starts) if starts else None,
            "latest_policy_end_date": max(ends) if ends else None,
            "customer_category": "NO_POLICY" if total == 0 else
            "PLATINUM" if total >= 5 else "GOLD" if total >= 3 else "STANDARD",
            "policy_holding_status": "NO_POLICY" if total == 0 else
            "ACTIVE_HOLDER" if counts["ACTIVE"] else "INACTIVE_HOLDER",
            "source_last_updated_at": latest(c["updated_at"], updated),
            "business_date": business_date,
            "etl_run_id": run_id,
        }
        output.append(row)
    return output


def _columns(conn, qualified_table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                   a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
            FROM pg_attribute a
            JOIN pg_class t ON t.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE t.oid = to_regclass(%s)
              AND a.attnum > 0 AND NOT a.attisdropped
        """, (qualified_table,))
        return {name: (dtype, not_null, default) for name, dtype, not_null, default
                in cur.fetchall()}


def validate_schema(conn):
    """Fail fast if the pre-deployed database schema differs from approved DDL."""
    problems = []
    for table, expected in SOURCE_COLUMNS.items():
        actual = _columns(conn, table)
        if not actual:
            problems.append(f"Missing source table: {table}")
            continue
        for column, dtype in expected.items():
            if column not in actual:
                problems.append(f"{table}.{column}: missing")
            elif actual[column][0] != dtype:
                problems.append(
                    f"{table}.{column}: expected {dtype}, got {actual[column][0]}"
                )

    actual = _columns(conn, TARGET)
    if not actual:
        problems.append(f"Missing target table: {TARGET}; deploy approved DDL first")
    else:
        for column, (dtype, required, has_default) in TARGET_SCHEMA.items():
            if column not in actual:
                problems.append(f"{TARGET}.{column}: missing")
                continue
            found_type, found_required, found_default = actual[column]
            if found_type != dtype:
                problems.append(f"{TARGET}.{column}: expected {dtype}, got {found_type}")
            if found_required != required:
                problems.append(f"{TARGET}.{column}: NOT NULL mismatch")
            if bool(found_default) != has_default:
                problems.append(f"{TARGET}.{column}: DEFAULT presence mismatch")
        extra = set(actual) - set(TARGET_SCHEMA)
        if extra:
            problems.append(f"{TARGET}: unexpected columns {sorted(extra)}")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT conname, contype, pg_get_constraintdef(oid)
                FROM pg_constraint WHERE conrelid = to_regclass(%s)
            """, (TARGET,))
            constraints = {name: (kind, definition)
                           for name, kind, definition in cur.fetchall()}
            for name in REQUIRED_CHECKS:
                if name not in constraints or constraints[name][0] != "c":
                    problems.append(f"{TARGET}: missing CHECK constraint {name}")
            pks = [definition for kind, definition in constraints.values()
                   if kind == "p"]
            if not any("(customer_id)" in definition for definition in pks):
                problems.append(f"{TARGET}: missing PRIMARY KEY(customer_id)")
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'insurance_tgt'
                  AND tablename = 'customer_policy_summary'
            """)
            indexes = {r[0] for r in cur.fetchall()}
            for name in REQUIRED_INDEXES - indexes:
                problems.append(f"{TARGET}: missing index {name}")

    if problems:
        raise RuntimeError("Schema validation failed:\n - " + "\n - ".join(problems))
    LOG.info("Approved source and target schema validation passed")


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


def persist(conn, rows, *, max_delete_count=0, max_delete_fraction=0.0):
    names = ", ".join(COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    assignments = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "customer_id"
    ) + ", etl_updated_at = CURRENT_TIMESTAMP"
    left = ", ".join(f"t.{c}" for c in COMPARE)
    right = ", ".join(f"EXCLUDED.{c}" for c in COMPARE)
    statement = (
        f"INSERT INTO {TARGET} AS t ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT (customer_id) DO UPDATE SET {assignments} "
        f"WHERE ({left}) IS DISTINCT FROM ({right})"
    )
    with conn.cursor() as cur:
        if rows:
            cur.executemany(statement, [tuple(row[c] for c in COLUMNS) for row in rows])
        ids = [row["customer_id"] for row in rows]
        cur.execute(f"SELECT COUNT(*) FROM {TARGET}")
        existing_count = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM {TARGET} WHERE NOT (customer_id = ANY(%s::bigint[]))",
            (ids,),
        )
        delete_count = cur.fetchone()[0]
        fraction = delete_count / existing_count if existing_count else 0.0
        if delete_count > max_delete_count or fraction > max_delete_fraction:
            raise RuntimeError(
                f"Deletion safety gate: requested={delete_count}, existing={existing_count}, "
                f"approved max_count={max_delete_count}, max_fraction={max_delete_fraction}"
            )
        cur.execute(
            f"DELETE FROM {TARGET} WHERE NOT (customer_id = ANY(%s::bigint[]))",
            (ids,),
        )
        deleted = cur.rowcount
    return deleted


def run(business_date, database_url, *, allow_empty_source=False,
        max_delete_count=0, max_delete_fraction=0.0, min_source_customers=1):
    run_id = uuid.uuid4()
    with psycopg.connect(database_url, autocommit=False, options="-c timezone=UTC") as conn:
        try:
            # REPEATABLE READ gives both SELECTs the same source snapshot.
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                # Prevent simultaneous runs from reconciling the same target.
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (48291017,))
            validate_schema(conn)
            customers, policies = extract(conn)
            LOG.info("Extracted customers=%d policies=%d", len(customers), len(policies))
            if not customers and not allow_empty_source:
                raise RuntimeError(
                    "Source customer_master is empty; refusing to delete all target rows. "
                    "Use --allow-empty-source only for an approved empty-source reconciliation."
                )
            if len(customers) < min_source_customers and not (
                allow_empty_source and len(customers) == 0
            ):
                raise RuntimeError(
                    "Source completeness gate: customer count below approved minimum"
                )
            rows = transform(customers, policies, business_date, run_id)
            deleted = persist(conn, rows, max_delete_count=max_delete_count,
                              max_delete_fraction=max_delete_fraction)
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TARGET}")
                target_count = cur.fetchone()[0]
                if target_count != len(rows):
                    raise RuntimeError(f"Customer reconciliation mismatch: source={len(rows)} target={target_count}")
                cur.execute(f"SELECT COALESCE(SUM(total_policies),0), COALESCE(SUM(total_premium),0) FROM {TARGET}")
                target_policies, target_premium = cur.fetchone()
                expected_policies = len(policies)
                expected_premium = money(sum((Decimal(p['premium_amount']) for p in policies
                                              if p['premium_amount'] is not None), Decimal(0)))
                if target_policies != expected_policies or target_premium != expected_premium:
                    raise RuntimeError("Policy/premium reconciliation mismatch")
            conn.commit()
            LOG.info("Committed run_id=%s customer_rows=%d deleted=%d",
                     run_id, len(rows), deleted)
            return run_id
        except Exception:
            conn.rollback()
            LOG.exception("ETL failed; PostgreSQL transaction rolled back")
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=date.fromisoformat,
                        help="Business date in YYYY-MM-DD format")
    parser.add_argument("--allow-empty-source", action="store_true",
                        help="Allow full target deletion when source has zero customers")
    parser.add_argument("--max-delete-count", type=int, default=0,
                        help="Approved maximum number of target rows that may be deleted")
    parser.add_argument("--max-delete-fraction", type=float, default=0.0,
                        help="Approved maximum fraction of existing target rows that may be deleted")
    parser.add_argument("--min-source-customers", type=int, default=1)
    args = parser.parse_args()
    if args.max_delete_count < 0 or not 0 <= args.max_delete_fraction <= 1 or args.min_source_customers < 0:
        parser.error("Invalid completeness/deletion safety thresholds")
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        parser.error("DATABASE_URL is required in environment or .env")
    run(args.date, url, allow_empty_source=args.allow_empty_source,
        max_delete_count=args.max_delete_count,
        max_delete_fraction=args.max_delete_fraction,
        min_source_customers=args.min_source_customers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    main()
