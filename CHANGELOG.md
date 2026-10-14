# Change log

## 1.1.0-rc1 — 2026-10-14 (review candidate; NOT production certified)
- Preserved uploaded v1.1 ZIP unmodified under `archive/`.
- Replaced accidentally copied Python code in `sql/verify_deployed_schema.sql` with read-only SQL evidence queries.
- Updated SQL transformation with orphan-policy, status, numeric and count validations; corrected nonexistent audit timestamp references.
- Renamed extensionless pytest file to `tests/test_customer_policy_etl.py`.
- Removed conflicting duplicate python-dotenv dependency declaration.
- Added DMS v1.1 and archived original DMS and STM, without claiming original document version.
- Preserved historical `sql/test.txt` in docs/archive.
- Added VERSION, .gitignore, and release notes.

## Outstanding review gates
- PostgreSQL integration/SIT tests and deployed-schema verification have not run.
- Confirm rounding parity and policy-level vs rounded customer totals reconciliation.
- Confirm business-date semantics, source completeness SLA, deletion thresholds, negative financial handling, and authoritative ETL runner.
- Direct execution of SQL bypasses runner safety gates; do not execute SQL directly in production.
- This is a candidate release; old v1.0 source code was not supplied as a separately verified baseline.
