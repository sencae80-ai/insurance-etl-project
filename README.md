# Insurance Customer Policy ETL — v1.1.0-rc1

PostgreSQL-to-PostgreSQL customer-policy full reconciliation. **Review candidate, not production certified.**

## Preserving versions
- `archive/insurance_etl_project_v1.1_uploaded_snapshot.zip`: unchanged uploaded v1.1 project.
- `docs/archive/`: original supplied DMS, STM, and older SQL test copy.
- `docs/current/`: updated DMS v1.1.
- Commit/tag approved releases in Git; no claim is made that a historical v1.0 source snapshot exists.

## Setup (PowerShell)
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -q
```

Configure database environment variables locally; do not commit `.env`.
Deploy/review `sql/create_customer_policy_summary.sql` separately (CREATE TABLE IF NOT EXISTS is not a migration).

## Run one ETL implementation, not both
```powershell
python .\source_code\customer_policy_etl.py --date 2026-10-14
# Alternative Python transformation implementation:
python .\python\customer_policy_etl.py --date 2026-10-14
```

SQL runner requires DB_HOST, DB_NAME, DB_USER, DB_PASSWORD and optional DB_PORT.
Python transformation runner requires DATABASE_URL. Both use the same advisory lock.
Default SQL runner deletion thresholds are zero. Approved deletions require explicit
`--max-delete-count` and `--max-delete-fraction` flags.

## Verify deployed schema (read only)
```powershell
psql -d YOUR_DATABASE -f .\sql\verify_deployed_schema.sql
```

See CHANGELOG.md for outstanding release gates.
