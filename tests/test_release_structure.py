from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]

def test_sql_runner_contract():
    sql = (ROOT / 'sql/customer_policy_transformation.sql').read_text()
    assert len(re.findall(r"DATE\s+'\d{4}-\d{2}-\d{2}'\s+AS\s+business_date", sql)) == 1
    assert len(re.findall(r'(?im)^\s*BEGIN\s*;', sql)) == 1
    assert len(re.findall(r'(?im)^\s*COMMIT\s*;', sql)) == 1
    assert 'r.etl_created_at' not in sql
    assert 'r.etl_updated_at' not in sql
    assert 'Orphan policies' in sql

def test_schema_verification_is_sql():
    text = (ROOT / 'sql/verify_deployed_schema.sql').read_text()
    assert 'information_schema.columns' in text
    assert 'from __future__' not in text

def test_preserved_docs():
    assert (ROOT / 'docs/current/Insurance_Customer_Policy_DMS_v1.1.docx').is_file()
    assert (ROOT / 'archive/insurance_etl_project_v1.1_uploaded_snapshot.zip').is_file()
