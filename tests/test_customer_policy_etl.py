import importlib.util
from pathlib import Path
from datetime import date, datetime, timezone
from decimal import Decimal
import uuid
import pytest
# Permit pure-transform unit tests in minimal CI environments without a DB driver.
# Integration tests still require the real psycopg package.
import sys, types
try:
    import psycopg
except ImportError:
    fake = types.ModuleType('psycopg')
    rows = types.ModuleType('psycopg.rows')
    rows.dict_row = object()
    sys.modules['psycopg'] = fake
    sys.modules['psycopg.rows'] = rows
try:
    import dotenv
except ImportError:
    fake_dotenv = types.ModuleType('dotenv')
    fake_dotenv.load_dotenv = lambda *a, **k: None
    sys.modules['dotenv'] = fake_dotenv

path = Path(__file__).resolve().parents[1] / 'python/customer_policy_etl.py'
spec = importlib.util.spec_from_file_location('etl', path)
etl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(etl)

NOW = datetime(2026, 10, 2, tzinfo=timezone.utc)
def customer(cid=1, **overrides):
    c = dict(customer_id=cid, customer_name='2nd cross road', gender='m',
        date_of_birth=date(2000, 1, 1), email='TEST@EXAMPLE.COM',
        mobile_number=' 123 ', city='2nd cross road', state='tamil nadu',
        customer_status='active', updated_at=NOW)
    c.update(overrides)
    return c

def policy(cid=1, status='active', amount='100.00', **overrides):
    p = dict(customer_id=cid, policy_status=status, premium_amount=Decimal(amount),
        sum_assured=Decimal('1000'), policy_start_date=date(2020,1,1),
        policy_end_date=date(2027,1,1), updated_at=NOW)
    p.update(overrides)
    return p

def run_transform(customers, policies):
    return etl.transform(customers, policies, date(2026,10,14), uuid.uuid4())

def test_no_policy():
    r = run_transform([customer()], [])[0]
    assert (r['total_policies'], r['customer_category'], r['policy_holding_status']) == (0,'NO_POLICY','NO_POLICY')

def test_initcap_digit_word():
    assert run_transform([customer()], [])[0]['city'] == '2nd Cross Road'

def test_gold_platinum_and_status():
    for count, expected in [(3,'GOLD'), (5,'PLATINUM')]:
        r = run_transform([customer()], [policy() for _ in range(count)])[0]
        assert r['customer_category'] == expected
        assert r['policy_holding_status'] == 'ACTIVE_HOLDER'
        assert r['total_policies'] == count

def test_invalid_status():
    with pytest.raises(ValueError, match='Invalid policy status'):
        run_transform([customer()], [policy(status=None)])

def test_orphan_policy():
    with pytest.raises(ValueError, match='Orphan policies'):
        run_transform([customer()], [policy(cid=999)])

def test_duplicate_customer():
    with pytest.raises(ValueError, match='Duplicate customer_id'):
        run_transform([customer(),customer()], [])

def test_future_dob():
    assert run_transform([customer(date_of_birth=date(2030,1,1))], [])[0]['age'] is None

def test_average_excludes_null():
    r = run_transform([customer()], [policy(amount='100'),policy(premium_amount=None)])[0]
    assert r['average_premium'] == Decimal('100.00')
    assert r['total_premium'] == Decimal('100.00')

def test_target_audit_schema():
    assert etl.TARGET_SCHEMA['etl_created_at'] == ('timestamp with time zone', True, True)
    assert etl.COLUMNS[-1] == 'etl_run_id'
    assert 'etl_created_at' not in etl.COLUMNS

def test_precision_overflow():
    with pytest.raises(OverflowError):
        run_transform([customer()], [policy(amount='9999999999999999.99'),policy(amount='1.00')])
