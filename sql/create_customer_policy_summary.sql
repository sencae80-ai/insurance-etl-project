
-- =====================================================
-- DATABASE : etl_quality_test_project
-- SCHEMA   : insurance_tgt
-- TABLE    : customer_policy_summary
-- PURPOSE  : Canonical target DDL
-- =====================================================

-- Step 1: Create schema
CREATE SCHEMA IF NOT EXISTS insurance_tgt;

-- Step 2: Create target table
CREATE TABLE IF NOT EXISTS insurance_tgt.customer_policy_summary
(
    -- Customer information (1-9)
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(200),
    gender VARCHAR(20),
    age INTEGER,
    email VARCHAR(255),
    mobile_number VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    customer_status VARCHAR(20),

    -- Policy aggregation (10-14)
    total_policies INTEGER NOT NULL DEFAULT 0,
    active_policies INTEGER NOT NULL DEFAULT 0,
    expired_policies INTEGER NOT NULL DEFAULT 0,
    cancelled_policies INTEGER NOT NULL DEFAULT 0,
    lapsed_policies INTEGER NOT NULL DEFAULT 0,

    -- Financial metrics (15-17)
    total_premium NUMERIC(18,2) NOT NULL DEFAULT 0,
    average_premium NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_sum_assured NUMERIC(20,2) NOT NULL DEFAULT 0,

    -- Policy dates (18-19)
    first_policy_start_date DATE,
    latest_policy_end_date DATE,

    -- Derived attributes (20-21)
    customer_category VARCHAR(30),
    policy_holding_status VARCHAR(30),

    -- ETL metadata (22-26)
    source_last_updated_at TIMESTAMPTZ,
    business_date DATE NOT NULL,

    etl_created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    etl_updated_at TIMESTAMPTZ
        DEFAULT CURRENT_TIMESTAMP,

    etl_run_id UUID NOT NULL,

    -- Constraints
    CONSTRAINT chk_total_policies
        CHECK (total_policies >= 0),

    CONSTRAINT chk_policy_reconciliation
        CHECK (
            total_policies =
                active_policies
                + expired_policies
                + cancelled_policies
                + lapsed_policies
        ),

    CONSTRAINT chk_customer_category
        CHECK (
            customer_category IN (
                'NO_POLICY',
                'STANDARD',
                'GOLD',
                'PLATINUM'
            )
        ),

    CONSTRAINT chk_policy_holding_status
        CHECK (
            policy_holding_status IN (
                'NO_POLICY',
                'ACTIVE_HOLDER',
                'INACTIVE_HOLDER'
            )
        )
);

-- Step 3: Create indexes
CREATE INDEX IF NOT EXISTS idx_customer_policy_business_date
ON insurance_tgt.customer_policy_summary(business_date);

CREATE INDEX IF NOT EXISTS idx_customer_policy_status
ON insurance_tgt.customer_policy_summary(policy_holding_status);

-- Additional financial constraints are excluded
-- until business decision ISS-007 is approved.
