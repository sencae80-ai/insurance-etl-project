
-- =====================================================
-- DATABASE: etl_quality_test_project
-- TARGET SCHEMA: insurance_tgt
-- TARGET TABLE: customer_policy_summary
-- =====================================================

-- Step 1: Create target schema
CREATE SCHEMA IF NOT EXISTS insurance_tgt;


-- Step 2: Create target table
CREATE TABLE IF NOT EXISTS
insurance_tgt.customer_policy_summary
(
    customer_id BIGINT PRIMARY KEY,

    -- Customer information
    customer_name VARCHAR(200),
    gender VARCHAR(20),
    age INTEGER,
    email VARCHAR(255),
    mobile_number VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    customer_status VARCHAR(20),

    -- Policy aggregation
    total_policies INTEGER NOT NULL DEFAULT 0,
    active_policies INTEGER NOT NULL DEFAULT 0,
    expired_policies INTEGER NOT NULL DEFAULT 0,
    cancelled_policies INTEGER NOT NULL DEFAULT 0,
    lapsed_policies INTEGER NOT NULL DEFAULT 0,

    -- Financial metrics
    total_premium NUMERIC(18,2) NOT NULL DEFAULT 0,
    average_premium NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_sum_assured NUMERIC(20,2) NOT NULL DEFAULT 0,

    -- Policy dates
    first_policy_start_date DATE,
    latest_policy_end_date DATE,

    -- Derived attributes
    customer_category VARCHAR(30),
    policy_holding_status VARCHAR(30),

    -- ETL metadata
    source_last_updated_at TIMESTAMP,
    business_date DATE NOT NULL,

    etl_created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    etl_updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    etl_run_id UUID NOT NULL,

    -- Constraints
    CONSTRAINT chk_total_policies
        CHECK (total_policies >= 0),

    CONSTRAINT chk_policy_counts
        CHECK (
            active_policies >= 0
            AND expired_policies >= 0
            AND cancelled_policies >= 0
            AND lapsed_policies >= 0
        ),

    CONSTRAINT chk_policy_status_total
        CHECK (
            total_policies =
                active_policies
                + expired_policies
                + cancelled_policies
                + lapsed_policies
        ),

    CONSTRAINT chk_total_premium
        CHECK (total_premium >= 0),

    CONSTRAINT chk_total_coverage
        CHECK (total_sum_assured >= 0),

    CONSTRAINT chk_age
        CHECK (age IS NULL OR age >= 0)
);


-- Step 3: Create indexes
CREATE INDEX IF NOT EXISTS idx_cps_business_date
ON insurance_tgt.customer_policy_summary(business_date);

CREATE INDEX IF NOT EXISTS idx_cps_customer_status
ON insurance_tgt.customer_policy_summary(customer_status);

CREATE INDEX IF NOT EXISTS idx_cps_source_updated
ON insurance_tgt.customer_policy_summary(source_last_updated_at);

CREATE INDEX IF NOT EXISTS idx_cps_run_id
ON insurance_tgt.customer_policy_summary(etl_run_id);
