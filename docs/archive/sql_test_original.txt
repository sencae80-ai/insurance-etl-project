
-- ============================================================
-- FILE: sql/customer_policy_transformation.sql
-- DATABASE: etl_quality_test_project
-- MODE: FULL RECONCILIATION + UPSERT
-- GRAIN: ONE ROW PER CUSTOMER
-- ============================================================

BEGIN;

-- ============================================================
-- 1. ETL EXECUTION PARAMETERS
-- ============================================================

CREATE TEMP TABLE etl_run_parameters
ON COMMIT DROP
AS
SELECT
    DATE '2026-10-02' AS business_date,
    gen_random_uuid() AS etl_run_id;

-- Change business_date for each execution.
-- PostgreSQL gen_random_uuid() requires a supported version
-- or the appropriate extension.

-- ============================================================
-- 2. STANDARDIZE CUSTOMER DATA
-- ============================================================

CREATE TEMP TABLE etl_customer_stage
ON COMMIT DROP
AS
SELECT
    c.customer_id,

    NULLIF(
        INITCAP(BTRIM(c.customer_name)),
        ''
    )::VARCHAR(200) AS customer_name,

    NULLIF(
        UPPER(BTRIM(c.gender)),
        ''
    )::VARCHAR(20) AS gender,

    CASE
        WHEN c.date_of_birth IS NULL
          OR c.date_of_birth > p.business_date
        THEN NULL
        ELSE EXTRACT(
            YEAR FROM AGE(
                p.business_date,
                c.date_of_birth
            )
        )::INTEGER
    END AS age,

    NULLIF(
        LOWER(BTRIM(c.email)),
        ''
    )::VARCHAR(255) AS email,

    NULLIF(
        BTRIM(c.mobile_number),
        ''
    )::VARCHAR(20) AS mobile_number,

    NULLIF(
        INITCAP(BTRIM(c.city)),
        ''
    )::VARCHAR(100) AS city,

    NULLIF(
        INITCAP(BTRIM(c.state)),
        ''
    )::VARCHAR(100) AS state,

    COALESCE(
        NULLIF(
            UPPER(BTRIM(c.customer_status)),
            ''
        ),
        'UNKNOWN'
    )::VARCHAR(20) AS customer_status,

    c.updated_at AS customer_updated_at

FROM insurance_src.customer_master c
CROSS JOIN etl_run_parameters p;

CREATE UNIQUE INDEX
ON etl_customer_stage(customer_id);


-- ============================================================
-- 3. AGGREGATE POLICY DATA
-- ============================================================

CREATE TEMP TABLE etl_policy_aggregate
ON COMMIT DROP
AS
SELECT
    p.customer_id,

    COUNT(*)::INTEGER AS total_policies,

    COUNT(*) FILTER (
        WHERE UPPER(BTRIM(p.policy_status)) = 'ACTIVE'
    )::INTEGER AS active_policies,

    COUNT(*) FILTER (
        WHERE UPPER(BTRIM(p.policy_status)) = 'EXPIRED'
    )::INTEGER AS expired_policies,

    COUNT(*) FILTER (
        WHERE UPPER(BTRIM(p.policy_status)) = 'CANCELLED'
    )::INTEGER AS cancelled_policies,

    COUNT(*) FILTER (
        WHERE UPPER(BTRIM(p.policy_status)) = 'LAPSED'
    )::INTEGER AS lapsed_policies,

    COALESCE(
        SUM(p.premium_amount),
        0
    )::NUMERIC(18,2) AS total_premium,

    COALESCE(
        AVG(p.premium_amount),
        0
    )::NUMERIC(18,2) AS average_premium,

    COALESCE(
        SUM(p.sum_assured),
        0
    )::NUMERIC(20,2) AS total_sum_assured,

    MIN(p.policy_start_date)
        AS first_policy_start_date,

    MAX(p.policy_end_date)
        AS latest_policy_end_date,

    MAX(p.updated_at)
        AS policy_updated_at

FROM insurance_src.policy_master p
GROUP BY p.customer_id;

CREATE UNIQUE INDEX
ON etl_policy_aggregate(customer_id);


-- ============================================================
-- 4. CREATE CONSOLIDATED STAGING TABLE
-- ============================================================

CREATE TEMP TABLE etl_customer_policy_final
ON COMMIT DROP
AS
SELECT
    c.customer_id,
    c.customer_name,
    c.gender,
    c.age,
    c.email,
    c.mobile_number,
    c.city,
    c.state,
    c.customer_status,

    COALESCE(p.total_policies, 0)
        AS total_policies,

    COALESCE(p.active_policies, 0)
        AS active_policies,

    COALESCE(p.expired_policies, 0)
        AS expired_policies,

    COALESCE(p.cancelled_policies, 0)
        AS cancelled_policies,

    COALESCE(p.lapsed_policies, 0)
        AS lapsed_policies,

    COALESCE(p.total_premium, 0)::NUMERIC(18,2)
        AS total_premium,

    COALESCE(p.average_premium, 0)::NUMERIC(18,2)
        AS average_premium,

    COALESCE(p.total_sum_assured, 0)::NUMERIC(20,2)
        AS total_sum_assured,

    p.first_policy_start_date,
    p.latest_policy_end_date,

    CASE
        WHEN COALESCE(p.total_policies, 0) = 0
            THEN 'NO_POLICY'
        WHEN p.total_policies >= 5
            THEN 'PLATINUM'
        WHEN p.total_policies >= 3
            THEN 'GOLD'
        ELSE 'STANDARD'
    END::VARCHAR(30) AS customer_category,

    CASE
        WHEN COALESCE(p.total_policies, 0) = 0
            THEN 'NO_POLICY'
        WHEN COALESCE(p.active_policies, 0) > 0
            THEN 'ACTIVE_HOLDER'
        ELSE 'INACTIVE_HOLDER'
    END::VARCHAR(30) AS policy_holding_status,

    CASE
        WHEN c.customer_updated_at IS NULL
            THEN p.policy_updated_at
        WHEN p.policy_updated_at IS NULL
            THEN c.customer_updated_at
        ELSE GREATEST(
            c.customer_updated_at,
            p.policy_updated_at
        )
    END AS source_last_updated_at,

    r.business_date,
    r.etl_run_id

FROM etl_customer_stage c

LEFT JOIN etl_policy_aggregate p
    ON c.customer_id = p.customer_id

CROSS JOIN etl_run_parameters r;

CREATE UNIQUE INDEX
ON etl_customer_policy_final(customer_id);


-- ============================================================
-- 5. SOURCE VALIDATION
-- ============================================================

DO $$
BEGIN

    IF EXISTS (
        SELECT 1
        FROM insurance_src.policy_master
        WHERE UPPER(BTRIM(policy_status)) NOT IN (
            'ACTIVE',
            'EXPIRED',
            'CANCELLED',
            'LAPSED'
        )
        OR policy_status IS NULL
    ) THEN
        RAISE EXCEPTION
            'Invalid policy status detected';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM etl_customer_policy_final
        WHERE total_policies <>
            active_policies
            + expired_policies
            + cancelled_policies
            + lapsed_policies
    ) THEN
        RAISE EXCEPTION
            'Policy status reconciliation failed';
    END IF;

END $$;


-- ============================================================
-- 6. INSERT / UPDATE TARGET
-- ============================================================

INSERT INTO insurance_tgt.customer_policy_summary
(
    customer_id,
    customer_name,
    gender,
    age,
    email,
    mobile_number,
    city,
    state,
    customer_status,

    total_policies,
    active_policies,
    expired_policies,
    cancelled_policies,
    lapsed_policies,

    total_premium,
    average_premium,
    total_sum_assured,

    first_policy_start_date,
    latest_policy_end_date,

    customer_category,
    policy_holding_status,

    source_last_updated_at,
    business_date,

    etl_run_id
)
SELECT
    customer_id,
    customer_name,
    gender,
    age,
    email,
    mobile_number,
    city,
    state,
    customer_status,

    total_policies,
    active_policies,
    expired_policies,
    cancelled_policies,
    lapsed_policies,

    total_premium,
    average_premium,
    total_sum_assured,

    first_policy_start_date,
    latest_policy_end_date,

    customer_category,
    policy_holding_status,

    source_last_updated_at,
    business_date,

    etl_run_id

FROM etl_customer_policy_final

ON CONFLICT (customer_id)
DO UPDATE SET

    customer_name = EXCLUDED.customer_name,
    gender = EXCLUDED.gender,
    age = EXCLUDED.age,
    email = EXCLUDED.email,
    mobile_number = EXCLUDED.mobile_number,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    customer_status = EXCLUDED.customer_status,

    total_policies = EXCLUDED.total_policies,
    active_policies = EXCLUDED.active_policies,
    expired_policies = EXCLUDED.expired_policies,
    cancelled_policies = EXCLUDED.cancelled_policies,
    lapsed_policies = EXCLUDED.lapsed_policies,

    total_premium = EXCLUDED.total_premium,
    average_premium = EXCLUDED.average_premium,
    total_sum_assured = EXCLUDED.total_sum_assured,

    first_policy_start_date =
        EXCLUDED.first_policy_start_date,

    latest_policy_end_date =
        EXCLUDED.latest_policy_end_date,

    customer_category = EXCLUDED.customer_category,

    policy_holding_status =
        EXCLUDED.policy_holding_status,

    source_last_updated_at =
        EXCLUDED.source_last_updated_at,

    business_date = EXCLUDED.business_date,

    etl_run_id = EXCLUDED.etl_run_id,

    etl_updated_at = CURRENT_TIMESTAMP

WHERE (
    insurance_tgt.customer_policy_summary.customer_name,
    insurance_tgt.customer_policy_summary.gender,
    insurance_tgt.customer_policy_summary.age,
    insurance_tgt.customer_policy_summary.email,
    insurance_tgt.customer_policy_summary.mobile_number,
    insurance_tgt.customer_policy_summary.city,
    insurance_tgt.customer_policy_summary.state,
    insurance_tgt.customer_policy_summary.customer_status,

    insurance_tgt.customer_policy_summary.total_policies,
    insurance_tgt.customer_policy_summary.active_policies,
    insurance_tgt.customer_policy_summary.expired_policies,
    insurance_tgt.customer_policy_summary.cancelled_policies,
    insurance_tgt.customer_policy_summary.lapsed_policies,

    insurance_tgt.customer_policy_summary.total_premium,
    insurance_tgt.customer_policy_summary.average_premium,
    insurance_tgt.customer_policy_summary.total_sum_assured,

    insurance_tgt.customer_policy_summary.first_policy_start_date,
    insurance_tgt.customer_policy_summary.latest_policy_end_date,

    insurance_tgt.customer_policy_summary.customer_category,
    insurance_tgt.customer_policy_summary.policy_holding_status,

    insurance_tgt.customer_policy_summary.source_last_updated_at,
    insurance_tgt.customer_policy_summary.business_date
)
IS DISTINCT FROM
(
    EXCLUDED.customer_name,
    EXCLUDED.gender,
    EXCLUDED.age,
    EXCLUDED.email,
    EXCLUDED.mobile_number,
    EXCLUDED.city,
    EXCLUDED.state,
    EXCLUDED.customer_status,

    EXCLUDED.total_policies,
    EXCLUDED.active_policies,
    EXCLUDED.expired_policies,
    EXCLUDED.cancelled_policies,
    EXCLUDED.lapsed_policies,

    EXCLUDED.total_premium,
    EXCLUDED.average_premium,
    EXCLUDED.total_sum_assured,

    EXCLUDED.first_policy_start_date,
    EXCLUDED.latest_policy_end_date,

    EXCLUDED.customer_category,
    EXCLUDED.policy_holding_status,

    EXCLUDED.source_last_updated_at,
    EXCLUDED.business_date
);


-- ============================================================
-- 7. REMOVE CUSTOMERS DELETED FROM SOURCE
-- ============================================================

DELETE FROM insurance_tgt.customer_policy_summary t
WHERE NOT EXISTS (
    SELECT 1
    FROM insurance_src.customer_master c
    WHERE c.customer_id = t.customer_id
);


-- ============================================================
-- 8. COMMIT TRANSACTION
-- ============================================================

COMMIT;
