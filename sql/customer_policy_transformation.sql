-- ============================================================
-- FILE: sql/customer_policy_transformation.sql
-- MODE: FULL RECONCILIATION + UPSERT (one row per customer)
-- Run only through source_code/customer_policy_etl.py for
-- source-completeness and deletion safety gates.
-- The runner replaces exactly one DATE literal and removes
-- the outer BEGIN/COMMIT, then owns commit/rollback.
-- ============================================================

BEGIN;

-- 1. Run parameters (date declaration required by current runner)
CREATE TEMP TABLE etl_run_parameters ON COMMIT DROP AS
SELECT
    DATE '2026-10-02' AS business_date,
    gen_random_uuid() AS etl_run_id;

-- 2. Validate source before constructing target aggregates.
-- Fail rather than silently ignoring orphan policies or bad statuses.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1
        FROM insurance_src.policy_master p
        WHERE p.policy_status IS NULL
           OR UPPER(BTRIM(p.policy_status)) NOT IN
               ('ACTIVE', 'EXPIRED', 'CANCELLED', 'LAPSED')
    ) THEN
        RAISE EXCEPTION 'Invalid or NULL policy status detected';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM insurance_src.policy_master p
        WHERE NOT EXISTS (
            SELECT 1 FROM insurance_src.customer_master c
            WHERE c.customer_id = p.customer_id
        )
    ) THEN
        RAISE EXCEPTION 'Orphan policies reference missing customers';
    END IF;
END $$;

-- 3. Normalize source customers. Unique index catches duplicate IDs.
CREATE TEMP TABLE etl_customer_stage ON COMMIT DROP AS
SELECT
    c.customer_id,
    NULLIF(INITCAP(BTRIM(c.customer_name)), '')::VARCHAR(200) AS customer_name,
    NULLIF(UPPER(BTRIM(c.gender)), '')::VARCHAR(20) AS gender,
    CASE
        WHEN c.date_of_birth IS NULL OR c.date_of_birth > r.business_date
        THEN NULL
        ELSE EXTRACT(YEAR FROM AGE(r.business_date, c.date_of_birth))::INTEGER
    END AS age,
    NULLIF(LOWER(BTRIM(c.email)), '')::VARCHAR(255) AS email,
    NULLIF(BTRIM(c.mobile_number), '')::VARCHAR(20) AS mobile_number,
    NULLIF(INITCAP(BTRIM(c.city)), '')::VARCHAR(100) AS city,
    NULLIF(INITCAP(BTRIM(c.state)), '')::VARCHAR(100) AS state,
    COALESCE(NULLIF(UPPER(BTRIM(c.customer_status)), ''), 'UNKNOWN')::VARCHAR(20)
        AS customer_status,
    c.updated_at AS customer_updated_at
FROM insurance_src.customer_master c
CROSS JOIN etl_run_parameters r;

CREATE UNIQUE INDEX ON etl_customer_stage(customer_id);

-- 4. Aggregate with unrestricted NUMERIC precision first.
-- Casting to NUMERIC(18,2)/(20,2) before validation would
-- cause an overflow before a helpful validation message.
CREATE TEMP TABLE etl_policy_aggregate_raw ON COMMIT DROP AS
SELECT
    p.customer_id,
    COUNT(*) AS total_policies,
    COUNT(*) FILTER (WHERE UPPER(BTRIM(p.policy_status)) = 'ACTIVE')
        AS active_policies,
    COUNT(*) FILTER (WHERE UPPER(BTRIM(p.policy_status)) = 'EXPIRED')
        AS expired_policies,
    COUNT(*) FILTER (WHERE UPPER(BTRIM(p.policy_status)) = 'CANCELLED')
        AS cancelled_policies,
    COUNT(*) FILTER (WHERE UPPER(BTRIM(p.policy_status)) = 'LAPSED')
        AS lapsed_policies,
    COALESCE(SUM(p.premium_amount), 0) AS total_premium,
    COALESCE(AVG(p.premium_amount), 0) AS average_premium,
    COALESCE(SUM(p.sum_assured), 0) AS total_sum_assured,
    MIN(p.policy_start_date) AS first_policy_start_date,
    MAX(p.policy_end_date) AS latest_policy_end_date,
    MAX(p.updated_at) AS policy_updated_at
FROM insurance_src.policy_master p
GROUP BY p.customer_id;

CREATE UNIQUE INDEX ON etl_policy_aggregate_raw(customer_id);

-- 5. Validate count and monetary precision before narrowing casts.
-- PostgreSQL NUMERIC(p,2) rounds to 2 decimal places on assignment.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM etl_policy_aggregate_raw
        WHERE total_policies > 2147483647
           OR active_policies > 2147483647
           OR expired_policies > 2147483647
           OR cancelled_policies > 2147483647
           OR lapsed_policies > 2147483647
    ) THEN
        RAISE EXCEPTION 'Policy count exceeds target INTEGER precision';
    END IF;

    IF EXISTS (
        SELECT 1 FROM etl_policy_aggregate_raw
        WHERE ABS(ROUND(total_premium, 2)) >= 10000000000000000::NUMERIC
           OR ABS(ROUND(average_premium, 2)) >= 10000000000000000::NUMERIC
           OR ABS(ROUND(total_sum_assured, 2)) >= 1000000000000000000::NUMERIC
    ) THEN
        RAISE EXCEPTION 'Policy financial aggregate exceeds target NUMERIC precision';
    END IF;

    IF EXISTS (
        SELECT 1 FROM etl_policy_aggregate_raw
        WHERE total_policies <> active_policies + expired_policies
                                + cancelled_policies + lapsed_policies
    ) THEN
        RAISE EXCEPTION 'Policy status reconciliation failed';
    END IF;
END $$;

-- 6. One consolidated target row per source customer.
-- Audit timestamps are deliberately excluded: target defaults
-- populate them on INSERT; etl_updated_at changes only on UPDATE.
CREATE TEMP TABLE etl_customer_policy_final ON COMMIT DROP AS
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
    COALESCE(p.total_policies, 0)::INTEGER AS total_policies,
    COALESCE(p.active_policies, 0)::INTEGER AS active_policies,
    COALESCE(p.expired_policies, 0)::INTEGER AS expired_policies,
    COALESCE(p.cancelled_policies, 0)::INTEGER AS cancelled_policies,
    COALESCE(p.lapsed_policies, 0)::INTEGER AS lapsed_policies,
    COALESCE(p.total_premium, 0)::NUMERIC(18,2) AS total_premium,
    COALESCE(p.average_premium, 0)::NUMERIC(18,2) AS average_premium,
    COALESCE(p.total_sum_assured, 0)::NUMERIC(20,2) AS total_sum_assured,
    p.first_policy_start_date,
    p.latest_policy_end_date,
    (CASE
        WHEN COALESCE(p.total_policies, 0) = 0 THEN 'NO_POLICY'
        WHEN p.total_policies >= 5 THEN 'PLATINUM'
        WHEN p.total_policies >= 3 THEN 'GOLD'
        ELSE 'STANDARD'
    END)::VARCHAR(30) AS customer_category,
    (CASE
        WHEN COALESCE(p.total_policies, 0) = 0 THEN 'NO_POLICY'
        WHEN COALESCE(p.active_policies, 0) > 0 THEN 'ACTIVE_HOLDER'
        ELSE 'INACTIVE_HOLDER'
    END)::VARCHAR(30) AS policy_holding_status,
    CASE
        WHEN c.customer_updated_at IS NULL THEN p.policy_updated_at
        WHEN p.policy_updated_at IS NULL THEN c.customer_updated_at
        ELSE GREATEST(c.customer_updated_at, p.policy_updated_at)
    END AS source_last_updated_at,
    r.business_date,
    r.etl_run_id
FROM etl_customer_stage c
LEFT JOIN etl_policy_aggregate_raw p ON c.customer_id = p.customer_id
CROSS JOIN etl_run_parameters r;

CREATE UNIQUE INDEX ON etl_customer_policy_final(customer_id);

-- 7. UPSERT: explicit columns are independent of physical table order.
INSERT INTO insurance_tgt.customer_policy_summary AS t (
    customer_id, customer_name, gender, age, email, mobile_number,
    city, state, customer_status, total_policies, active_policies,
    expired_policies, cancelled_policies, lapsed_policies,
    total_premium, average_premium, total_sum_assured,
    first_policy_start_date, latest_policy_end_date,
    customer_category, policy_holding_status,
    source_last_updated_at, business_date, etl_run_id
)
SELECT
    customer_id, customer_name, gender, age, email, mobile_number,
    city, state, customer_status, total_policies, active_policies,
    expired_policies, cancelled_policies, lapsed_policies,
    total_premium, average_premium, total_sum_assured,
    first_policy_start_date, latest_policy_end_date,
    customer_category, policy_holding_status,
    source_last_updated_at, business_date, etl_run_id
FROM etl_customer_policy_final
ON CONFLICT (customer_id) DO UPDATE SET
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
    first_policy_start_date = EXCLUDED.first_policy_start_date,
    latest_policy_end_date = EXCLUDED.latest_policy_end_date,
    customer_category = EXCLUDED.customer_category,
    policy_holding_status = EXCLUDED.policy_holding_status,
    source_last_updated_at = EXCLUDED.source_last_updated_at,
    business_date = EXCLUDED.business_date,
    etl_run_id = EXCLUDED.etl_run_id,
    etl_updated_at = CURRENT_TIMESTAMP
WHERE ROW(
    t.customer_name, t.gender, t.age, t.email, t.mobile_number,
    t.city, t.state, t.customer_status, t.total_policies,
    t.active_policies, t.expired_policies, t.cancelled_policies,
    t.lapsed_policies, t.total_premium, t.average_premium,
    t.total_sum_assured, t.first_policy_start_date,
    t.latest_policy_end_date, t.customer_category,
    t.policy_holding_status, t.source_last_updated_at, t.business_date
) IS DISTINCT FROM ROW(
    EXCLUDED.customer_name, EXCLUDED.gender, EXCLUDED.age,
    EXCLUDED.email, EXCLUDED.mobile_number, EXCLUDED.city,
    EXCLUDED.state, EXCLUDED.customer_status, EXCLUDED.total_policies,
    EXCLUDED.active_policies, EXCLUDED.expired_policies,
    EXCLUDED.cancelled_policies, EXCLUDED.lapsed_policies,
    EXCLUDED.total_premium, EXCLUDED.average_premium,
    EXCLUDED.total_sum_assured, EXCLUDED.first_policy_start_date,
    EXCLUDED.latest_policy_end_date, EXCLUDED.customer_category,
    EXCLUDED.policy_holding_status, EXCLUDED.source_last_updated_at,
    EXCLUDED.business_date
);

-- 8. Full reconciliation deletion.
-- The Python runner must approve the potential deletion count
-- and fraction BEFORE this script executes.
DELETE FROM insurance_tgt.customer_policy_summary AS t
WHERE NOT EXISTS (
    SELECT 1 FROM insurance_src.customer_master AS c
    WHERE c.customer_id = t.customer_id
);

COMMIT;
