-- Read-only deployed PostgreSQL schema evidence (no DDL/DML).
SELECT current_database() AS database_name, current_setting('TimeZone') AS session_timezone;

SELECT ordinal_position, column_name, data_type, udt_name,
       character_maximum_length, numeric_precision, numeric_scale,
       is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'insurance_tgt' AND table_name = 'customer_policy_summary'
ORDER BY ordinal_position;

SELECT conname AS constraint_name, contype AS constraint_type,
       convalidated AS validated, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'insurance_tgt.customer_policy_summary'::regclass
ORDER BY conname;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'insurance_tgt' AND tablename = 'customer_policy_summary'
ORDER BY indexname;

SELECT conname, convalidated, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'insurance_src.policy_master'::regclass AND contype = 'f'
ORDER BY conname;

SELECT table_schema, table_name, column_name, data_type, udt_name
FROM information_schema.columns
WHERE (table_schema, table_name) IN
      (('insurance_src','customer_master'), ('insurance_src','policy_master'))
  AND column_name = 'updated_at'
ORDER BY table_name;

SELECT COUNT(*) AS target_column_count
FROM information_schema.columns
WHERE table_schema='insurance_tgt' AND table_name='customer_policy_summary';
