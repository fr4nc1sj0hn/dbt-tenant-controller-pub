-- reload_and_validate_raw.sql
--
-- Loads the generated seed CSVs into the raw schema using psql \copy.
--
-- Example (matches requested format):
--   \copy raw_customer_000.raw_dim1 FROM '/data/raw_data/raw_dim1.csv' CSV HEADER;
--
-- Usage examples:
--   # default raw_schema=raw_sample
--   psql -h <host> -p <port> -U <user> -d <db> -f reload_and_validate_raw.sql

-- Assumes the CSVs are available inside Postgres at:
--   /data/raw_data/raw_dim1.csv .. raw_dim9.csv, raw_transactions.csv
--

\set ON_ERROR_STOP on


-- Repeatable reloads
TRUNCATE TABLE
  raw_customer_000.raw_dim1,
  raw_customer_000.raw_dim2,
  raw_customer_000.raw_dim3,
  raw_customer_000.raw_dim4,
  raw_customer_000.raw_dim5,
  raw_customer_000.raw_dim6,
  raw_customer_000.raw_dim7,
  raw_customer_000.raw_dim8,
  raw_customer_000.raw_dim9,
  raw_customer_000.raw_transactions;

-- Load dimensions
\copy raw_customer_000.raw_dim1 FROM '/data/raw_data/raw_dim1.csv' CSV HEADER;
\copy raw_customer_000.raw_dim2 FROM '/data/raw_data/raw_dim2.csv' CSV HEADER;
\copy raw_customer_000.raw_dim3 FROM '/data/raw_data/raw_dim3.csv' CSV HEADER;
\copy raw_customer_000.raw_dim4 FROM '/data/raw_data/raw_dim4.csv' CSV HEADER;
\copy raw_customer_000.raw_dim5 FROM '/data/raw_data/raw_dim5.csv' CSV HEADER;
\copy raw_customer_000.raw_dim6 FROM '/data/raw_data/raw_dim6.csv' CSV HEADER;
\copy raw_customer_000.raw_dim7 FROM '/data/raw_data/raw_dim7.csv' CSV HEADER;
\copy raw_customer_000.raw_dim8 FROM '/data/raw_data/raw_dim8.csv' CSV HEADER;
\copy raw_customer_000.raw_dim9 FROM '/data/raw_data/raw_dim9.csv' CSV HEADER;

-- Load fact table
\copy raw_customer_000.raw_transactions FROM '/data/raw_data/raw_transactions.csv' CSV HEADER;

-- Quick sanity row counts
\echo '\nRow counts in schema :' raw_customer_000
SELECT 'raw_dim1' AS table_name, COUNT(*) AS row_count FROM raw_customer_000.raw_dim1
UNION ALL SELECT 'raw_dim2', COUNT(*) FROM raw_customer_000.raw_dim2
UNION ALL SELECT 'raw_dim3', COUNT(*) FROM raw_customer_000.raw_dim3
UNION ALL SELECT 'raw_dim4', COUNT(*) FROM raw_customer_000.raw_dim4
UNION ALL SELECT 'raw_dim5', COUNT(*) FROM raw_customer_000.raw_dim5
UNION ALL SELECT 'raw_dim6', COUNT(*) FROM raw_customer_000.raw_dim6
UNION ALL SELECT 'raw_dim7', COUNT(*) FROM raw_customer_000.raw_dim7
UNION ALL SELECT 'raw_dim8', COUNT(*) FROM raw_customer_000.raw_dim8
UNION ALL SELECT 'raw_dim9', COUNT(*) FROM raw_customer_000.raw_dim9
UNION ALL SELECT 'raw_transactions', COUNT(*) FROM raw_customer_000.raw_transactions
ORDER BY table_name;
