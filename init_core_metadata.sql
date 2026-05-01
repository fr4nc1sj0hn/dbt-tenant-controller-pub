-- init_core_metadata.sql
--
-- Purpose
--   Bootstrap the metadata contract tables used by this repo:
--     - core.metadata
--     - core.metadata_columns
--   …and create/grant a runtime role: dbt_user
--
-- How to run (examples)
--   # from your host machine
--   psql -h localhost -p 5432 -U postgres -d analytics -f init_core_metadata.sql
--
-- Notes
--   - This script is designed for a *fresh* database/container.
--   - If you already have old versions of these tables with different columns,
--     you may need to DROP them first.
--   - Creating roles requires a privileged user (often `postgres`).

BEGIN;

-- 1) Schema
CREATE SCHEMA IF NOT EXISTS core;

-- 2) Tables
-- core.metadata is the model-level contract expected by app/project_generator.py
-- Required columns (generator validates):
--   sourcetablename, stagingtablename, modelname
-- Optional columns:
--   description, active, table_type
CREATE TABLE IF NOT EXISTS core.metadata (
    modelname        TEXT PRIMARY KEY,
    sourcetablename  TEXT NOT NULL,
    stagingtablename TEXT NOT NULL,
    table_type       TEXT NOT NULL DEFAULT 'dimensions', -- allowed: dimensions|fact
    description      TEXT,
    active           BOOLEAN NOT NULL DEFAULT TRUE
);

-- core.metadata_columns is the column-level contract expected by app/project_generator.py
-- Required columns (generator validates):
--   sourcetablename, column_name
-- Optional columns (generator will default if missing, but we include them):
--   table_type, column_type, description, is_primary_key, is_unique, is_nullable
CREATE TABLE IF NOT EXISTS core.metadata_columns (
    sourcetablename  TEXT NOT NULL,
    column_name      TEXT NOT NULL,
    table_type       TEXT NOT NULL DEFAULT 'raw',
    column_type      TEXT,
    description      TEXT,
    is_primary_key   BOOLEAN NOT NULL DEFAULT FALSE,
    is_unique        BOOLEAN NOT NULL DEFAULT FALSE,
    is_nullable      BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT metadata_columns_pk PRIMARY KEY (sourcetablename, column_name, table_type)
);

-- 3) Role/user + privileges
-- Create dbt_user if it doesn't exist.
-- IMPORTANT: This does NOT set a password.
-- If you need password auth, run (example):
--   ALTER ROLE dbt_user WITH PASSWORD 'your_strong_password';
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt_user') THEN
        CREATE ROLE dbt_user
		WITH LOGIN 
        PASSWORD '<password>';
    END IF;
END
$$;

-- Allow connecting to the analytics database
GRANT CONNECT ON DATABASE analytics TO dbt_user;

-- Grant privileges on schema core
GRANT USAGE, CREATE ON SCHEMA core TO dbt_user;

-- Existing objects
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA core TO dbt_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA core TO dbt_user;

-- Future objects (created by the role executing this script)
ALTER DEFAULT PRIVILEGES IN SCHEMA core
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dbt_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA core
GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO dbt_user;

COMMIT;
