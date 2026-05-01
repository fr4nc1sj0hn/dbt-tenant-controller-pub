-- seed_raw_schema.sql

\set ON_ERROR_STOP on

-- Defaults (if not passed via -v)
\if :{?raw_schema}
\else
  \set raw_schema raw_sample
\endif

\if :{?dbt_role}
\else
  \set dbt_role dbt_user
\endif

BEGIN;

-- 1) Create schema
CREATE SCHEMA IF NOT EXISTS :"raw_schema";

-- 2) Grants (schema-level)
GRANT USAGE ON SCHEMA :"raw_schema" TO :"dbt_role";
GRANT CREATE ON SCHEMA :"raw_schema" TO :"dbt_role";

-- 3) Default privileges (future objects)
ALTER DEFAULT PRIVILEGES IN SCHEMA :"raw_schema"
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"dbt_role";

ALTER DEFAULT PRIVILEGES IN SCHEMA :"raw_schema"
GRANT USAGE, SELECT ON SEQUENCES TO :"dbt_role";


drop table if exists :"raw_schema".raw_transactions;
drop table if exists :"raw_schema".raw_dim1;
drop table if exists :"raw_schema".raw_dim2;
drop table if exists :"raw_schema".raw_dim3;
drop table if exists :"raw_schema".raw_dim4;
drop table if exists :"raw_schema".raw_dim5;
drop table if exists :"raw_schema".raw_dim6;
drop table if exists :"raw_schema".raw_dim7;
drop table if exists :"raw_schema".raw_dim8;
drop table if exists :"raw_schema".raw_dim9;


create table if not exists :"raw_schema".raw_dim1 (
    dim1_code          varchar(50) primary key,
    dim1_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim2 (
    dim2_code          varchar(50) primary key,
    dim2_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim3 (
    dim3_code          varchar(50) primary key,
    dim3_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim4 (
    dim4_code          varchar(50) primary key,
    dim4_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim5 (
    dim5_code          varchar(50) primary key,
    dim5_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim6 (
    dim6_code          varchar(50) primary key,
    dim6_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim7 (
    dim7_code          varchar(50) primary key,
    dim7_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim8 (
    dim8_code          varchar(50) primary key,
    dim8_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

create table if not exists :"raw_schema".raw_dim9 (
    dim9_code          varchar(50) primary key,
    dim9_label         varchar(100),
    sort_order             int,
    is_active              boolean,
    source_system          varchar(50),
    ingestion_timestamp    timestamp
);

CREATE TABLE IF NOT EXISTS :"raw_schema".raw_transactions (
    record_id                varchar(20),
    transaction_id           varchar(20),
    dim1_code                varchar(50),
    dim2_code                varchar(50),
    dim3_code                varchar(50),
    dim4_code                varchar(50),
    dim5_code                varchar(50),
    dim6_code                varchar(50),
    dim7_code                varchar(50),
    dim8_code                varchar(50),
    dim9_code                varchar(50),
    measure1                 decimal(10,2),
    measure2                 decimal(10,2),
    measure3                 decimal(10,2),
    measure4                 decimal(10,2),
    measure5                 decimal(10,2),
    transaction_timestamp    timestamp,
    source_system            varchar(50),
    ingestion_timestamp      timestamp,
    PRIMARY KEY (record_id, transaction_id)
);

-- 5) Explicit grants (for existing table)
GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE :"raw_schema".raw_transactions
TO :"dbt_role";

COMMIT;