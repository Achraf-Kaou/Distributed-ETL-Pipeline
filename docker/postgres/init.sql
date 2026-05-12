-- =============================================================================
-- docker/postgres/init.sql — PostgreSQL Source Database Initialisation
-- =============================================================================
--
-- Executed automatically by the postgres container on first startup via the
-- Docker Entrypoint Init mechanism:
--   /docker-entrypoint-initdb.d/init.sql
--
-- This script runs ONCE when the `postgres_data` volume is empty. Subsequent
-- container restarts skip this script because the data directory already exists.
-- To re-initialise: `docker compose down -v && docker compose up -d`
--
-- DATABASE: etl_db (created by POSTGRES_DB env var before this script runs)
-- USER:     etl_user
--
-- PURPOSE:
--   Provides a realistic relational source for the ETL extract stage.
--   The employees table is one of three sources that are unified, deduplicated,
--   and transformed by the pipeline. The departments table is loaded separately
--   as a join dimension (not included in the main extract, only in the join step).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- TABLE: employees
-- ---------------------------------------------------------------------------
-- Primary employee source dataset for this pipeline.
--
-- Column notes:
--   id         SERIAL — auto-incrementing surrogate key used for partitioned reads.
--   email      UNIQUE — business key used for cross-source deduplication.
--   created_at — used as an example of a source-system audit column; not mapped
--                to the pipeline schema but available for advanced use cases.
-- ---------------------------------------------------------------------------
CREATE TABLE employees (
    id         SERIAL PRIMARY KEY,
    full_name  VARCHAR(100),
    age        INT,
    email      VARCHAR(100) UNIQUE,   -- Business key: used for dedup across sources
    department VARCHAR(50),
    salary     NUMERIC(12, 2),
    join_date  DATE,
    status     VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- TABLE: departments
-- ---------------------------------------------------------------------------
-- Reference/dimension table used in the join step (employees-with-departments).
-- NOT included in the main flat-file or DB extract — it is read directly by
-- the join configuration as the right-side DataFrame.
--
-- dept_id is the join key that maps to the employees.department column.
-- ---------------------------------------------------------------------------
CREATE TABLE departments (
    dept_id  VARCHAR(10) PRIMARY KEY,   -- Join key: matches employees.department values
    dept_name VARCHAR(100),
    manager   VARCHAR(100),
    location  VARCHAR(100),
    budget    NUMERIC(15, 2)
);

-- ---------------------------------------------------------------------------
-- SAMPLE DATA
-- ---------------------------------------------------------------------------
-- Minimal seed data to demonstrate the pipeline end-to-end.
-- Alice and Bob are also present in the MySQL source to demonstrate cross-source
-- deduplication: the postgres version wins (priority = 1 in dedup config).
-- ---------------------------------------------------------------------------
INSERT INTO departments VALUES
    ('IT', 'Information Technology', 'Alice Dupont', 'Paris',  1200000),
    ('HR', 'Human Resources',        'Bob Martin',   'Lyon',   450000);

INSERT INTO employees (full_name, age, email, department, salary, join_date, status)
VALUES
    ('Alice Dupont', 28, 'alice.dupont@company.fr', 'IT', 45000, '2023-05-15', 'Active'),
    ('Bob Martin',   34, 'bob.martin@company.fr',   'HR', 52000, '2022-11-20', 'Active');