-- =============================================================================
-- docker/mysql/init.sql — MySQL 8 Source Database Initialisation
-- =============================================================================
--
-- Executed automatically by the mysql container on first startup via:
--   /docker-entrypoint-initdb.d/init.sql
--
-- DATABASE: etl_db  (created by MYSQL_DATABASE env var before this script runs)
-- USER:     etl_user
--
-- PURPOSE:
--   Provides a secondary employee source dataset with a slightly different
--   department set (IT + Finance vs Postgres's IT + HR). When the pipeline
--   unions both sources and deduplicates, only unique emails survive with the
--   highest-priority (postgres=1) version winning on ties.
--
-- SCHEMA NOTES vs PostgreSQL:
--   - id uses INT AUTO_INCREMENT instead of SERIAL (MySQL equivalent)
--   - salary uses DECIMAL instead of NUMERIC (functionally equivalent)
--   - No `created_at` column — illustrates that sources can have different schemas;
--     unionByName fills missing columns with null automatically.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- TABLE: employees
-- ---------------------------------------------------------------------------
CREATE TABLE employees (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    full_name  VARCHAR(100),
    age        INT,
    email      VARCHAR(100) UNIQUE,   -- Business key for cross-source deduplication
    department VARCHAR(50),
    salary     DECIMAL(12, 2),
    join_date  DATE,
    status     VARCHAR(20)
    -- Note: no created_at — Spark's unionByName fills the missing column with null
);

-- ---------------------------------------------------------------------------
-- TABLE: departments
-- ---------------------------------------------------------------------------
CREATE TABLE departments (
    dept_id   VARCHAR(10) PRIMARY KEY,
    dept_name VARCHAR(100),
    manager   VARCHAR(100),
    location  VARCHAR(100),
    budget    DECIMAL(15, 2)
);

-- ---------------------------------------------------------------------------
-- SAMPLE DATA
-- ---------------------------------------------------------------------------
-- Intentionally uses a different department set (Finance instead of HR)
-- to demonstrate that the pipeline handles multi-source schema divergence.
-- No employee rows inserted here — the pipeline demonstrates dedup using
-- the flat-file and PostgreSQL sources; add rows here to test MySQL priority.
-- ---------------------------------------------------------------------------
INSERT INTO departments VALUES
    ('IT',      'Information Technology', 'Alice Dupont', 'Paris',     1200000),
    ('Finance', 'Finance Department',     'Paul Dubois',  'Marseille', 890000);