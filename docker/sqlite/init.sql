-- =============================================================================
-- docker/sqlite/init.sql — SQLite Source Database Initialisation
-- =============================================================================
--
-- Executed by the `sqlite-init` Docker container (alpine + sqlite3) on startup:
--   sqlite3 /sqlite/data/etl.db < /init.sql
--
-- The resulting file is written to docker/sqlite/data/etl.db (bind-mount).
-- The pipeline reads it directly via the SQLite JDBC driver:
--   jdbc:sqlite:docker/sqlite/data/etl.db
--
-- PURPOSE:
--   Provides a third employee source — a file-based database that requires
--   no server infrastructure. Useful for demonstrating that the pipeline
--   handles heterogeneous source types (network DBs + file-based DBs + files).
--
-- SQLITE TYPE SYSTEM NOTES:
--   SQLite uses dynamic typing with type affinities. Column types declared here
--   are advisory — SQLite stores values in the most efficient representation.
--   The JDBC driver maps SQLite affinities to Java types for Spark:
--     INTEGER → Long,  REAL → Double,  TEXT → String
--
-- DEDUPLICATION NOTE:
--   Sophie Bernard uses a unique email not present in other sources, so she
--   survives deduplication as a new record with priority=3 (sqlite source).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- TABLE: employees
-- ---------------------------------------------------------------------------
CREATE TABLE employees (
    id         INTEGER PRIMARY KEY,   -- INTEGER PRIMARY KEY = rowid alias in SQLite
    full_name  TEXT,
    age        INTEGER,
    email      TEXT UNIQUE,           -- Business key for cross-source deduplication
    department TEXT,
    salary     REAL,
    join_date  TEXT,                  -- SQLite has no DATE type; stored as ISO-8601 string
    status     TEXT
    -- Note: no created_at or SERIAL — SQLite uses rowid implicitly as PK
);

-- ---------------------------------------------------------------------------
-- TABLE: departments
-- ---------------------------------------------------------------------------
CREATE TABLE departments (
    dept_id   TEXT PRIMARY KEY,
    dept_name TEXT,
    manager   TEXT,
    location  TEXT,
    budget    REAL
);

-- ---------------------------------------------------------------------------
-- SAMPLE DATA
-- ---------------------------------------------------------------------------
-- Sophie Bernard is a unique employee not present in Postgres or MySQL sources.
-- She demonstrates that new records from lower-priority sources are still
-- correctly ingested when they do not conflict with higher-priority sources.
-- ---------------------------------------------------------------------------
INSERT INTO employees (full_name, age, email, department, salary, join_date, status)
VALUES
    ('Sophie Bernard', 29, 'sophie.b@company.fr', 'Marketing', 49000, '2024-02-28', 'Active');