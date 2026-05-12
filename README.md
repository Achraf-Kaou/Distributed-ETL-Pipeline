# Distributed ETL Pipeline

[![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?logo=scala&logoColor=white)](https://scala-lang.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![SBT](https://img.shields.io/badge/SBT-1.x-blue)](https://www.scala-sbt.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose)

A **fully configuration-driven, distributed ETL pipeline** built with **Scala 2.13** and **Apache Spark 3.5**. It extracts data from heterogeneous sources (flat files, JDBC databases, REST APIs), applies a multi-stage transformation chain, constructs a star-schema warehouse model, and loads results into Parquet, CSV, and a relational database — all without changing a single line of code.

Designed as an academic + portfolio-grade project that demonstrates real-world data engineering concepts at an intermediate level.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Star Schema & Warehouse Design](#star-schema--warehouse-design)
- [Configuration Reference](#configuration-reference)
- [Quick Start](#quick-start)
- [Pipeline Stages](#pipeline-stages)
- [Logging & Monitoring](#logging--monitoring)
- [Sample Data](#sample-data)
- [Enterprise Test Data Additions](#enterprise-test-data-additions)
- [Developer Experience Assets](#developer-experience-assets)
- [Sample Production Config](#sample-production-config)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)

---

## Project Overview

### What it is

A production-style ETL pipeline that:
- **Extracts** employee data from PostgreSQL, MySQL, SQLite, CSV/JSON flat files, and optional REST APIs
- **Transforms** it through cleaning, deduplication, enrichment joins, and aggregations
- **Loads** the results as Parquet/CSV files and upserts a star-schema model into a warehouse database

### Why it exists

To demonstrate, in a single runnable project, the core concepts every data engineer encounters on the job:

| Concept | Implemented As |
|---|---|
| Multi-source ingestion | CSV, JSON, Parquet, JDBC (3 engines), REST API |
| Schema normalisation | `toSnakeCase` column renaming + `column-mapping` config |
| Data quality enforcement | Null checks, type casting, null-ratio filtering |
| Business-key deduplication | Window `row_number()` with recency + source priority |
| Dimension enrichment | Left join against a department CSV |
| Aggregation | Config-driven `groupBy + agg` with named metrics |
| Star schema modelling | `dense_rank()` surrogate keys, dim + fact tables |
| Upsert loading | Staging-table pattern with `ON CONFLICT DO UPDATE` |
| Bad-row isolation | Quarantine Parquet files per stage/run |
| Observability | Structured JSON audit log + per-stage timing |
| Config-driven behaviour | All parameters in HOCON `application.conf` |

### Educational goals

- Understand how Apache Spark executes distributed data transformations
- Learn the Extract → Transform → Load pattern at code level
- Understand shuffle, partitioning, Window functions, and lazy evaluation
- See how a star schema is built programmatically from raw data
- Practice reading and writing JDBC sources from Spark

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline Architecture                        │
├──────────────────┬──────────────────────────┬────────────────────────────┤
│    EXTRACT        │       TRANSFORM           │         LOAD               │
│                  │                          │                            │
│  FlatFiles       │  1. Normalize columns    │  Parquet files             │
│  ├─ CSV / TSV    │  2. Clean (7 steps)      │  CSV files                 │
│  ├─ JSON         │  3. Quarantine bad rows  │                            │
│  ├─ Parquet      │  4. Deduplicate          │  Star Schema (JDBC)        │
│  ├─ Excel        │  5. Join dimensions      │  ├─ dim_department          │
│  └─ XML          │  6. Aggregate metrics    │  ├─ dim_employee            │
│                  │  7. Build star schema    │  ├─ dim_date                │
│  JDBC Databases  │                          │  └─ fact_employee_metrics   │
│  ├─ PostgreSQL   │  QualityChecks           │                            │
│  ├─ MySQL        │  ├─ Null detection       │  Upsert (ON CONFLICT)      │
│  └─ SQLite       │  └─ Duplicate detection  │  Postgres / MySQL / SQLite  │
│                  │                          │                            │
│  REST API        ├──────────────────────────┤  Audit Log (NDJSON)        │
│  └─ GET / POST   │   ORCHESTRATION           │  Quarantine (Parquet)      │
│                  │  PipelineOrchestrator    │                            │
│                  │  ├─ Stage lifecycle       │                            │
│                  │  ├─ fail-fast / skip      │                            │
│                  │  └─ Audit recording       │                            │
└──────────────────┴──────────────────────────┴────────────────────────────┘
```

### Distributed Processing

Spark runs in `local[*]` mode by default (all CPU cores, single JVM). Every transformation is expressed as a Spark DAG — no data is processed until an **Action** (`.count()`, `.write`) is called. Switching to a cluster requires only changing `spark.master` in config.

Key Spark concepts exercised:
- **Shuffle**: `groupBy`, `join`, `Window` functions all trigger data redistribution across partitions
- **Lazy evaluation**: transformations build a logical plan; no computation until an Action
- **Partitioned JDBC reads**: parallel database extraction with configurable split bounds
- **`unionByName`**: schema-safe union across heterogeneous sources

---

## Features

| Feature | Detail |
|---|---|
| **Config-first** | All behaviour driven by `application.conf` — no code changes needed |
| **Multi-source extraction** | CSV, JSON, Parquet, Excel, XML, TSV; PostgreSQL, MySQL, SQLite; REST GET/POST |
| **Partitioned JDBC reads** | Parallel DB extraction via `partitionColumn` range splitting |
| **7-step data cleaning** | snake_case normalisation → string normalisation → null dropping → null-ratio filter → fill defaults → type casting → dedup |
| **Deterministic deduplication** | `PARTITION BY key ORDER BY recency DESC, priority ASC` → keep rank=1 |
| **Flexible joins** | Same-name keys (`keyColumns`) or different-name keys (`keyMappings`); all join types |
| **Aggregations** | `sum`, `avg`, `max`, `min`, `count` with `group-by` and auto-suffixed output columns |
| **Star schema builder** | Auto-generates `dim_department`, `dim_employee`, `dim_date`, `fact_employee_metrics` with `dense_rank()` surrogate keys |
| **Upsert loader** | Staging-table pattern; `ON CONFLICT DO UPDATE` (Postgres/SQLite), `ON DUPLICATE KEY UPDATE` (MySQL) |
| **Quarantine zone** | Bad rows (nulls, missing keys) written to Parquet per stage/run for investigation |
| **Quality gates** | Critical-column null checks + duplicate key detection between stages |
| **Stage orchestration** | Enable/disable individual stages via config; fail-fast or continue on error |
| **Structured audit log** | JSON Lines file with per-stage metrics and run-level summary |
| **SQL injection prevention** | All JDBC identifiers validated and quoted before interpolation |

---

## Tech Stack

| Component | Version | Role |
|---|---|---|
| Scala | 2.13.14 | Pipeline implementation language |
| Apache Spark | 3.5.1 | Distributed data processing engine |
| spark-sql | 3.5.1 | DataFrame API, SQL functions, Window expressions |
| Typesafe Config | 1.4.3 | HOCON configuration parsing |
| com.lihaoyi:requests | 0.9.0 | Synchronous HTTP client for API extraction |
| spark-excel | 3.5.0_0.20.3 | Excel (.xlsx/.xls) file reading |
| spark-xml | 0.18.0 | XML file reading |
| PostgreSQL JDBC | 42.7.3 | PostgreSQL driver |
| MySQL Connector/J | 8.0.33 | MySQL 8 driver |
| SQLite JDBC | 3.45.3.0 | SQLite file-based DB driver |
| Log4j 2 | 2.23.1 | Structured logging (console + rolling JSON file) |
| munit | 1.0.0 | Unit testing framework |
| Docker Compose | v2 | PostgreSQL, MySQL, SQLite, pgAdmin services |
| SBT | 1.x | Build tool |

---

## Project Structure

```
Distributed-ETL-Pipeline/
├── application.conf              # Master pipeline configuration (HOCON)
├── build.sbt                     # SBT build: dependencies, JVM options, Scala version
│
├── data/
│   └── raw/                      # Input datasets (auto-scanned by extract stage)
│       ├── employees_source1.csv     # Primary CSV source (standard schema)
│       ├── employees_source2.csv     # CSV with non-standard column names (remapped via config)
│       ├── employees_complex.json    # Nested JSON (auto-flattened by pipeline)
│       ├── departments.csv           # Join dimension (excluded from main extract)
│       └── enterprise/               # Expanded test datasets (api/csv/json/parquet)
│
├── docker/
│   ├── docker-compose.yml            # 5 services: postgres, mysql, sqlite-init, load-db, pgadmin
│   ├── postgres/init.sql             # PostgreSQL schema + seed data
│   ├── mysql/init.sql                # MySQL schema + seed data
│   └── sqlite/
│       ├── init.sql                  # SQLite schema + seed data
│       └── data/etl.db               # SQLite file (created by docker compose up)
│
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── log4j2.xml            # Log4j 2: console pattern + rolling JSON file appender
│   │   └── scala/
│   │       ├── Main.scala            # Entry point + EtlPipeline orchestration class
│   │       ├── config/
│   │       │   └── PipelineConfig.scala  # HOCON → typed case classes; safe default accessors
│   │       ├── extract/
│   │       │   ├── ExtractFlatFiles.scala  # Multi-format file reader (CSV/JSON/Parquet/Excel/XML)
│   │       │   ├── ExtractDatabase.scala   # JDBC reader (Postgres/MySQL/SQLite); partitioned reads
│   │       │   └── ExtractApi.scala        # REST API reader (GET/POST); root-field flattening
│   │       ├── transform/
│   │       │   ├── TransformClean.scala      # 7-step cleaning chain
│   │       │   ├── TransformDeduplicate.scala # Window-based business-key deduplication
│   │       │   ├── Transformjoin.scala        # Parameterised join with key validation & prefixing
│   │       │   ├── TransformAggregate.scala   # Config-driven groupBy + agg
│   │       │   └── QualityChecks.scala        # Post-stage null/duplicate validation
│   │       ├── load/
│   │       │   ├── StarSchemaBuilder.scala    # Builds dim + fact DataFrames with dense_rank() keys
│   │       │   └── WarehouseLoader.scala      # JDBC upsert loader (staging-table pattern)
│   │       ├── quality/
│   │       │   └── QuarantineHandler.scala    # Isolates bad rows to Parquet quarantine zone
│   │       ├── orchestration/
│   │       │   └── PipelineOrchestrator.scala # Stage lifecycle: logging, timing, audit, fail-fast
│   │       ├── logging/
│   │       │   └── PipelineLogger.scala       # SLF4J wrapper with per-stage timing
│   │       └── audit/
│   │           └── AuditLogger.scala          # Append-only JSON Lines audit trail
│   └── test/
│       └── scala/                     # Unit and integration tests by module
│           ├── config/
│           ├── extract/
│           ├── load/
│           ├── orchestration/
│           ├── support/
│           ├── transform/
│           ├── EtlPipelineIntegrationSpec.scala
│           └── MySuite.scala
│
├── scripts/                          # Developer helper scripts (env/infra/test/run/package)
├── makefile                          # Shortcut targets wrapping scripts/
│
├── output/                           # Auto-created pipeline output directory
│   ├── final/<timestamp>_final/      #   Parquet and CSV output per run
│   ├── quarantine/<runId>/           #   Bad rows per stage and reason
│   └── final/warehouse/audit.log     #   JSON Lines audit log
│
└── logs/
    └── etl-pipeline.log              # Rolling JSON log file (Log4j 2)
```

---

## Data Flow

```
                    ┌─────────────────────────────────────┐
                    │            EXTRACT STAGE             │
                    │                                     │
   employees_*.csv ─┤                                     │
   employees*.json ─┤──► unionByName ──────────────────► rawDF
   postgres.employees─┤   (schema-safe union;             │
   mysql.employees ─┤    fills missing cols with null)    │
   sqlite.employees─┤                                     │
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │           CLEAN STAGE                │
                    │  1. snake_case column names          │
                    │  2. Normalise string values          │
                    │  3. Quarantine null critical cols    │
                    │  4. Drop rows with >70% nulls        │
                    │  5. Fill non-critical nulls          │
                    │  6. Cast columns to target types     │
                    │  7. Drop exact duplicate rows        │
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │           DEDUP STAGE                │
                    │  PARTITION BY email                  │
                    │  ORDER BY join_date DESC,            │
                    │           source_priority ASC        │
                    │  → keep row_number() = 1             │
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │            JOIN STAGE                │
                    │  LEFT JOIN departments.csv           │
                    │  ON employee.department=dept.dept_id │
                    │  → adds dim_dept_name, dim_manager,  │
                    │    dim_city, dim_budget columns      │
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │         AGGREGATE STAGE              │
                    │  GROUP BY department, country        │
                    │  → salary_sum, salary_avg,           │
                    │    salary_max, id_count, age_avg     │
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │        BUILD_STAR STAGE              │
                    │  dim_department  (dense_rank SK)     │
                    │  dim_employee    (dense_rank SK)     │
                    │  dim_date        (YYYYMMDD int SK)   │
                    │  fact_employee_metrics (FKs+measures)│
                    └─────────────────────────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │            LOAD STAGE                │
                    │  Write aggregated DF → Parquet + CSV │
                    │  Upsert star schema → load-db (PG)  │
                    └─────────────────────────────────────┘
```

---

## Star Schema & Warehouse Design

The warehouse follows the classical **Kimball star schema** pattern:

```
dim_department
  department_sk  PK  (surrogate, dense_rank over department_name)
  department_name

dim_employee
  employee_sk    PK  (surrogate, dense_rank over employee_bk)
  employee_bk        (business key: email or id)
  full_name, email, age, status, country, city

dim_date
  date_sk        PK  (YYYYMMDD integer, e.g. 20240115)
  date_value
  year, month, day

fact_employee_metrics
  employee_sk    FK → dim_employee
  department_sk  FK → dim_department
  date_sk        FK → dim_date
  salary             (additive measure)
  employee_count     (additive measure; always 1 per row)
```

**Surrogate key strategy**: `dense_rank()` over the ordered business key produces compact, sequential integer keys. The same input always produces the same key (deterministic, idempotent).

**Upsert loading**: dimensions and fact tables are loaded using the staging-table pattern:
1. Spark writes new data to `stg_<table>` (overwrite)
2. A direct JDBC connection runs `INSERT ... ON CONFLICT ... DO UPDATE` into the target table
3. Existing rows are updated; new rows are inserted atomically

---

## Configuration Reference

All configuration lives in `application.conf` under the `etl` root key.

### Spark settings (`etl.spark`)
| Key | Default | Description |
|---|---|---|
| `app-name` | `"Distributed ETL Pipeline"` | Spark UI application name |
| `master` | `"local[*]"` | Spark master URL |
| `shuffle-partitions` | `8` | Partitions after shuffles (joins, groupBy) |

### Extract settings (`etl.extract`)

**Flat files**: set `enabled = false` and `path` to your data directory.

**Databases**: each entry supports `db-type` (`postgres`/`mysql`/`sqlite`), connection details, and optional partitioned-read fields (`partition-column`, `lower-bound`, `upper-bound`, `num-partitions`).

**APIs**: `url`, `method` (GET/POST), `params`, `headers`, `root-field` (for envelope responses).

### Transform settings (`etl.transform`)

**Cleaning** (`etl.transform.clean`):
| Key | Description |
|---|---|
| `critical-columns` | Columns that must be non-null; rows with nulls here are quarantined |
| `fill-values` | Default values for nullable columns |
| `cast-columns` | Explicit type casts: `{ age = "integer", salary = "double" }` |
| `drop-rows-with-nulls-threshold` | Drop rows with this fraction of null columns (e.g. `0.7`) |
| `string-columns` | Columns to trim + lowercase |

**Deduplication** (`etl.transform.deduplicate`):
| Key | Description |
|---|---|
| `key-columns` | Business key columns defining row uniqueness |
| `recency-column` | Most recent value wins (DESC ordering) |
| `source-priority` | `{ postgres = 1, mysql = 2 }` — lower number = higher trust |

**Joins** (`etl.transform.joins[]`):
| Key | Description |
|---|---|
| `right-source-type` | `"flat"` / `"api"` / `"db"` |
| `join-type` | `"inner"` / `"left"` / `"right"` / `"full"` |
| `key-columns` | Same-name join keys |
| `key-mappings` | `[{ left = "dept", right = "dept_id" }]` for different column names |
| `right-prefix` | Prefix all right-side columns, e.g. `"dim_"` |

---

## Quick Start

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Java (JDK) | 11 or 17 | Required by Spark |
| SBT | 1.x | Build and run the project |
| Docker | 20+ | Database containers (optional) |
| Docker Compose | v2 | Orchestrate containers (optional) |

### 1 — Clone the repository

```bash
git clone https://github.com/Achraf-Kaou/Distributed-ETL-Pipeline.git
cd Distributed-ETL-Pipeline
```

### 2 — Start database containers (optional)

```bash
cd docker
docker compose up -d
cd ..
```

> Wait ~15 seconds for databases to initialise. To skip Docker entirely, set all database entries to `enabled = false` in `application.conf`.  
> Shortcut: `make infra-up` (and later `make infra-down`).

### 3 — Run the pipeline

```bash
sbt run
```

Output is written to `output/final/<timestamp>/`.  
Shortcut: `make run` (`make run-prod` uses `application.prod.conf`).

### 4 — Run tests

```bash
sbt test
```

Shortcut: `make test`.

### 5 — Inspect the warehouse (optional)

Open **pgAdmin** at [http://localhost:8080](http://localhost:8080):
- Email: `load_user@email.com`
- Password: `load_pass`
- Add server → Host: `load-db`, Port: `5432`, User: `load_user`

### Example: flat-files-only mode

Disable all databases and APIs in `application.conf`:
```hocon
databases = [
  { name = "postgres-employees", enabled = false, ... },
  { name = "mysql-employees",    enabled = false, ... },
  { name = "sqlite-employees",   enabled = false, ... }
]
apis = [{ name = "jsonplaceholder-users", enabled = false, ... }]
```
Then run `sbt run` — only `data/raw/*.csv` and `.json` are processed.

### Example: run only the extract and clean stages

```hocon
orchestration {
  stages = ["extract", "clean"]
}
```

---

## Pipeline Stages

| Stage | Method | Input | Output |
|---|---|---|---|
| `extract` | `extractAllSources()` | Files, DBs, APIs | Raw unified DataFrame |
| `clean` | `clean()` | Raw DF | Cleaned DF + quarantine files |
| `dedup` | `deduplicate()` | Cleaned DF | Deduplicated DF (1 row per email) |
| `join` | `applyConfiguredJoins()` | Deduped DF | Enriched DF (+ department columns) |
| `aggregate` | `aggregate()` | Enriched DF | Aggregated DF (dept/country summaries) |
| `build_star` | `StarSchemaBuilder.build()` | Enriched DF | StarSchemaResult (4 DataFrames) |
| `load` | `save()` + `WarehouseLoader` | Aggregated DF + StarSchema | Parquet/CSV files + JDBC warehouse |

Any stage can be disabled by removing its name from `orchestration.stages` in config.

---

## Logging & Monitoring

### Console output

The pipeline emits step-by-step progress to stdout using emoji markers:
```
📥 Extraction Phase - Loading all sources
📦 [Postgres] Connecting via JDBC: jdbc:postgresql://localhost:5432/etl_db
✅ [Postgres] Loaded 2 rows | 9 columns
🧹 TransformClean — starting
  Input rows: 8   Rows after: 7   Rows dropped: 1
✅ Deduplication complete — 7 → 5 rows removed: 2
```

### Structured log file

`logs/etl-pipeline.log` receives JSON-formatted log events via Log4j 2's rolling file appender:
- Rotates daily and at 50 MB
- Old files compressed as `.log.gz`

### Audit log

`output/final/warehouse/audit.log` — one JSON record per line:

```json
{"ts":"2024-06-01T10:15:30Z","type":"stage_metrics","runId":"a3f2...","stage":"clean","rowsIn":8,"rowsOut":7,"durationMs":1240,"status":"SUCCESS"}
{"ts":"2024-06-01T10:15:55Z","type":"run_summary","runId":"a3f2...","startTime":"...","endTime":"...","status":"SUCCESS","totalExtracted":8,"totalLoaded":0}
```

### Quarantine files

`output/quarantine/<runId>/<stage>_<reason>/` — Parquet files containing rejected rows with added columns:
- `quarantine_reason` — e.g. `"critical_nulls"`, `"missing_join_keys"`
- `quarantine_timestamp` — ISO-8601 timestamp of when the row was quarantined

---

## Sample Data

| File | Format | Notes |
|---|---|---|
| `employees_source1.csv` | CSV | Standard schema: `id, full_name, age, email, department, salary, join_date, status` |
| `employees_source2.csv` | CSV | Non-standard column names (`dept`, `startdate`) — remapped via `column-mapping` config |
| `employees_complex.json` | JSON | Nested `employees` array — auto-flattened by pipeline |
| `departments.csv` | CSV | Excluded from main extract; used as the join dimension right-side |
| `enterprise/csv/customers_master.csv` | CSV | Customer mastering dataset with mixed quality records |
| `enterprise/json/orders_events.json` | JSON | Event-style order payloads with lifecycle updates |
| `enterprise/parquet/products_catalog.parquet` | Parquet | Product dimension-style source dataset |
| `enterprise/parquet/transactions_late_arrivals.parquet` | Parquet | Late-arriving transaction updates for replay testing |
| `enterprise/api/customers_response.json` | JSON | Mock API payload for customer endpoint testing |
| `enterprise/api/orders_response.json` | JSON | Mock API payload for order endpoint testing |

---

**Parquet output is empty?** Verify `data/raw/` contains supported files and `exclude-name-contains` is not too broad.

---

## Enterprise Test Data Additions

The repository now includes richer datasets for employee mastering, customer onboarding, order events, product catalogs, transaction corrections, API mock payloads, and seeded relational databases. The data intentionally contains duplicates, nulls, malformed emails, mixed casing, invalid dates, conflicting source records, and late-arriving updates so the cleaning, deduplication, join, aggregation, quarantine, and warehouse-loading paths can be exercised realistically.

## Developer Experience Assets

- `makefile` — common developer workflows.
- `scripts/validate-env.sh` — environment readiness checks.
- `scripts/start-infra.sh` / `scripts/stop-infra.sh` — Docker lifecycle helpers.
- `scripts/wait-for-services.sh` / `scripts/common.sh` — shared readiness and utility helpers.
- `scripts/reset-databases.sh` / `scripts/load-seed-data.sh` — repeatable seed refresh.
- `scripts/run-etl.sh` / `scripts/run-tests.sh` / `scripts/package-project.sh` — execution helpers.
- `scripts/start-api-mock.sh` / `scripts/troubleshoot.sh` — API mock and diagnostics helpers.
- `docs/developer-onboarding.md` — quick-start workflow.
- `docs/troubleshooting.md` — common operational fixes.

## Sample Production Config

Use `application.prod.conf` when you want environment-variable-driven configuration without modifying the default `application.conf`.

---

## Troubleshooting

**Databases not connecting?**
Set `enabled = false` on the failing source. The pipeline logs a warning and continues when `skip-non-critical-source-failures = true`.

**Out of memory?**
Reduce `shuffle-partitions` in config or increase `-Xmx` in `build.sbt` (currently 2G).

**Parquet output is empty?**
Verify `data/raw/` contains supported files and `exclude-name-contains` is not too broad.

**SQLite file not found?**
Run `docker compose up -d` in the `docker/` directory to create `docker/sqlite/data/etl.db`.

**pgAdmin can't connect to load-db?**
Use the Docker internal hostname `load-db` (not `localhost`) when adding the server in pgAdmin. Port is `5432` (internal), not `5433`.

---

## Future Improvements

| Improvement | Description |
|---|---|
| **Kafka / Spark Structured Streaming** | Replace batch extraction with real-time stream ingestion from Kafka topics |
| **Apache Airflow orchestration** | Replace the internal stage list with a full DAG scheduler (retries, SLAs, dependencies) |
| **Delta Lake / Apache Iceberg** | Add ACID-compliant table format for incremental updates, time travel, and schema evolution |
| **Data quality dashboard** | Expose quarantine metrics and audit log via a Grafana or Metabase dashboard |
| **CI/CD pipeline** | Add GitHub Actions workflow: `sbt test` + Docker build + deploy on merge |
| **Cloud deployment** | Package as an EMR / Dataproc / Databricks job; replace local paths with S3/GCS |
| **Schema registry** | Enforce schema contracts on each source using Confluent Schema Registry or Spark schema validation |
| **Secrets management** | Replace plain-text credentials with `${?ENV_VAR}` HOCON substitutions or Vault integration |
| **Partitioned output** | Write final Parquet partitioned by `year/month/day` for efficient downstream query pruning |
| **dbt integration** | Move SQL-based transformations (aggregations, warehouse models) to dbt for better lineage tracking |
