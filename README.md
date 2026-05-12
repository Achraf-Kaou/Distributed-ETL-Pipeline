# Distributed ETL Pipeline

[![Scala](https://img.shields.io/badge/Scala-2.13-DC322F?logo=scala&logoColor=white)](https://scala-lang.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![SBT](https://img.shields.io/badge/SBT-1.x-blue)](https://www.scala-sbt.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose)

A robust, scalable, and **fully configuration-driven** ETL pipeline built with **Scala 2.13** and **Apache Spark 3.5**. It extracts data from heterogeneous sources (flat files, JDBC databases, REST APIs), applies a multi-stage transformation chain, builds a star-schema warehouse model, and loads results into Parquet, CSV, and a relational target — all without changing a single line of code.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [Pipeline Stages](#pipeline-stages)
- [Data Sources](#data-sources)
- [Transformations](#transformations)
- [Star Schema & Warehouse](#star-schema--warehouse)
- [Testing](#testing)
- [Sample Data](#sample-data)
- [Tech Stack](#tech-stack)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         ETL Pipeline Flow                            │
├──────────────┬───────────────┬──────────────┬────────────────────────┤
│   EXTRACT    │   TRANSFORM   │    LOAD      │     ORCHESTRATION      │
│              │               │              │                        │
│ FlatFiles    │ Clean         │ Parquet      │ PipelineOrchestrator   │
│ CSV/JSON     │ Deduplicate   │ CSV          │  - Stage control       │
│ Parquet      │ Join          │ StarSchema   │  - fail-fast           │
│ Excel/XML    │ Aggregate     │ dim + fact   │  - skip bad sources    │
│              │ QualityChecks │ Upsert JDBC  │                        │
│ JDBC DB      │               │              │ PipelineLogger         │
│ Postgres     │               │              │  - Leveled logging     │
│ MySQL        │               │              │  - Step timing         │
│ SQLite       │               │              │                        │
│              │               │              │ QualityChecks          │
│ REST API     │               │              │  - Null detection      │
│ GET / POST   │               │              │  - Duplicate check     │
└──────────────┴───────────────┴──────────────┴────────────────────────┘
```

---

## Features

| Feature | Description |
|---|---|
| **Config-first** | All behavior driven by `application.conf` — no code changes needed |
| **Multi-source** | Flat files (CSV, JSON, Parquet, Excel, XML, TSV), JDBC (Postgres, MySQL, SQLite), REST API |
| **Partitioned JDBC** | Parallel database reads via `partitionColumn` for large tables |
| **Smart cleaning** | 7-step cleaning chain: snake_case normalization, null handling, type casting |
| **Deterministic dedup** | Window-based deduplication: most recent row wins, source priority as tiebreaker |
| **Flexible joins** | `keyColumns` (same name) or `keyMappings` (different names), all join types |
| **Aggregations** | `sum`, `avg`, `max`, `min`, `count` with configurable group-by columns |
| **Star schema** | Auto-builds `dim_department`, `dim_employee`, `dim_date`, `fact_employee_metrics` |
| **Upsert loader** | `ON CONFLICT ... DO UPDATE` (Postgres/SQLite) and `ON DUPLICATE KEY UPDATE` (MySQL) |
| **Quality gates** | Critical-column null checks and duplicate key detection run between stages |
| **Orchestration** | Stage filtering, `fail-fast` mode, skip-bad-source mode |
| **Observability** | Leveled logger with per-stage timing in milliseconds |

---

## Project Structure

```
.
├── application.conf              # Master configuration file (HOCON)
├── build.sbt                     # SBT build definition
├── data/
│   └── raw/                      # Input flat files
│       ├── employees_source1.csv
│       ├── employees_source2.csv
│       ├── employees_complex.json
│       └── departments.csv
├── docker/
│   ├── docker-compose.yml        # Postgres + MySQL + SQLite containers
│   ├── postgres/init.sql
│   ├── mysql/init.sql
│   └── sqlite/init.sql
├── output/                       # Pipeline outputs (auto-created)
│   └── final/<timestamp>/parquet/
├── src/
│   ├── main/scala/
│   │   ├── Main.scala
│   │   ├── config/PipelineConfig.scala
│   │   ├── extract/
│   │   │   ├── ExtractFlatFiles.scala
│   │   │   ├── ExtractDatabase.scala
│   │   │   └── ExtractApi.scala
│   │   ├── transform/
│   │   │   ├── TransformClean.scala
│   │   │   ├── TransformDeduplicate.scala
│   │   │   ├── TransformJoin.scala
│   │   │   ├── TransformAggregate.scala
│   │   │   └── QualityChecks.scala
│   │   ├── load/
│   │   │   ├── StarSchemaBuilder.scala
│   │   │   └── WarehouseLoader.scala
│   │   ├── logging/PipelineLogger.scala
│   │   └── orchestration/PipelineOrchestrator.scala
│   └── test/scala/MySuite.scala
└── project/build.properties
```

---

## Quick Start

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Java (JDK) | 11 or 17 | Required by Spark |
| SBT | 1.x | Build tool |
| Docker | 20+ | Database containers (optional) |
| Docker Compose | v2 | Orchestrate containers (optional) |

### 1 — Clone the repository

```bash
git clone https://github.com/Achraf-Kaou/Distributed-ETL-Pipeline.git
cd Distributed-ETL-Pipeline
```

### 2 — Start the database containers (optional)

```bash
cd docker
docker compose up -d
cd ..
```

Wait ~15 seconds for databases to initialize. To skip databases entirely, set all database sources to `enabled = false` in `application.conf`.

### 3 — Run the pipeline

```bash
sbt run
```

Outputs are written to `output/final/<timestamp>/`.

### 4 — Run the tests

```bash
sbt test
```

---

## Configuration Reference

### `etl.spark`

```hocon
spark {
  app-name           = "Distributed ETL Pipeline"
  master             = "local[*]"
  shuffle-partitions = 8
}
```

### `etl.extract`

```hocon
flat-files {
  enabled               = true
  path                  = "data/raw"
  exclude-name-contains = ["departments"]
}

databases = [{
  name           = "postgres-employees"
  enabled        = true
  db-type        = "postgres"    # postgres | mysql | sqlite
  host           = "localhost"
  port           = 5432
  database       = "etl_db"
  user           = "etl_user"
  password       = "etl_pass"
  table-or-query = "employees"
  source-tag     = "postgres"
  # Optional parallel partitioned read:
  # partition-column = "id"
  # lower-bound      = 1
  # upper-bound      = 10000
  # num-partitions   = 4
}]
```

### `etl.transform`

```hocon
normalize-columns = true
column-mapping    = { dept = "department", startdate = "join_date" }

clean {
  critical-columns                  = ["id", "full_name", "email"]
  fill-values                       = { country = "Unknown", age = "0" }
  cast-columns                      = { age = "integer", salary = "double" }
  drop-rows-with-nulls-threshold    = 0.7
}

deduplicate {
  key-columns     = ["email"]
  recency-column  = "join_date"
  source-priority = { postgres = 1, mysql = 2, sqlite = 3 }
}

joins = [{
  name                = "employees-with-departments"
  right-source-type   = "flat"
  right-path-or-query = "data/raw/departments.csv"
  join-type           = "left"
  key-mappings        = [{ left = "department", right = "dept_id" }]
  right-prefix        = "dim_"
}]

aggregation {
  enabled  = true
  group-by = ["department", "country"]
  metrics  = [
    { column = "salary", function = "sum" }
    { column = "salary", function = "avg" }
    { column = "id",     function = "count" }
  ]
}
```

---

## Pipeline Stages

| Stage | Description |
|---|---|
| `extract` | Union of all enabled sources into one DataFrame |
| `clean` | 7-step cleaning chain |
| `dedup` | Window-based business-key deduplication |
| `join` | Left-join with dimension/enrichment tables |
| `aggregate` | Group-by aggregations |
| `build_star` | Builds dimension and fact DataFrames |
| `load` | Writes Parquet/CSV and performs JDBC upsert |

Any stage can be disabled by removing it from the `stages` list in config.

---

## Data Sources

### Supported flat-file formats

| Extension | Notes |
|---|---|
| `.csv`, `.txt`, `.tsv` | Auto-infer schema, UTF-8 |
| `.json` | Multi-line; auto-flattens nested structs |
| `.parquet` | Column pruning applied |
| `.xlsx`, `.xls` | Sheet1 by default |
| `.xml` | `rowTag = "row"` by default |

### Supported databases

| Database | Notes |
|---|---|
| PostgreSQL | Parallel partitioned reads supported |
| MySQL | SSL disabled by default |
| SQLite | File-based, no authentication needed |

---

## Transformations

### Deduplication strategy

When the same employee appears in multiple sources, the pipeline keeps the single best version:

```
PARTITION BY email
ORDER BY join_date DESC, source_priority ASC
→ keep row_number = 1
```

### Aggregation output example

Input: employees IT/France with salaries 45000 and 72000.
Output with `group-by = ["department", "country"]`:

| department | country | salary_sum | salary_avg | id_count |
|---|---|---|---|---|
| IT | France | 117000 | 58500 | 2 |
| HR | France | 52000 | 52000 | 1 |

---

## Star Schema & Warehouse

```
dim_department  (department_sk PK, department_name)
dim_employee    (employee_sk PK, employee_bk, full_name, email, age, status, country, city)
dim_date        (date_sk PK, date_value, year, month, day)

fact_employee_metrics
  (employee_sk FK, department_sk FK, date_sk FK, salary, employee_count)
```

Surrogate keys are generated using `dense_rank()` window functions. The loader writes dimensions and fact tables using upsert semantics (staging table + merge).

---

## Testing

```bash
sbt test
```

Three test cases cover: config parsing, quality checks (null and duplicate detection), and star schema construction.

---

## Sample Data

| File | Format | Notes |
|---|---|---|
| `employees_source1.csv` | CSV | Standard schema |
| `employees_source2.csv` | CSV | Non-standard column names, remapped via `column-mapping` |
| `employees_complex.json` | JSON | Nested objects, auto-flattened |
| `departments.csv` | CSV | Used in the join step |

---

## Tech Stack

| Library | Version | Purpose |
|---|---|---|
| `spark-core` / `spark-sql` | 3.5.1 | Distributed computation |
| `com.typesafe:config` | 1.4.3 | HOCON configuration |
| `com.lihaoyi:requests` | 0.9.0 | HTTP client |
| `spark-excel` | 3.5.0_0.20.3 | Excel reading |
| `spark-xml` | 0.18.0 | XML reading |
| PostgreSQL / MySQL / SQLite drivers | latest | JDBC connections |
| `munit` | 1.0.0 | Unit testing |

---

## Troubleshooting

**Databases not connecting?** Set `enabled = false` on that source. The pipeline logs a warning and continues.

**Out of memory?** Reduce `shuffle-partitions` in config.

**Parquet output is empty?** Verify `data/raw/` contains supported files and `exclude-name-contains` is not too broad.