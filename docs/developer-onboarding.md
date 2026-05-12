# Developer Onboarding

## Quick setup

1. Copy `.env.example` to `.env` and adjust ports or credentials if needed.
2. Run `make validate` to verify Java, Docker, Python, and SBT.
3. Run `make infra-up` to start PostgreSQL, MySQL, SQLite seed initialization, and pgAdmin.
4. Run `make run` to execute the ETL pipeline against the default config.
5. Run `make test` to execute the automated test suite.

## Common commands

- `make infra-reset` — remove database volumes, refresh SQLite, and clear generated outputs.
- `make seed-data` — rebuild the seeded databases from the checked-in SQL files.
- `make run-prod` — execute the pipeline with `application.prod.conf`.
- `make package` — run tests and produce the packaged artifact.
- `make api-mock` — serve API mock payloads from `data/raw/enterprise/api`.

## Data catalog

- `data/raw/*.csv|json` — default employee + department pipeline inputs.
- `data/raw/enterprise/csv` — richer customer master data.
- `data/raw/enterprise/json` — order event scenarios, including duplicates and invalid dates.
- `data/raw/enterprise/parquet` — product and transaction snapshots.
- `docker/*/init.sql` — relational seed data for PostgreSQL, MySQL, and SQLite.

## Outputs

- `output/final` — final CSV/Parquet pipeline outputs.
- `output/audit` — run and stage metrics.
- `output/quarantine` — quarantined rows captured during cleaning, deduplication, joins, and aggregations.
