# Distributed ETL Pipeline (Scala + Spark)

Generic and reusable ETL pipeline with config-driven orchestration:

1. **Extract** from flat files, APIs, and JDBC databases
2. **Clean** data with configurable null handling, casting, and normalization
3. **Deduplicate** with configurable business keys and source priority
4. **Join** datasets with configurable key columns or key mappings
5. **Load** into Parquet/CSV outputs

## Stack

- Scala 2.13
- Apache Spark 3.5.x
- SBT

## Configuration-first orchestration

All behavior is controlled from:

- `./application.conf`

Main configuration sections:

- `etl.extract.flat-files`: enable/disable and path-based flat-file ingestion
- `etl.extract.apis`: list of API sources (enable per source)
- `etl.extract.databases`: list of JDBC sources (Postgres/MySQL/SQLite)
- `etl.transform.clean`: cleaning rules
- `etl.transform.deduplicate`: dedup keys/recency/source-priority
- `etl.transform.joins`: join definitions with key mapping support
- `etl.load`: enabled output formats

## Running the pipeline

From repository root:

```bash
sbt run
```

Outputs are written under:

- `./output/final/`

Each run writes separate subfolders per format (`parquet`, `csv`) to avoid overwriting.

## Docker DB sources

The repository includes Docker services in:

- `./docker/docker-compose.yml`

When DB services are unavailable, enabled DB extractions are skipped with warnings.

## Public API suggestions for future enrichment

Two free public APIs that can be joined with employee location/country data:

1. **REST Countries API**
   - `https://restcountries.com`
   - Enrich by `country` (region, ISO code, currency, population)
2. **Open-Meteo API**
   - `https://api.open-meteo.com`
   - Enrich by city geolocation/weather context for location analytics
