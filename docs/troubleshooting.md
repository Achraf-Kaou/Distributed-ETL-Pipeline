# Troubleshooting

## SBT not found

Install SBT locally before running `make run`, `make test`, or `make package`. `make validate` fails fast when it is missing.

## Docker services fail health checks

- Run `make troubleshoot` to inspect container status.
- Review `docker/docker-compose.yml` health checks and confirm the mapped ports are free.
- Run `make infra-reset && make infra-up` to recreate seeded databases from scratch.

## pgAdmin cannot connect

- Confirm PostgreSQL is healthy with `docker inspect etl-postgres`.
- Ensure the pgAdmin container is running on `http://localhost:5050`.
- The bundled `docker/pgadmin/servers.json` pre-registers the `postgres` service name.

## Warehouse load issues

- Remove stale generated output with `make infra-reset`.
- Re-run the pipeline and inspect `output/audit/audit.log` for stage-level metrics.
- Validate that the configured warehouse SQLite path or JDBC credentials match the chosen config file.

## Mock API validation

- Start the local server with `make api-mock`.
- Browse `http://localhost:8081/customers_response.json` or `http://localhost:8081/orders_response.json`.
