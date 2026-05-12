SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

help:
@echo "Available targets: env validate infra-up infra-down infra-reset seed-data api-mock run run-prod test package troubleshoot"

env validate:
@./scripts/validate-env.sh

infra-up:
@./scripts/start-infra.sh

infra-down:
@./scripts/stop-infra.sh

infra-reset:
@./scripts/reset-databases.sh

seed-data:
@./scripts/load-seed-data.sh

api-mock:
@./scripts/start-api-mock.sh

run:
@./scripts/run-etl.sh application.conf

run-prod:
@./scripts/run-etl.sh application.prod.conf

test:
@./scripts/run-tests.sh

package:
@./scripts/package-project.sh

troubleshoot:
@./scripts/troubleshoot.sh