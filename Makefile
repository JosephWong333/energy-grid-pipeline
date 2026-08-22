# energy-grid-pipeline — common workflows
#
#   make setup        install everything (editable + dev tools)
#   make dev          full local pipeline on synthetic fixtures (no API key)
#   make backfill     load full history from the EIA API (needs EIA_API_KEY)
#   make incremental  refresh recent hours from the EIA API
#   make build        dbt build (models + tests)
#   make test         pytest + dbt build
#   make lint         ruff
#   make charts       render README charts from the marts
#   make docs         generate dbt docs (target/static_index.html)
#   make clean        remove local warehouse + build artifacts

DBT_FLAGS := --project-dir dbt --profiles-dir dbt
# Fixture workflows run against their own warehouse. Real and synthetic data
# never share a database file; the ingester enforces this with contamination
# guards even if you point them at each other manually.
DEV_DB    := data/dev_fixtures.duckdb

.PHONY: setup fixtures ingest-fixtures build-dev charts-dev dev backfill replay replay-raw incremental incremental-raw build test lint charts docs clean

setup:
	pip install -r requirements.lock
	pip install -e . --no-deps
	dbt deps $(DBT_FLAGS)

fixtures:
	python scripts/generate_dev_fixtures.py

ingest-fixtures:
	GRID_DB_PATH=$(DEV_DB) python -m grid_pipeline.ingest --mode fixtures

build-dev:
	GRID_DB_PATH=$(DEV_DB) dbt build $(DBT_FLAGS) --full-refresh

charts-dev:
	GRID_DB_PATH=$(DEV_DB) python scripts/make_charts.py

dev: fixtures ingest-fixtures build-dev charts-dev
	@echo "Local pipeline complete on $(DEV_DB) (synthetic fixtures — charts are watermarked)."

backfill:
	python -m grid_pipeline.ingest --mode backfill

# Repair OLD upstream restatements: re-fetch raw history from a date,
# ignoring watermarks (idempotent upserts make this safe). A dbt
# --full-refresh rebuilds marts from raw; it cannot repair raw. This can.
# Usage: make replay SINCE=2024-01-01
# A replay is only half done when raw is repaired: marts built before the
# repair still hold the old values outside the incremental lookback, so the
# composed target re-fetches raw AND full-refreshes every mart. replay-raw
# exists for the rare case where you want to stage several source repairs
# before paying for one rebuild.
replay: replay-raw
	dbt build $(DBT_FLAGS) --full-refresh

replay-raw:
	python -m grid_pipeline.ingest --mode backfill --replay-since $(SINCE)

# A daily refresh is only half done when raw is current: marts still hold
# yesterday until dbt runs. The composed target does both; incremental-raw
# exists for ingest-only (the nightly workflow composes its own steps).
incremental: incremental-raw
	dbt build $(DBT_FLAGS)

incremental-raw:
	python -m grid_pipeline.ingest --mode incremental

build:
	dbt build $(DBT_FLAGS)

# Self-contained on a fresh checkout: builds its own fixture warehouse.
test: fixtures ingest-fixtures
	pytest
	ruff check .
	GRID_DB_PATH=$(DEV_DB) dbt build $(DBT_FLAGS) --full-refresh

lint:
	ruff check .

charts:
	python scripts/make_charts.py

docs:
	dbt docs generate --static $(DBT_FLAGS)
	@echo "open dbt/target/static_index.html"

clean:
	rm -rf data/*.duckdb data/*.duckdb.wal data/sample dbt/target dbt/dbt_packages dbt/logs
