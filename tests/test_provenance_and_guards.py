"""Regression tests for the fixture-contamination class of bugs.

Background: an external review demonstrated that loading fixtures and then
running a real backfill against the same warehouse (a) skipped real history
because fixture watermarks were near-current, and (b) flipped a global
source flag that un-watermarked charts still containing synthetic rows.
These tests pin the fixes: row-level provenance, fail-closed classification,
and mutual refusal between fixture and real modes.
"""

from datetime import datetime

import pytest

from grid_pipeline import db
from grid_pipeline.client import EIARow
from grid_pipeline.ingest import ContaminationError, guard_against_contamination
from grid_pipeline.provenance import data_provenance


def _row(hour: int, value: float, series: str = "D") -> EIARow:
    return EIARow(
        period_utc=datetime(2026, 6, 1, hour),
        respondent="CISO",
        series=series,
        value=value,
        units="megawatthours",
    )


@pytest.fixture
def con():
    connection = db.connect(":memory:")
    yield connection
    connection.close()


def test_provenance_empty_warehouse(con):
    assert data_provenance(con) == "empty"


def test_provenance_pure_real_and_pure_fixtures(con):
    db.upsert_rows(con, "region_data", [_row(0, 1.0)], source="eia_api")
    assert data_provenance(con) == "real"
    db.upsert_rows(con, "fuel_mix", [_row(1, 2.0, "SUN")], source="dev_fixtures")
    assert data_provenance(con) == "mixed"


def test_provenance_fails_closed_on_unknown_source(con):
    # Rows written by some other tool, or a pre-lineage warehouse: never
    # classified as real.
    con.execute(
        "INSERT INTO raw.eia_region_data VALUES "
        "(TIMESTAMP '2026-06-01 00:00', 'CISO', 'D', 1.0, 'megawatthours', "
        "TIMESTAMP '2026-06-01 00:00', 'unknown')"
    )
    assert data_provenance(con) == "unknown"


def test_real_modes_refuse_fixture_warehouse(con):
    db.upsert_rows(con, "region_data", [_row(0, 1.0)], source="dev_fixtures")
    with pytest.raises(ContaminationError):
        guard_against_contamination(con, "backfill", "data/energy_grid.duckdb")
    with pytest.raises(ContaminationError):
        guard_against_contamination(con, "incremental", "data/energy_grid.duckdb")


def test_fixture_mode_refuses_real_warehouse_and_cloud_paths(con):
    db.upsert_rows(con, "region_data", [_row(0, 1.0)], source="eia_api")
    with pytest.raises(ContaminationError):
        guard_against_contamination(con, "fixtures", "data/energy_grid.duckdb")
    empty = db.connect(":memory:")
    with pytest.raises(ContaminationError):
        guard_against_contamination(empty, "fixtures", "md:energy_grid")
    empty.close()


def test_clean_paths_are_allowed(con):
    guard_against_contamination(con, "backfill", "data/energy_grid.duckdb")  # empty ok
    db.upsert_rows(con, "region_data", [_row(0, 1.0)], source="eia_api")
    guard_against_contamination(con, "incremental", "data/energy_grid.duckdb")
