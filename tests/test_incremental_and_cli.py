"""The nightly path (run_incremental) and the CLI exit-code contract."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta

import pytest

from grid_pipeline import db, ingest
from grid_pipeline.client import PERIOD_FORMAT, EIAError, EIARow, ThrottledError
from grid_pipeline.config import load_config


class _Stub:
    """API-shaped stub: one row at each window's end; optionally raises."""

    def __init__(self, exc: Exception | None = None):
        self.calls: list[tuple[str, str, str, str]] = []
        self.exc = exc

    def iter_rows(self, route, *, facets=None, start=None, end=None, tiebreak_column=None):
        self.calls.append((route, facets["respondent"][0], start, end))
        if self.exc:
            raise self.exc
        key = "type" if "region-data" in route else "fueltype"
        yield {"period": end, "respondent": facets["respondent"][0],
               key: "D" if key == "type" else "SUN",
               "value": "1", "value-units": "megawatthours"}


@pytest.fixture()
def cfg():
    # One BA, recent start: fast, and independent of the real BA list.
    return dataclasses.replace(load_config(), balancing_authorities=["CISO"],
                               backfill_start="2026-01-01")


@pytest.fixture()
def con(tmp_path):
    c = db.connect(str(tmp_path / "t.duckdb"))
    yield c
    c.close()


def _p(s: str) -> datetime:
    return datetime.strptime(s, PERIOD_FORMAT)


def test_incremental_refetches_lookback_from_each_watermark(con, cfg):
    wm = datetime(2026, 9, 29, 5)
    for route in ("region_data", "fuel_mix"):
        db.set_watermark(con, route, "CISO", wm)
    stub = _Stub()
    ingest.run_incremental(cfg, con, stub)
    for route in ("region-data", "fuel-type-data"):
        calls = [c for c in stub.calls if c[0].endswith(route)]
        assert _p(calls[0][2]) == wm - timedelta(hours=cfg.incremental_lookback_hours)
        assert _p(calls[-1][3]) > wm  # reaches "now"
    # watermark advanced to the newest row seen, and the run stamped itself
    assert db.get_watermark(con, "region_data", "CISO") == _p(stub.calls[-1][3])
    assert db.get_load_meta(con, "last_incremental_at") is not None


def test_normal_night_is_one_request_even_across_a_month_boundary(con, cfg, monkeypatch):
    """Same request shape as before month windows existed: [wm - lookback, now]."""
    class _Oct1(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 1, 7, 25, tzinfo=tz)

    monkeypatch.setattr(ingest, "datetime", _Oct1)
    wm = datetime(2026, 10, 1, 6)  # the lookback starts in September
    for route in ("region_data", "fuel_mix"):
        db.set_watermark(con, route, "CISO", wm)
    stub = _Stub()
    ingest.run_incremental(cfg, con, stub)
    assert len(stub.calls) == 2  # one per route
    for _, _, start, end in stub.calls:
        assert _p(start) == wm - timedelta(hours=cfg.incremental_lookback_hours)
        assert end == "2026-10-01T07"


def test_incremental_bootstraps_pair_without_watermark_in_month_windows(con, cfg):
    stub = _Stub()
    ingest.run_incremental(cfg, con, stub)
    region = [c for c in stub.calls if c[0].endswith("region-data")]
    assert region[0][2] == "2026-01-01T00"
    assert len(region) > 1 and all(s[:7] == e[:7] for _, _, s, e in region)


def test_incremental_catch_up_after_stale_watermark_uses_month_windows(con, cfg):
    """Interrupted bootstrap or a long outage: resume must stay resumable."""
    for route in ("region_data", "fuel_mix"):
        db.set_watermark(con, route, "CISO", datetime(2026, 2, 10))
    stub = _Stub()
    ingest.run_incremental(cfg, con, stub)
    assert all(s[:7] == e[:7] for _, _, s, e in stub.calls), stub.calls[:2]


@pytest.mark.parametrize("exc,code", [(ThrottledError("slow down"), 3), (EIAError("boom"), 4)])
def test_main_exit_codes(tmp_path, monkeypatch, exc, code):
    # Set GRID_DB_PATH explicitly: load_config() loads .env, and a developer
    # .env pointing at md:energy_grid must never retarget a test.
    monkeypatch.setenv("GRID_DB_PATH", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("EIA_API_KEY", "dummy")
    monkeypatch.setattr(ingest, "EIAClient", lambda **kw: _Stub(exc=exc))
    assert ingest.main(["--mode", "incremental"]) == code


def test_main_exit_code_contamination(tmp_path, monkeypatch):
    path = tmp_path / "w.duckdb"
    c = db.connect(str(path))
    db.upsert_rows(c, "region_data",
                   [EIARow(datetime(2026, 6, 1), "CISO", "D", 1.0, "megawatthours")],
                   source="dev_fixtures")
    c.close()
    monkeypatch.setenv("GRID_DB_PATH", str(path))
    monkeypatch.setenv("EIA_API_KEY", "dummy")
    monkeypatch.setattr(ingest, "EIAClient", lambda **kw: _Stub())
    assert ingest.main(["--mode", "incremental"]) == 2


def test_ingest_lookback_fits_inside_dbt_reprocess_window():
    """Raw restated by ingest must fall inside what the fact model re-reads."""
    import yaml

    from grid_pipeline.config import REPO_ROOT
    dbt = yaml.safe_load((REPO_ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))
    assert load_config().incremental_lookback_hours < dbt["vars"]["late_arrival_lookback_hours"]
