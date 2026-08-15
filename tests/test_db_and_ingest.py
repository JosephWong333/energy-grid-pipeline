"""Storage-layer and ingestion-logic tests against a throwaway DuckDB."""

from __future__ import annotations

from datetime import datetime

import pytest

from grid_pipeline import db
from grid_pipeline.client import EIARow
from grid_pipeline.ingest import _month_windows


@pytest.fixture()
def con(tmp_path):
    return db.connect(str(tmp_path / "test.duckdb"))


def _row(hour: int, value: float | None, series: str = "D") -> EIARow:
    return EIARow(
        period_utc=datetime(2026, 6, 1, hour),
        respondent="CISO",
        series=series,
        value=value,
        units="megawatthours",
    )


def test_upsert_is_idempotent_and_replaces(con):
    db.upsert_rows(con, "region_data", [_row(0, 100.0), _row(1, 200.0)], source="dev_fixtures")
    # Re-loading the same hour with a restated value must replace, not duplicate.
    db.upsert_rows(con, "region_data", [_row(0, 150.0)], source="dev_fixtures")

    rows = con.execute(
        "SELECT period_utc, value FROM raw.eia_region_data ORDER BY period_utc"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][1] == 150.0  # restated value won


def test_null_values_are_stored_not_dropped(con):
    db.upsert_rows(con, "region_data", [_row(0, None)], source="dev_fixtures")
    (value,) = con.execute("SELECT value FROM raw.eia_region_data").fetchone()
    assert value is None


def test_watermark_roundtrip(con):
    assert db.get_watermark(con, "region_data", "CISO") is None
    db.set_watermark(con, "region_data", "CISO", datetime(2026, 6, 1, 23))
    assert db.get_watermark(con, "region_data", "CISO") == datetime(2026, 6, 1, 23)


def test_load_meta_roundtrip(con):
    db.set_load_meta(con, "source", "dev_fixtures")
    assert db.get_load_meta(con, "source") == "dev_fixtures"
    db.set_load_meta(con, "source", "eia_api")
    assert db.get_load_meta(con, "source") == "eia_api"


def test_month_windows_cover_range_without_gaps_or_overlap():
    start = datetime(2026, 1, 15)
    end = datetime(2026, 3, 10, 12)
    windows = list(_month_windows(start, end))

    assert windows[0][0] == start
    assert windows[-1][1] == end
    # contiguous: each window starts exactly 1h after the previous ends
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        assert (next_start - prev_end).total_seconds() == 3600
    assert len(windows) == 3  # Jan, Feb, Mar partials


def test_intra_batch_duplicate_keeps_last_value(con):
    """A key repeated within ONE batch must resolve last-wins, matching the
    late-arrival semantics of the pipeline (regression: DuckDB's
    INSERT OR REPLACE keeps the first intra-batch duplicate)."""
    db.upsert_rows(
        con,
        "region_data",
        [_row(0, 100.0), _row(0, 999.0)],
        source="dev_fixtures",
    )
    value = con.execute("SELECT value FROM raw.eia_region_data").fetchone()[0]
    assert value == 999.0


# --------------------------------------------------------------------------- #
# Replay / watermark semantics (regressions for the pilot-poisoning bug)
# --------------------------------------------------------------------------- #
class _CapturingClient:
    """API-shaped stub: yields one record per query, records every window."""

    def __init__(self):
        self.calls: list[tuple[str, str | None, str | None]] = []

    def iter_rows(self, route, *, facets=None, start=None, end=None,
                  tiebreak_column=None):
        self.calls.append((route, start, end))
        series_key = "type" if "region-data" in route else "fueltype"
        yield {"period": start, "respondent": facets["respondent"][0],
               series_key: "D" if series_key == "type" else "SUN",
               "value": "100.0", "value-units": "megawatthours"}


@pytest.fixture()
def cfg():
    from grid_pipeline.ingest import load_config
    return load_config()


def test_set_watermark_is_monotonic(con):
    db.set_watermark(con, "region_data", "CISO", datetime(2026, 6, 30))
    db.set_watermark(con, "region_data", "CISO", datetime(2026, 6, 1))  # older
    assert db.get_watermark(con, "region_data", "CISO") == datetime(2026, 6, 30)


def test_replay_never_touches_watermarks(con, cfg):
    """An interrupted or completed replay must not create, advance, or
    regress any watermark — otherwise a short pilot poisons the backfill."""
    from grid_pipeline.ingest import run_backfill
    db.set_watermark(con, "region_data", "CISO", datetime(2026, 6, 30))
    run_backfill(cfg, con, _CapturingClient(),
                 replay_since=datetime(2026, 6, 20))
    assert db.get_watermark(con, "region_data", "CISO") == datetime(2026, 6, 30)
    assert db.get_watermark(con, "region_data", "ERCO") is None
    assert db.get_watermark(con, "fuel_mix", "CISO") is None
    n = con.execute("select count(*) from raw.eia_region_data").fetchone()[0]
    assert n > 0  # rows landed even though watermarks did not move


def test_pilot_replay_then_backfill_still_covers_full_history(con, cfg):
    """The recommended pilot sequence: replay on a fresh warehouse, then an
    ordinary backfill. The backfill must start at backfill_start, not at
    'last week' — the exact bug this guards against."""
    from grid_pipeline.ingest import run_backfill
    run_backfill(cfg, con, _CapturingClient(),
                 replay_since=datetime(2026, 7, 4))
    backfill_client = _CapturingClient()
    run_backfill(cfg, con, backfill_client)
    first_start = backfill_client.calls[0][1]
    assert first_start.startswith(cfg.backfill_start[:7])  # 2019-01, not July


# --------------------------------------------------------------------------- #
# Response integrity: violations raise BEFORE any upsert or watermark motion
# --------------------------------------------------------------------------- #
class _RiggedClient(_CapturingClient):
    def __init__(self, respondent=None, period=None, units=None):
        super().__init__()
        self._respondent, self._period, self._units = respondent, period, units

    def iter_rows(self, route, *, facets=None, start=None, end=None,
                  tiebreak_column=None):
        series_key = "type" if "region-data" in route else "fueltype"
        yield {"period": self._period or start,
               "respondent": self._respondent or facets["respondent"][0],
               series_key: "D" if series_key == "type" else "SUN",
               "value": "100.0",
               "value-units": self._units or "megawatthours"}


@pytest.mark.parametrize("rig,match", [
    ({"respondent": "HACK"}, "other respondent"),
    ({"period": "2031-01-01T00"}, "outside the requested window"),
    ({"units": "gigawatthours"}, "unexpected value-units"),
])
def test_response_integrity_violations_raise_before_loading(con, cfg, rig, match):
    from grid_pipeline.client import EIAError
    from grid_pipeline.ingest import run_backfill
    with pytest.raises(EIAError, match=match):
        run_backfill(cfg, con, _RiggedClient(**rig))
    assert con.execute("select count(*) from raw.eia_region_data").fetchone()[0] == 0
    assert db.get_watermark(con, "region_data", "CISO") is None
