"""The nightly path (run_incremental) and the CLI exit-code contract."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest

from grid_pipeline import db, ingest
from grid_pipeline.client import PERIOD_FORMAT, EIAError, EIARow, ThrottledError
from grid_pipeline.config import load_config


class _Stub:
    """API-shaped stub: one row at each window's end; optionally raises.

    ``fail_at`` is the 1-based number of the one call that raises ``exc``;
    without it, ``exc`` is raised on every call.
    """

    def __init__(self, exc: Exception | None = None, fail_at: int | None = None):
        self.calls: list[tuple[str, str, str, str]] = []
        self.exc = exc
        self.fail_at = fail_at

    def iter_rows(self, route, *, facets=None, start=None, end=None, tiebreak_column=None):
        self.calls.append((route, facets["respondent"][0], start, end))
        if self.exc and self.fail_at in (None, len(self.calls)):
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


# --- resuming from a checkpoint ------------------------------------------- #

EXHAUSTED = EIAError("EIA API still failing after 7 attempts (ReadTimeout)")


@pytest.fixture()
def cfg3(cfg):
    return dataclasses.replace(cfg, balancing_authorities=["CISO", "ERCO", "PJM"])


def _seed_recent_watermarks(con, cfg) -> None:
    """Every pair a few hours behind, so each one is a single request."""
    wm = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    for route in ("region_data", "fuel_mix"):
        for ba in cfg.balancing_authorities:
            db.set_watermark(con, route, ba, wm - timedelta(hours=5))


def _pairs(stub: _Stub) -> list[str]:
    """The stub's calls as 'route_key/BA', in order."""
    key = {"region-data": "region_data", "fuel-type-data": "fuel_mix"}
    return [f"{key[route.rsplit('/', 1)[-1]]}/{ba}" for route, ba, _, _ in stub.calls]


def test_checkpoint_resume_fetches_only_the_unfinished_pairs(con, cfg3, tmp_path, caplog):
    """A re-run after a failure picks up at the failed pair, not at the first."""
    _seed_recent_watermarks(con, cfg3)
    checkpoint = tmp_path / "ingest-checkpoint"

    first = _Stub(exc=EXHAUSTED, fail_at=2)
    with pytest.raises(EIAError), caplog.at_level("INFO", logger="grid_pipeline.ingest"):
        ingest.run_incremental(cfg3, con, first, checkpoint=checkpoint)
    assert _pairs(first) == ["region_data/CISO", "region_data/ERCO"]
    assert checkpoint.read_text().split() == ["region_data/CISO"]
    assert "region_data/ERCO failed" in caplog.text  # the log names the pair
    assert db.get_load_meta(con, "last_incremental_at") is None

    second = _Stub()
    with caplog.at_level("INFO", logger="grid_pipeline.ingest"):
        ingest.run_incremental(cfg3, con, second, checkpoint=checkpoint)
    assert _pairs(second) == ["region_data/ERCO", "region_data/PJM",
                              "fuel_mix/CISO", "fuel_mix/ERCO", "fuel_mix/PJM"]
    assert "1 of 6 pairs already refreshed" in caplog.text
    assert db.get_load_meta(con, "last_incremental_at") is not None


def test_a_finished_run_leaves_no_checkpoint_behind(con, cfg3, tmp_path):
    """A leftover file would make the next night skip every pair."""
    _seed_recent_watermarks(con, cfg3)
    checkpoint = tmp_path / "ingest-checkpoint"
    ingest.run_incremental(cfg3, con, _Stub(), checkpoint=checkpoint)
    assert not checkpoint.exists()
    again = _Stub()
    ingest.run_incremental(cfg3, con, again, checkpoint=checkpoint)
    assert len(again.calls) == 6


def test_without_a_checkpoint_a_rerun_starts_over(con, cfg3):
    """The default is unchanged: `make incremental` always fetches every pair."""
    _seed_recent_watermarks(con, cfg3)
    with pytest.raises(EIAError):
        ingest.run_incremental(cfg3, con, _Stub(exc=EXHAUSTED, fail_at=2))
    again = _Stub()
    ingest.run_incremental(cfg3, con, again)
    assert len(again.calls) == 6


def test_checkpoint_lists_a_pair_only_when_all_its_windows_loaded(con, cfg, tmp_path):
    """A catch-up that dies between month windows is not a finished pair."""
    for route in ("region_data", "fuel_mix"):
        db.set_watermark(con, route, "CISO", datetime(2026, 2, 10))
    checkpoint = tmp_path / "ingest-checkpoint"
    with pytest.raises(EIAError):  # February loads, March fails
        ingest.run_incremental(cfg, con, _Stub(exc=EXHAUSTED, fail_at=2),
                               checkpoint=checkpoint)
    assert not checkpoint.exists()

    resumed = _Stub()
    ingest.run_incremental(cfg, con, resumed, checkpoint=checkpoint)
    # Re-entered at its own watermark (the end of February), not skipped.
    lookback = timedelta(hours=cfg.incremental_lookback_hours)
    assert _pairs(resumed)[0] == "region_data/CISO"
    assert _p(resumed.calls[0][2]) == datetime(2026, 2, 28, 23) - lookback


def test_main_checkpoint_resumes_across_invocations(tmp_path, monkeypatch):
    """What the nightly's retry loop does: the same command, three times."""
    path = tmp_path / "w.duckdb"
    monkeypatch.setenv("GRID_DB_PATH", str(path))
    monkeypatch.setenv("EIA_API_KEY", "dummy")
    cfg = load_config()
    c = db.connect(str(path))
    _seed_recent_watermarks(c, cfg)
    c.close()
    n_pairs = 2 * len(cfg.balancing_authorities)

    clients = [_Stub(exc=EXHAUSTED, fail_at=4), _Stub(exc=EXHAUSTED, fail_at=1), _Stub()]
    queue = iter(clients)
    monkeypatch.setattr(ingest, "EIAClient", lambda **kw: next(queue))
    checkpoint = tmp_path / "not-created-yet" / "ingest-checkpoint"
    argv = ["--mode", "incremental", "--checkpoint", str(checkpoint)]

    assert [ingest.main(argv) for _ in clients] == [4, 4, 0]
    # Three pairs, then the fourth fails twice, then the fourth and the rest:
    # every pair is fetched successfully exactly once.
    assert [len(client.calls) for client in clients] == [4, 1, n_pairs - 3]
    assert _pairs(clients[1]) == [_pairs(clients[0])[3]] == [_pairs(clients[2])[0]]
    assert not checkpoint.exists()


def test_checkpoint_flag_is_incremental_only(tmp_path):
    with pytest.raises(SystemExit) as err:
        ingest.main(["--mode", "backfill", "--checkpoint", str(tmp_path / "c")])
    assert err.value.code == 2
    assert not (tmp_path / "c").exists()


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
