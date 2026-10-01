"""The BigQuery publish script: retries, source guards, touched partitions
and the escalation to one full replace.

scripts/ is not a package, so the module is loaded from its file path. No
Cloud SDK, network or credentials are needed: subprocess and sleep are stubbed.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

from grid_pipeline import db
from grid_pipeline.client import EIARow

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "export_to_cloud.py"


@pytest.fixture()
def export(monkeypatch):
    spec = importlib.util.spec_from_file_location("export_to_cloud", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve string annotations through sys.modules.
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _fake_run(codes: list[int], calls: list):
    def run(cmd, *args, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, codes.pop(0))
    return run


def test_publish_call_retries_then_succeeds(export, monkeypatch):
    calls, sleeps = [], []
    monkeypatch.setattr(export.subprocess, "run", _fake_run([1, 1, 0], calls))
    monkeypatch.setattr(export.time, "sleep", sleeps.append)
    export.run(["bq", "load"], dry_run=False)
    assert len(calls) == 3
    assert sleeps == [10, 30]


def test_publish_call_gives_up_after_three_attempts(export, monkeypatch):
    calls = []
    monkeypatch.setattr(export.subprocess, "run", _fake_run([1, 1, 1], calls))
    monkeypatch.setattr(export.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit, match="after 3 attempts"):
        export.run(["bq", "load"], dry_run=False)
    assert len(calls) == 3


def _warehouse(tmp_path, source: str):
    con = db.connect(str(tmp_path / "w.duckdb"))
    db.upsert_rows(con, "region_data",
                   [EIARow(datetime(2026, 9, 1, 7), "CISO", "D", 1.0, "megawatthours")],
                   source=source)
    return con


def test_refuses_fixture_data_but_dry_run_is_allowed(export, tmp_path):
    con = _warehouse(tmp_path, "dev_fixtures")
    with pytest.raises(SystemExit, match="refusing to publish fixtures"):
        export.refuse_unsafe_source(con, "md:energy_grid", dry_run=False, allow_local=False)
    export.refuse_unsafe_source(con, "md:energy_grid", dry_run=True, allow_local=False)


def test_refuses_a_local_warehouse_unless_allowed(export, tmp_path):
    con = _warehouse(tmp_path, "eia_api")
    with pytest.raises(SystemExit, match="local warehouse"):
        export.refuse_unsafe_source(con, "data/energy_grid.duckdb",
                                    dry_run=False, allow_local=False)
    export.refuse_unsafe_source(con, "data/energy_grid.duckdb",
                                dry_run=False, allow_local=True)
    export.refuse_unsafe_source(con, "md:energy_grid", dry_run=False, allow_local=False)


def test_touched_partitions_finds_older_days_a_recent_ingest_rewrote(export, tmp_path):
    con = _warehouse(tmp_path, "eia_api")  # ingested "now", so it counts as recent
    con.execute("""create table main.fct_grid_hourly as
                   select 'CISO' as ba_code, timestamp '2026-09-01 07:00' as period_utc,
                          date '2026-09-01' as local_date
                   union all
                   select 'CISO', timestamp '2026-09-30 07:00', date '2026-09-30'""")
    fct = next(m for m in export.MARTS if m.name == "fct_grid_hourly")
    assert export.touched_partitions(con, fct, before=date(2026, 9, 26)) == [date(2026, 9, 1)]
    # Nothing older than the window was re-ingested -> nothing extra to send.
    assert export.touched_partitions(con, fct, before=date(2026, 9, 1)) == []


def test_a_replay_escalates_to_one_full_replace(export, tmp_path, monkeypatch, capsys):
    """Hundreds of touched days must not become hundreds of per-day load jobs."""
    con = _warehouse(tmp_path, "eia_api")
    con.execute("""create table main.fct_grid_hourly as
                   select 'CISO' as ba_code, timestamp '2026-09-01 07:00' as period_utc,
                          date '2026-09-01' as local_date
                   union all
                   select 'CISO', timestamp '2026-09-30 07:00', date '2026-09-30'""")
    con.close()
    monkeypatch.setattr(export, "DB_PATH", str(tmp_path / "w.duckdb"))
    monkeypatch.setattr(export, "MAX_TOUCHED_PARTITIONS", 0)  # 1 touched day > 0
    monkeypatch.setattr(sys, "argv", ["export_to_cloud.py", "--mode", "incremental",
                                      "--dry-run", "--tables", "fct_grid_hourly"])
    assert export.main() == 0
    out = capsys.readouterr().out
    assert "one full replace instead" in out
    assert "--time_partitioning_field=local_date" in out  # the whole-table path
    assert "fct_grid_hourly$" not in out                   # no per-day loads
