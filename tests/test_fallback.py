"""The fallback: loading from EIA's bulk file on a night the API is silent.

The scene, built from the same verbatim excerpt as test_bulk.py: the last
good API night left CISO with demand through Nov 25 23:00 UTC and with
generation, interchange and fuel through local midnight (08:00 UTC). Then
the API stops answering, and the file written on Nov 27 has every hour
through Nov 27 08:00.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from pathlib import Path

import pytest

from grid_pipeline import db, ingest
from grid_pipeline.bulk import BulkClient, BulkFileMissing, BulkFormatError
from grid_pipeline.client import PERIOD_FORMAT, EIAError, UnavailableError, parse_row
from grid_pipeline.config import load_config
from grid_pipeline.ingest import ContaminationError, guard_against_contamination
from grid_pipeline.provenance import data_provenance

EXCERPT = (Path(__file__).parent / "fixtures" / "eia930_balance_excerpt.csv").read_text(
    encoding="utf-8")
WRITTEN = datetime(2026, 11, 27, 15, 42, 6)
REGION = "electricity/rto/region-data"
FUEL = "electricity/rto/fuel-type-data"
DEMAND_EDGE = "2026-11-25T23"  # the region watermark: demand arrives hourly
DAILY_EDGE = "2026-11-25T08"   # generation, interchange and fuel arrive daily
SUMMARY = ("Loaded 561 rows for 2 of 2 pairs from EIA's bulk file (written "
           "2026-11-27 15:42 UTC), through 2026-11-27T08 UTC. The API's own rows "
           "replace them on the next night it answers.")


def _p(period: str) -> datetime:
    return datetime.strptime(period, PERIOD_FORMAT)


def _bulk(text: str = EXCERPT, respondents=("CISO",)) -> BulkClient:
    def fetch(url: str):
        if "EIA930_BALANCE_2026_Jul_Dec.csv" not in url:
            raise BulkFileMissing(url)
        return text, WRITTEN

    return BulkClient(respondents, fetch=fetch)


def _file_rows(route: str, key: str, start: str, end: str):
    return [parse_row(r, key) for r in _bulk().iter_rows(
        route, facets={"respondent": ["CISO"]}, start=start, end=end)]


def _seed_last_good_night(con, demand_edge: str = DEMAND_EDGE, daily_edge: str = DAILY_EDGE):
    region = [r for r in _file_rows(REGION, "type", "2026-11-25T00", demand_edge)
              if r.series == "D" or r.period_utc <= _p(daily_edge)]
    fuel = _file_rows(FUEL, "fueltype", "2026-11-25T00", daily_edge)
    db.upsert_rows(con, "region_data", region, source="eia_api")
    db.upsert_rows(con, "fuel_mix", fuel, source="eia_api")
    db.set_watermark(con, "region_data", "CISO", _p(demand_edge))
    db.set_watermark(con, "fuel_mix", "CISO", _p(daily_edge))


def _by_source(con, table: str) -> dict[str, int]:
    return dict(con.execute(f"SELECT _source, count(*) FROM raw.{table} GROUP BY 1").fetchall())


def _snapshot(con, source: str):
    return con.execute(
        "SELECT 'r', respondent, period_utc, metric_code, value, _ingested_at "
        "FROM raw.eia_region_data WHERE _source = ? UNION ALL "
        "SELECT 'f', respondent, period_utc, fuel_code, value, _ingested_at "
        "FROM raw.eia_fuel_mix WHERE _source = ? ORDER BY ALL", [source, source]).fetchall()


def _watermarks(con):
    return con.execute("SELECT route_key, respondent, max_period FROM raw._ingest_watermarks "
                       "ORDER BY ALL").fetchall()


@pytest.fixture()
def cfg():
    return dataclasses.replace(load_config(), balancing_authorities=["CISO"])


@pytest.fixture()
def con(tmp_path):
    c = db.connect(str(tmp_path / "t.duckdb"))
    yield c
    c.close()


# --- what it loads -----------------------------------------------------------

def test_fallback_adds_only_what_the_warehouse_lacks(con, cfg):
    _seed_last_good_night(con)
    api_before, marks_before = _snapshot(con, "eia_api"), _watermarks(con)
    assert _by_source(con, "eia_region_data") == {"eia_api": 24 + 9 + 9}

    assert ingest.run_fallback(cfg, con, _bulk()) == SUMMARY

    # Demand from the hour after its edge, the daily series from theirs:
    # the hole between the two edges is filled too.
    assert _by_source(con, "eia_region_data") == {"eia_api": 42, "eia_bulk": 33 + 48 + 48}
    assert _by_source(con, "eia_fuel_mix") == {"eia_api": 9 * 9, "eia_bulk": 48 * 9}
    newest = dict(con.execute("SELECT metric_code, max(period_utc) FROM raw.eia_region_data "
                              "GROUP BY 1").fetchall())
    assert set(newest.values()) == {_p("2026-11-27T08")}
    # What the API delivered is exactly as it was, and so is every watermark:
    # the API's next answer starts from the same point as if this never ran.
    assert _snapshot(con, "eia_api") == api_before
    assert _watermarks(con) == marks_before
    assert data_provenance(con) == "real"
    assert db.get_load_meta(con, "last_fallback_at") is not None
    assert db.get_load_meta(con, "last_incremental_at") is None


def test_an_api_row_is_never_overwritten(con, cfg):
    _seed_last_good_night(con)
    # The API's value for this hour differs from the file's (a restatement
    # the file's older snapshot does not have).
    con.execute("UPDATE raw.eia_region_data SET value = 99999 WHERE metric_code = 'D' "
                "AND period_utc = TIMESTAMP '2026-11-25 23:00'")
    ingest.run_fallback(cfg, con, _bulk())
    assert con.execute("SELECT value, _source FROM raw.eia_region_data WHERE metric_code = 'D' "
                       "AND period_utc = TIMESTAMP '2026-11-25 23:00'").fetchone() == (
        99999.0, "eia_api")


def test_a_newer_file_corrects_what_an_older_one_loaded(con, cfg):
    _seed_last_good_night(con)
    ingest.run_fallback(cfg, con, _bulk())
    hour = "TIMESTAMP '2026-11-26 08:00'"
    old = con.execute(f"SELECT value FROM raw.eia_region_data WHERE metric_code = 'D' "
                      f"AND period_utc = {hour}").fetchone()[0]
    line = next(ln for ln in EXCERPT.splitlines()
                if ln.startswith("CISO,") and ",11/26/2026 8:00:00 AM," in ln)
    assert f",{old:.0f}," in line
    revised = EXCERPT.replace(line, line.replace(f",{old:.0f},", ",12345,"))
    api_before = _snapshot(con, "eia_api")

    assert ingest.run_fallback(cfg, con, _bulk(revised)) == SUMMARY  # the same rows again

    assert con.execute(f"SELECT value, _source FROM raw.eia_region_data WHERE metric_code = 'D' "
                       f"AND period_utc = {hour}").fetchone() == (12345.0, "eia_bulk")
    assert _snapshot(con, "eia_api") == api_before
    assert _by_source(con, "eia_region_data") == {"eia_api": 42, "eia_bulk": 129}


def test_nothing_newer_in_the_file_is_said_plainly(con, cfg):
    # A night whose API run succeeded: the warehouse is ahead of the file.
    _seed_last_good_night(con, demand_edge="2026-11-27T08", daily_edge="2026-11-27T08")
    assert ingest.run_fallback(cfg, con, _bulk()) == (
        "EIA's bulk file (written 2026-11-27 15:42 UTC) holds nothing the warehouse lacks.")
    assert "eia_bulk" not in _by_source(con, "eia_region_data")


# --- when the file is not believed --------------------------------------------

def test_a_pair_where_file_and_api_disagree_is_left_alone(con, cfg, caplog):
    _seed_last_good_night(con)
    # As if the file's hours were read one off: every demand value differs.
    con.execute("UPDATE raw.eia_region_data SET value = value + 1 WHERE metric_code = 'D'")
    with caplog.at_level("WARNING"):
        summary = ingest.run_fallback(cfg, con, _bulk())
    assert ("region_data/CISO: left alone. The bulk file and the API agree on only 18 of "
            "the 42 values both have") in caplog.text
    assert summary == (
        "Loaded 432 rows for 1 of 2 pairs from EIA's bulk file (written 2026-11-27 15:42 "
        "UTC), through 2026-11-27T08 UTC. The API's own rows replace them on the next night "
        "it answers. Left alone, because the file could not be confirmed against the API's "
        "rows there: region_data/CISO.")
    assert _by_source(con, "eia_region_data") == {"eia_api": 42}
    assert _by_source(con, "eia_fuel_mix") == {"eia_api": 81, "eia_bulk": 432}


def test_the_agreement_threshold_sits_between_what_was_measured(con, cfg):
    """Nov 27, 2026: 94% agreement at worst when read right, 38% at best when
    read an hour off. The threshold has to keep those two apart."""
    assert 0.38 < ingest.FALLBACK_MIN_AGREEMENT < 0.94
    _seed_last_good_night(con)
    # 2 of 42 values restated since (95%): believed.
    con.execute("UPDATE raw.eia_region_data SET value = value + 1 WHERE metric_code = 'D' "
                "AND period_utc >= TIMESTAMP '2026-11-25 22:00'")
    assert "Left alone" not in ingest.run_fallback(cfg, con, _bulk())


def test_too_few_shared_values_are_not_enough_to_believe_the_file(con, cfg, caplog):
    # Only seven hours of API rows overlap the file: 7 demand values.
    region = _file_rows(REGION, "type", "2026-11-25T00", "2026-11-25T06")
    db.upsert_rows(con, "region_data", [r for r in region if r.series == "D"], source="eia_api")
    db.set_watermark(con, "region_data", "CISO", _p("2026-11-25T06"))
    assert len(region) // 3 < ingest.FALLBACK_MIN_SHARED
    with caplog.at_level("WARNING"), pytest.raises(EIAError, match="could not be confirmed"):
        ingest.run_fallback(cfg, con, _bulk())
    assert "Only 7 values to check the bulk file against, and 24 are needed" in caplog.text
    assert _by_source(con, "eia_region_data") == {"eia_api": 7}


def test_a_file_believed_nowhere_loads_nothing_and_fails(con, cfg):
    _seed_last_good_night(con)
    con.execute("UPDATE raw.eia_region_data SET value = value + 1")
    con.execute("UPDATE raw.eia_fuel_mix SET value = value + 1")
    with pytest.raises(EIAError, match="could not be confirmed against the API's rows"):
        ingest.run_fallback(cfg, con, _bulk())
    assert _snapshot(con, "eia_bulk") == []


def test_a_pair_with_no_watermark_is_not_started_from_the_file(con, cfg):
    assert ingest.run_fallback(cfg, con, _bulk()) == (
        "No pair has a watermark yet, so the bulk file had nothing to fill.")
    assert data_provenance(con) == "empty"


def test_a_file_with_a_changed_layout_loads_nothing(con, cfg):
    _seed_last_good_night(con)
    changed = EXCERPT.replace("from Geothermal (Adjusted)", "from Fusion (Adjusted)")
    with pytest.raises(BulkFormatError):
        ingest.run_fallback(cfg, con, _bulk(changed))
    assert _snapshot(con, "eia_bulk") == []


# --- afterwards: the API answers again ----------------------------------------

class _ApiBack:
    """The API once it answers again: the file's values, plus later demand."""

    def iter_rows(self, route, *, facets=None, start=None, end=None, tiebreak_column=None):
        yield from _bulk().iter_rows(route, facets=facets, start=start, end=end)
        if route.endswith("region-data"):
            yield {"period": "2026-11-27T09", "respondent": "CISO", "type": "D",
                   "value": "21000", "value-units": "megawatthours"}


def test_when_the_api_answers_again_its_rows_replace_the_bulk_rows(con, cfg):
    _seed_last_good_night(con)
    ingest.run_fallback(cfg, con, _bulk())
    assert _by_source(con, "eia_region_data")["eia_bulk"] == 129

    ingest.run_incremental(cfg, con, _ApiBack())

    # Every hour the fallback filled lies inside the lookback the API
    # re-fetches, because the fallback moved no watermark.
    assert _by_source(con, "eia_region_data") == {"eia_api": 57 * 3 + 1}
    assert _by_source(con, "eia_fuel_mix") == {"eia_api": 57 * 9}
    assert db.get_watermark(con, "region_data", "CISO") == _p("2026-11-27T09")
    assert db.get_watermark(con, "fuel_mix", "CISO") == _p("2026-11-27T08")


# --- provenance: bulk rows are real rows --------------------------------------

def test_bulk_rows_count_as_real_and_never_mix_with_fixtures(con, cfg):
    _seed_last_good_night(con)
    ingest.run_fallback(cfg, con, _bulk())
    assert db.warehouse_sources(con) == {"eia_api", "eia_bulk"}
    assert data_provenance(con) == "real"
    for mode in ("incremental", "backfill", "fallback"):
        guard_against_contamination(con, mode, "md:energy_grid")  # no refusal
    with pytest.raises(ContaminationError):
        guard_against_contamination(con, "fixtures", "data/dev_fixtures.duckdb")
    db.upsert_rows(con, "region_data", _file_rows(REGION, "type", DEMAND_EDGE, DEMAND_EDGE),
                   source="dev_fixtures")
    assert data_provenance(con) == "mixed"
    with pytest.raises(ContaminationError):
        guard_against_contamination(con, "fallback", "md:energy_grid")


def test_a_warehouse_of_bulk_rows_alone_is_real(con):
    db.upsert_rows(con, "fuel_mix", _file_rows(FUEL, "fueltype", DAILY_EDGE, DAILY_EDGE),
                   source="eia_bulk")
    assert data_provenance(con) == "real"


# --- the command line ---------------------------------------------------------

@pytest.fixture()
def cli(tmp_path, monkeypatch):
    """main() against a seeded warehouse, with EIA replaced by the excerpt."""
    path = tmp_path / "w.duckdb"
    c = db.connect(str(path))
    _seed_last_good_night(c)
    c.close()
    monkeypatch.setenv("GRID_DB_PATH", str(path))
    monkeypatch.delenv("EIA_API_KEY", raising=False)  # the fallback needs no key
    monkeypatch.setattr(ingest, "gateway_status", lambda base_url: "HTTP 403 in 0.0s")
    monkeypatch.setattr(ingest, "get_api_key",
                        lambda: pytest.fail("the fallback must not ask for the API key"))

    def use(text: str = EXCERPT, error: Exception | None = None):
        def fetch(url: str):
            if error:
                raise error
            if "EIA930_BALANCE_2026_Jul_Dec.csv" not in url:
                raise BulkFileMissing(url)
            return text, WRITTEN

        monkeypatch.setattr(ingest, "BulkClient",
                            lambda respondents: BulkClient(respondents, fetch=fetch))
        return path

    return use


def test_cli_prints_one_line_the_nightly_can_quote(cli, capsys, caplog):
    path = cli()
    with caplog.at_level("INFO"):
        assert ingest.main(["--mode", "fallback"]) == 0
    assert capsys.readouterr().out == SUMMARY + "\n"
    assert "EIA's API, asked without a key: HTTP 403 in 0.0s" in caplog.text
    con = db.connect(str(path))
    assert _by_source(con, "eia_region_data") == {"eia_api": 42, "eia_bulk": 129}
    con.close()


@pytest.mark.parametrize("text,error,code", [
    (EXCERPT, UnavailableError("EIA's bulk file could not be downloaded in 3 tries"), 5),
    (EXCERPT, EIAError("EIA's bulk file could not be downloaded in 3 tries (HTTP 403)"), 4),
    (EXCERPT.replace("from Geothermal (Adjusted)", "from Fusion (Adjusted)"), None, 4),
])
def test_cli_exit_codes_when_the_file_cannot_help(cli, capsys, text, error, code):
    path = cli(text, error)
    assert ingest.main(["--mode", "fallback"]) == code
    assert capsys.readouterr().out == ""  # nothing for the nightly to quote
    con = db.connect(str(path))
    assert _by_source(con, "eia_region_data") == {"eia_api": 42}
    con.close()


def test_cli_checkpoint_belongs_to_incremental_only(cli, tmp_path):
    cli()
    with pytest.raises(SystemExit):
        ingest.main(["--mode", "fallback", "--checkpoint", str(tmp_path / "cp")])
