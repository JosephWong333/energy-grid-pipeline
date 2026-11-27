"""The nightly's ingest step: what it retries, and how a bad night ends.

These tests run the step's actual shell from .github/workflows/nightly.yml,
so the policy the workflow documents is the one it executes: first against a
stub `python` that returns scripted exit codes, then against the real ingest
with a scripted EIA that fails the way EIA did on Nov 25 and on Nov 26, 2026.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from grid_pipeline import bulk, db
from grid_pipeline.bulk import BulkClient
from grid_pipeline.client import PERIOD_FORMAT, parse_row
from grid_pipeline.config import REPO_ROOT, load_config

STEP_NAME = "Incremental ingest (EIA API -> MotherDuck)"
RESUMED = "::warning title=Ingest resumed::"
BULK_USED = "::warning title=EIA API silent - bulk file used::"
NO_NEW_DATA = "::warning title=EIA API silent - no new data::"
DRILL = "::notice title=Bulk file drill::"
FALLBACK = "-m grid_pipeline.ingest --mode fallback"
STUB_SUMMARY = "Loaded 5 rows from the stub file."
EXCERPT = Path(__file__).parent / "fixtures" / "eia930_balance_excerpt.csv"

pytestmark = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None,
    reason="runs the workflow's bash; GitHub's Linux runner is the target",
)


def _workflow() -> dict:
    path = REPO_ROOT / ".github" / "workflows" / "nightly.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _refresh_job() -> dict:
    return _workflow()["jobs"]["refresh"]


def _ingest_step() -> dict:
    return next(s for s in _refresh_job()["steps"] if s.get("name") == STEP_NAME)


ATTEMPTS = int(_ingest_step()["env"]["INGEST_ATTEMPTS"])


def _incremental(tmp_path) -> str:
    # Every attempt is the same command with the same checkpoint file, which
    # lives in the job's temp folder: a fresh job can never inherit one.
    return ("-m grid_pipeline.ingest --mode incremental "
            f"--checkpoint {tmp_path}/ingest-checkpoint")


def _run_step_with(tmp_path, stub_body: str, extra_env: dict[str, str] | None = None):
    """Run the step's shell with `python` replaced by a script of ours."""
    step = _ingest_step()
    stub = tmp_path / "bin" / "python"
    stub.parent.mkdir()
    stub.write_text("#!/usr/bin/env bash\n" + stub_body)
    stub.chmod(0o755)
    env = {
        "PATH": f"{stub.parent}{os.pathsep}{os.environ['PATH']}",
        "RUNNER_TEMP": str(tmp_path),  # GitHub sets it: a fresh folder per job
        "INGEST_ATTEMPTS": step["env"]["INGEST_ATTEMPTS"],
        "INGEST_RETRY_WAIT_SECONDS": "0",  # the policy is under test, not the pause
        "INGEST_SOURCE": "api",  # what the workflow passes on a scheduled run
        **(extra_env or {}),
    }
    # GitHub runs `run:` blocks with `bash -e`.
    return subprocess.run(["bash", "-e", "-c", step["run"]], env=env,
                          capture_output=True, text=True, timeout=120)


def _run_step(tmp_path, exit_codes: list[int], source: str = "api"):
    """Run the step with a stub `python` that exits with each code in turn.

    Like the real fallback, the stub prints its one summary line only when
    it succeeds.
    """
    calls = tmp_path / "calls.txt"
    queue = tmp_path / "queue.txt"
    calls.touch()
    queue.write_text("".join(f"{code}\n" for code in exit_codes))
    proc = _run_step_with(
        tmp_path,
        f'echo "$*" >> "{calls}"\n'
        f'code=$(head -n 1 "{queue}")\n'
        f'tail -n +2 "{queue}" > "{queue}.next" && mv "{queue}.next" "{queue}"\n'
        f'case "$*" in *"--mode fallback"*) [ "$code" = "0" ] && echo "{STUB_SUMMARY}";; esac\n'
        # 97: the step ran more often than the test scripted.
        'exit "${code:-97}"\n',
        {"INGEST_SOURCE": source},
    )
    return proc, calls.read_text().splitlines()


# --- the resume loop -----------------------------------------------------------

@pytest.mark.parametrize("exit_codes,expected,resumes", [
    ([0], 0, 0),         # a normal night: one run, no noise
    ([4, 0], 0, 1),      # one bad spell: resumes, ends green, and says so
    ([5, 0], 0, 1),      # the same when EIA simply did not answer
    ([5, 4, 0], 0, 2),
    ([4] * (ATTEMPTS - 1) + [0], 0, ATTEMPTS - 1),  # green on the last attempt
    ([4] * ATTEMPTS, 4, ATTEMPTS - 1),              # EIA keeps saying no: red
    ([5] * (ATTEMPTS - 1) + [4], 4, ATTEMPTS - 1),  # the last attempt decides
    ([2], 2, 0),         # contamination guard: never retried
    ([3], 3, 0),         # EIA asked for a long wait: tomorrow's run resumes
    ([1], 1, 0),         # a crash is a bug, not a slow spell
    ([4, 2], 2, 1),      # a resume that hits a non-resumable error stops
    ([5, 1], 1, 1),
])
def test_ingest_step_resumes_only_on_exit_4_and_5(tmp_path, exit_codes, expected, resumes):
    proc, calls = _run_step(tmp_path, exit_codes)
    assert proc.returncode == expected, proc.stderr
    assert calls == [_incremental(tmp_path)] * len(exit_codes)  # the fallback never ran
    assert proc.stdout.count(RESUMED) == resumes
    assert proc.stdout.count("::warning") == resumes  # and nothing else is flagged
    assert "::notice" not in proc.stdout


# --- the API never answered ----------------------------------------------------

@pytest.mark.parametrize("exit_codes", [
    [5] * ATTEMPTS,              # Nov 26: the API answered nothing, attempt after attempt
    [4] * (ATTEMPTS - 1) + [5],  # the last attempt decides
])
def test_a_silent_api_is_answered_from_the_bulk_file_and_the_step_ends_green(tmp_path,
                                                                             exit_codes):
    """An outage upstream is not this pipeline failing. The step loads what
    it can from EIA's bulk file, quotes what that did, and ends green."""
    proc, calls = _run_step(tmp_path, exit_codes + [0])
    assert proc.returncode == 0, proc.stderr
    # It still asked the API as often as it is allowed to, and only then the file.
    assert calls == [_incremental(tmp_path)] * ATTEMPTS + [FALLBACK]
    assert proc.stdout.count(RESUMED) == ATTEMPTS - 1
    warning = next(ln for ln in proc.stdout.splitlines() if ln.startswith(BULK_USED))
    assert f"did not answer in {ATTEMPTS} attempts. {STUB_SUMMARY}" in warning
    assert proc.stdout.count("::warning") == ATTEMPTS
    assert NO_NEW_DATA not in proc.stdout


@pytest.mark.parametrize("fallback_exit", [5, 4, 2, 1])
def test_when_the_bulk_file_cannot_help_the_step_still_ends_green_and_says_so(tmp_path,
                                                                              fallback_exit):
    """Whatever stops the fallback, the night is no worse than without one:
    the build, the publish and the freshness check run on what is there."""
    proc, calls = _run_step(tmp_path, [5] * ATTEMPTS + [fallback_exit])
    assert proc.returncode == 0, proc.stderr
    assert calls == [_incremental(tmp_path)] * ATTEMPTS + [FALLBACK]
    warning = next(ln for ln in proc.stdout.splitlines() if ln.startswith(NO_NEW_DATA))
    assert f"(exit code {fallback_exit})" in warning
    assert "missing or incomplete" in warning
    assert BULK_USED not in proc.stdout and STUB_SUMMARY not in proc.stdout
    assert proc.stdout.count("::warning") == ATTEMPTS


# --- the drill -----------------------------------------------------------------

def test_the_drill_runs_only_the_fallback_and_fails_if_the_fallback_does(tmp_path):
    proc, calls = _run_step(tmp_path, [0], source="bulk-file")
    assert proc.returncode == 0, proc.stderr
    assert calls == [FALLBACK]  # the API is not asked at all
    assert f"{DRILL}{STUB_SUMMARY}" in proc.stdout
    assert "::warning" not in proc.stdout

    other = tmp_path / "failing"
    other.mkdir()
    proc, calls = _run_step(other, [4], source="bulk-file")
    assert proc.returncode == 4  # a drill that fails must be red
    assert calls == [FALLBACK]
    assert "::notice" not in proc.stdout


def test_the_drill_is_a_manual_choice_and_a_scheduled_run_reads_the_api():
    triggers = _workflow().get("on") or _workflow()[True]  # YAML reads a bare `on` as True
    choice = triggers["workflow_dispatch"]["inputs"]["ingest_source"]
    assert choice["type"] == "choice"
    assert choice["options"] == ["api", "bulk-file"] and choice["default"] == "api"
    # A scheduled run has no inputs, so the expression has to fall back to the API.
    assert _ingest_step()["env"]["INGEST_SOURCE"] == "${{ inputs.ingest_source || 'api' }}"


# --- what still fails a night, and the time it all takes -------------------------

def test_old_data_still_fails_a_night_that_ended_soft():
    """The soft ending is only safe because the age of the data is still
    judged: an outage has a deadline. Both judges run after the ingest and
    can fail the job. The build's coverage test counts each BA's usable
    hours against the clock (it is the one that fails first, a little over
    two days in), and source freshness has an error threshold."""
    steps = _refresh_job()["steps"]
    names = [s.get("name") for s in steps]
    ingest, build, freshness = (names.index(n) for n in
                                (STEP_NAME, "dbt build (prod)", "Source freshness"))
    assert ingest < build < freshness
    assert "dbt build" in steps[build]["run"] and "if" not in steps[build]
    assert "dbt source freshness" in steps[freshness]["run"]
    assert "steps.build.outcome == 'success'" in steps[freshness]["if"]
    assert not any(s.get("continue-on-error") for s in steps)

    dbt = REPO_ROOT / "dbt"
    coverage = " ".join((dbt / "tests" / "assert_all_seeded_bas_reporting.sql")
                        .read_text(encoding="utf-8").split())
    assert "severity='error'" in coverage
    assert "period_utc >= timezone('UTC', now()) - interval '72 hours'" in coverage
    sources = yaml.safe_load(
        (dbt / "models" / "staging" / "sources.yml").read_text(encoding="utf-8"))
    sla = sources["sources"][0]["freshness"]
    assert sla["warn_after"]["period"] == sla["error_after"]["period"] == "hour"
    assert 0 < sla["warn_after"]["count"] < sla["error_after"]["count"]


def test_resume_pauses_fit_inside_the_job_timeout():
    """The pauses alone must leave most of the job's budget for the work."""
    job, env = _refresh_job(), _ingest_step()["env"]
    attempts, wait = int(env["INGEST_ATTEMPTS"]), int(env["INGEST_RETRY_WAIT_SECONDS"])
    assert attempts >= 2
    assert (attempts - 1) * wait <= job["timeout-minutes"] * 60 / 4


def test_a_dead_eia_ends_well_inside_the_job_timeout():
    """Worst case: every attempt spends the client's whole retry budget on its
    first request, and then the bulk file's server hangs too, on two files
    (a window across Jan 1 or Jul 1). The step must reach its own ending and
    leave time for the build, the publish and the freshness check, not be
    cut off by the job timeout, so the client's tries, the attempts, the
    pauses and the download are budgeted together."""
    job, env = _refresh_job(), _ingest_step()["env"]
    attempts, wait = int(env["INGEST_ATTEMPTS"]), int(env["INGEST_RETRY_WAIT_SECONDS"])
    eia = load_config().eia
    backoff = sum(eia.backoff_base_seconds * 2**n for n in range(eia.max_retries - 1))
    one_request = eia.max_retries * eia.request_timeout_seconds + backoff
    one_file = (bulk.DOWNLOAD_TRIES * sum(bulk.DOWNLOAD_TIMEOUT)
                + (bulk.DOWNLOAD_TRIES - 1) * bulk.DOWNLOAD_RETRY_SECONDS)
    worst = attempts * one_request + (attempts - 1) * wait + 2 * one_file
    assert worst <= job["timeout-minutes"] * 60 / 2


# --- the real ingest, with a scripted EIA ----------------------------------------

# Stands in for `python -m grid_pipeline.ingest ...`: the real CLI and the real
# warehouse code. The API client follows a script, and the bulk file is the
# verbatim excerpt in tests/fixtures instead of a download.
_SCRIPTED_INGEST = '''
import json
import pathlib
import sys
from datetime import datetime

from grid_pipeline import ingest
from grid_pipeline.bulk import BulkClient, BulkFileMissing
from grid_pipeline.client import UnavailableError

state_path, argv = pathlib.Path(sys.argv[1]), sys.argv[2:]


class ScriptedEIA:
    """Answers like the API, except on the request numbers in `fail_on`
    (counted across every attempt; "all" means every request), where it
    fails like a client that ran out of tries on timeouts."""

    def __init__(self, **kwargs):
        pass

    def iter_rows(self, route, *, facets=None, start=None, end=None, tiebreak_column=None):
        ba = facets["respondent"][0]
        state = json.loads(state_path.read_text())
        state["requests"].append(f"{route.rsplit('/', 1)[-1]}/{ba}")
        state_path.write_text(json.dumps(state))
        if state["fail_on"] == "all" or len(state["requests"]) in state["fail_on"]:
            raise UnavailableError("EIA API still failing after 7 attempts (ReadTimeout)")
        key = "type" if "region-data" in route else "fueltype"
        yield {"period": end, "respondent": ba, key: "D" if key == "type" else "SUN",
               "value": "1", "value-units": "megawatthours"}


def excerpt(url):
    if "EIA930_BALANCE_2026_Jul_Dec.csv" not in url:
        raise BulkFileMissing(url)
    text = pathlib.Path(json.loads(state_path.read_text())["excerpt"]).read_text(encoding="utf-8")
    return text, datetime(2026, 11, 27, 15, 42, 6)


ingest.EIAClient = ScriptedEIA
ingest.BulkClient = lambda respondents: BulkClient(respondents, fetch=excerpt)
ingest.gateway_status = lambda base_url: "HTTP 403 in 0.0s"
sys.exit(ingest.main(argv))
'''


def _seed_ciso_as_the_last_good_night_left_it(con) -> None:
    """Demand through Nov 25 23:00 UTC; generation, interchange and fuel
    through local midnight (08:00 UTC). All of it from the API."""
    file = BulkClient(["CISO"], fetch=lambda url: (EXCERPT.read_text(encoding="utf-8"), None))
    edges = {"region_data": "2026-11-25T23", "fuel_mix": "2026-11-25T08"}
    for route_key, path, key in (("region_data", "electricity/rto/region-data", "type"),
                                 ("fuel_mix", "electricity/rto/fuel-type-data", "fueltype")):
        rows = [parse_row(r, key) for r in file.iter_rows(
            path, facets={"respondent": ["CISO"]}, start="2026-11-25T00", end=edges[route_key])]
        daily_edge = datetime.strptime(edges["fuel_mix"], PERIOD_FORMAT)
        rows = [r for r in rows if r.series == "D" or r.period_utc <= daily_edge]
        db.upsert_rows(con, route_key, rows, source="eia_api")
        db.set_watermark(con, route_key, "CISO",
                         datetime.strptime(edges[route_key], PERIOD_FORMAT))


def _run_scripted(tmp_path, fail_on, source: str = "api", ciso_from_november: bool = False):
    """Run the step against the real ingest with the scripted EIA.

    Returns the finished process, the requests the API client saw, how many
    watermarks moved, the configured BAs, and the warehouse's path.
    """
    warehouse = tmp_path / "w.duckdb"
    bas = load_config().balancing_authorities
    # A normal night: every pair a few hours behind, so one request each.
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    seeded = now - timedelta(hours=5)
    con = db.connect(str(warehouse))
    for route in ("region_data", "fuel_mix"):
        for ba in bas:
            if not (ciso_from_november and ba == "CISO"):
                db.set_watermark(con, route, ba, seeded)
    if ciso_from_november:
        _seed_ciso_as_the_last_good_night_left_it(con)
    marks = con.execute("SELECT * FROM raw._ingest_watermarks ORDER BY ALL").fetchall()
    con.close()

    state = tmp_path / "state.json"
    state.write_text(json.dumps({"requests": [], "fail_on": fail_on, "excerpt": str(EXCERPT)}))
    driver = tmp_path / "scripted_ingest.py"
    driver.write_text(_SCRIPTED_INGEST)
    proc = _run_step_with(
        tmp_path,
        'shift 2  # drop "-m grid_pipeline.ingest"\n'
        f'exec "{sys.executable}" "{driver}" "{state}" "$@"\n',
        {"GRID_DB_PATH": str(warehouse), "EIA_API_KEY": "dummy", "INGEST_SOURCE": source},
    )

    requests = json.loads(state.read_text())["requests"]
    con = db.connect(str(warehouse))
    after = con.execute("SELECT * FROM raw._ingest_watermarks ORDER BY ALL").fetchall()
    con.close()
    moved = sum(a[2] != b[2] for a, b in zip(after, marks, strict=True))
    return proc, requests, moved, bas, warehouse


def _bulk_rows(warehouse) -> dict[str, int]:
    con = db.connect(str(warehouse))
    try:
        return dict(con.execute(
            "SELECT respondent, count(*) FROM (SELECT respondent, _source FROM "
            "raw.eia_region_data UNION ALL SELECT respondent, _source FROM raw.eia_fuel_mix) "
            "WHERE _source = 'eia_bulk' GROUP BY 1").fetchall())
    finally:
        con.close()


def test_nov_25_failure_pattern_completes_because_each_attempt_resumes(tmp_path):
    """Nov 25, 2026: three attempts, and each died on one exhausted request.

    Every attempt had started again at the first pair, so the three failures
    used up the whole budget. With the checkpoint the same three failures
    cost one request each: no pair is fetched twice and the night completes.
    """
    fail_on = [3, 4, 16]  # the third pair, the same pair again, then a later one
    proc, requests, moved, bas, warehouse = _run_scripted(tmp_path, fail_on)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.count(RESUMED) == len(fail_on)
    # It finished from the API: nothing to excuse, and the file was not needed.
    assert proc.stdout.count("::warning") == len(fail_on)
    assert _bulk_rows(warehouse) == {}
    answered = [r for n, r in enumerate(requests, start=1) if n not in fail_on]
    n_pairs = 2 * len(bas)
    assert len(requests) == n_pairs + len(fail_on)
    assert len(answered) == len(set(answered)) == n_pairs  # each pair exactly once
    assert not (tmp_path / "ingest-checkpoint").exists()
    assert moved == n_pairs  # every pair's watermark advanced


def test_nov_26_failure_pattern_ends_green_on_the_bulk_file(tmp_path):
    """Nov 26-27, 2026: the API answered nothing. Three runs of five
    attempts, and every attempt died on its first request.

    Now the step turns to EIA's bulk file. CISO is seeded as the last good
    night left it, so the file can be checked against the API's rows and
    its newer hours are loaded. The other BAs have no API rows to check the
    file against here, so they are left alone. No watermark moves either
    way: the API's next answer starts where it would have without this.
    """
    proc, requests, moved, bas, warehouse = _run_scripted(
        tmp_path, "all", ciso_from_november=True)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert requests == [f"region-data/{bas[0]}"] * ATTEMPTS  # the first pair, every time
    assert proc.stdout.count(RESUMED) == ATTEMPTS - 1
    warning = next(ln for ln in proc.stdout.splitlines() if ln.startswith(BULK_USED))
    assert (f"did not answer in {ATTEMPTS} attempts. Loaded 561 rows for 2 of {2 * len(bas)} "
            "pairs from EIA's bulk file (written 2026-11-27 15:42 UTC), through "
            "2026-11-27T08 UTC.") in warning
    assert f"and {2 * len(bas) - 2 - 4} more." in warning  # the pairs left alone
    assert _bulk_rows(warehouse) == {"CISO": 561}
    assert moved == 0


def test_nov_26_failure_pattern_without_a_usable_file_still_ends_green(tmp_path):
    """The same night with a warehouse the file cannot be checked against:
    nothing is loaded, the warning says so, and the step is green."""
    proc, requests, moved, bas, warehouse = _run_scripted(tmp_path, "all")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert len(requests) == ATTEMPTS
    assert proc.stdout.count(NO_NEW_DATA) == 1 and BULK_USED not in proc.stdout
    assert "(exit code 4)" in proc.stdout
    assert _bulk_rows(warehouse) == {} and moved == 0


def test_the_drill_against_the_real_ingest_never_asks_the_api(tmp_path):
    proc, requests, moved, bas, warehouse = _run_scripted(
        tmp_path, "all", source="bulk-file", ciso_from_november=True)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert requests == []
    assert f"{DRILL}Loaded 561 rows for 2 of {2 * len(bas)} pairs" in proc.stdout
    assert "::warning" not in proc.stdout
    assert _bulk_rows(warehouse) == {"CISO": 561} and moved == 0
