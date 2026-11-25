"""The nightly's ingest step resumes on exit code 4 and on nothing else.

These tests run the step's actual shell from .github/workflows/nightly.yml,
so the retry policy the workflow documents is the one it executes: first
against a stub `python` that returns scripted exit codes, then against the
real ingest with a scripted EIA client that fails the way EIA did on
Nov 25, 2026.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from grid_pipeline import db
from grid_pipeline.config import REPO_ROOT, load_config

STEP_NAME = "Incremental ingest (EIA API -> MotherDuck)"

pytestmark = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None,
    reason="runs the workflow's bash; GitHub's Linux runner is the target",
)


def _refresh_job() -> dict:
    path = REPO_ROOT / ".github" / "workflows" / "nightly.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"]["refresh"]


def _ingest_step() -> dict:
    return next(s for s in _refresh_job()["steps"] if s.get("name") == STEP_NAME)


ATTEMPTS = int(_ingest_step()["env"]["INGEST_ATTEMPTS"])


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
        **(extra_env or {}),
    }
    # GitHub runs `run:` blocks with `bash -e`.
    return subprocess.run(["bash", "-e", "-c", step["run"]], env=env,
                          capture_output=True, text=True, timeout=120)


def _run_step(tmp_path, exit_codes: list[int]):
    """Run the step with a stub `python` that exits with each code in turn."""
    calls = tmp_path / "calls.txt"
    queue = tmp_path / "queue.txt"
    calls.touch()
    queue.write_text("".join(f"{code}\n" for code in exit_codes))
    proc = _run_step_with(
        tmp_path,
        f'echo "$*" >> "{calls}"\n'
        f'code=$(head -n 1 "{queue}")\n'
        f'tail -n +2 "{queue}" > "{queue}.next" && mv "{queue}.next" "{queue}"\n'
        # 97: the step ran more often than the test scripted.
        'exit "${code:-97}"\n',
    )
    return proc, calls.read_text().splitlines()


@pytest.mark.parametrize("exit_codes,expected,warnings", [
    ([0], 0, 0),         # a normal night: one run, no noise
    ([4, 0], 0, 1),      # one bad spell: resumes, ends green, and says so
    ([4, 4, 0], 0, 2),
    ([4] * (ATTEMPTS - 1) + [0], 0, ATTEMPTS - 1),  # green on the last attempt
    ([4] * ATTEMPTS, 4, ATTEMPTS - 1),              # still failing on it: red
    ([2], 2, 0),         # contamination guard: never retried
    ([3], 3, 0),         # EIA asked for a long wait: tomorrow's run resumes
    ([1], 1, 0),         # a crash is a bug, not a slow spell
    ([4, 2], 2, 1),      # a resume that hits a non-resumable error stops
])
def test_ingest_step_resumes_only_on_exit_4(tmp_path, exit_codes, expected, warnings):
    proc, calls = _run_step(tmp_path, exit_codes)
    assert proc.returncode == expected, proc.stderr
    # Every attempt is the same command with the same checkpoint file, which
    # lives in the job's temp folder: a fresh job can never inherit one.
    command = ("-m grid_pipeline.ingest --mode incremental "
               f"--checkpoint {tmp_path}/ingest-checkpoint")
    assert calls == [command] * len(exit_codes)
    assert proc.stdout.count("::warning") == warnings


def test_resume_pauses_fit_inside_the_job_timeout():
    """The pauses alone must leave most of the job's budget for the work."""
    job, env = _refresh_job(), _ingest_step()["env"]
    attempts, wait = int(env["INGEST_ATTEMPTS"]), int(env["INGEST_RETRY_WAIT_SECONDS"])
    assert attempts >= 2
    assert (attempts - 1) * wait <= job["timeout-minutes"] * 60 / 4


def test_a_dead_eia_goes_red_well_inside_the_job_timeout():
    """Worst case: every attempt spends the client's whole retry budget on its
    first request. That night must end red on exit 4, not be cut off by the
    job timeout, so the client's tries, the attempts and the pauses are
    budgeted together."""
    job, env = _refresh_job(), _ingest_step()["env"]
    attempts, wait = int(env["INGEST_ATTEMPTS"]), int(env["INGEST_RETRY_WAIT_SECONDS"])
    eia = load_config().eia
    backoff = sum(eia.backoff_base_seconds * 2**n for n in range(eia.max_retries - 1))
    one_request = eia.max_retries * eia.request_timeout_seconds + backoff
    assert attempts * one_request + (attempts - 1) * wait <= job["timeout-minutes"] * 60 / 2


# Stands in for `python -m grid_pipeline.ingest ...`: the real CLI and the real
# warehouse code, with the EIA client swapped for one that follows a script.
_SCRIPTED_INGEST = '''
import json
import pathlib
import sys

from grid_pipeline import ingest
from grid_pipeline.client import EIAError

state_path, argv = pathlib.Path(sys.argv[1]), sys.argv[2:]


class ScriptedEIA:
    """Answers like the API, except on the request numbers in `fail_on`
    (counted across every attempt), where it fails like an exhausted client."""

    def __init__(self, **kwargs):
        pass

    def iter_rows(self, route, *, facets=None, start=None, end=None, tiebreak_column=None):
        ba = facets["respondent"][0]
        state = json.loads(state_path.read_text())
        state["requests"].append(f"{route.rsplit('/', 1)[-1]}/{ba}")
        state_path.write_text(json.dumps(state))
        if len(state["requests"]) in state["fail_on"]:
            raise EIAError("EIA API still failing after 7 attempts (ReadTimeout)")
        key = "type" if "region-data" in route else "fueltype"
        yield {"period": end, "respondent": ba, key: "D" if key == "type" else "SUN",
               "value": "1", "value-units": "megawatthours"}


ingest.EIAClient = ScriptedEIA
sys.exit(ingest.main(argv))
'''


def test_nov_25_failure_pattern_completes_because_each_attempt_resumes(tmp_path):
    """Nov 25, 2026: three attempts, and each died on one exhausted request.

    Every attempt had started again at the first pair, so the three failures
    used up the whole budget. With the checkpoint the same three failures
    cost one request each: no pair is fetched twice and the night completes.
    """
    warehouse = tmp_path / "w.duckdb"
    cfg = load_config()
    # A normal night: every pair a few hours behind, so one request each.
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    seeded = now - timedelta(hours=5)
    con = db.connect(str(warehouse))
    for route in ("region_data", "fuel_mix"):
        for ba in cfg.balancing_authorities:
            db.set_watermark(con, route, ba, seeded)
    con.close()

    fail_on = [3, 4, 16]  # the third pair, the same pair again, then a later one
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"requests": [], "fail_on": fail_on}))
    driver = tmp_path / "scripted_ingest.py"
    driver.write_text(_SCRIPTED_INGEST)

    proc = _run_step_with(
        tmp_path,
        'shift 2  # drop "-m grid_pipeline.ingest"\n'
        f'exec "{sys.executable}" "{driver}" "{state}" "$@"\n',
        {"GRID_DB_PATH": str(warehouse), "EIA_API_KEY": "dummy"},
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.count("::warning title=Ingest resumed::") == len(fail_on)
    requests = json.loads(state.read_text())["requests"]
    answered = [r for n, r in enumerate(requests, start=1) if n not in fail_on]
    n_pairs = 2 * len(cfg.balancing_authorities)
    assert len(requests) == n_pairs + len(fail_on)
    assert len(answered) == len(set(answered)) == n_pairs  # each pair exactly once
    assert not (tmp_path / "ingest-checkpoint").exists()
    con = db.connect(str(warehouse))
    moved = con.execute("SELECT count(*) FROM raw._ingest_watermarks WHERE max_period > ?",
                        [seeded]).fetchone()[0]
    con.close()
    assert moved == n_pairs  # every pair's watermark advanced
