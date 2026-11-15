"""The nightly's ingest step resumes on exit code 4 and on nothing else.

These tests run the step's actual shell from .github/workflows/nightly.yml
against a stub `python` that returns scripted exit codes, so the retry
policy the workflow documents is the one it executes.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest
import yaml

from grid_pipeline.config import REPO_ROOT

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


def _run_step(tmp_path, exit_codes: list[int]):
    """Run the step with a stub `python` that exits with each code in turn."""
    step = _ingest_step()
    calls = tmp_path / "calls.txt"
    queue = tmp_path / "queue.txt"
    calls.touch()
    queue.write_text("".join(f"{code}\n" for code in exit_codes))
    stub = tmp_path / "bin" / "python"
    stub.parent.mkdir()
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> "{calls}"\n'
        f'code=$(head -n 1 "{queue}")\n'
        f'tail -n +2 "{queue}" > "{queue}.next" && mv "{queue}.next" "{queue}"\n'
        # 97: the step ran more often than the test scripted.
        'exit "${code:-97}"\n'
    )
    stub.chmod(0o755)
    env = {
        "PATH": f"{stub.parent}{os.pathsep}{os.environ['PATH']}",
        "INGEST_ATTEMPTS": step["env"]["INGEST_ATTEMPTS"],
        "INGEST_RETRY_WAIT_SECONDS": "0",  # the policy is under test, not the pause
    }
    # GitHub runs `run:` blocks with `bash -e`.
    proc = subprocess.run(["bash", "-e", "-c", step["run"]], env=env,
                          capture_output=True, text=True, timeout=60)
    return proc, calls.read_text().splitlines()


@pytest.mark.parametrize("exit_codes,expected,warnings", [
    ([0], 0, 0),         # a normal night: one run, no noise
    ([4, 0], 0, 1),      # one bad spell: resumes, ends green, and says so
    ([4, 4, 0], 0, 2),
    ([4, 4, 4], 4, 2),   # still failing on the last attempt: red
    ([2], 2, 0),         # contamination guard: never retried
    ([3], 3, 0),         # EIA asked for a long wait: tomorrow's run resumes
    ([1], 1, 0),         # a crash is a bug, not a slow spell
    ([4, 2], 2, 1),      # a resume that hits a non-resumable error stops
])
def test_ingest_step_resumes_only_on_exit_4(tmp_path, exit_codes, expected, warnings):
    proc, calls = _run_step(tmp_path, exit_codes)
    assert proc.returncode == expected, proc.stderr
    assert calls == ["-m grid_pipeline.ingest --mode incremental"] * len(exit_codes)
    assert proc.stdout.count("::warning") == warnings


def test_resume_pauses_fit_inside_the_job_timeout():
    """The pauses alone must leave most of the job's budget for the work."""
    job, env = _refresh_job(), _ingest_step()["env"]
    attempts, wait = int(env["INGEST_ATTEMPTS"]), int(env["INGEST_RETRY_WAIT_SECONDS"])
    assert attempts >= 2
    assert (attempts - 1) * wait <= job["timeout-minutes"] * 60 / 4
