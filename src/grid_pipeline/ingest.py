"""Ingestion orchestrator: EIA API (or fixtures) -> DuckDB raw tables.

Modes
-----
backfill     Full history from config start, one (route, BA, month) window
             at a time. Watermarks advance per completed window, so a crash
             resumes at the last finished month.
incremental  Per (route, BA): if a watermark exists, re-fetch a trailing
             lookback window (EIA restates recent hours); if none exists,
             bootstrap that pair from the configured start. A normal night
             is one request. A range longer than a month (a bootstrap, or
             a catch-up after a long outage) is walked in the same
             resumable month windows a backfill uses, so it is as
             crash-safe as a local backfill. With --checkpoint, a re-run
             after a failure fetches only the pairs the failed run did
             not finish.
fallback     For a night the API does not answer: load the hours the
             warehouse lacks from EIA's bulk file (see bulk.py). It only
             adds rows the warehouse does not have, only for a pair where
             the file agrees with what the API already delivered, and it
             never moves a watermark, so the API's next answer re-fetches
             and replaces what it loaded. Prints one line saying what it
             did.
fixtures     Load seeded synthetic pages through the REAL parse and
             upsert code — used by CI and local dev. (It skips the HTTP
             client and the per-window response checks, which pytest
             covers with stubbed sessions.)

Exit codes
----------
0  done
1  a crash (a bug)
2  the contamination guard refused the warehouse
3  EIA asked for a wait longer than the client's cap; resume later
4  EIA said no or sent something wrong (a 4xx, a response that failed a
   check, a bulk file that is not laid out as expected)
5  EIA did not answer: timeouts or 5xx on every try

After 3, 4 and 5 everything already loaded is saved, and re-running the same
command resumes.

Contamination guards
--------------------
Fixture data and real data must never share a warehouse. Fixture mode
defaults to its own database file (data/dev_fixtures.duckdb), refuses cloud
(md:) paths, and refuses any warehouse containing real rows. Real modes
refuse any warehouse containing fixture rows. Row-level provenance
(db._source) makes the guards trustworthy even if metadata is lost.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import db
from .bulk import BulkClient
from .client import (
    PERIOD_FORMAT,
    EIAClient,
    EIAError,
    EIARow,
    ThrottledError,
    UnavailableError,
    gateway_status,
    parse_row,
)
from .config import REPO_ROOT, get_api_key, load_config
from .provenance import SOURCE_API, SOURCE_BULK, SOURCE_FIXTURES, data_provenance

log = logging.getLogger("grid_pipeline.ingest")

FIXTURES_DB_DEFAULT = "data/dev_fixtures.duckdb"
# Longer incremental ranges are fetched in month windows, like a backfill.
MAX_SINGLE_REQUEST_SPAN = timedelta(days=31)
FIXTURE_PAGES_DIR = REPO_ROOT / "data" / "sample"
# The bulk file is trusted for a pair only where it agrees with what the API
# already delivered: at least FALLBACK_MIN_SHARED values present in both, and
# at least FALLBACK_MIN_AGREEMENT of them equal. Measured on Nov 27, 2026:
# nine BAs agreed on every value and the two with missing reports on 94%.
# Read one hour off, the same file agrees on at most 1% of a BA's region
# values and 38% of its fuel values (series that sit at zero or stay flat).
FALLBACK_MIN_SHARED = 24
FALLBACK_MIN_AGREEMENT = 0.8


def build_routes(cfg) -> list[dict]:
    """Route descriptors: API path, facet filters, and the JSON series key."""
    return [
        {
            "key": "region_data",
            "path": cfg.region_data_path,
            "series_key": "type",
            "tiebreak": "type",
            "facets": lambda ba, c=cfg: {"respondent": [ba], "type": c.region_data_types},
        },
        {
            "key": "fuel_mix",
            "path": cfg.fuel_mix_path,
            "series_key": "fueltype",
            "tiebreak": "fueltype",
            "facets": lambda ba: {"respondent": [ba]},
        },
    ]


class ContaminationError(RuntimeError):
    """Refusing to mix synthetic and real data in one warehouse."""


def resolve_db_path(mode: str, cfg) -> str:
    """Pick the warehouse path for a mode.

    GRID_DB_PATH always wins if set. Otherwise real modes use the configured
    warehouse and fixture mode uses its own separate file, so the default
    workflows physically cannot cross-contaminate.
    """
    env = os.environ.get("GRID_DB_PATH")
    if env:
        return env
    if mode == "fixtures":
        return str(REPO_ROOT / FIXTURES_DB_DEFAULT)
    return str(REPO_ROOT / cfg.db_path)


def is_cloud_path(db_path: str) -> bool:
    """DuckDB sends 'md:', 'MD:' and 'motherduck:' paths alike to MotherDuck."""
    return db_path.lower().startswith(("md:", "motherduck:"))


def guard_against_contamination(con, mode: str, db_path: str) -> None:
    verdict = data_provenance(con)
    if mode == "fixtures":
        if is_cloud_path(db_path):
            raise ContaminationError(
                "Refusing to load fixtures into a MotherDuck/cloud warehouse "
                f"({db_path}). Fixtures belong in a local dev database."
            )
        if verdict in ("real", "mixed", "unknown"):
            raise ContaminationError(
                f"Refusing to load fixtures: {db_path} already contains "
                f"{verdict} data. Point GRID_DB_PATH at a separate dev file "
                f"(default: {FIXTURES_DB_DEFAULT}) or delete this warehouse."
            )
    else:  # real modes
        if verdict in ("fixtures", "mixed", "unknown"):
            raise ContaminationError(
                f"Refusing to load real EIA data: {db_path} contains {verdict} "
                "rows. A warehouse that ever held synthetic rows cannot be "
                "trusted as real — delete it (or set GRID_DB_PATH elsewhere) "
                "and backfill clean."
            )


def _month_windows(start: datetime, end: datetime):
    """Contiguous [window_start, window_end] pairs, hour-granular, by month."""
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end:
        if cursor.month == 12:
            nxt = cursor.replace(year=cursor.year + 1, month=1)
        else:
            nxt = cursor.replace(month=cursor.month + 1)
        yield max(cursor, start), min(nxt - timedelta(hours=1), end)
        cursor = nxt


def _fetch_window(client, route: dict, ba: str, start: datetime,
                  end: datetime) -> list[EIARow]:
    """One window's rows, parsed and checked. Nothing is written."""
    rows = [
        parse_row(rec, route["series_key"])
        for rec in client.iter_rows(
            route["path"],
            facets=route["facets"](ba),
            start=start.strftime(PERIOD_FORMAT),
            end=end.strftime(PERIOD_FORMAT),
            tiebreak_column=route["tiebreak"],
        )
    ]
    # Response integrity — validated BEFORE any upsert or watermark motion.
    # A response that violates its own request contract (wrong respondent,
    # rows outside the asked window, unexpected units) is not partially
    # usable data; it's evidence the API or our facets misbehaved, and
    # loading it could advance the wrong watermark or corrupt analytics.
    # Raising is safe: ingestion is resumable by design.
    wrong_ba = {r.respondent for r in rows} - {ba}
    if wrong_ba:
        raise EIAError(
            f"{route['key']}/{ba}: response contains rows for other "
            f"respondent(s) {sorted(wrong_ba)} — facet filtering failed; "
            "refusing to load or advance watermarks.")
    stray = [r for r in rows if not (start <= r.period_utc <= end)]
    if stray:
        raise EIAError(
            f"{route['key']}/{ba}: {len(stray)} row(s) outside the requested "
            f"window {start}..{end} (e.g. {stray[0].period_utc}) — refusing "
            "to load; an out-of-window period could poison the watermark.")
    odd_units = {r.units for r in rows if r.value is not None} - {"megawatthours"}
    if odd_units:
        raise EIAError(
            f"{route['key']}/{ba}: unexpected value-units {sorted(odd_units)} "
            "— refusing to publish values whose unit is unverified.")
    return rows


def _load_window(con, client, route: dict, ba: str, start: datetime, end: datetime,
                 source: str, advance_watermark: bool = True) -> int:
    rows = _fetch_window(client, route, ba, start, end)
    n = db.upsert_rows(con, route["key"], rows, source=source)
    if rows and advance_watermark:
        db.set_watermark(con, route["key"], ba, max(r.period_utc for r in rows))
    return n


def run_backfill(cfg, con, client, replay_since: datetime | None = None) -> None:
    """Full-history load, or — with ``replay_since`` — a forced re-fetch of
    history from a date, the tool for repairing OLD upstream restatements.
    (A dbt --full-refresh rebuilds marts from raw; it cannot repair raw
    itself. This can, because upserts are idempotent.)

    Replay watermark semantics: a replay NEVER reads or writes watermarks.
    On a fresh warehouse, a short pilot replay therefore leaves no
    watermarks behind, and a subsequent ordinary backfill still starts at
    backfill_start (2019) instead of believing itself current. On a complete
    warehouse, an interrupted replay cannot regress the resume point of
    ordinary ingestion (set_watermark is additionally monotonic). The cost:
    a re-run replay restarts at ``since`` — acceptable, because every window
    is an idempotent upsert.

    Known limitation, by design: replay repairs inserts and value changes
    only. If EIA deletes a previously published row outright, the stale
    local row survives; reconciling deletions would require diffing whole
    windows transactionally, which this project documents as future work
    rather than pretending to do.
    """
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    backfill_start = datetime.strptime(cfg.backfill_start, "%Y-%m-%d")
    total = 0
    for route in build_routes(cfg):
        for ba in cfg.balancing_authorities:
            if replay_since is not None:
                effective_start = max(replay_since, backfill_start)
                for w_start, w_end in _month_windows(effective_start, now):
                    total += _load_window(con, client, route, ba, w_start, w_end,
                                          SOURCE_API, advance_watermark=False)
                continue
            else:
                wm = db.get_watermark(con, route["key"], ba)
                effective_start = (wm + timedelta(hours=1)) if wm else backfill_start
            if effective_start >= now:
                log.info("%s/%s already current", route["key"], ba)
                continue
            for w_start, w_end in _month_windows(effective_start, now):
                n = _load_window(con, client, route, ba, w_start, w_end, SOURCE_API)
                total += n
                log.info("%s/%s %s..%s: %d rows", route["key"], ba,
                         w_start.date(), w_end.date(), n)
    db.set_load_meta(con, "source", SOURCE_API)
    db.set_load_meta(con, "last_backfill_at", datetime.now(UTC).isoformat())
    log.info("Backfill complete: %d rows upserted", total)


def _read_checkpoint(path: Path | None) -> set[str]:
    """The 'route_key/BA' pairs an earlier run of this command finished."""
    if path is None or not path.exists():
        return set()
    lines = path.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip()}


def run_incremental(cfg, con, client, checkpoint: Path | None = None) -> None:
    """Refresh every (route, BA) pair from its own watermark.

    ``checkpoint`` turns a re-run after a failure into a real resume. Each
    pair is appended to the file once all of its windows are loaded, pairs
    already listed are skipped, and the file is removed when a run
    completes. Without it a re-run fetches every pair again, so during an
    EIA slow spell nothing a failed run finished counts toward the next
    one (Nov 25, 2026: one attempt got through at least 18 of the 22
    pairs, and the two after it started from the first pair and died
    sooner). Skipping is safe: a listed pair's rows and watermark are
    already saved, and the next night re-fetches its lookback as usual.
    """
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    backfill_start = datetime.strptime(cfg.backfill_start, "%Y-%m-%d")
    lookback = timedelta(hours=cfg.incremental_lookback_hours)
    done = _read_checkpoint(checkpoint)
    pairs = [(route, ba) for route in build_routes(cfg)
             for ba in cfg.balancing_authorities]
    todo = [(route, ba) for route, ba in pairs if f"{route['key']}/{ba}" not in done]
    if len(todo) < len(pairs):
        log.info("Resuming from %s: %d of %d pairs already refreshed, "
                 "fetching the other %d", checkpoint, len(pairs) - len(todo),
                 len(pairs), len(todo))
    total = 0
    for route, ba in todo:
        pair = f"{route['key']}/{ba}"
        wm = db.get_watermark(con, route["key"], ba)
        if wm is None:
            log.info("%s has no watermark — bootstrapping full history", pair)
        # A normal night is one request, [wm - lookback, now]. A range
        # longer than a month (a new pair, or a catch-up after a long
        # outage) walks month windows instead of one fragile giant range,
        # and a crash keeps every finished month.
        start = backfill_start if wm is None else wm - lookback
        if now - start > MAX_SINGLE_REQUEST_SPAN:
            windows = list(_month_windows(start, now))
        else:
            windows = [(start, now)]
        began = time.monotonic()
        rows = 0
        try:
            for w_start, w_end in windows:
                rows += _load_window(con, client, route, ba, w_start, w_end,
                                     SOURCE_API)
        except EIAError:
            # Name the pair: the client's error only knows the route.
            log.error("%s failed after %.0fs", pair, time.monotonic() - began)
            raise
        log.info("%s: %d rows in %.1fs", pair, rows, time.monotonic() - began)
        total += rows
        if checkpoint is not None:
            with checkpoint.open("a", encoding="utf-8") as fh:
                fh.write(f"{pair}\n")
    db.set_load_meta(con, "source", SOURCE_API)
    db.set_load_meta(con, "last_incremental_at", datetime.now(UTC).isoformat())
    if checkpoint is not None:
        # The run is complete, so nothing is left to resume. Leaving the
        # file would make the next run skip every pair.
        checkpoint.unlink(missing_ok=True)
    log.info("Incremental complete: %d rows upserted", total)


def run_fallback(cfg, con, bulk) -> str:
    """Load the hours the warehouse lacks from EIA's bulk file.

    For a night the API did not answer. Three rules keep the second source
    from ever leaving the warehouse worse than it found it:

    * Only rows the warehouse does not have are added, plus rows an earlier
      fallback added, which a newer file may correct. A row the API
      delivered is never touched: restating those is the API's job, and the
      file can be older than the row it would replace.
    * A pair is loaded only if the file agrees with the API rows already
      here (FALLBACK_MIN_SHARED, FALLBACK_MIN_AGREEMENT). A file whose
      layout, hours or units have changed fails that check.
    * No watermark moves. The API's next answer therefore re-fetches from
      the same point as if this had never run, and replaces these rows.

    Returns one sentence saying what happened, for the nightly's warning.
    """
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    lookback = timedelta(hours=cfg.incremental_lookback_hours)
    checked, loaded = 0, 0
    filled: list[str] = []
    refused: list[str] = []
    newest: datetime | None = None
    for route in build_routes(cfg):
        for ba in cfg.balancing_authorities:
            pair = f"{route['key']}/{ba}"
            wm = db.get_watermark(con, route["key"], ba)
            if wm is None:
                log.info("%s has no watermark: the bulk file does not start a pair", pair)
                continue
            checked += 1
            start = wm - lookback
            rows = _fetch_window(bulk, route, ba, start, now)
            have = db.window_rows(con, route["key"], ba, start, now)
            shared = same = 0
            fill: list[EIARow] = []
            for row in rows:
                value, source = have.get((row.period_utc, row.series), (None, None))
                if source == SOURCE_API:
                    shared += 1
                    same += value == row.value
                elif source in (None, SOURCE_BULK):
                    fill.append(row)
            if shared < FALLBACK_MIN_SHARED:
                log.warning("%s: left alone. Only %d values to check the bulk file "
                            "against, and %d are needed", pair, shared, FALLBACK_MIN_SHARED)
                refused.append(pair)
                continue
            if same < FALLBACK_MIN_AGREEMENT * shared:
                log.warning("%s: left alone. The bulk file and the API agree on "
                            "only %d of the %d values both have", pair, same, shared)
                refused.append(pair)
                continue
            n = db.upsert_rows(con, route["key"], fill, source=SOURCE_BULK)
            log.info("%s: %d rows from the bulk file, which agrees with the API "
                     "on %d of the %d values both have", pair, n, same, shared)
            if n:
                loaded += n
                filled.append(pair)
                top = max(r.period_utc for r in fill)
                newest = top if newest is None else max(newest, top)
    if checked and len(refused) == checked:
        raise EIAError("The bulk file could not be confirmed against the API's rows "
                       "for any pair; nothing was loaded from it.")
    db.set_load_meta(con, "last_fallback_at", datetime.now(UTC).isoformat())
    written = (f"written {bulk.last_modified:%Y-%m-%d %H:%M} UTC"
               if bulk.last_modified else "write time unknown")
    if not checked:
        summary = "No pair has a watermark yet, so the bulk file had nothing to fill."
    elif loaded:
        summary = (f"Loaded {loaded:,} rows for {len(filled)} of {checked} pairs from "
                   f"EIA's bulk file ({written}), through {newest:%Y-%m-%dT%H} UTC. The "
                   "API's own rows replace them on the next night it answers.")
    else:
        summary = f"EIA's bulk file ({written}) holds nothing the warehouse lacks."
    if refused:
        more = f" and {len(refused) - 4} more" if len(refused) > 4 else ""
        summary += (" Left alone, because the file could not be confirmed against "
                    f"the API's rows there: {', '.join(refused[:4])}{more}.")
    log.info("Fallback complete: %s", summary)
    return summary


def run_fixtures(cfg, con) -> None:
    # Deterministic by construction: the contamination guard has already
    # proven this warehouse contains nothing real, so wipe raw state and
    # rebuild it from the pages alone. Without this, repeated dev runs
    # accumulate history beyond the advertised fixture window.
    for table in ("raw.eia_region_data", "raw.eia_fuel_mix",
                  "raw._ingest_watermarks", "raw._load_meta"):
        con.execute(f"DELETE FROM {table}")
    log.info("Reset fixture warehouse state")
    pages = sorted(FIXTURE_PAGES_DIR.glob("*.json"))
    if not pages:
        raise SystemExit(
            "No fixture pages found. Run: python scripts/generate_dev_fixtures.py"
        )
    total = 0
    per_ba_max: dict[tuple[str, str], datetime] = {}
    for page_path in pages:
        payload = json.loads(page_path.read_text())
        route_key = payload["_route_key"]
        series_key = next(r["series_key"] for r in build_routes(cfg) if r["key"] == route_key)
        rows = [parse_row(rec, series_key) for rec in payload["response"]["data"]]
        total += db.upsert_rows(con, route_key, rows, source=SOURCE_FIXTURES)
        for r in rows:
            k = (route_key, r.respondent)
            if k not in per_ba_max or r.period_utc > per_ba_max[k]:
                per_ba_max[k] = r.period_utc
    for (route_key, ba), max_p in per_ba_max.items():
        db.set_watermark(con, route_key, ba, max_p)
    db.set_load_meta(con, "source", SOURCE_FIXTURES)
    log.info("Loaded %d fixture rows from %d pages into %s",
             total, len(pages), con.execute("PRAGMA database_list").fetchone()[2])


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",
                        choices=["backfill", "incremental", "fallback", "fixtures"],
                        required=True)
    parser.add_argument(
        "--replay-since", metavar="YYYY-MM-DD",
        help="(backfill only) ignore watermarks and re-fetch history from this "
             "date — repairs old upstream restatements via idempotent upserts")
    parser.add_argument(
        "--checkpoint", metavar="PATH", type=Path,
        help="(incremental only) record each finished (route, BA) pair in this "
             "file and skip the pairs already listed, so a re-run after a "
             "failure resumes; the file is removed when a run completes")
    args = parser.parse_args(argv)
    replay_since = (datetime.strptime(args.replay_since, "%Y-%m-%d")
                    if args.replay_since else None)
    if replay_since and args.mode != "backfill":
        parser.error("--replay-since only applies to --mode backfill")
    if args.checkpoint and args.mode != "incremental":
        parser.error("--checkpoint only applies to --mode incremental")
    if args.checkpoint:
        args.checkpoint.parent.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    db_path = resolve_db_path(args.mode, cfg)
    if not is_cloud_path(db_path):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = db.connect(db_path)
    try:
        guard_against_contamination(con, args.mode, db_path)
        if args.mode == "fixtures":
            run_fixtures(cfg, con)
        elif args.mode == "fallback":
            # One line for the log of an outage night: is it the gateway or
            # the data service behind it that is not answering?
            log.info("EIA's API, asked without a key: %s",
                     gateway_status(cfg.eia.base_url))
            # The only line on stdout, so the nightly can quote it.
            print(run_fallback(cfg, con, BulkClient(cfg.balancing_authorities)))
        else:
            client = EIAClient(
                api_key=get_api_key(),
                base_url=cfg.eia.base_url,
                page_size=cfg.eia.page_size,
                sleep_between_requests=cfg.eia.sleep_between_requests,
                max_retries=cfg.eia.max_retries,
                backoff_base_seconds=cfg.eia.backoff_base_seconds,
                timeout=cfg.eia.request_timeout_seconds,
            )
            if args.mode == "backfill":
                run_backfill(cfg, con, client, replay_since=replay_since)
            else:
                run_incremental(cfg, con, client, checkpoint=args.checkpoint)
    except ContaminationError as exc:
        log.error(str(exc))
        return 2
    except ThrottledError as exc:
        log.error("%s", exc)
        log.error("Progress up to this point is saved; re-run the same command "
                  "to resume.")
        return 3
    except UnavailableError as exc:
        log.error("%s", exc)
        log.error("EIA is not answering. Nothing from the failing window was "
                  "loaded; completed windows are saved. Re-run the same "
                  "command to resume.")
        return 5
    except EIAError as exc:
        log.error("%s", exc)
        log.error("Nothing from the failing window was loaded; completed "
                  "windows are saved. Re-run the same command to resume.")
        return 4
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
