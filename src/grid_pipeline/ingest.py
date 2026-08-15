"""Ingestion orchestrator: EIA API (or fixtures) -> DuckDB raw tables.

Modes
-----
backfill     Full history from config start, one (route, BA, month) window
             at a time. Watermarks advance per completed window, so a crash
             resumes at the last finished month.
incremental  Per (route, BA): if a watermark exists, re-fetch a trailing
             lookback window (EIA restates recent hours); if none exists,
             bootstrap that pair with the same resumable month windows a
             backfill uses. First runs against an empty prod warehouse are
             therefore just as crash-safe as a local backfill.
fixtures     Load deterministic synthetic pages through the REAL parse and
             upsert path — used by CI and local dev.

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
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import db
from .client import PERIOD_FORMAT, EIAClient, EIAError, ThrottledError, parse_row
from .config import REPO_ROOT, get_api_key, load_config
from .provenance import SOURCE_API, SOURCE_FIXTURES, data_provenance

log = logging.getLogger("grid_pipeline.ingest")

FIXTURES_DB_DEFAULT = "data/dev_fixtures.duckdb"
FIXTURE_PAGES_DIR = REPO_ROOT / "data" / "sample"


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


def guard_against_contamination(con, mode: str, db_path: str) -> None:
    verdict = data_provenance(con)
    if mode == "fixtures":
        if db_path.startswith("md:"):
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


def _load_window(con, client, route: dict, ba: str, start: datetime, end: datetime,
                 source: str, advance_watermark: bool = True) -> int:
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


def run_incremental(cfg, con, client) -> None:
    now = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    backfill_start = datetime.strptime(cfg.backfill_start, "%Y-%m-%d")
    lookback = timedelta(hours=cfg.incremental_lookback_hours)
    total = 0
    for route in build_routes(cfg):
        for ba in cfg.balancing_authorities:
            wm = db.get_watermark(con, route["key"], ba)
            if wm is None:
                # Never-loaded pair: bootstrap with resumable month windows,
                # exactly like a backfill, instead of one fragile giant range.
                log.info("%s/%s has no watermark — bootstrapping full history",
                         route["key"], ba)
                for w_start, w_end in _month_windows(backfill_start, now):
                    total += _load_window(con, client, route, ba, w_start, w_end,
                                          SOURCE_API)
                continue
            start = wm - lookback
            total += _load_window(con, client, route, ba, start, now, SOURCE_API)
    db.set_load_meta(con, "source", SOURCE_API)
    db.set_load_meta(con, "last_incremental_at", datetime.now(UTC).isoformat())
    log.info("Incremental complete: %d rows upserted", total)


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
    parser.add_argument("--mode", choices=["backfill", "incremental", "fixtures"],
                        required=True)
    parser.add_argument(
        "--replay-since", metavar="YYYY-MM-DD",
        help="(backfill only) ignore watermarks and re-fetch history from this "
             "date — repairs old upstream restatements via idempotent upserts")
    args = parser.parse_args(argv)
    replay_since = (datetime.strptime(args.replay_since, "%Y-%m-%d")
                    if args.replay_since else None)
    if replay_since and args.mode != "backfill":
        parser.error("--replay-since only applies to --mode backfill")

    cfg = load_config()
    db_path = resolve_db_path(args.mode, cfg)
    if not db_path.startswith("md:"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = db.connect(db_path)
    try:
        guard_against_contamination(con, args.mode, db_path)
        if args.mode == "fixtures":
            run_fixtures(cfg, con)
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
                run_incremental(cfg, con, client)
    except ContaminationError as exc:
        log.error(str(exc))
        return 2
    except ThrottledError as exc:
        log.error("%s", exc)
        log.error("Progress up to this point is saved; re-run the same command "
                  "to resume.")
        return 3
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
