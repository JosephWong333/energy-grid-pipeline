"""Export the dbt marts from DuckDB to Parquet on GCS, then load them into BigQuery.

WHY THIS EXISTS (and why it is not dbt-bigquery)
DuckDB/MotherDuck remains the single transform engine: every model, test and
watermark stays exactly where it is. This script is a *serving* layer bolted on
after `dbt build` succeeds -- it reads finished marts and republishes them so a
BI tool can reach them without a MotherDuck token. Nothing upstream changes, and
if this script fails the warehouse is still correct. Running the same models on
two engines (dbt-bigquery + cross-db macros) is the alternative; it doubles the
test surface for no gain while the fact table is under a gigabyte.

IDEMPOTENCY
The nightly dbt run reprocesses a trailing `late_arrival_lookback_hours` window
(96h) because EIA restates recent hours. This script mirrors that idea one layer
up: it re-exports the last LOOKBACK_DAYS of `local_date` and loads each day into
its own BigQuery partition with `--replace` on a partition decorator
(`table$YYYYMMDD`). A partition is overwritten wholesale, never appended to, so
running this twice -- or five times -- on the same day yields the same row count.
That is the partition-level insert_overwrite behaviour, done with a load job
instead of DML (load jobs are free; DML is not).

LOOKBACK_DAYS is 5, not 4. The dbt lookback is 96h on `period_utc`, but the
BigQuery partition key is `local_date`, which is wall-clock in each BA's own
timezone. Up to 8 hours of skew (CISO is UTC-7/8) can push a restated UTC hour
into an earlier local date, so the window gets one extra day of slack.

SCHEMA
There is no hand-written BigQuery DDL. The table is created by its first load
directly from the Parquet footer, with partitioning and clustering applied at
creation. A new column in a dbt model therefore propagates on the next full
load instead of silently mismatching a 43-column schema file that drifted.

Two casts are applied on the way out, because DuckDB's naked TIMESTAMP has no
zone and lands in BigQuery as DATETIME:
  * period_utc -> TIMESTAMPTZ, so BigQuery stores a real TIMESTAMP. Without it
    a BI tool renders a UTC instant as wall-clock and the duck curve shifts by
    the timezone offset.
  * mart_hourly_profile.month -> DATE. It is a month-start bucket, and DATE is
    what date controls in BI tools expect.
period_local stays DATETIME on purpose: it genuinely is wall-clock with no zone.

Usage:
    python -m scripts.export_to_cloud --mode full          # seed / after a new BA
    python -m scripts.export_to_cloud --mode incremental   # nightly
    python -m scripts.export_to_cloud --mode incremental --dry-run
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

PROJECT = os.environ.get("GCP_PROJECT_ID", "eia-grid-930")
BUCKET = os.environ.get("GCS_BUCKET", "eia-grid-930-marts")
DATASET = os.environ.get("BQ_DATASET", "energy_grid")
DB_PATH = os.environ.get("GRID_DB_PATH", "data/energy_grid.duckdb")
LOOKBACK_DAYS = int(os.environ.get("EXPORT_LOOKBACK_DAYS", "5"))


@dataclass(frozen=True)
class Mart:
    """One mart and how it is published.

    partition_field set => incremental mode loads it one day per partition.
    partition_field None => always replaced whole (these are dbt `table`
    materializations, fully rebuilt every run, and all are under 30k rows).
    """

    name: str
    projection: str
    partition_field: str | None = None
    clustering: str | None = None


MARTS: tuple[Mart, ...] = (
    Mart(
        name="fct_grid_hourly",
        # REPLACE rewrites one column and leaves the other 42 untouched, so a
        # new dbt column flows through without editing this file.
        projection="* replace (period_utc at time zone 'UTC' as period_utc)",
        partition_field="local_date",
        clustering="ba_code",
    ),
    Mart(name="mart_grid_daily", projection="*"),
    Mart(name="mart_hourly_profile", projection="* replace (month::date as month)"),
    Mart(name="mart_identity_residuals", projection="*"),
)


def _tool(name: str) -> str:
    """Resolve gcloud/bq, which are .cmd shims on Windows.

    shutil.which honours PATHEXT; CreateProcess (what subprocess uses) only
    ever appends .exe, so a bare "bq" would not resolve on Windows.
    """
    found = shutil.which(name)
    if not found:
        sys.exit(f"error: `{name}` not found on PATH. Install/init the Google Cloud SDK.")
    return found


def run(cmd: list[str], *, dry_run: bool) -> None:
    printable = " ".join(cmd)
    print(f"  $ {printable}", flush=True)
    if dry_run:
        return
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"error: command failed with exit {result.returncode}: {printable}")


def connect() -> duckdb.DuckDBPyConnection:
    # MotherDuck picks up MOTHERDUCK_TOKEN from the environment. read_only is
    # only meaningful (and only safe) for a local file.
    if DB_PATH.startswith("md:"):
        return duckdb.connect(DB_PATH)
    if not Path(DB_PATH).exists():
        sys.exit(f"error: DuckDB file not found: {DB_PATH}")
    return duckdb.connect(DB_PATH, read_only=True)


def export_parquet(
    con: duckdb.DuckDBPyConnection, mart: Mart, where: str, out: Path
) -> int:
    sql = f"select {mart.projection} from main.{mart.name}{where}"
    n = con.execute(f"select count(*) from ({sql})").fetchone()[0]
    if n == 0:
        return 0
    # snappy, not zstd: universally accepted by BigQuery's Parquet reader.
    con.execute(f"copy ({sql}) to '{out.as_posix()}' (format parquet, compression snappy)")
    return n


def publish(
    mart: Mart,
    local: Path,
    gcs_key: str,
    bq_target: str,
    *,
    create_flags: list[str],
    dry_run: bool,
) -> None:
    uri = f"gs://{BUCKET}/{gcs_key}"
    run([_tool("gcloud"), "storage", "cp", str(local), uri], dry_run=dry_run)
    run(
        [
            _tool("bq"),
            f"--project_id={PROJECT}",
            "load",
            "--source_format=PARQUET",
            "--replace",
            *create_flags,
            f"{DATASET}.{bq_target}",
            uri,
        ],
        dry_run=dry_run,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=("full", "incremental"), default="incremental")
    ap.add_argument("--dry-run", action="store_true", help="print commands, change nothing")
    ap.add_argument("--tables", nargs="*", help="restrict to these mart names")
    args = ap.parse_args()

    marts = MARTS
    if args.tables:
        known = {m.name for m in MARTS}
        unknown = set(args.tables) - known
        if unknown:
            sys.exit(f"error: unknown mart(s): {', '.join(sorted(unknown))}")
        marts = tuple(m for m in MARTS if m.name in args.tables)

    con = connect()
    print(f"source : {DB_PATH}")
    print(f"target : {PROJECT}:{DATASET}  <-  gs://{BUCKET}")
    print(f"mode   : {args.mode}" + (f" (lookback {LOOKBACK_DAYS}d)" if args.mode == "incremental" else ""))
    print()

    tmp = Path(tempfile.mkdtemp(prefix="eia-export-"))
    total = 0
    try:
        for mart in marts:
            incremental = args.mode == "incremental" and mart.partition_field

            if not incremental:
                # Whole table, one file, one load job. For the fact this is the
                # seed path: BigQuery routes rows to their partitions itself, so
                # ~2,800 partitions land in a single job rather than 2,800 jobs.
                print(f"{mart.name}: full replace")
                local = tmp / f"{mart.name}.parquet"
                n = export_parquet(con, mart, "", local)
                if n == 0:
                    print("  (no rows, skipped)")
                    continue
                flags = []
                if mart.partition_field:
                    flags = [
                        f"--time_partitioning_field={mart.partition_field}",
                        "--time_partitioning_type=DAY",
                        f"--clustering_fields={mart.clustering}",
                    ]
                print(f"  {n:,} rows")
                publish(
                    mart,
                    local,
                    f"marts/{mart.name}/full/part-000.parquet",
                    mart.name,
                    create_flags=flags,
                    dry_run=args.dry_run,
                )
                total += n
                continue

            field = mart.partition_field
            max_date = con.execute(f"select max({field}) from main.{mart.name}").fetchone()[0]
            if max_date is None:
                print(f"{mart.name}: empty, skipped")
                continue
            start: date = max_date - timedelta(days=LOOKBACK_DAYS - 1)
            print(f"{mart.name}: partitions {start} .. {max_date}")

            day = start
            while day <= max_date:
                stamp = day.strftime("%Y%m%d")
                local = tmp / f"{mart.name}-{stamp}.parquet"
                n = export_parquet(con, mart, f" where {field} = date '{day}'", local)
                if n == 0:
                    print(f"  {day}: no rows, skipped")
                    day += timedelta(days=1)
                    continue
                print(f"  {day}: {n:,} rows")
                # No --time_partitioning_field here: the decorator names the
                # partition, and passing both is rejected. --replace therefore
                # overwrites exactly this one day and touches nothing else.
                publish(
                    mart,
                    local,
                    f"marts/{mart.name}/{field}={day}/part-000.parquet",
                    f"{mart.name}${stamp}",
                    create_flags=[],
                    dry_run=args.dry_run,
                )
                total += n
                day += timedelta(days=1)
    finally:
        con.close()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    print(f"done: {total:,} rows published" + (" (dry run, nothing changed)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
