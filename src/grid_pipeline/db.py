"""DuckDB storage layer.

Design notes
------------
* Raw tables carry primary keys on the natural grain and are written with
  INSERT OR REPLACE, so every load is idempotent: re-running a window (after
  a crash, or deliberately for the late-arrival lookback) replaces rather
  than duplicates.
* Every raw row records its provenance in ``_source`` ('eia_api' or
  'dev_fixtures'). Provenance lives on the rows themselves — not only in a
  global flag — so synthetic data can never masquerade as real data even if
  metadata is lost or a warehouse is loaded from mixed runs.
* Writes are vectorized: rows are registered as a DataFrame and inserted
  with a single INSERT OR REPLACE ... SELECT. This is ~300x faster than
  per-row executemany (measured: ~490 rows/s vs ~160k rows/s), which is the
  difference between a multi-hour and a sub-minute insert phase on a full
  2019-present backfill.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pandas as pd

from .client import EIARow

TABLE_FOR_ROUTE = {
    "region_data": "raw.eia_region_data",
    "fuel_mix": "raw.eia_fuel_mix",
}
CODE_COL_FOR_ROUTE = {
    "region_data": "metric_code",
    "fuel_mix": "fuel_code",
}

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.eia_region_data (
    period_utc   TIMESTAMP NOT NULL,
    respondent   VARCHAR   NOT NULL,
    metric_code  VARCHAR   NOT NULL,
    value        DOUBLE,
    value_units  VARCHAR,
    _ingested_at TIMESTAMP NOT NULL,
    _source      VARCHAR   NOT NULL DEFAULT 'unknown',
    PRIMARY KEY (respondent, period_utc, metric_code)
);

CREATE TABLE IF NOT EXISTS raw.eia_fuel_mix (
    period_utc   TIMESTAMP NOT NULL,
    respondent   VARCHAR   NOT NULL,
    fuel_code    VARCHAR   NOT NULL,
    value        DOUBLE,
    value_units  VARCHAR,
    _ingested_at TIMESTAMP NOT NULL,
    _source      VARCHAR   NOT NULL DEFAULT 'unknown',
    PRIMARY KEY (respondent, period_utc, fuel_code)
);

CREATE TABLE IF NOT EXISTS raw._ingest_watermarks (
    route_key  VARCHAR NOT NULL,
    respondent VARCHAR NOT NULL,
    max_period TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (route_key, respondent)
);

CREATE TABLE IF NOT EXISTS raw._load_meta (
    key        VARCHAR PRIMARY KEY,
    value      VARCHAR,
    updated_at TIMESTAMP
);
"""

# Older warehouses created before row-level provenance existed.
_MIGRATIONS = [
    "ALTER TABLE raw.eia_region_data ADD COLUMN IF NOT EXISTS _source VARCHAR DEFAULT 'unknown'",
    "ALTER TABLE raw.eia_fuel_mix   ADD COLUMN IF NOT EXISTS _source VARCHAR DEFAULT 'unknown'",
]


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open (or create) the warehouse and ensure the raw schema exists."""
    con = duckdb.connect(db_path)
    con.execute(_DDL)
    for migration in _MIGRATIONS:
        con.execute(migration)
    return con


def upsert_rows(
    con: duckdb.DuckDBPyConnection,
    route_key: str,
    rows: list[EIARow],
    source: str,
) -> int:
    """Idempotently upsert parsed rows, stamping row-level provenance.

    ``source`` is mandatory on purpose: provenance must be an explicit
    decision at every call site, never a default.
    """
    if not rows:
        return 0
    table = TABLE_FOR_ROUTE[route_key]
    code_col = CODE_COL_FOR_ROUTE[route_key]
    ingested_at = datetime.now(UTC).replace(tzinfo=None)

    frame = pd.DataFrame(
        {
            "period_utc": [r.period_utc for r in rows],
            "respondent": [r.respondent for r in rows],
            code_col: [r.series for r in rows],
            "value": pd.array([r.value for r in rows], dtype="float64"),
            "value_units": [r.units for r in rows],
            "_ingested_at": ingested_at,
            "_source": source,
        }
    )
    # If the same natural key appears twice in one batch (e.g. the API
    # restates a row mid-window), keep the LAST occurrence. DuckDB's
    # INSERT OR REPLACE ... SELECT keeps the FIRST intra-batch duplicate,
    # which would silently preserve the stale value — the opposite of this
    # pipeline's late-arrival semantics. (Verified empirically; the old
    # per-row executemany path was last-wins.)
    frame = frame.drop_duplicates(
        subset=["respondent", "period_utc", code_col], keep="last"
    )
    con.register("_upsert_batch", frame)
    try:
        con.execute(
            f"INSERT OR REPLACE INTO {table} "
            f"(period_utc, respondent, {code_col}, value, value_units, _ingested_at, _source) "
            f"SELECT period_utc, respondent, {code_col}, value, value_units, "
            "_ingested_at, _source FROM _upsert_batch"
        )
    finally:
        con.unregister("_upsert_batch")
    return len(rows)


def warehouse_sources(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Distinct row-level provenance values across both raw tables."""
    result = con.execute(
        "SELECT DISTINCT _source FROM raw.eia_region_data "
        "UNION SELECT DISTINCT _source FROM raw.eia_fuel_mix"
    ).fetchall()
    return {row[0] for row in result}


def get_watermark(
    con: duckdb.DuckDBPyConnection, route_key: str, respondent: str
) -> datetime | None:
    row = con.execute(
        "SELECT max_period FROM raw._ingest_watermarks WHERE route_key = ? AND respondent = ?",
        [route_key, respondent],
    ).fetchone()
    return row[0] if row else None


def set_watermark(
    con: duckdb.DuckDBPyConnection, route_key: str, respondent: str, max_period: datetime
) -> None:
    # Monotonic by construction: a watermark is a HIGH-water mark, so no
    # code path may ever lower one. (A replay window that finishes at an old
    # period must not regress the resume point of ordinary ingestion.)
    con.execute(
        """
        INSERT INTO raw._ingest_watermarks VALUES (?, ?, ?, ?)
        ON CONFLICT (route_key, respondent) DO UPDATE SET
            max_period = greatest(_ingest_watermarks.max_period,
                                  excluded.max_period),
            updated_at = excluded.updated_at
        """,
        [route_key, respondent, max_period, datetime.now(UTC).replace(tzinfo=None)],
    )


def set_load_meta(con: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    con.execute(
        "INSERT OR REPLACE INTO raw._load_meta VALUES (?, ?, ?)",
        [key, value, datetime.now(UTC).replace(tzinfo=None)],
    )


def get_load_meta(con: duckdb.DuckDBPyConnection, key: str) -> str | None:
    row = con.execute("SELECT value FROM raw._load_meta WHERE key = ?", [key]).fetchone()
    return row[0] if row else None
