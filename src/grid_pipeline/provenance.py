"""Warehouse provenance: is this data real, synthetic, or unknown?

Classification is fail-closed and based on ROW-LEVEL lineage (the _source
column on every raw row), not on a global flag. A global flag can be
overwritten by a later run and silently launder synthetic rows; the rows
themselves cannot lie about where they came from.

Real rows come from EIA by one of two roads, and each row says which:
    'eia_api'  — the API, the normal nightly path
    'eia_bulk' — EIA's bulk file, loaded on a night the API did not answer
                 (see bulk.py); the API's own row replaces it later

Verdicts:
    'real'     — every raw row came from EIA (the API or its bulk file)
    'fixtures' — every raw row came from the dev fixture generator
    'mixed'    — real and synthetic rows together (a contaminated warehouse)
    'empty'    — no raw rows at all
    'unknown'  — any row with unrecognized/missing provenance

Consumers must treat anything other than 'real' as not-real: charts
watermark, real ingestion modes refuse to write. Missing metadata is never
interpreted as "probably fine".
"""

from __future__ import annotations

import duckdb

from . import db

SOURCE_API = "eia_api"
SOURCE_BULK = "eia_bulk"
SOURCE_FIXTURES = "dev_fixtures"
REAL_SOURCES = frozenset({SOURCE_API, SOURCE_BULK})


def data_provenance(con: duckdb.DuckDBPyConnection) -> str:
    sources = db.warehouse_sources(con)
    if not sources:
        return "empty"
    if sources - REAL_SOURCES - {SOURCE_FIXTURES}:
        return "unknown"
    if sources <= REAL_SOURCES:
        return "real"
    if sources == {SOURCE_FIXTURES}:
        return "fixtures"
    return "mixed"
