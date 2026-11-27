"""EIA's bulk file: a second road to the same data, for nights the API is silent.

EIA publishes the Hourly Electric Grid Monitor twice. The API answers
queries. A plain web server also hands out the same hours as one CSV per
half year (EIA930_BALANCE_2026_Jul_Dec.csv, about 7 MB gzipped). On
Nov 26-27, 2026 the API answered none of 105 tries over 14 hours. The copy
of the file written on the 27th held the hours the warehouse was missing,
through each BA's previous local midnight.

What was checked against the live API on Nov 27, 2026:

* An API ``period`` is the file's "UTC Time at End of Hour".
* An API ``value`` is the file's "(Adjusted)" column: the reported number,
  or EIA's imputed one where the report was missing or flagged. (BPAT
  interchange on Oct 1: reported -93, adjusted 0, and the API says 0.)
* The API has no row where the adjusted cell is blank, which is how a fuel
  that a BA does not report looks in the file.
* Region data, all 11 BAs, Nov 23-27: 3,325 values in both sources and 33
  of them different. All 33 are demand hours of two BAs whose reports were
  missing: the file held EIA's imputed values, and the API, pulled three
  hours after the file was written, had others.
* Fuel data, four BAs, Nov 24-25: 1,824 values, none different.

So the file is the same data as of the moment it was written, and its rows
are treated as provisional: ingest.run_fallback loads only what the
warehouse lacks, labels it 'eia_bulk', and never moves a watermark, so the
API's next answer re-fetches and replaces them.

This module only reads the file. BulkClient answers ``iter_rows`` with
records shaped like the API's, so the ingest validates and stores them
through the same code as API rows.
"""

from __future__ import annotations

import csv
import logging
import time
from collections.abc import Callable, Collection, Iterator
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .client import PERIOD_FORMAT, EIAError, UnavailableError

logger = logging.getLogger(__name__)

BULK_URL = ("https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/"
            "EIA930_BALANCE_{year}_{half}.csv")
BA_COLUMN = "Balancing Authority"
TIME_COLUMN = "UTC Time at End of Hour"
TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"  # 11/24/2026 12:00:00 AM
UNITS = "megawatthours"  # what the API calls the same numbers

# API series code -> the file's column. Region data is matched by exact name.
REGION_COLUMNS = {
    "D": "Demand (MW) (Adjusted)",
    "NG": "Net Generation (MW) (Adjusted)",
    "TI": "Total Interchange (MW) (Adjusted)",
}
# Fuel columns read "Net Generation (MW) from <fuel> (Adjusted)". They are
# matched on <fuel>, lower-cased and with runs of spaces collapsed, because
# EIA's own header has a doubled space and a typo ("Solar witho Integrated").
# Both spellings are listed, so a corrected header keeps working. The 13
# codes the 11 BAs report were matched to the API by value; WNB, UES and UNK
# are matched by name only.
FUEL_PREFIX = "Net Generation (MW) from "
ADJUSTED_SUFFIX = " (Adjusted)"
FUEL_CODES = {
    "coal": "COL",
    "natural gas": "NG",
    "nuclear": "NUC",
    "all petroleum products": "OIL",
    "hydropower excluding pumped storage": "WAT",
    "pumped storage": "PS",
    "solar without integrated battery storage": "SUN",
    "solar with integrated battery storage": "SNB",
    "solar witho integrated battery storage": "SNB",
    "wind without integrated battery storage": "WND",
    "wind with integrated battery storage": "WNB",
    "battery storage": "BAT",
    "other energy storage": "OES",
    "unknown energy storage": "UES",
    "geothermal": "GEO",
    "other fuel sources": "OTH",
    "unknown fuel sources": "UNK",
}

DOWNLOAD_TRIES = 3
DOWNLOAD_TIMEOUT = (10, 60)  # seconds: to connect, and between bytes
DOWNLOAD_RETRY_SECONDS = 10


class BulkFormatError(EIAError):
    """The file is not laid out the way this module was checked against."""


class BulkFileMissing(EIAError):
    """No file at that address (a half year that is not published yet)."""


# One parsed file: {BA: {period: ({region code: value}, {fuel code: value})}}
Parsed = dict[str, dict[datetime, tuple[dict[str, str], dict[str, str]]]]


def _squash(name: str) -> str:
    return " ".join(name.split())


def _columns(header: list[str]) -> tuple[int, int, dict[str, int], dict[str, int]]:
    """Find the columns to read, or refuse the file.

    Refusing is the point: a renamed or added column means the layout this
    module was checked against has changed, and guessing would put numbers
    under the wrong series.
    """
    names = [_squash(h) for h in header]
    index = {name: i for i, name in enumerate(names)}
    for column in (BA_COLUMN, TIME_COLUMN, *REGION_COLUMNS.values()):
        if column not in index:
            raise BulkFormatError(f"EIA's bulk file has no column {column!r}")
    fuel: dict[str, int] = {}
    for i, name in enumerate(names):
        if not (name.startswith(FUEL_PREFIX) and name.endswith(ADJUSTED_SUFFIX)):
            continue
        label = name[len(FUEL_PREFIX):-len(ADJUSTED_SUFFIX)].strip().lower()
        code = FUEL_CODES.get(label)
        if code is None:
            raise BulkFormatError(
                f"EIA's bulk file has a fuel column this pipeline does not "
                f"know: {header[i]!r}")
        if code in fuel:
            raise BulkFormatError(
                f"EIA's bulk file has two columns for fuel {code}: "
                f"{header[fuel[code]]!r} and {header[i]!r}")
        fuel[code] = i
    if not fuel:
        raise BulkFormatError("EIA's bulk file has no adjusted fuel columns")
    region = {code: index[name] for code, name in REGION_COLUMNS.items()}
    return index[BA_COLUMN], index[TIME_COLUMN], region, fuel


def parse_balance_file(text: str, respondents: Collection[str]) -> Parsed:
    """Read one BALANCE file, keeping only the BAs asked for.

    Blank cells are left out, as the API leaves out the row. Values stay the
    strings EIA wrote, like the API's.
    """
    reader = csv.reader(text.lstrip("﻿").splitlines())
    try:
        header = next(reader)
    except StopIteration:
        raise BulkFormatError("EIA's bulk file is empty") from None
    ba_col, time_col, region_cols, fuel_cols = _columns(header)
    width = max(ba_col, time_col, *region_cols.values(), *fuel_cols.values()) + 1
    parsed: Parsed = {ba: {} for ba in respondents}
    for line_no, row in enumerate(reader, start=2):
        if not row or row[ba_col] not in parsed:
            continue
        if len(row) < width:
            raise BulkFormatError(
                f"EIA's bulk file line {line_no} has {len(row)} fields, "
                f"expected at least {width}")
        try:
            period = datetime.strptime(row[time_col], TIME_FORMAT)
        except ValueError:
            raise BulkFormatError(
                f"EIA's bulk file line {line_no}: unreadable {TIME_COLUMN} "
                f"{row[time_col]!r}") from None
        parsed[row[ba_col]][period] = (
            {code: row[i] for code, i in region_cols.items() if row[i] != ""},
            {code: row[i] for code, i in fuel_cols.items() if row[i] != ""},
        )
    return parsed


def _half(moment: datetime) -> tuple[int, str]:
    return moment.year, "Jan_Jun" if moment.month <= 6 else "Jul_Dec"


def files_for(start: datetime, end: datetime) -> list[tuple[int, str]]:
    """The half-year files that can hold hours in [start, end], oldest first.

    A file starts at each BA's local midnight, so the first UTC hours of
    Jan 1 and Jul 1 sit at the end of the file before. Hence the 12 hours.
    """
    cursor = min(start - timedelta(hours=12), end)
    halves = [_half(cursor)]
    while halves[-1] != _half(end):
        year, half = halves[-1]
        halves.append((year, "Jul_Dec") if half == "Jan_Jun" else (year + 1, "Jan_Jun"))
    return halves


def http_fetch(url: str) -> tuple[str, datetime | None]:
    """Download one file: its text, and when EIA last wrote it (UTC).

    A web server in trouble can answer 200 with an error or challenge page,
    so anything that does not start like the file counts as a failed try.
    """
    last = ""
    answered = False
    for attempt in range(1, DOWNLOAD_TRIES + 1):
        try:
            resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as exc:
            last = type(exc).__name__
        else:
            if resp.status_code == 404:
                raise BulkFileMissing(f"EIA has no bulk file at {url}")
            # The server names no charset; decoding here also keeps requests
            # from guessing one over 40 MB.
            text = resp.content.decode("utf-8-sig", errors="replace")
            if resp.status_code == 200 and BA_COLUMN in text[:200]:
                return text, _http_date(resp.headers.get("Last-Modified"))
            answered = answered or resp.status_code < 500
            last = (f"HTTP {resp.status_code}" if resp.status_code != 200
                    else "a page that is not the file")
        logger.warning("Bulk file download failed (try %d/%d): %s",
                       attempt, DOWNLOAD_TRIES, last)
        if attempt < DOWNLOAD_TRIES:
            time.sleep(DOWNLOAD_RETRY_SECONDS)
    message = f"EIA's bulk file could not be downloaded in {DOWNLOAD_TRIES} tries ({last})"
    raise EIAError(message) if answered else UnavailableError(message)


def _http_date(value: str | None) -> datetime | None:
    try:
        stamp = parsedate_to_datetime(value or "")
    except (TypeError, ValueError):
        return None
    if stamp.tzinfo is None:
        return stamp
    return stamp.astimezone(UTC).replace(tzinfo=None)


class BulkClient:
    """Answers ``iter_rows`` from the bulk file, in the API's record shape."""

    def __init__(
        self,
        respondents: Collection[str],
        fetch: Callable[[str], tuple[str, datetime | None]] = http_fetch,
        url_template: str = BULK_URL,
    ):
        self.respondents = list(respondents)
        self.fetch = fetch
        self.url_template = url_template
        self.last_modified: datetime | None = None  # newest file read so far
        self._files: dict[tuple[int, str], Parsed | None] = {}

    def _file(self, half: tuple[int, str]) -> Parsed | None:
        if half not in self._files:
            url = self.url_template.format(year=half[0], half=half[1])
            try:
                text, modified = self.fetch(url)
            except BulkFileMissing:
                logger.info("EIA has no bulk file for %d %s", *half)
                self._files[half] = None
            else:
                self._files[half] = parse_balance_file(text, self.respondents)
                if modified and (self.last_modified is None or modified > self.last_modified):
                    self.last_modified = modified
                written = f"{modified:%Y-%m-%d %H:%M} UTC" if modified else "unknown"
                logger.info("Read EIA's bulk file for %d %s (%.1f MB, written: %s)",
                            *half, len(text) / 1e6, written)
        return self._files[half]

    def iter_rows(
        self,
        route: str,
        *,
        frequency: str = "hourly",
        facets: dict[str, list[str]] | None = None,
        start: str | None = None,
        end: str | None = None,
        tiebreak_column: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Every record for the query, sorted by period, then series code."""
        if frequency != "hourly" or not start or not end:
            raise EIAError("The bulk file answers hourly queries with a start and an end")
        facets = facets or {}
        if route.endswith("region-data"):
            series_key, part = "type", 0
            codes = set(facets.get("type") or REGION_COLUMNS)
        elif route.endswith("fuel-type-data"):
            series_key, part = "fueltype", 1
            codes = set(facets.get("fueltype") or FUEL_CODES.values())
        else:
            raise EIAError(f"The bulk file has no data for route {route!r}")
        first = datetime.strptime(start, PERIOD_FORMAT)
        last = datetime.strptime(end, PERIOD_FORMAT)
        files = [f for f in (self._file(half) for half in files_for(first, last)) if f]
        if not files:
            raise EIAError(f"EIA has no bulk file covering {start}..{end}")
        for ba in facets.get("respondent") or self.respondents:
            hours: dict[datetime, dict[str, str]] = {}
            for parsed in files:  # oldest first, so a newer file wins a shared hour
                if ba not in parsed:
                    raise EIAError(f"The bulk file was not read for respondent {ba!r}")
                for period, values in parsed[ba].items():
                    if first <= period <= last:
                        hours[period] = values[part]
            for period in sorted(hours):
                for code in sorted(hours[period]):
                    if code in codes:
                        yield {"period": period.strftime(PERIOD_FORMAT),
                               "respondent": ba, series_key: code,
                               "value": hours[period][code], "value-units": UNITS}
