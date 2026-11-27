"""EIA's bulk file as a second source: how it is read, and what is refused.

The fixture is a verbatim excerpt of EIA930_BALANCE_2026_Jul_Dec.csv as EIA
served it on Nov 27, 2026 (written 15:42 UTC): the header and 71 rows for
five BAs. The values expected here are what the live API returned for the
same hours that day, so these tests pin the mapping that was checked by
hand: which column is which series, and which timestamp is the period.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path

import pytest
import requests

from grid_pipeline import bulk
from grid_pipeline.bulk import (
    BulkClient,
    BulkFileMissing,
    BulkFormatError,
    files_for,
    http_fetch,
    parse_balance_file,
)
from grid_pipeline.client import EIAError, EIARow, UnavailableError, parse_row

EXCERPT = (Path(__file__).parent / "fixtures" / "eia930_balance_excerpt.csv").read_text(
    encoding="utf-8")
HEADER = EXCERPT.splitlines()[0]
WRITTEN = datetime(2026, 11, 27, 15, 42, 6)
REGION = "electricity/rto/region-data"
FUEL = "electricity/rto/fuel-type-data"


def _client(text: str = EXCERPT, respondents=("BPAT", "CISO", "SOCO", "SWPP")) -> BulkClient:
    def fetch(url: str):
        if "EIA930_BALANCE_2026_Jul_Dec.csv" not in url:
            raise BulkFileMissing(url)
        return text, WRITTEN

    return BulkClient(respondents, fetch=fetch)


def _rows(client: BulkClient, route: str, ba: str, start: str, end: str, **facets):
    return list(client.iter_rows(route, facets={"respondent": [ba], **facets},
                                 start=start, end=end))


def _values(rows, key):
    return {r[key]: r["value"] for r in rows}


# --- the mapping: same numbers as the API, under the same keys ---------------

@pytest.mark.parametrize("period,api", [
    ("2026-11-25T00", {"D": "26697", "NG": "20476", "TI": "-3199"}),
    ("2026-11-25T09", {"D": "23032", "NG": "16566", "TI": "-6739"}),
    ("2026-11-25T23", {"D": "27163", "NG": "17422", "TI": "-2688"}),
])
def test_region_rows_are_what_the_api_returned(period, api):
    rows = _rows(_client(), REGION, "CISO", period, period)
    assert _values(rows, "type") == api
    assert rows[0] == {"period": period, "respondent": "CISO", "type": "D",
                       "value": api["D"], "value-units": "megawatthours"}


def test_fuel_rows_are_what_the_api_returned():
    rows = _rows(_client(), FUEL, "SOCO", "2026-11-22T21", "2026-11-22T21")
    # Twelve fuels, as in the API: SOCO reports no GEO, WNB, UES or UNK, and
    # a fuel a BA does not report is a blank cell here and no row there.
    assert _values(rows, "fueltype") == {
        "BAT": "-157", "COL": "2848", "NG": "13422", "NUC": "8216", "OES": "0",
        "OIL": "-2", "OTH": "54", "PS": "85", "SNB": "-2", "SUN": "209",
        "WAT": "93", "WND": "0"}
    assert [r["fueltype"] for r in rows] == sorted(r["fueltype"] for r in rows)


def test_the_adjusted_value_is_read_not_the_reported_one():
    # BPAT reported -93 MW of interchange; EIA adjusted it to 0, and 0 is
    # what the API serves.
    line = next(ln for ln in EXCERPT.splitlines()
                if ln.startswith("BPAT,") and "10/01/2026 3:00:00 PM" in ln)
    assert ",-93," in line
    ti = _values(_rows(_client(), REGION, "BPAT", "2026-10-01T15", "2026-10-01T15"), "type")["TI"]
    assert ti == "0"
    # SOCO reported -9881 MW of solar; the adjusted column has 209.
    sun = _values(_rows(_client(), FUEL, "SOCO", "2026-11-22T22", "2026-11-22T22"),
                  "fueltype")["SUN"]
    assert sun == "209"


def test_a_blank_cell_gives_no_row():
    # SWPP's reports stopped at 06:00 UTC on Nov 26. EIA imputed demand and
    # generation for 07:00 and left interchange blank.
    assert set(_values(_rows(_client(), REGION, "SWPP", "2026-11-26T06", "2026-11-26T06"),
                       "type")) == {"D", "NG", "TI"}
    assert set(_values(_rows(_client(), REGION, "SWPP", "2026-11-26T07", "2026-11-26T07"),
                       "type")) == {"D", "NG"}
    # The file runs ahead of the data: these hours hold a forecast only.
    assert _rows(_client(), REGION, "CISO", "2026-11-27T09", "2026-11-27T10") == []
    assert _rows(_client(), FUEL, "CISO", "2026-11-27T09", "2026-11-27T10") == []


def test_rows_go_through_the_same_parser_as_api_rows():
    region = _rows(_client(), REGION, "CISO", "2026-11-25T00", "2026-11-25T00")
    assert parse_row(region[0], "type") == EIARow(
        period_utc=datetime(2026, 11, 25, 0), respondent="CISO", series="D",
        value=26697.0, units="megawatthours")
    fuel = _rows(_client(), FUEL, "SOCO", "2026-11-22T21", "2026-11-22T21")
    assert parse_row(fuel[0], "fueltype").series == "BAT"


def test_window_and_type_filters_and_the_order_of_rows():
    rows = _rows(_client(), REGION, "CISO", "2026-11-25T01", "2026-11-25T03", type=["D", "TI"])
    assert [(r["period"], r["type"]) for r in rows] == [
        ("2026-11-25T01", "D"), ("2026-11-25T01", "TI"),
        ("2026-11-25T02", "D"), ("2026-11-25T02", "TI"),
        ("2026-11-25T03", "D"), ("2026-11-25T03", "TI")]
    everything = _rows(_client(), REGION, "CISO", "2026-11-01T00", "2026-12-31T23")
    assert everything[0]["period"] == "2026-11-25T00"
    assert everything[-1]["period"] == "2026-11-27T08"  # the last hour with data
    assert len(everything) == 57 * 3


def test_only_the_respondents_asked_for_are_kept():
    assert set(parse_balance_file(EXCERPT, ["CISO"])) == {"CISO"}
    assert len(parse_balance_file(EXCERPT, ["CISO"])["CISO"]) == 59
    with pytest.raises(EIAError, match="AECI"):
        _rows(_client(), REGION, "AECI", "2026-07-01T00", "2026-07-02T00")
    with pytest.raises(EIAError, match="no data for route"):
        _rows(_client(), "electricity/rto/interchange-data", "CISO", "2026-11-25T00",
              "2026-11-25T01")


# --- the header: EIA's typos, and a layout that has changed ------------------

def test_eias_header_typos_and_their_correction_both_read():
    # As served on Nov 27, 2026. If EIA fixes these, the file must still read.
    assert "from Solar witho Integrated Battery Storage (Adjusted)" in HEADER
    assert "from Pumped Storage  (Adjusted)" in HEADER
    corrected = EXCERPT.replace("Solar witho Integrated", "Solar with Integrated").replace(
        "Pumped Storage  (Adjusted)", "Pumped Storage (Adjusted)")
    assert corrected != EXCERPT
    args = (FUEL, "SOCO", "2026-11-22T21", "2026-11-22T23")
    assert _rows(_client(corrected), *args) == _rows(_client(), *args)
    assert {"PS", "SNB"} <= set(_values(_rows(_client(), *args), "fueltype"))


@pytest.mark.parametrize("old,new,message", [
    ("from Geothermal (Adjusted)", "from Fusion (Adjusted)", "does not know"),
    ("from Wind with Integrated Battery Storage (Adjusted)",
     "from Solar with Integrated Battery Storage (Adjusted)", "two columns for fuel SNB"),
    ('"UTC Time at End of Hour"', '"UTC Time"', "no column 'UTC Time at End of Hour'"),
    ('"Demand (MW) (Adjusted)"', '"Demand (Adjusted)"', "no column 'Demand"),
    ("11/25/2026 12:00:00 AM", "2026-11-25 00:00", "unreadable"),
])
def test_a_layout_that_has_changed_is_refused(old, new, message):
    assert old in EXCERPT
    with pytest.raises(BulkFormatError, match=message):
        parse_balance_file(EXCERPT.replace(old, new), ["CISO"])


def test_a_file_that_is_not_the_file_is_refused():
    with pytest.raises(BulkFormatError, match="empty"):
        parse_balance_file("", ["CISO"])
    with pytest.raises(BulkFormatError, match="no column"):
        parse_balance_file("<html><body>Access denied</body></html>", ["CISO"])
    short = HEADER + "\nCISO,11/24/2026,17\n"
    with pytest.raises(BulkFormatError, match="line 2 has 3 fields"):
        parse_balance_file(short, ["CISO"])
    no_fuel = io.StringIO()
    csv.writer(no_fuel, quoting=csv.QUOTE_ALL).writerow(
        name for name in next(csv.reader([HEADER])) if " from " not in name)
    with pytest.raises(BulkFormatError, match="no adjusted fuel columns"):
        parse_balance_file(no_fuel.getvalue(), ["CISO"])


# --- which files, and how often ----------------------------------------------

@pytest.mark.parametrize("start,end,expected", [
    (datetime(2026, 11, 23), datetime(2026, 11, 27, 19), [(2026, "Jul_Dec")]),
    # A file starts at local midnight, so early Jan 1 UTC is in the old file.
    (datetime(2027, 1, 1, 3), datetime(2027, 1, 2, 17), [(2026, "Jul_Dec"), (2027, "Jan_Jun")]),
    (datetime(2026, 6, 29), datetime(2026, 7, 2), [(2026, "Jan_Jun"), (2026, "Jul_Dec")]),
    (datetime(2026, 7, 1, 13), datetime(2026, 7, 2), [(2026, "Jul_Dec")]),
    (datetime(2025, 12, 1), datetime(2027, 1, 2),
     [(2025, "Jul_Dec"), (2026, "Jan_Jun"), (2026, "Jul_Dec"), (2027, "Jan_Jun")]),
])
def test_files_for(start, end, expected):
    assert files_for(start, end) == expected


def test_a_half_year_that_is_not_published_yet_is_skipped_and_files_are_read_once():
    asked: list[str] = []

    def fetch(url: str):
        asked.append(url.rsplit("/", 1)[-1])
        if "2026_Jul_Dec" not in url:
            raise BulkFileMissing(url)
        return EXCERPT, WRITTEN

    client = BulkClient(["CISO"], fetch=fetch)
    for _ in range(2):
        rows = _rows(client, REGION, "CISO", "2026-11-25T00", "2027-01-02T00")
        assert len(rows) == 57 * 3
    assert asked == ["EIA930_BALANCE_2026_Jul_Dec.csv", "EIA930_BALANCE_2027_Jan_Jun.csv"]
    assert client.last_modified == WRITTEN
    with pytest.raises(EIAError, match="no bulk file covering"):
        _rows(client, REGION, "CISO", "2027-03-01T00", "2027-03-02T00")


# --- the download ------------------------------------------------------------

class _Response:
    def __init__(self, status: int, body: str, headers: dict | None = None):
        self.status_code = status
        self.content = body.encode("utf-8")
        self.headers = headers or {}


def _serve(monkeypatch, *answers):
    """Make requests.get return (or raise) each answer in turn."""
    calls: list[str] = []
    queue = list(answers)

    def get(url, timeout):
        calls.append(url)
        answer = queue.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(bulk.requests, "get", get)
    monkeypatch.setattr(bulk.time, "sleep", lambda seconds: None)
    return calls


FILE = _Response(200, EXCERPT, {"Last-Modified": "Fri, 27 Nov 2026 15:42:06 GMT"})
CHALLENGE = _Response(200, "<html><body>Checking your browser...</body></html>")


def test_download_returns_the_text_and_when_eia_wrote_it(monkeypatch):
    calls = _serve(monkeypatch, FILE)
    text, written = http_fetch("https://example.test/file.csv")
    assert text == EXCERPT and written == WRITTEN
    assert calls == ["https://example.test/file.csv"]
    _serve(monkeypatch, _Response(200, EXCERPT))
    assert http_fetch("https://example.test/file.csv") == (EXCERPT, None)


def test_download_tries_again_past_a_page_that_is_not_the_file(monkeypatch, caplog):
    calls = _serve(monkeypatch, CHALLENGE, requests.ReadTimeout(), FILE)
    with caplog.at_level("WARNING"):
        text, _ = http_fetch("https://example.test/file.csv")
    assert text == EXCERPT and len(calls) == 3
    assert "a page that is not the file" in caplog.text and "ReadTimeout" in caplog.text


def test_download_that_never_gets_the_file_says_which_kind_of_failure(monkeypatch):
    # The server answered, with the wrong thing: for a person to look at.
    _serve(monkeypatch, CHALLENGE, CHALLENGE, CHALLENGE)
    with pytest.raises(EIAError, match="not the file") as wrong:
        http_fetch("https://example.test/file.csv")
    assert not isinstance(wrong.value, UnavailableError)
    _serve(monkeypatch, _Response(403, "Forbidden"), _Response(403, "Forbidden"),
           _Response(403, "Forbidden"))
    with pytest.raises(EIAError, match="HTTP 403") as denied:
        http_fetch("https://example.test/file.csv")
    assert not isinstance(denied.value, UnavailableError)
    # Nobody answered: the same kind of outage as a silent API.
    _serve(monkeypatch, requests.ConnectTimeout(), requests.ConnectionError(),
           _Response(503, "busy"))
    with pytest.raises(UnavailableError, match="3 tries"):
        http_fetch("https://example.test/file.csv")


def test_a_missing_file_is_not_retried(monkeypatch):
    calls = _serve(monkeypatch, _Response(404, "Not Found"))
    with pytest.raises(BulkFileMissing):
        http_fetch("https://example.test/file.csv")
    assert len(calls) == 1
