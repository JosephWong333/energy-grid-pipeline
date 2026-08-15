"""Client unit tests. No network: responses are stubbed."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from grid_pipeline.client import EIAClient, EIAError, ThrottledError, parse_row

FIXTURES = Path(__file__).parent / "fixtures"


class FakeResponse:
    def __init__(self, status_code: int, body: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    """Returns queued responses and records the params of every call."""

    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        return self._responses.pop(0)


def make_client(responses: list[FakeResponse], **kwargs) -> tuple[EIAClient, FakeSession]:
    session = FakeSession(responses)
    sleeps: list[float] = []
    client = EIAClient(
        api_key="test-key",
        session=session,
        sleep=sleeps.append,  # capture instead of waiting
        **kwargs,
    )
    client._captured_sleeps = sleeps  # type: ignore[attr-defined]
    return client, session


def page(records: list[dict], total: int) -> dict:
    return {"response": {"total": str(total), "data": records}}


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
def test_parse_row_coerces_real_api_shapes():
    body = json.loads((FIXTURES / "eia_response_page.json").read_text())
    rows = [parse_row(r, "type") for r in body["response"]["data"]]

    assert rows[0].value == 21500.0          # string -> float
    assert rows[1].value == pytest.approx(20874.5)  # already numeric
    assert rows[2].value is None             # null preserved, row kept
    assert rows[3].value == -3120.0          # negative interchange is legitimate
    assert rows[0].period_utc == datetime(2026, 6, 1, 0)
    assert rows[0].respondent == "CISO"
    assert rows[3].series == "TI"


def test_parse_row_fuel_series_key():
    rec = {
        "period": "2026-06-01T12",
        "respondent": "ERCO",
        "fueltype": "WND",
        "value": "9500.2",
        "value-units": "megawatthours",
    }
    row = parse_row(rec, "fueltype")
    assert row.series == "WND"
    assert row.value == pytest.approx(9500.2)


# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #
def test_iter_rows_walks_offsets_until_total():
    recs = [{"period": f"2026-06-01T{h:02d}", "respondent": "CISO", "type": "D",
             "value": "1", "value-units": "megawatthours"} for h in range(5)]
    client, session = make_client(
        [FakeResponse(200, page(recs[:3], total=5)), FakeResponse(200, page(recs[3:], total=5))],
        page_size=3,
    )

    got = list(client.iter_rows("electricity/rto/region-data",
                                facets={"respondent": ["CISO"], "type": ["D"]}))

    assert len(got) == 5
    assert [c["params"]["offset"] for c in session.calls] == [0, 3]
    # facets serialized the way the v2 API expects
    assert session.calls[0]["params"]["facets[respondent][]"] == ["CISO"]
    assert session.calls[0]["params"]["api_key"] == "test-key"


def test_iter_rows_stops_on_empty_page():
    client, session = make_client([FakeResponse(200, page([], total=0))])
    assert list(client.iter_rows("electricity/rto/region-data")) == []
    assert len(session.calls) == 1


# --------------------------------------------------------------------------- #
# Retry behavior
# --------------------------------------------------------------------------- #
def test_retries_on_429_and_honors_retry_after():
    ok = FakeResponse(200, page([], total=0))
    client, session = make_client(
        [FakeResponse(429, headers={"Retry-After": "7"}), ok]
    )
    list(client.iter_rows("electricity/rto/region-data"))
    assert len(session.calls) == 2
    assert 7.0 in client._captured_sleeps  # type: ignore[attr-defined]


def test_retries_on_500_with_exponential_backoff():
    ok = FakeResponse(200, page([], total=0))
    client, _ = make_client(
        [FakeResponse(500), FakeResponse(502), ok],
        backoff_base_seconds=1.0,
        sleep_between_requests=0,  # silence pacing; this test is about backoff
    )
    list(client.iter_rows("electricity/rto/region-data"))
    assert client._captured_sleeps[:2] == [1.0, 2.0]  # type: ignore[attr-defined]


def test_gives_up_after_max_retries():
    client, _ = make_client([FakeResponse(503)] * 3, max_retries=3)
    with pytest.raises(EIAError, match="after 3 retries"):
        list(client.iter_rows("electricity/rto/region-data"))


def test_non_retryable_error_fails_immediately():
    client, session = make_client([FakeResponse(403, {"error": "invalid api key"})])
    with pytest.raises(EIAError, match="403"):
        list(client.iter_rows("electricity/rto/region-data"))
    assert len(session.calls) == 1


# --------------------------------------------------------------------------- #
# Throttling, Retry-After cap, transport retries, truncation guard
# --------------------------------------------------------------------------- #
def test_throttles_between_separate_queries():
    """Pacing applies before EVERY request, not just between pages of one
    query — single-page month windows must not burst-fire."""
    responses = [FakeResponse(200, page([], total=0)) for _ in range(3)]
    session = FakeSession(responses)
    sleeps: list[float] = []
    fake_now = [100.0]
    client = EIAClient(
        api_key="k", session=session, sleep=sleeps.append,
        sleep_between_requests=0.4, clock=lambda: fake_now[0],
    )
    for _ in range(3):  # three back-to-back single-page queries, zero time passing
        list(client.iter_rows("electricity/rto/region-data"))
    assert sleeps == [pytest.approx(0.4), pytest.approx(0.4)]


def test_retry_after_beyond_cap_aborts_cleanly():
    client, _ = make_client(
        [FakeResponse(429, headers={"Retry-After": "27304"})],
        max_retry_after_seconds=300,
    )
    with pytest.raises(ThrottledError, match="27304"):
        list(client.iter_rows("electricity/rto/region-data"))


def test_transport_timeout_is_retried():
    import requests as _requests

    class FlakySession(FakeSession):
        def __init__(self, responses):
            super().__init__(responses)
            self._raised = False

        def get(self, url, params=None, timeout=None):
            if not self._raised:
                self._raised = True
                raise _requests.Timeout("boom")
            return super().get(url, params=params, timeout=timeout)

    session = FlakySession([FakeResponse(200, page([], total=0))])
    sleeps: list[float] = []
    client = EIAClient(api_key="k", session=session, sleep=sleeps.append,
                       sleep_between_requests=0)
    list(client.iter_rows("electricity/rto/region-data"))
    assert len(session.calls) == 1  # the retry after the timeout succeeded


def test_truncated_pagination_raises_instead_of_silently_completing():
    truncated = [FakeResponse(200, page([{"period": "2026-01-01T00"}], total=5000)),
                 FakeResponse(200, page([], total=5000))]
    client, _ = make_client(truncated, sleep_between_requests=0)
    with pytest.raises(EIAError, match="Truncated"):
        list(client.iter_rows("electricity/rto/region-data"))


def test_secondary_sort_column_is_requested():
    client, session = make_client([FakeResponse(200, page([], total=0))],
                                  sleep_between_requests=0)
    list(client.iter_rows("electricity/rto/region-data", tiebreak_column="type"))
    params = session.calls[0]["params"]
    assert params["sort[1][column]"] == "type"
    assert params["sort[1][direction]"] == "asc"
