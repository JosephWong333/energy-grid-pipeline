"""Minimal, robust client for the EIA API v2.

Design notes
------------
* The API caps responses at 5000 rows, so everything is built around
  transparent offset pagination (`iter_rows`).
* Throttling happens before EVERY HTTP request (min-interval pacing), not
  merely between pages of one query — a backfill of single-page month
  windows must not burst-fire requests. EIA asks clients to stay well under
  ~5 requests/second.
* Retries cover 429/5xx AND transport-level failures (timeouts, connection
  resets, malformed JSON), with exponential backoff. `Retry-After` is
  honored up to a hard cap; a server demanding an absurd wait (the shared
  demo key can return values in the hours) aborts cleanly with
  ThrottledError instead — ingestion is resumable by design, so "come back
  later" beats a 7-hour sleep inside a CI job.
* Pagination is sorted by (period, series) so ties within an hour can't
  straddle page boundaries nondeterministically, and an empty page while
  offset < total raises instead of silently accepting a truncated result
  (which would otherwise advance watermarks past never-fetched rows).
* EIA's JSON is loosely typed: numbers arrive as strings, `total` is a
  string, `value` can be null. Parsing is defensive and never assumes types.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
TRANSPORT_ERRORS = (
    requests.Timeout,
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    ValueError,  # resp.json() decode failures
)

# EIA hourly periods look like "2024-06-01T13" (UTC, hour precision).
PERIOD_FORMAT = "%Y-%m-%dT%H"


class EIAError(RuntimeError):
    """Raised when the EIA API returns an unrecoverable error."""


class ThrottledError(EIAError):
    """The server demanded a wait beyond our cap. Resume the run later."""


@dataclass(frozen=True)
class EIARow:
    """One parsed observation from any /rto/ route."""

    period_utc: datetime
    respondent: str
    series: str  # 'D' | 'NG' | 'TI' for region-data; fuel code for fuel-type-data
    value: float | None
    units: str


def _coerce_float(raw: Any) -> float | None:
    """EIA sends numbers as strings, numbers, or null. Normalize to float | None."""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("Unparseable value from EIA: %r", raw)
        return None


def parse_row(record: dict[str, Any], series_key: str) -> EIARow:
    """Convert one raw API record into a typed EIARow.

    `series_key` is 'type' for region-data and 'fueltype' for fuel-type-data.
    """
    return EIARow(
        period_utc=datetime.strptime(record["period"], PERIOD_FORMAT),
        respondent=str(record["respondent"]),
        series=str(record[series_key]),
        value=_coerce_float(record.get("value")),
        units=str(record.get("value-units") or ""),
    )


class EIAClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.eia.gov/v2",
        page_size: int = 5000,
        sleep_between_requests: float = 0.4,
        max_retries: int = 5,
        backoff_base_seconds: float = 2.0,
        max_retry_after_seconds: float = 300.0,
        timeout: float = 30.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.sleep_between_requests = sleep_between_requests
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.max_retry_after_seconds = max_retry_after_seconds
        self.timeout = timeout
        self.session = session or requests.Session()
        self._sleep = sleep  # injectable so tests don't actually wait
        self._clock = clock
        self._last_request_at: float | None = None

    # ------------------------------------------------------------------ #
    # HTTP layer
    # ------------------------------------------------------------------ #
    def _throttle(self) -> None:
        """Enforce a minimum interval before EVERY request, across queries."""
        if self._last_request_at is not None:
            elapsed = self._clock() - self._last_request_at
            remaining = self.sleep_between_requests - elapsed
            if remaining > 0:
                self._sleep(remaining)
        self._last_request_at = self._clock()

    def _get(self, route: str, params: dict[str, Any]) -> dict[str, Any]:
        """GET one page with throttling and retry/backoff. Returns parsed JSON."""
        url = f"{self.base_url}/{route}/data/"
        merged = {"api_key": self.api_key, **params}

        last_error = ""
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                resp = self.session.get(url, params=merged, timeout=self.timeout)
                if resp.status_code == 200:
                    body = resp.json()  # may raise ValueError -> retried
                    if "response" not in body:
                        # v2 reports request-level errors inside a 200 sometimes.
                        raise EIAError(f"Unexpected EIA payload for {route}: {body}")
                    return body
            except TRANSPORT_ERRORS as exc:
                wait = self.backoff_base_seconds * (2**attempt)
                last_error = type(exc).__name__
                logger.warning(
                    "Transport error on %s (attempt %d/%d): %s — retrying in %.1fs",
                    route, attempt + 1, self.max_retries, last_error, wait,
                )
                self._sleep(wait)
                continue

            if resp.status_code in RETRYABLE_STATUS:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.replace(".", "", 1).isdigit():
                    wait = float(retry_after)
                    if wait > self.max_retry_after_seconds:
                        raise ThrottledError(
                            f"EIA asked us to wait {wait:.0f}s (Retry-After) on "
                            f"{route}; cap is {self.max_retry_after_seconds:.0f}s. "
                            "Aborting cleanly — the run is resumable, re-run later."
                        )
                else:
                    wait = self.backoff_base_seconds * (2**attempt)
                last_error = f"HTTP {resp.status_code}"
                logger.warning(
                    "EIA %s on %s (attempt %d/%d) — retrying in %.1fs",
                    resp.status_code, route, attempt + 1, self.max_retries, wait,
                )
                self._sleep(wait)
                continue

            raise EIAError(
                f"EIA API error {resp.status_code} on {route}: {resp.text[:500]}"
            )

        raise EIAError(f"EIA API still failing after {self.max_retries} retries ({last_error})")

    # ------------------------------------------------------------------ #
    # Pagination
    # ------------------------------------------------------------------ #
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
        """Yield every raw record for a query, walking offset pagination.

        Sorted by period ascending (plus a secondary tiebreak column when
        given — many rows share one hourly period, and a stable total order
        is what makes offset pagination safe across page boundaries).
        """
        params: dict[str, Any] = {
            "frequency": frequency,
            "data[0]": "value",
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "length": self.page_size,
        }
        if tiebreak_column:
            params["sort[1][column]"] = tiebreak_column
            params["sort[1][direction]"] = "asc"
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        for facet, values in (facets or {}).items():
            params[f"facets[{facet}][]"] = values

        offset = 0
        while True:
            page = self._get(route, {**params, "offset": offset})
            response = page["response"]
            rows = response.get("data") or []
            total = int(response.get("total") or 0)  # 'total' arrives as a string

            if not rows:
                if offset < total:
                    raise EIAError(
                        f"Truncated pagination on {route}: empty page at offset "
                        f"{offset} but server reports total={total}. Refusing to "
                        "treat a partial result as complete."
                    )
                return

            yield from rows
            offset += len(rows)
            if offset >= total:
                return
