"""A small OpenAQ v3 client: authentication, rate limiting, retries, pagination.

The API allows 60 requests a minute and 2,000 an hour per key, and repeatedly
going over can get a key banned, so the client throttles itself before the
server has to say no.
"""

from __future__ import annotations

import logging
import random
import time
from collections import deque
from collections.abc import Callable, Iterator
from typing import Any

import requests

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.openaq.org/v3"
PAGE_SIZE = 1000  # the documented maximum

# Worth retrying: the request was fine, the server or network was not.
RETRYABLE_STATUS = frozenset({408, 500, 502, 503, 504})


class OpenAQError(RuntimeError):
    """The API returned an error that retrying will not fix."""


class AuthError(OpenAQError):
    """The API key is missing, wrong, or suspended."""


class NotFoundError(OpenAQError):
    """The requested resource does not exist."""


class RateLimiter:
    """Sliding-window limiter that enforces several limits at once.

    Each rule is (max_calls, period_seconds). Before a call, `acquire` sleeps
    until every rule has room. The defaults sit just under the published
    limits to leave headroom for clock drift between us and the server.
    """

    def __init__(
        self,
        rules: tuple[tuple[int, float], ...] = ((55, 60.0), (1900, 3600.0)),
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._rules = rules
        self._windows: list[deque[float]] = [deque() for _ in rules]
        self._clock = clock
        self._sleep = sleep

    def acquire(self) -> None:
        while True:
            now = self._clock()
            wait = 0.0
            for (max_calls, period), calls in zip(self._rules, self._windows, strict=True):
                while calls and now - calls[0] >= period:
                    calls.popleft()
                if len(calls) >= max_calls:
                    wait = max(wait, period - (now - calls[0]))
            if wait <= 0:
                break
            log.info("Rate limit window full, sleeping %.1fs", wait)
            self._sleep(wait)
        for calls in self._windows:
            calls.append(now)


class OpenAQClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        *,
        session: requests.Session | None = None,
        limiter: RateLimiter | None = None,
        max_attempts: int = 5,
        timeout: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
        self._limiter = limiter or RateLimiter(sleep=sleep)
        self._max_attempts = max_attempts
        self._timeout = timeout
        self._sleep = sleep
        self.request_count = 0

    # -- low level ---------------------------------------------------------

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET one page, retrying what is worth retrying."""
        url = f"{self._base_url}{path}"
        last_problem = "no attempt made"

        for attempt in range(1, self._max_attempts + 1):
            self._limiter.acquire()
            self.request_count += 1
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_problem = f"{type(exc).__name__}: {exc}"
                delay = self._backoff(attempt)
            else:
                status = response.status_code
                if status == 200:
                    return response.json()
                if status in (401, 403):
                    raise AuthError(f"{status} from {path}. Check OPENAQ_API_KEY.")
                if status == 404:
                    raise NotFoundError(f"404 from {path}")
                if status == 429:
                    # The server tells us how long until the window resets.
                    delay = self._reset_seconds(response)
                elif status in RETRYABLE_STATUS:
                    delay = self._backoff(attempt)
                else:
                    # 422 and friends mean our request is wrong. Retrying an
                    # identical bad request only wastes the rate limit.
                    raise OpenAQError(f"{status} from {path}: {response.text[:300]}")
                last_problem = f"HTTP {status}"

            if attempt < self._max_attempts:
                log.warning(
                    "%s on %s (attempt %d/%d), retrying in %.1fs",
                    last_problem,
                    path,
                    attempt,
                    self._max_attempts,
                    delay,
                )
                self._sleep(delay)

        raise OpenAQError(f"Gave up on {path} after {self._max_attempts} attempts: {last_problem}")

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield every result across all pages.

        `meta.found` is not reliable enough to count pages with (it can be a
        string such as ">1000"), so the loop stops on the first short page.
        """
        page = 1
        while True:
            body = self.get(path, {**(params or {}), "limit": PAGE_SIZE, "page": page})
            results = body.get("results") or []
            yield from results
            if len(results) < PAGE_SIZE:
                return
            page += 1

    # -- resources ---------------------------------------------------------

    def iter_locations(
        self, *, bbox: str, iso: str | None = None, monitors_only: bool = False
    ) -> Iterator[dict[str, Any]]:
        """Every monitoring location inside a bounding box."""
        params: dict[str, Any] = {"bbox": bbox, "order_by": "id", "sort_order": "asc"}
        if iso:
            params["iso"] = iso
        if monitors_only:
            params["monitor"] = "true"
        return self.paginate("/locations", params)

    def iter_sensor_hours(
        self, sensor_id: int, *, datetime_from: str, datetime_to: str
    ) -> Iterator[dict[str, Any]]:
        """Hourly averages for one sensor between two ISO 8601 datetimes."""
        return self.paginate(
            f"/sensors/{sensor_id}/hours",
            {"datetime_from": datetime_from, "datetime_to": datetime_to},
        )

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Exponential backoff with jitter: about 2s, 4s, 8s, ... capped at 60s.

        Jitter stops many clients that failed together from retrying together.
        """
        return min(60.0, 2.0**attempt) + random.uniform(0, 1)

    @staticmethod
    def _reset_seconds(response: requests.Response) -> float:
        try:
            reset = float(response.headers.get("x-ratelimit-reset", 60))
        except ValueError:
            reset = 60.0
        return min(max(reset, 1.0), 600.0) + 1.0
