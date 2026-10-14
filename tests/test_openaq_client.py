import pytest
import requests
import responses

from pipeline.openaq_client import (
    PAGE_SIZE,
    AuthError,
    NotFoundError,
    OpenAQClient,
    OpenAQError,
    RateLimiter,
)

BASE = "https://api.test/v3"


class FakeClock:
    """A clock that only moves when something sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(clock=None, **kwargs):
    clock = clock or FakeClock()
    limiter = RateLimiter(clock=clock.time, sleep=clock.sleep)
    return OpenAQClient("test-key", BASE, limiter=limiter, sleep=clock.sleep, **kwargs), clock


# -- rate limiter -----------------------------------------------------------


def test_limiter_lets_calls_through_until_the_window_is_full():
    clock = FakeClock()
    limiter = RateLimiter(rules=((3, 60.0),), clock=clock.time, sleep=clock.sleep)
    for _ in range(3):
        limiter.acquire()
    assert clock.sleeps == []

    limiter.acquire()  # the fourth call has to wait for the first to age out
    assert clock.sleeps == [60.0]


def test_limiter_enforces_the_slower_rule_too():
    clock = FakeClock()
    limiter = RateLimiter(rules=((2, 1.0), (3, 100.0)), clock=clock.time, sleep=clock.sleep)
    for _ in range(4):
        limiter.acquire()
    # Call 3 waits 1s for the per-second rule; call 4 waits for the long rule.
    assert clock.sleeps[0] == 1.0
    assert clock.now >= 100.0


# -- requests ---------------------------------------------------------------


@responses.activate
def test_sends_the_api_key_header():
    responses.get(f"{BASE}/locations", json={"results": []})
    client, _ = make_client()
    client.get("/locations")
    assert responses.calls[0].request.headers["X-API-Key"] == "test-key"


@responses.activate
def test_paginates_until_a_short_page():
    full_page = [{"id": i} for i in range(PAGE_SIZE)]
    responses.get(f"{BASE}/locations", json={"meta": {"found": ">1000"}, "results": full_page})
    responses.get(f"{BASE}/locations", json={"results": [{"id": "last"}]})
    client, _ = make_client()

    results = list(client.paginate("/locations", {"bbox": "1,2,3,4"}))

    assert len(results) == PAGE_SIZE + 1
    assert [c.request.params["page"] for c in responses.calls] == ["1", "2"]
    assert responses.calls[0].request.params["limit"] == str(PAGE_SIZE)
    assert responses.calls[0].request.params["bbox"] == "1,2,3,4"


@responses.activate
def test_waits_for_the_reset_header_on_429():
    responses.get(f"{BASE}/x", status=429, headers={"x-ratelimit-reset": "17"})
    responses.get(f"{BASE}/x", json={"results": [1]})
    client, clock = make_client()

    assert client.get("/x") == {"results": [1]}
    assert clock.sleeps == [18.0]  # reset + 1s of margin
    assert client.request_count == 2


@responses.activate
def test_retries_server_errors_and_timeouts():
    responses.get(f"{BASE}/x", status=503)
    responses.get(f"{BASE}/x", body=requests.Timeout("slow"))
    responses.get(f"{BASE}/x", json={"results": []})
    client, clock = make_client()

    assert client.get("/x") == {"results": []}
    assert len(clock.sleeps) == 2
    assert clock.sleeps[0] < clock.sleeps[1]  # backoff grows


@responses.activate
def test_gives_up_after_max_attempts():
    responses.get(f"{BASE}/x", status=500)
    client, _ = make_client(max_attempts=3)

    with pytest.raises(OpenAQError, match="after 3 attempts"):
        client.get("/x")
    assert len(responses.calls) == 3


@responses.activate
@pytest.mark.parametrize(
    ("status", "error"),
    [(401, AuthError), (403, AuthError), (404, NotFoundError), (422, OpenAQError)],
)
def test_does_not_retry_errors_that_will_not_change(status, error):
    responses.get(f"{BASE}/x", status=status, json={"detail": "nope"})
    client, clock = make_client()

    with pytest.raises(error):
        client.get("/x")
    assert len(responses.calls) == 1
    assert clock.sleeps == []


@responses.activate
def test_sensor_hours_builds_the_documented_request():
    responses.get(f"{BASE}/sensors/42/hours", json={"results": []})
    client, _ = make_client()

    list(
        client.iter_sensor_hours(
            42, datetime_from="2026-10-06T00:00:00Z", datetime_to="2026-10-07T01:00:00Z"
        )
    )

    params = responses.calls[0].request.params
    assert params["datetime_from"] == "2026-10-06T00:00:00Z"
    assert params["datetime_to"] == "2026-10-07T01:00:00Z"
