import os
import threading
import time
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, timedelta

import dlt
import requests  # stdlib requests: cookie-session persistence across queries

from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import (
    SPACETRACK_MAX_PER_HOUR,
    SPACETRACK_MAX_PER_MIN,
    SPACETRACK_MAX_RETRIES,
    SPACETRACK_RETRY_WAIT_DEFAULT_S,
)

BASE_URL = "https://www.space-track.org"
DEV_BASE_URL = "https://for-testing-only.space-track.org"

_TRUTHY = {"1", "true", "yes"}

# Per-run override of the host choice. A backfill fans the same asset across many
# processes; rather than relying on an operator to remember the env toggle, the asset
# sets this for the run (see force_test_host) so every query in that run goes to the
# unlimited test host. ContextVar (not a plain global) keeps the override scoped to the
# run that set it. Extraction is single-threaded (see _RateLimiter) so the value is
# visible to every query_class/login_session call in the run.
_force_test_host: ContextVar[bool] = ContextVar("spacetrack_force_test_host", default=False)


@contextmanager
def force_test_host():
    """Force the test host for the duration of the block, restoring the prior state after."""
    token = _force_test_host.set(True)
    try:
        yield
    finally:
        _force_test_host.reset(token)


def _use_test_host() -> bool:
    """True when queries should hit the unlimited test host instead of prod.

    On when either the per-run override (force_test_host) is active or the env toggle
    SPACETRACK_USE_TEST_HOST is set truthy (1/true/yes, case-insensitive). Both are read
    at call time so the choice takes effect per run without re-import. The test host has
    no rate limits and the throttle is bypassed against it.
    """
    if _force_test_host.get():
        return True
    return os.getenv("SPACETRACK_USE_TEST_HOST", "").strip().lower() in _TRUTHY


def _base_url() -> str:
    """Test host when the toggle is on, else the production host."""
    return DEV_BASE_URL if _use_test_host() else BASE_URL


class _RateLimiter:
    """Sliding-window limiter enforcing per-minute AND per-hour caps simultaneously.

    In-process: paces requests within a single run. Across separate runs (a fresh
    process each in the deployed launcher) state does not persist — which is why
    backfills are auto-routed to the unlimited test host (see force_test_host) rather
    than relying on this limiter to pace them. `now`/`sleep` are injectable so tests
    drive a fake clock with no real sleeping. The lock is held across the sleep — fine
    under dlt's single-threaded extraction; do not combine with parallelized resources.
    """

    def __init__(self, max_per_min, max_per_hour, *, now=time.monotonic, sleep=time.sleep):
        self._windows = [(60.0, max_per_min), (3600.0, max_per_hour)]
        self._events: deque[float] = deque()
        self._now = now
        self._sleep = sleep
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            while True:
                now = self._now()
                horizon = now - 3600.0  # longest window
                while self._events and self._events[0] <= horizon:
                    self._events.popleft()
                wait = 0.0
                for window, cap in self._windows:
                    cutoff = now - window
                    in_window = [t for t in self._events if t > cutoff]
                    if len(in_window) >= cap:
                        wait = max(wait, in_window[0] + window - now)
                if wait <= 0.0:
                    self._events.append(now)
                    return
                self._sleep(wait)


_LIMITER = _RateLimiter(SPACETRACK_MAX_PER_MIN, SPACETRACK_MAX_PER_HOUR)


def _throttle() -> None:
    """Acquire a rate-limit slot before a prod API call; no-op against the test host."""
    if not _use_test_host():
        _LIMITER.acquire()


def _retry_after_seconds(resp) -> float:
    """Seconds to wait before retrying a 429, taken from the Retry-After header.

    space-track sends Retry-After as an integer second count. Fall back to the
    configured default when the header is absent or in the (unsupported) HTTP-date form.
    """
    raw = resp.headers.get("Retry-After")
    if raw is not None:
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            pass  # HTTP-date form — use the default backoff instead
    return SPACETRACK_RETRY_WAIT_DEFAULT_S


def _request(session: requests.Session, method: str, url: str, *, sleep=None, **kwargs):
    """Throttled HTTP request that backs off and retries on HTTP 429.

    The in-process limiter paces a single run, but a backfill fans partitions out
    across separate processes whose limiters cannot see each other, so space-track can
    still answer 429. Honor Retry-After (or the configured default) and retry up to
    SPACETRACK_MAX_RETRIES times — each retry re-acquires a throttle slot — rather than
    failing the whole asset. The response is returned without raise_for_status so callers
    keep their own status handling; an unrecovered 429 surfaces when the caller raises.
    """
    do_sleep = sleep if sleep is not None else time.sleep
    resp = None
    for attempt in range(SPACETRACK_MAX_RETRIES + 1):
        _throttle()
        resp = getattr(session, method)(url, **kwargs)
        if resp.status_code == 429 and attempt < SPACETRACK_MAX_RETRIES:
            do_sleep(_retry_after_seconds(resp))
            continue
        break
    return resp


def spacetrack_credentials() -> tuple[str, str]:
    """(username, password) from dlt secrets (env SPACETRACK_USERNAME / _PASSWORD)."""
    return dlt.secrets["spacetrack_username"], dlt.secrets["spacetrack_password"]


def _looks_like_json_payload(resp) -> bool:
    """True if the body parses as JSON (not an HTML login redirect)."""
    try:
        resp.json()
        return True
    except ValueError:
        return False


def login_session() -> requests.Session:
    """Authenticate and return a cookie-bearing session.

    space-track may return HTTP 200 even on bad credentials, and the success/failure
    body is unspecified, so we verify auth with one trivial authenticated probe rather
    than matching a body string. An unauthenticated session redirects to the login page
    (non-JSON body) instead of returning a JSON list.
    """
    username, password = spacetrack_credentials()
    session = requests.Session()
    resp = _request(
        session,
        "post",
        f"{_base_url()}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    probe = _request(
        session,
        "get",
        f"{_base_url()}/basicspacedata/query/class/boxscore/limit/1/format/json",
        timeout=60,
    )
    if probe.status_code != 200 or not _looks_like_json_payload(probe):
        raise RuntimeError(
            "space-track login failed (check SPACETRACK_USERNAME / SPACETRACK_PASSWORD)"
        )
    return session


def query_class(session: requests.Session, cls: str, *segments: str):
    """GET /basicspacedata/query/class/<cls>/<segments>/format/json -> parsed JSON."""
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{_base_url()}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    resp = _request(session, "get", url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)
