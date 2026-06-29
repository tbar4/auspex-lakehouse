import os
import threading
import time
from collections import deque
from collections.abc import Iterator
from datetime import date, timedelta

import dlt
import requests  # stdlib requests: cookie-session persistence across queries

from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import (
    SPACETRACK_MAX_PER_HOUR,
    SPACETRACK_MAX_PER_MIN,
)

BASE_URL = "https://www.space-track.org"
DEV_BASE_URL = "https://for-testing-only.space-track.org"

_TRUTHY = {"1", "true", "yes"}


def _use_test_host() -> bool:
    """True if SPACETRACK_USE_TEST_HOST is set truthy (1/true/yes, case-insensitive).

    Read at call time so the toggle takes effect per run without re-import. The
    test host has no rate limits and the throttle is bypassed against it.
    """
    return os.getenv("SPACETRACK_USE_TEST_HOST", "").strip().lower() in _TRUTHY


def _base_url() -> str:
    """Test host when the toggle is on, else the production host."""
    return DEV_BASE_URL if _use_test_host() else BASE_URL


class _RateLimiter:
    """Sliding-window limiter enforcing per-minute AND per-hour caps simultaneously.

    In-process: paces requests within a single run. Across separate runs (a fresh
    process each in the deployed launcher) state does not persist — heavy multi-run
    backfills should use the test host instead. `now`/`sleep` are injectable so tests
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
    _throttle()
    resp = session.post(
        f"{_base_url()}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    _throttle()
    probe = session.get(
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
    _throttle()
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)
