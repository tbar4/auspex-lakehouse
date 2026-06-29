# Space-Track Rate-Limit Enforcement + Test-Host Switch — Design

**Date:** 2026-06-29
**Status:** Approved (design); pending implementation plan
**Builds on:** the existing space-track provider (`2026-06-28-spacetrack-design.md`).
This change is confined to `sources/spacetrack/_common.py` and `config.py`; it does **not**
touch the assets, scheduling, the `spacetrack_api` pool, or the snapshot/incremental factories.

## Goal

Make the space-track source **physically unable** to exceed space-track.org's published API
limits — **< 30 requests / minute** and **< 300 requests / hour** — during any single run,
and wire a sanctioned escape hatch for unlimited testing/backfill against
`https://for-testing-only.space-track.org`.

## Why (current-state audit)

- **Steady-state daily runs are already compliant.** Each of the 6 classes runs once/day on a
  staggered cron; a run is ~3 requests (login `POST` + auth-probe `GET` + the query `GET`).
  ~18 requests/day total — far under both limits.
- **Backfill is NOT enforced.** The `spacetrack_api` pool (limit 1) controls *concurrency*,
  not *rate* — there is no throttle anywhere. A wide date-range backfill of the daily-partitioned
  incremental classes (`decay`/`cdm`/`tip`) loops `query_class` per day with no delay; at even
  ~1 req/sec that exceeds 30/min. Compliance during backfill currently relies on a human
  batching gently — fragile.
- **The test host is dead-wired.** `DEV_BASE_URL = "https://for-testing-only.space-track.org"`
  exists in `_common.py` but nothing uses it; `login_session` and `query_class` hardcode
  `BASE_URL`. There is no way to point at the unlimited host without a code edit.

## Decisions (locked)

| Decision | Choice |
|----------|--------|
| Enforcement model | **In-process rate limiter** + **env-toggled test-host switch** |
| Host switch | Boolean env `SPACETRACK_USE_TEST_HOST` (default `false`). `false` → `BASE_URL` + throttle ON; `true` → `DEV_BASE_URL` + throttle OFF |
| Throttle scope | **Per-run (in-process)** — paces requests within a run; covers the wide-range single-run backfill (the real risk). Heavy multi-run backfills use the test host. NOT cross-run durable. |
| Limits | `SPACETRACK_MAX_PER_MIN = 25`, `SPACETRACK_MAX_PER_HOUR = 250` — deliberately **under** the 30/300 ceilings for headroom (login+probe overhead, clock jitter). Tunable constants in `config.py`. |
| Counted requests | **All three** API call sites: login `POST`, auth-probe `GET`, and every `query_class` `GET`. |
| Test-host throttle | **Bypassed** when the test host is on (the entire point of the test host). |
| Files touched | `sources/spacetrack/_common.py`, `sources/spacetrack/config.py` (+ tests) |
| Base branch | `feat/spacetrack-rate-limit` off `main` |

## Component 1 — Host switch (`_common.py`)

`BASE_URL` / `DEV_BASE_URL` already exist. Add resolution helpers and route all URL building
through them. Env is read at **call time** (so tests and per-run config take effect without
re-import).

```python
import os

BASE_URL = "https://space-track.org"
DEV_BASE_URL = "https://for-testing-only.space-track.org"


def _use_test_host() -> bool:
    """True if SPACETRACK_USE_TEST_HOST is set truthy (1/true/yes, case-insensitive)."""
    return os.getenv("SPACETRACK_USE_TEST_HOST", "").strip().lower() in {"1", "true", "yes"}


def _base_url() -> str:
    """Test host when the toggle is on, else the production host."""
    return DEV_BASE_URL if _use_test_host() else BASE_URL
```

`login_session` and `query_class` build URLs from `_base_url()` instead of the hardcoded
`BASE_URL`. With the toggle unset (the default), `_base_url() == BASE_URL`, so existing
behavior — and the existing URL assertions in `test_spacetrack_common` — are unchanged.

## Component 2 — In-process rate limiter (`_common.py`)

A sliding-window limiter enforcing **two windows simultaneously** (per-minute and per-hour).
`acquire()` drops expired timestamps, computes the longest wait across both windows, sleeps if
needed, then records the request. `now`/`sleep` are injectable so tests drive a fake clock
(no real sleeping). A `threading.Lock` guards the deque (the `spacetrack_api` pool already
serializes ops, so this is cheap insurance, not load-bearing).

```python
import threading
import time
from collections import deque

from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import (
    SPACETRACK_MAX_PER_HOUR,
    SPACETRACK_MAX_PER_MIN,
)


class _RateLimiter:
    """Sliding-window limiter enforcing per-minute AND per-hour caps.

    In-process: paces requests within a single run. Across separate runs (a fresh
    process each in the deployed launcher) state does not persist — heavy multi-run
    backfills should use the test host instead (see design's operational note).
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
```

`_throttle()` is called **immediately before** each HTTP call: both calls in `login_session`
(the `POST` and the probe `GET`) and the single `GET` in `query_class`.

## Component 3 — Wiring (`_common.py`)

```python
def login_session() -> requests.Session:
    username, password = spacetrack_credentials()
    session = requests.Session()
    _throttle()
    resp = session.post(f"{_base_url()}/ajaxauth/login", data={...}, timeout=60)
    resp.raise_for_status()
    _throttle()
    probe = session.get(f"{_base_url()}/basicspacedata/query/class/boxscore/limit/1/format/json", timeout=60)
    ...


def query_class(session, cls, *segments):
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{_base_url()}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    _throttle()
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()
```

## Component 4 — Config (`config.py`)

Add the tunable caps next to the existing rate-limit documentation table:

```python
# Hard request-rate caps enforced in-process by the _RateLimiter in _common.py.
# Set UNDER space-track's published ceilings (<30/min, <300/hr) for headroom against
# the per-run login+probe overhead and clock jitter. Tune if backfills feel too slow
# — or flip SPACETRACK_USE_TEST_HOST=true to backfill against the unlimited test host.
SPACETRACK_MAX_PER_MIN = 25
SPACETRACK_MAX_PER_HOUR = 250
```

## Operational note (docs)

In-process throttling guarantees compliance for any single run. For heavy multi-run backfills
against prod, the guidance is: set `SPACETRACK_USE_TEST_HOST=true` and backfill against the
unlimited host. Add `SPACETRACK_USE_TEST_HOST` to `.env.example` (commented, default off).

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Toggle unset / malformed | `_use_test_host()` returns `False` → prod host, throttle on (safe default) |
| Throttle would exceed a cap | `acquire()` sleeps until the oldest in-window request expires, then proceeds — no error, no dropped request |
| Test host on | `_throttle()` is a no-op; requests are not paced |
| Real-clock single request | Under-cap → `acquire()` records and returns immediately (no measurable delay) |

## Testing

All mocked — no real HTTP, no real sleeping (inject fake `now`/`sleep`).

- **Host switch:** `_use_test_host()` is `False` when env unset and for non-truthy values;
  `True` for `1`/`true`/`yes` (case-insensitive). `_base_url()` returns `BASE_URL` by default,
  `DEV_BASE_URL` when the toggle is on.
- **Limiter (fake clock):** under cap, `acquire()` does not sleep; the `(cap+1)`-th request
  within a window forces a `sleep` of the computed remaining-window duration; once the oldest
  event expires, the next `acquire()` proceeds without sleeping; the per-hour window also trips
  independently of the per-minute window.
- **Counted call sites:** with the prod default, `login_session` invokes `_throttle()` for both
  the `POST` and the probe `GET`, and `query_class` invokes it for its `GET` (assert via a
  monkeypatched `_LIMITER.acquire` counter).
- **Test-host bypass:** with `SPACETRACK_USE_TEST_HOST=true`, `query_class`/`login_session` do
  **not** call `_LIMITER.acquire` (monkeypatch it to raise; assert no raise) and URLs use
  `DEV_BASE_URL`.
- **Regression:** existing `test_spacetrack_common` URL assertions (built from `c.BASE_URL`)
  still pass with the toggle unset.

## Out of Scope

- **Cross-run durable rate budget** (persisting timestamps across separate runs/processes) —
  explicitly deferred; the test host is the answer for heavy multi-run backfill.
- Changes to the assets, the staggered cron schedule, or the `spacetrack_api` pool.
- Retrofitting NASA/CelesTrak providers with throttling (their limits differ and are handled
  separately).
- Automatic retry/backoff on `429`/`5xx` (the existing fail-loud + re-materialize behavior is unchanged).
