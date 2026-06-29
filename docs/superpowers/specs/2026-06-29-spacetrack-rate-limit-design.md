# Space-Track Rate-Limit Enforcement + Test-Host Switch — Design

**Date:** 2026-06-29
**Status:** Approved (design, post adversarial review); pending implementation plan
**Builds on:** the existing space-track provider (`2026-06-28-spacetrack-design.md`).
This change is confined to `sources/spacetrack/_common.py` and `config.py`; it does **not**
touch the assets, scheduling, the `spacetrack_api` pool, or the snapshot/incremental factories.

## Goal

Keep the space-track source within space-track.org's published API limits — **< 30 requests /
minute** and **< 300 requests / hour** — through two complementary mechanisms:

1. An **in-process per-run rate limiter** that bounds requests within any single run/process.
   This makes the **scheduled daily runs** (the only prod traffic) provably compliant and is a
   safety net for any single run that issues many requests.
2. A **test-host switch** (`https://for-testing-only.space-track.org`) that is the **sanctioned
   path for backfill** — backfill is where the limits are actually at risk, and the limiter
   alone does **not** bound it (see *Backfill compliance* below).

This split is deliberate and was confirmed by adversarial review: the limiter is correct and
sufficient for steady-state prod, but the test host — not the limiter — is what makes backfill
safe.

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
| Throttle scope | **Per-run (in-process) safety net.** Bounds requests within any single run/process — makes scheduled daily runs provably compliant. Does **NOT** bound the default per-partition multi-run backfill (each partition is a fresh process; the limiter resets). Backfill compliance comes from the **test host**, not the throttle. NOT cross-run durable. |
| Backfill | **Use the test host** (`SPACETRACK_USE_TEST_HOST=true`). The assets are deliberately left unchanged (no `BackfillPolicy`), so a prod backfill is the default per-partition multi-run that the throttle cannot pace. See *Backfill compliance*. |
| Limits | `SPACETRACK_MAX_PER_MIN = 25`, `SPACETRACK_MAX_PER_HOUR = 250` — conservatively **under** the 30/300 ceilings (login+probe are counted, so this is plain headroom for clock jitter, not extra room for them). Tunable constants in `config.py`. |
| Counted requests | **All three** API call sites: login `POST`, auth-probe `GET`, and every `query_class` `GET`. |
| Test-host throttle | **Bypassed** when the test host is on (the entire point of the test host). |
| Files touched | `sources/spacetrack/_common.py`, `sources/spacetrack/config.py` (+ tests) |
| Base branch | `feat/spacetrack-rate-limit` off `main` |

## Backfill compliance (explicit)

The in-process limiter **does not** make a prod backfill compliant, and this is intentional —
the adversarial review confirmed why:

- The incremental assets (`decay`/`cdm`/`tip`) set **no `BackfillPolicy`**, so Dagster backfills
  them **one run per partition**. Under the deployed `DefaultRunLauncher` each run is a **fresh
  subprocess** with a fresh module-level `_LIMITER`, so the limiter resets every run and cannot
  bound the cross-run request rate. ~3 requests/run firing as fast as the `spacetrack_api` pool
  frees can exceed 30/min.
- We deliberately do **not** add a single-run backfill policy (that would expand scope into the
  assets and let one op `time.sleep` for up to ~an hour while holding the shared `spacetrack_api`
  pool slot, starving the daily scheduled classes).

**Therefore the sanctioned way to backfill is the test host:** set `SPACETRACK_USE_TEST_HOST=true`
and backfill against `for-testing-only.space-track.org`, which has no limits (and the throttle is
bypassed there anyway). Prod backfill, if ever truly needed, has **no automated guard** and must be
done gently by hand — but the expectation is that backfill goes to the test host.

> **⚠ Test-host data caveat (verify-live).** The test host is space-track's sandbox mirror; its
> dataset is **not guaranteed identical** to prod (may be stale or sampled). Because both hosts
> write into the **same** bronze Delta tables (`dataset_name="bronze"`), backfilling with the
> toggle on lands test-host rows in **production** bronze. Before using it to backfill tables you
> rely on, confirm the test host returns prod-equivalent data for the classes in question; if not,
> treat the test host as for **connectivity/throttle testing only**, not data backfill.

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
serializes ops, so this is cheap insurance, not load-bearing). The lock is held across the
`sleep` — fine under dlt's single-threaded extraction; if a resource were ever marked
`@dlt.resource(parallelized=True)`, one waiter would serialize all extraction, so don't combine
this limiter with parallelized space-track resources.

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
# Login POST + auth probe + each query GET are ALL counted. Set conservatively under
# space-track's published ceilings (<30/min, <300/hr) — plain headroom for clock jitter.
# These bound a single run only; to backfill, flip SPACETRACK_USE_TEST_HOST=true and use
# the unlimited test host (the throttle does NOT pace multi-run backfills — see design).
SPACETRACK_MAX_PER_MIN = 25
SPACETRACK_MAX_PER_HOUR = 250
```

## Operational note (docs)

- In-process throttling guarantees compliance for any single run; backfill goes to the test host
  (see *Backfill compliance*).
- **The toggle is container-global and not per-run.** `_use_test_host()` reads the env of the
  `auspex_user_code` container, which is fixed at container start from `.env`. Flipping it means
  editing `.env` and **restarting `auspex_user_code`**, which points **every** space-track run —
  including the daily scheduled crons — at the test host until flipped back. Safe procedure:
  **pause the space-track schedules** (or run the backfill in a window where they don't fire),
  flip the toggle, backfill, then revert and unpause. There is no per-asset/per-run override.
- Add `SPACETRACK_USE_TEST_HOST` to `.env.example` (commented, default off). The deployed `.env`
  must carry it for the toggle to be reachable in the container.

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Toggle unset / malformed | `_use_test_host()` returns `False` → prod host, throttle on (safe default) |
| Throttle would exceed a cap | `acquire()` sleeps until the oldest in-window request expires, then proceeds — no error, no dropped request |
| Test host on | `_throttle()` is a no-op; requests are not paced |
| Real-clock single request | Under-cap → `acquire()` records and returns immediately (no measurable delay) |

## Testing

All mocked — no real HTTP, no real sleeping (inject fake `now`/`sleep`).

- **Fake-clock contract (load-bearing).** `acquire()` is `while True: ... sleep(wait)`, so the
  injected `sleep` **must advance the injected `now`** by `wait` — otherwise `now` is unchanged on
  re-entry, the wait recomputes identically, and `acquire()` **busy-loops forever**. The limiter
  tests use a single fake clock object whose `sleep(dt)` increments its own `now()` by `dt`; pass
  its `now`/`sleep` into `_RateLimiter(...)`. Spell this out in the plan so the tests don't hang.
- **Singleton isolation.** The module-level `_LIMITER` is shared mutable state. Any test that
  exercises the real `_throttle()` path must replace `_common._LIMITER` with a fresh
  fake-clock `_RateLimiter` via a fixture (and restore it after), so timestamps don't accumulate
  across tests or trip a real `time.sleep`.
- **Host switch:** `_use_test_host()` is `False` when env unset and for non-truthy values;
  `True` for `1`/`true`/`yes` (case-insensitive). `_base_url()` returns `BASE_URL` by default,
  `DEV_BASE_URL` when the toggle is on.
- **Limiter (fake clock):** under cap, `acquire()` does not sleep; the `(cap+1)`-th request
  within a window forces a `sleep` of the computed remaining-window duration; once the oldest
  event expires (fake clock advanced by the fake `sleep`), the next `acquire()` proceeds without
  sleeping; the per-hour window also trips independently of the per-minute window.
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
