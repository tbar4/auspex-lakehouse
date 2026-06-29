# Space-Track Rate-Limit + Test-Host Switch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the space-track source within < 30 req/min and < 300 req/hr via an in-process per-run rate limiter, and wire an env toggle to point at the unlimited test host for backfill/testing.

**Architecture:** Add a sliding-window `_RateLimiter` (enforcing both windows) and a `_throttle()` gate to `sources/spacetrack/_common.py`, plus an env-driven host switch (`_use_test_host()` / `_base_url()`). `login_session` and `query_class` build URLs from `_base_url()` and call `_throttle()` before every HTTP call; the throttle is bypassed when the test host is on. Limits live as tunable constants in `config.py`. No changes to assets, scheduling, or the pool.

**Tech Stack:** Python stdlib (`os`, `time`, `threading`, `collections.deque`), `requests`, dlt; pytest with an injected fake clock.

**Spec:** `docs/superpowers/specs/2026-06-29-spacetrack-rate-limit-design.md`

## Global Constraints

Every task implicitly includes these (verbatim from the spec):

- **Limits:** `SPACETRACK_MAX_PER_MIN = 25`, `SPACETRACK_MAX_PER_HOUR = 250` — conservatively under the 30/300 ceilings. Login `POST` + auth probe `GET` + each `query_class` `GET` are **all** counted.
- **Host switch:** boolean env `SPACETRACK_USE_TEST_HOST` (default `false`). `false` → `BASE_URL` (`https://space-track.org`) + throttle **ON**; `true` → `DEV_BASE_URL` (`https://for-testing-only.space-track.org`) + throttle **OFF**. Read at call time. Truthy = `1`/`true`/`yes` (case-insensitive).
- **Throttle scope:** in-process per-run **safety net only**. It does NOT bound the default per-partition multi-run backfill. Backfill compliance comes from the test host. Do **NOT** add a `BackfillPolicy` or otherwise touch the assets, the cron schedule, or the `spacetrack_api` pool.
- **Fake-clock test contract (load-bearing):** `acquire()` is `while True: ... sleep(wait)`. The injected `sleep` **must advance** the injected `now` by `wait`, or `acquire()` busy-loops forever. Limiter tests use one fake clock whose `sleep(dt)` adds `dt` to its `now`.
- **Singleton isolation:** the module-level `_LIMITER` is shared mutable state. Tests exercising the real `_throttle()` path must replace `_common._LIMITER` with a fresh instance (and the default-host URL/login regression tests must not hit a real `time.sleep`).
- **Files touched:** `sources/spacetrack/_common.py`, `sources/spacetrack/config.py`, `.env.example` (+ tests). Nothing else.

## File Structure

**Modify:**
- `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py` — add `os` import + host-switch helpers (Task 1); add limiter/throttle + stdlib imports + config import (Task 2); wire `login_session`/`query_class` (Task 3).
- `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py` — add the two cap constants (Task 2).
- `.env.example` — add commented `SPACETRACK_USE_TEST_HOST` (Task 3).
- `tests/test_spacetrack_common.py` — host-switch tests (Task 1); isolation fixture + wiring tests (Task 3).

**Create:**
- `tests/test_spacetrack_ratelimit.py` — `_RateLimiter` unit tests (fake clock) + config-cap tests + `_throttle` gate tests (Task 2).

**Branch:** `feat/spacetrack-rate-limit` (already created and checked out; spec already committed). Do **not** recreate the branch.

> **Commit hygiene:** the working tree carries unrelated user WIP (`pyproject.toml`, `uv.lock`, a `docs/...descriptive-names-design.md`). `git add` only the exact paths each task names. Never `git add -A`.

---

### Task 1: Host-switch helpers

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py`
- Test: `tests/test_spacetrack_common.py`

**Interfaces:**
- Produces: `_use_test_host() -> bool` (reads env `SPACETRACK_USE_TEST_HOST`, truthy = `1`/`true`/`yes` case-insensitive, default `False`); `_base_url() -> str` (`DEV_BASE_URL` when test host on, else `BASE_URL`).

- [ ] **Step 1: Write the failing tests (append to `tests/test_spacetrack_common.py`)**

```python
def test_use_test_host_default_false(monkeypatch):
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    assert c._use_test_host() is False


def test_use_test_host_truthy_values(monkeypatch):
    for v in ["1", "true", "TRUE", "Yes", " yes "]:
        monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", v)
        assert c._use_test_host() is True, v


def test_use_test_host_non_truthy_values(monkeypatch):
    for v in ["0", "false", "no", "", "off"]:
        monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", v)
        assert c._use_test_host() is False, v


def test_base_url_switches_on_toggle(monkeypatch):
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    assert c._base_url() == c.BASE_URL
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    assert c._base_url() == c.DEV_BASE_URL
```

(`c` is already imported at the top of this file as
`import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_common.py -k "use_test_host or base_url" -v`
Expected: FAIL — `AttributeError: module ... has no attribute '_use_test_host'`.

- [ ] **Step 3: Add the helpers to `_common.py`**

Add `os` to the imports (top of file) and the two helpers just after the
`DEV_BASE_URL` line.

Change the import block:

```python
import os
from collections.abc import Iterator
from datetime import date, timedelta

import dlt
import requests  # stdlib requests: cookie-session persistence across queries
```

After `DEV_BASE_URL = "https://for-testing-only.space-track.org"` add:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_common.py -k "use_test_host or base_url" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py tests/test_spacetrack_common.py`
Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py tests/test_spacetrack_common.py
git commit -m "feat(spacetrack): env-toggled prod/test host switch"
```

---

### Task 2: Rate limiter + config caps + throttle gate

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py`
- Create: `tests/test_spacetrack_ratelimit.py`

**Interfaces:**
- Consumes: `_use_test_host()` (Task 1).
- Produces: `SPACETRACK_MAX_PER_MIN = 25`, `SPACETRACK_MAX_PER_HOUR = 250` (in `config.py`); `_RateLimiter(max_per_min, max_per_hour, *, now=time.monotonic, sleep=time.sleep)` with `.acquire() -> None`; module singleton `_LIMITER`; `_throttle() -> None` (calls `_LIMITER.acquire()` unless the test host is on).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_spacetrack_ratelimit.py`:

```python
import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c


class FakeClock:
    """Controllable clock whose sleep() advances its own time, so the limiter's
    `while True: sleep(wait)` loop terminates under test (load-bearing contract)."""

    def __init__(self, start=0.0):
        self.t = start

    def now(self):
        return self.t

    def sleep(self, dt):
        self.t += dt


class _AcquireCounter:
    def __init__(self, log):
        self._log = log

    def acquire(self):
        self._log.append("acquire")


def test_under_cap_does_not_sleep():
    clock = FakeClock()
    lim = c._RateLimiter(3, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()
    lim.acquire()
    assert clock.t == 0.0  # under the per-minute cap -> no wait


def test_per_minute_cap_forces_one_window_wait():
    clock = FakeClock()
    lim = c._RateLimiter(2, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()  # t=0
    lim.acquire()  # t=0 -> at the per-minute cap of 2
    lim.acquire()  # 3rd within 60s -> sleep until the oldest ages out
    assert clock.t == 60.0


def test_recovers_after_window_passes():
    clock = FakeClock()
    lim = c._RateLimiter(2, 100, now=clock.now, sleep=clock.sleep)
    lim.acquire()
    lim.acquire()
    lim.acquire()           # slept to t=60
    before = clock.t
    lim.acquire()           # room again -> no further sleep
    assert clock.t == before


def test_per_hour_cap_trips_independently():
    clock = FakeClock()
    lim = c._RateLimiter(1000, 2, now=clock.now, sleep=clock.sleep)  # tight per-hour
    lim.acquire()  # t=0
    lim.acquire()  # t=0
    lim.acquire()  # 3rd within the hour -> sleep ~3600s
    assert clock.t == 3600.0


def test_config_caps_are_under_published_ceilings():
    from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import (
        SPACETRACK_MAX_PER_HOUR,
        SPACETRACK_MAX_PER_MIN,
    )

    assert SPACETRACK_MAX_PER_MIN == 25 and SPACETRACK_MAX_PER_MIN < 30
    assert SPACETRACK_MAX_PER_HOUR == 250 and SPACETRACK_MAX_PER_HOUR < 300


def test_throttle_acquires_on_prod(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_LIMITER", _AcquireCounter(calls))
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    c._throttle()
    assert calls == ["acquire"]


def test_throttle_bypassed_on_test_host(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_LIMITER", _AcquireCounter(calls))
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    c._throttle()
    assert calls == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_ratelimit.py -v`
Expected: FAIL — `AttributeError: ... has no attribute '_RateLimiter'` (and the config import error for the caps test).

- [ ] **Step 3: Add the cap constants to `config.py`**

Append to `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py`:

```python
# Hard request-rate caps enforced in-process by the _RateLimiter in _common.py.
# Login POST + auth probe + each query GET are ALL counted. Set conservatively under
# space-track's published ceilings (<30/min, <300/hr) — plain headroom for clock jitter.
# These bound a SINGLE run only; to backfill, flip SPACETRACK_USE_TEST_HOST=true and use
# the unlimited test host (the throttle does NOT pace multi-run backfills — see design).
SPACETRACK_MAX_PER_MIN = 25
SPACETRACK_MAX_PER_HOUR = 250
```

- [ ] **Step 4: Add the limiter, singleton, and throttle to `_common.py`**

Extend the import block (add stdlib imports + the config import):

```python
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
```

Add the limiter + singleton + throttle just after `_base_url()` (from Task 1):

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_ratelimit.py -v`
Expected: PASS (7 tests), and the run returns promptly (no real sleeping — fake clock).

- [ ] **Step 6: Lint**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/spacetrack/ tests/test_spacetrack_ratelimit.py`
Expected: `All checks passed!`

- [ ] **Step 7: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py tests/test_spacetrack_ratelimit.py
git commit -m "feat(spacetrack): in-process sliding-window rate limiter + caps"
```

---

### Task 3: Wire login/query + env example

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py`
- Modify: `.env.example`
- Test: `tests/test_spacetrack_common.py`

**Interfaces:**
- Consumes: `_base_url()` (Task 1), `_throttle()`, `_RateLimiter`, `_LIMITER` (Task 2).
- Produces: `login_session`/`query_class` that build URLs from `_base_url()` and call `_throttle()` before every HTTP request.

- [ ] **Step 1: Add the isolation fixture + wiring tests (append to `tests/test_spacetrack_common.py`)**

First add `import pytest` if not already present at the top, then append:

```python
@pytest.fixture(autouse=True)
def _fresh_limiter(monkeypatch):
    # Isolate the shared singleton AND guarantee no real time.sleep: a never-tripping
    # limiter for every test in this file that exercises the real _throttle path.
    monkeypatch.setattr(c, "_LIMITER", c._RateLimiter(10**9, 10**9))


def test_query_class_calls_throttle(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_throttle", lambda: calls.append(1))
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp
    c.query_class(sess, "boxscore")
    assert calls == [1]


def test_login_session_calls_throttle_for_post_and_probe(monkeypatch):
    calls = []
    monkeypatch.setattr(c, "_throttle", lambda: calls.append(1))
    probe = Mock(status_code=200)
    probe.json.return_value = [{"ok": 1}]
    fake, _ = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))
    c.login_session()
    assert calls == [1, 1]  # one before the POST, one before the probe GET


def test_query_class_uses_test_host_url_when_toggled(monkeypatch):
    monkeypatch.setenv("SPACETRACK_USE_TEST_HOST", "true")
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp
    c.query_class(sess, "boxscore")
    assert sess.get.call_args[0][0].startswith(c.DEV_BASE_URL)
```

(The existing `_fake_requests` helper and `Mock`/`pytest` imports already live in this file.)

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_common.py -k "calls_throttle or test_host_url" -v`
Expected: FAIL — `query_class`/`login_session` don't call `_throttle` yet (counters empty), and the URL still uses `BASE_URL`.

- [ ] **Step 3: Wire `login_session` and `query_class` in `_common.py`**

Replace the body of `login_session` (the POST + probe) to throttle before each call and use `_base_url()`:

```python
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
```

Replace `query_class` to throttle before the GET and use `_base_url()`:

```python
def query_class(session: requests.Session, cls: str, *segments: str):
    """GET /basicspacedata/query/class/<cls>/<segments>/format/json -> parsed JSON."""
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{_base_url()}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    _throttle()
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()
```

- [ ] **Step 4: Run the full space-track test files to verify pass + no regression**

Run: `uv run pytest tests/test_spacetrack_common.py tests/test_spacetrack_ratelimit.py -v`
Expected: PASS — new wiring tests pass; the existing URL/login regression tests still pass (default host → `_base_url() == BASE_URL`; the autouse fixture prevents any real sleep).

- [ ] **Step 5: Add the toggle to `.env.example`**

After the existing space-track block (the `SPACETRACK_PASSWORD=...` line), add:

```
# Point space-track ingestion at the unlimited test host (for-testing-only.space-track.org)
# for backfill/testing. Default off (prod) + throttle on. NOTE: container-global — flipping
# this redirects ALL space-track runs incl. scheduled crons; restart auspex_user_code after
# changing, and pause the space-track schedules during a test-host window.
# SPACETRACK_USE_TEST_HOST=false
```

- [ ] **Step 6: Run the full suite + lint**

Run: `uv run pytest -q`
Expected: PASS (all prior tests + the new spacetrack tests).

Run: `uv run ruff check src/ tests/`
Expected: `All checks passed!`

- [ ] **Step 7: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py .env.example tests/test_spacetrack_common.py
git commit -m "feat(spacetrack): throttle + host-switch wired into login/query; env example"
```

---

### Task 4: Live verification (manual, optional gate)

**Files:** none (operational check).

**Interfaces:** consumes the full feature from Tasks 1–3.

> Requires space-track credentials. Optional; run once to confirm real behavior.

- [ ] **Step 1: Confirm prod login + a single query succeed unthrottled**

With `SPACETRACK_USE_TEST_HOST` unset, materialize one snapshot asset (e.g. `dlt_spacetrack_boxscore`) in `dg dev`. Expected: succeeds in seconds (3 requests, well under the caps — no perceptible throttle delay).

- [ ] **Step 2: Confirm the test-host toggle routes correctly**

Set `SPACETRACK_USE_TEST_HOST=true` in the environment and materialize the same asset. Expected: it queries `for-testing-only.space-track.org` (verify via run logs / a network trace) and no throttle delay occurs. Unset afterward.

- [ ] **Step 3: (Optional) Observe throttling on a forced burst**

Temporarily lower `SPACETRACK_MAX_PER_MIN` to a small value (e.g. 2) in `config.py`, run a multi-day single partition-range materialization of an incremental class, and confirm the run paces (visible `time.sleep` gaps in the op) rather than bursting. Revert the constant afterward.

---

## Post-implementation

After Task 3 (all unit tests green), the work is mergeable. Task 4 is an optional live check. Use `superpowers:finishing-a-development-branch` to open the PR for `feat/spacetrack-rate-limit`.
