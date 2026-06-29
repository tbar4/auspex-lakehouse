# Space-Track.org Bronze Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest six space-track.org data classes (GP, SATCAT, BOXSCORE, DECAY, CDM, TIP) as bronze Delta tables in the existing Dagster + dlt lakehouse, one isolated asset per class.

**Architecture:** A new `sources/spacetrack/` provider package with two resource factories (snapshot + incremental) driven by registries. Cookie-session auth (`login_session`) happens once per run inside each asset body — never at import. Each class gets its own dlt pipeline and its own `@dlt_assets`, all bound to a new `spacetrack_api` concurrency pool (limit 1). Snapshot classes are unpartitioned merge/replace; incremental classes are daily-partitioned, date-windowed merges.

**Tech Stack:** Python 3.12, Dagster 1.13.11, `dagster_dlt` 0.29.11, dlt 1.28.1, `deltalake` + Polars, `requests` (stdlib session for cookie auth), pytest. Destination: dlt `filesystem` → Delta tables on the S3-compatible bronze bucket.

## Global Constraints

- **Provider package path:** `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/`.
- **Credentials:** read via `dlt.secrets["spacetrack_username"]` / `dlt.secrets["spacetrack_password"]` (resolved from env `SPACETRACK_USERNAME` / `SPACETRACK_PASSWORD`). Never hard-code secrets; files here are committed.
- **No import-time HTTP or Delta I/O.** Login and queries must only run inside asset bodies at materialization time. Constructing a source with `session=None` must make zero network calls (resources are lazy generators).
- **Auth is cookie-session:** `POST https://www.space-track.org/ajaxauth/login` with form fields `identity` + `password`; reuse the resulting `requests.Session` for all queries in that run.
- **Pool:** every space-track `@dlt_assets` sets `pool="spacetrack_api"` (constant `SPACETRACK_API_POOL`). The pool is declared in `dagster.yaml` with `limit: 1`, `granularity: 'op'`.
- **One asset + one pipeline per class** (failure isolation; per-class daily limits). Pipeline names: `spacetrack_<name>`, `dataset_name="bronze"`.
- **Write dispositions / formats:** all tables `table_format="delta"`. GP/SATCAT `merge` on `NORAD_CAT_ID`; BOXSCORE `replace` (no key); DECAY/CDM/TIP `merge` on the keys in the registry.
- **Rate limits (per class):** GP 1/hr, SATCAT 1/day (after 1700 UTC), BOXSCORE 1/day, DECAY 1/day, CDM 3/day, TIP 1/hr; overall <30/min, <300/hr. Schedule all space-track crons after 1700 UTC.
- **Conventional commits**, matching repo style (`feat(bronze): ...`, `test: ...`, `chore: ...`). Branch: `feat/spacetrack` (already created, based on `origin/main`).
- **Tooling:** run tests with `uv run pytest`; lint with `uv run ruff check`. Keep new files ruff-clean.
- **Spec:** `docs/superpowers/specs/2026-06-28-spacetrack-design.md`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py` | Per-class single-resource sources + per-class pipelines dict + package re-exports |
| `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py` | `BASE_URL`, `spacetrack_credentials`, `login_session`, `_looks_like_json_payload`, `query_class`, `iter_days` |
| `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py` | `SPACETRACK_API_POOL` + rate-limit documenting constants |
| `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py` | `_snapshot_resource` factory + `SNAPSHOT_CLASSES` registry |
| `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py` | `_incremental_resource` factory + `INCREMENTAL_CLASSES` registry |
| `src/auspex_lakehouse/bronze/dlt/sources/__init__.py` | (modify) re-export space-track public names alongside NASA |
| `src/auspex_lakehouse/bronze/dlt/assets.py` | (modify) per-class `@dlt_assets` + `SpaceTrackDltTranslator` |
| `dagster.yaml` | (modify) add `spacetrack_api` pool |
| `.env.example` | (modify) add `SPACETRACK_USERNAME` / `SPACETRACK_PASSWORD` |
| `tests/test_spacetrack_common.py` | auth + query-helper unit tests |
| `tests/test_spacetrack_sources.py` | factory + registry + source/pipeline tests |
| `tests/test_spacetrack_assets.py` | asset wiring tests |

---

## Task 1: Live API verification probe (⚠ gating, requires credentials)

This confirms the spec's `⚠ verify-live` assumptions against the authenticated API **before** the registries are committed. It is a spike, not TDD: it produces *recorded findings* used to confirm or correct the constants in Tasks 3–4.

**Files:**
- Create (throwaway, do NOT commit): `/private/tmp/claude-501/-Users-tbarnes-projects-python-auspex-lakehouse/780c4c13-8769-4d91-b3f7-a530f45de5c0/scratchpad/st_probe.py`

**Interfaces:**
- Produces: confirmed values for `cdm` vs `cdm_public`; the unique-key field(s) for each class; whether a no-`limit` query returns the full set (and the observed row counts → `min_rows` floors); the `CURRENT/Y` SATCAT segment; the real failed-login response shape.

- [ ] **Step 1: Ensure credentials are exported**

The probe reads `SPACETRACK_USERNAME` / `SPACETRACK_PASSWORD` from the environment. Confirm they are set (they live in `.env`; export them into the shell for this probe only):

Run: `bash -c 'test -n "$SPACETRACK_USERNAME" && test -n "$SPACETRACK_PASSWORD" && echo OK || echo MISSING'`
Expected: `OK` (if `MISSING`, `set -a && source .env && set +a` first).

- [ ] **Step 2: Write the probe script**

```python
# st_probe.py — throwaway live-API probe; do NOT commit.
import os
import requests

BASE = "https://www.space-track.org"
s = requests.Session()
r = s.post(f"{BASE}/ajaxauth/login",
           data={"identity": os.environ["SPACETRACK_USERNAME"],
                 "password": os.environ["SPACETRACK_PASSWORD"]}, timeout=60)
print("LOGIN", r.status_code, repr(r.text[:200]))


def probe(label, cls, *segments):
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{BASE}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    resp = s.get(url, timeout=120)
    try:
        data = resp.json()
        n = len(data) if isinstance(data, list) else "non-list"
        sample = data[0] if isinstance(data, list) and data else {}
        print(f"\n=== {label} ({cls}) -> {resp.status_code}, rows={n}")
        print("KEYS:", sorted(sample.keys()))
    except ValueError:
        print(f"\n=== {label} ({cls}) -> {resp.status_code}, NON-JSON:", repr(resp.text[:200]))


probe("GP", "gp", "orderby", "NORAD_CAT_ID")
probe("SATCAT", "satcat", "CURRENT", "Y", "orderby", "NORAD_CAT_ID")
probe("BOXSCORE", "boxscore")
probe("DECAY", "decay", "MSG_EPOCH", ">now-7")
probe("CDM cdm_public", "cdm_public", "limit", "5")
probe("CDM cdm", "cdm", "limit", "5")
probe("TIP", "tip", "limit", "5")
```

- [ ] **Step 3: Run the probe and record findings**

Run: `cd /Users/tbarnes/projects/python/auspex-lakehouse && uv run python /private/tmp/claude-501/-Users-tbarnes-projects-python-auspex-lakehouse/780c4c13-8769-4d91-b3f7-a530f45de5c0/scratchpad/st_probe.py`
Expected: a `LOGIN 200 ...` line and one block per class with `rows=` and `KEYS:`.

Record (these drive Tasks 3–4):
- **GP / SATCAT row counts** → set conservative `min_rows` floors (e.g. ~half the observed count, rounded down). If either returns a suspiciously round number (e.g. exactly 10000/20000) it may be capped — note it for paging follow-up.
- **CDM:** which of `cdm` / `cdm_public` returns rows for your account; confirm `CDM_ID` and `CREATED` are in `KEYS`.
- **DECAY KEYS:** confirm `NORAD_CAT_ID`, `MSG_EPOCH`, `PRECEDENCE` exist.
- **TIP KEYS:** confirm `NORAD_CAT_ID`, `MSG_EPOCH`, `INSERT_EPOCH` exist.
- **BOXSCORE:** confirm it is a small aggregate (rows in the low hundreds, no obvious unique id).
- **Login body** on success (likely empty/`""`) — informs nothing to change (we use the probe-based check) but note it.

- [ ] **Step 4: Reconcile with the spec registries**

If any finding contradicts the spec defaults (e.g. CDM is `cdm` not `cdm_public`, or a key is missing), note the exact correction to apply when writing `SNAPSHOT_CLASSES` / `INCREMENTAL_CLASSES` in Tasks 3 and 4. No commit in this task (the probe script is not committed).

---

## Task 2: Auth + query helpers (`_common.py`)

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py` (empty for now — makes the package importable)
- Create: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py`
- Test: `tests/test_spacetrack_common.py`

**Interfaces:**
- Produces:
  - `BASE_URL: str = "https://www.space-track.org"`
  - `spacetrack_credentials() -> tuple[str, str]`
  - `login_session() -> requests.Session`
  - `query_class(session: requests.Session, cls: str, *segments: str) -> Any` (parsed JSON; list for these classes)
  - `iter_days(start_date: date, end_date: date) -> Iterator[date]` (inclusive)
  - `_looks_like_json_payload(resp) -> bool`

- [ ] **Step 1: Create the empty package init**

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py
```
(Leave it empty in this task; Task 5 fills it in. It just needs to exist so the package imports.)

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_spacetrack_common.py
from datetime import date
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c


def test_iter_days_is_inclusive():
    assert list(c.iter_days(date(2026, 1, 1), date(2026, 1, 3))) == [
        date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
    ]


def test_query_class_builds_url_with_segments():
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = [{"a": 1}]
    sess.get.return_value = resp

    out = c.query_class(sess, "gp", "orderby", "NORAD_CAT_ID")

    assert out == [{"a": 1}]
    assert sess.get.call_args[0][0] == (
        "https://www.space-track.org/basicspacedata/query/class/gp/"
        "orderby/NORAD_CAT_ID/format/json"
    )


def test_query_class_builds_url_without_segments():
    sess = Mock()
    resp = Mock(raise_for_status=Mock())
    resp.json.return_value = []
    sess.get.return_value = resp

    c.query_class(sess, "boxscore")

    assert sess.get.call_args[0][0] == (
        "https://www.space-track.org/basicspacedata/query/class/boxscore/format/json"
    )


def _fake_requests(probe_resp):
    sess = Mock()
    sess.post.return_value = Mock(status_code=200, raise_for_status=Mock())
    sess.get.return_value = probe_resp
    fake = Mock()
    fake.Session.return_value = sess
    return fake, sess


def test_login_success_returns_session(monkeypatch):
    probe = Mock(status_code=200)
    probe.json.return_value = [{"ok": 1}]
    fake, sess = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))

    result = c.login_session()

    assert result is sess
    assert sess.post.call_args.kwargs["data"] == {"identity": "user", "password": "pass"}


def test_login_failure_raises_on_non_json_probe(monkeypatch):
    probe = Mock(status_code=200)
    probe.json.side_effect = ValueError("not json")
    fake, _ = _fake_requests(probe)
    monkeypatch.setattr(c, "requests", fake)
    monkeypatch.setattr(c, "spacetrack_credentials", lambda: ("user", "pass"))

    with pytest.raises(RuntimeError):
        c.login_session()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_common.py -v`
Expected: FAIL — `ModuleNotFoundError` / `AttributeError` (`_common` has no such members).

- [ ] **Step 4: Write the implementation**

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py
from collections.abc import Iterator
from datetime import date, timedelta

import dlt
import requests  # stdlib requests: cookie-session persistence across queries

BASE_URL = "https://www.space-track.org"


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
    resp = session.post(
        f"{BASE_URL}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    probe = session.get(
        f"{BASE_URL}/basicspacedata/query/class/boxscore/limit/1/format/json",
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
    url = f"{BASE_URL}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_common.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/spacetrack/ tests/test_spacetrack_common.py`
Expected: no errors.

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py \
        src/auspex_lakehouse/bronze/dlt/sources/spacetrack/_common.py \
        tests/test_spacetrack_common.py
git commit -m "feat(bronze): add space-track auth + query helpers"
```

---

## Task 3: Snapshot factory + registry (`snapshot.py`)

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py`
- Test: `tests/test_spacetrack_sources.py` (snapshot section)

**Interfaces:**
- Consumes: `query_class` from `_common`.
- Produces:
  - `_snapshot_resource(name, cls, primary_key, segments, write_disposition, min_rows) -> DltResource`
  - `SNAPSHOT_CLASSES: list[tuple]` — entries `(name, class, primary_key, segments, write_disposition, min_rows)`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_spacetrack_sources.py
from datetime import date
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot as snap


def test_snapshot_resource_yields_each_row(monkeypatch):
    monkeypatch.setattr(snap, "query_class",
                        lambda session, *seg: [{"NORAD_CAT_ID": 1}, {"NORAD_CAT_ID": 2}])
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID",
                                  ("orderby", "NORAD_CAT_ID"), "merge", 0)
    rows = list(res(session=Mock()))
    assert len(rows) == 2
    assert res.name == "gp"


def test_snapshot_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: None)
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", (), "merge", 0)
    assert list(res(session=Mock())) == []


def test_snapshot_resource_raises_below_floor(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: [{"x": 1}])
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", (), "merge", 10)
    with pytest.raises(RuntimeError):
        list(res(session=Mock()))


def test_snapshot_registry_shape():
    by = {e[0]: e for e in snap.SNAPSHOT_CLASSES}
    assert [e[0] for e in snap.SNAPSHOT_CLASSES] == ["gp", "satcat", "boxscore"]
    assert by["gp"][4] == "merge" and by["gp"][2] == "NORAD_CAT_ID"
    assert by["satcat"][4] == "merge" and by["satcat"][2] == "NORAD_CAT_ID"
    assert by["boxscore"][4] == "replace" and by["boxscore"][2] is None
    assert by["boxscore"][5] is None  # no row-count floor for the small aggregate
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_sources.py -v`
Expected: FAIL — `ModuleNotFoundError` for `snapshot`.

- [ ] **Step 3: Write the implementation**

> Apply any Task 1 corrections (e.g. SATCAT segment, `min_rows` floors from observed counts). Defaults below assume GP/SATCAT each return well over 10000 rows.

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import query_class


def _snapshot_resource(name, cls, primary_key, segments, write_disposition, min_rows):
    @dlt.resource(
        name=name,
        write_disposition=write_disposition,
        primary_key=primary_key,        # None for replace tables
        table_format="delta",
    )
    def _resource(session):
        data = query_class(session, cls, *segments)
        if not isinstance(data, list):  # tolerate empty/non-list bodies
            return
        if min_rows and len(data) < min_rows:
            # Suspected truncation / implicit row-cap — fail loudly rather than
            # silently writing a short catalog. Floor is conservative.
            raise RuntimeError(
                f"{name}: {len(data)} rows < floor {min_rows}; suspected row-cap"
            )
        yield from data

    return _resource


SNAPSHOT_CLASSES = [
    # (name, class, primary_key, segments, write_disposition, min_rows)
    ("gp",       "gp",       "NORAD_CAT_ID", ("orderby", "NORAD_CAT_ID"),                  "merge",   10000),
    ("satcat",   "satcat",   "NORAD_CAT_ID", ("CURRENT", "Y", "orderby", "NORAD_CAT_ID"), "merge",   10000),
    ("boxscore", "boxscore", None,           (),                                          "replace", None),
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_sources.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py tests/test_spacetrack_sources.py`

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py \
        tests/test_spacetrack_sources.py
git commit -m "feat(bronze): add space-track snapshot factory + registry"
```

---

## Task 4: Incremental factory + registry (`incremental.py`)

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py`
- Test: `tests/test_spacetrack_sources.py` (append incremental section)

**Interfaces:**
- Consumes: `iter_days`, `query_class` from `_common`.
- Produces:
  - `_incremental_resource(name, cls, primary_key, date_predicate) -> DltResource`
  - `INCREMENTAL_CLASSES: list[tuple]` — entries `(name, class, primary_key, date_predicate)`

- [ ] **Step 1: Write the failing tests (append to `tests/test_spacetrack_sources.py`)**

```python
import auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental as inc


def test_incremental_resource_windows_each_day(monkeypatch):
    calls = []

    def fake_query(session, cls, predicate, window):
        calls.append((cls, predicate, window))
        return [{"id": window}]

    monkeypatch.setattr(inc, "query_class", fake_query)
    res = inc._incremental_resource("decay", "decay", ["NORAD_CAT_ID"], "MSG_EPOCH")
    rows = list(res(Mock(), date(2026, 1, 1), date(2026, 1, 2)))

    assert calls == [
        ("decay", "MSG_EPOCH", "2026-01-01--2026-01-02"),
        ("decay", "MSG_EPOCH", "2026-01-02--2026-01-03"),
    ]
    assert len(rows) == 2
    assert res.name == "decay"


def test_incremental_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(inc, "query_class", lambda *a: None)
    res = inc._incremental_resource("tip", "tip", ["NORAD_CAT_ID"], "INSERT_EPOCH")
    assert list(res(Mock(), date(2026, 1, 1), date(2026, 1, 1))) == []


def test_incremental_registry_shape():
    by = {e[0]: e for e in inc.INCREMENTAL_CLASSES}
    assert [e[0] for e in inc.INCREMENTAL_CLASSES] == ["decay", "cdm", "tip"]
    assert by["cdm"][1] == "cdm_public"
    assert by["cdm"][2] == "CDM_ID"
    assert by["decay"][3] == "MSG_EPOCH"
    assert by["tip"][3] == "INSERT_EPOCH"
    assert all(len(e) == 4 for e in inc.INCREMENTAL_CLASSES)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_sources.py -k incremental -v`
Expected: FAIL — `ModuleNotFoundError` for `incremental`.

- [ ] **Step 3: Write the implementation**

> Apply any Task 1 corrections (CDM class name, key fields).

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py
from datetime import timedelta

import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import iter_days, query_class


def _incremental_resource(name, cls, primary_key, date_predicate):
    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource(session, start_date, end_date):
        for day in iter_days(start_date, end_date):
            window = f"{day.isoformat()}--{(day + timedelta(days=1)).isoformat()}"
            data = query_class(session, cls, date_predicate, window)
            if isinstance(data, list):
                yield from data

    return _resource


INCREMENTAL_CLASSES = [
    # (name, class, primary_key, date_predicate)
    ("decay", "decay",      ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("cdm",   "cdm_public", "CDM_ID",                                    "CREATED"),
    ("tip",   "tip",        ["NORAD_CAT_ID", "MSG_EPOCH"],               "INSERT_EPOCH"),
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_sources.py -v`
Expected: PASS (snapshot + incremental, 7 tests total).

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py tests/test_spacetrack_sources.py`

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py \
        tests/test_spacetrack_sources.py
git commit -m "feat(bronze): add space-track incremental factory + registry"
```

---

## Task 5: Per-class sources + pipelines (`spacetrack/__init__.py`) + re-exports

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py` (replace the empty placeholder)
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`
- Test: `tests/test_spacetrack_sources.py` (append source/pipeline section)

**Interfaces:**
- Consumes: registries + factories from `snapshot`/`incremental`; `login_session` from `_common`.
- Produces (importable from `auspex_lakehouse.bronze.dlt.sources`):
  - `snapshot_source(name, session=None) -> DltSource` (one resource)
  - `incremental_source(name, start_date, end_date, session=None) -> DltSource` (one resource)
  - `spacetrack_pipelines: dict[str, dlt.Pipeline]` (6 entries, names `spacetrack_<name>`)
  - `login_session`, `SNAPSHOT_CLASSES`, `INCREMENTAL_CLASSES`

- [ ] **Step 1: Write the failing tests (append to `tests/test_spacetrack_sources.py`)**

```python
def test_snapshot_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import snapshot_source
    src = snapshot_source("gp")  # session=None -> no HTTP
    assert set(src.resources.keys()) == {"gp"}


def test_incremental_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import incremental_source
    src = incremental_source("decay", start_date=date(2026, 1, 1), end_date=date(2026, 1, 1))
    assert set(src.resources.keys()) == {"decay"}


def test_pipelines_dict_has_all_six():
    from auspex_lakehouse.bronze.dlt.sources import spacetrack_pipelines
    assert set(spacetrack_pipelines) == {"gp", "satcat", "boxscore", "decay", "cdm", "tip"}
    assert spacetrack_pipelines["gp"].pipeline_name == "spacetrack_gp"
    assert spacetrack_pipelines["decay"].dataset_name == "bronze"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_sources.py -k "source_exposes or pipelines_dict" -v`
Expected: FAIL — `ImportError` (names not exported).

- [ ] **Step 3: Implement the package `__init__.py`**

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import login_session
from auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental import (
    INCREMENTAL_CLASSES,
    _incremental_resource,
)
from auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot import (
    SNAPSHOT_CLASSES,
    _snapshot_resource,
)

SNAPSHOT_BY_NAME = {e[0]: e for e in SNAPSHOT_CLASSES}
INCREMENTAL_BY_NAME = {e[0]: e for e in INCREMENTAL_CLASSES}


@dlt.source
def snapshot_source(name, session=None):
    n, cls, pk, segs, wd, floor = SNAPSHOT_BY_NAME[name]
    return [_snapshot_resource(n, cls, pk, segs, wd, floor)(session)]


@dlt.source
def incremental_source(name, start_date, end_date, session=None):
    n, cls, pk, pred = INCREMENTAL_BY_NAME[name]
    return [_incremental_resource(n, cls, pk, pred)(session, start_date, end_date)]


def _pipeline(name):
    return dlt.pipeline(
        pipeline_name=f"spacetrack_{name}",   # distinct working dir per class
        destination="filesystem",
        dataset_name="bronze",                # tables land at bronze/<name>
    )


spacetrack_pipelines = {
    name: _pipeline(name)
    for name in list(SNAPSHOT_BY_NAME) + list(INCREMENTAL_BY_NAME)
}

__all__ = [
    "snapshot_source",
    "incremental_source",
    "spacetrack_pipelines",
    "login_session",
    "SNAPSHOT_CLASSES",
    "INCREMENTAL_CLASSES",
]
```

- [ ] **Step 4: Re-export from the aggregate `sources/__init__.py`**

Add these imports and `__all__` entries to `src/auspex_lakehouse/bronze/dlt/sources/__init__.py` (keep the existing NASA block):

```python
from auspex_lakehouse.bronze.dlt.sources.spacetrack import (
    INCREMENTAL_CLASSES,
    SNAPSHOT_CLASSES,
    incremental_source,
    login_session,
    snapshot_source,
    spacetrack_pipelines,
)
```

Append to the existing `__all__` list:

```python
    "snapshot_source",
    "incremental_source",
    "spacetrack_pipelines",
    "login_session",
    "SNAPSHOT_CLASSES",
    "INCREMENTAL_CLASSES",
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_sources.py -v`
Expected: PASS (all source tests). Also confirm no network call occurred (tests construct with `session=None`).

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/`

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/__init__.py \
        src/auspex_lakehouse/bronze/dlt/sources/__init__.py \
        tests/test_spacetrack_sources.py
git commit -m "feat(bronze): wire space-track per-class sources + pipelines"
```

---

## Task 6: Config constant + infra wiring (`config.py`, `dagster.yaml`, `.env.example`)

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py`
- Modify: `dagster.yaml`
- Modify: `.env.example`
- Test: `tests/test_spacetrack_sources.py` (append config test)

**Interfaces:**
- Produces: `SPACETRACK_API_POOL: str = "spacetrack_api"` (consumed by `assets.py` in Task 7).

- [ ] **Step 1: Write the failing test (append to `tests/test_spacetrack_sources.py`)**

```python
def test_pool_constant():
    from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import SPACETRACK_API_POOL
    assert SPACETRACK_API_POOL == "spacetrack_api"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_spacetrack_sources.py::test_pool_constant -v`
Expected: FAIL — `ModuleNotFoundError` for `config`.

- [ ] **Step 3: Create `config.py`**

```python
# src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py
"""space-track provider constants.

Rate limits (per class) enforced operationally via scheduling + the pool:
    GP        1 / hour       (randomize the minute)
    SATCAT    1 / day        (updated after 1700 UTC)
    BOXSCORE  1 / day
    DECAY     1 / day
    CDM       3 / day        (or 1 / hour for a specific event)
    TIP       1 / hour
    Overall   < 30 / minute, < 300 / hour
All space-track crons run after 1700 UTC so SATCAT reflects the day's update.
"""

SPACETRACK_API_POOL = "spacetrack_api"  # Dagster pool serializing space-track API access
```

- [ ] **Step 4: Add the pool to `dagster.yaml`**

Extend the existing `concurrency.pools` block (keep `nasa_api`):

```yaml
concurrency:
  pools:
    nasa_api:
      limit: 1
      granularity: 'op'
    spacetrack_api:        # separate provider, separate budget
      limit: 1
      granularity: 'op'
```

- [ ] **Step 5: Add credentials to `.env.example`**

Append after the NASA API block:

```
# ---- space-track.org (consumed by dlt as dlt.secrets["spacetrack_username"/"_password"]) ----
SPACETRACK_USERNAME=your_spacetrack_username
SPACETRACK_PASSWORD=your_spacetrack_password
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_spacetrack_sources.py::test_pool_constant -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/spacetrack/config.py \
        dagster.yaml .env.example tests/test_spacetrack_sources.py
git commit -m "feat(ops): add spacetrack_api pool + credential config"
```

---

## Task 7: Per-class assets (`assets.py`)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py`
- Test: `tests/test_spacetrack_assets.py`

**Interfaces:**
- Consumes: `snapshot_source`, `incremental_source`, `spacetrack_pipelines`, `login_session` (from `sources`); `SPACETRACK_API_POOL` (from `config`); `daily_partitions` (from `partitions`).
- Produces (module-level `AssetsDefinition`s, discovered by `load_assets_from_package_module`):
  `spacetrack_gp_assets`, `spacetrack_satcat_assets`, `spacetrack_boxscore_assets`, `spacetrack_decay_assets`, `spacetrack_cdm_assets`, `spacetrack_tip_assets`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_spacetrack_assets.py
from dagster import AssetsDefinition


def _load():
    import auspex_lakehouse.bronze.dlt.assets as a
    return a


def test_six_spacetrack_assets_exist():
    a = _load()
    names = [
        "spacetrack_gp_assets", "spacetrack_satcat_assets", "spacetrack_boxscore_assets",
        "spacetrack_decay_assets", "spacetrack_cdm_assets", "spacetrack_tip_assets",
    ]
    for n in names:
        assert isinstance(getattr(a, n), AssetsDefinition), n


def test_snapshot_assets_unpartitioned_incremental_partitioned():
    a = _load()
    assert a.spacetrack_gp_assets.partitions_def is None
    assert a.spacetrack_boxscore_assets.partitions_def is None
    assert a.spacetrack_decay_assets.partitions_def is not None


def test_all_spacetrack_assets_use_the_pool():
    a = _load()
    for n in ["spacetrack_gp_assets", "spacetrack_satcat_assets",
              "spacetrack_boxscore_assets", "spacetrack_decay_assets",
              "spacetrack_cdm_assets", "spacetrack_tip_assets"]:
        assert getattr(a, n).op.pool == "spacetrack_api", n


def test_spacetrack_assets_in_group():
    a = _load()
    for spec in a.spacetrack_gp_assets.specs:
        assert spec.group_name == "spacetrack"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spacetrack_assets.py -v`
Expected: FAIL — `AttributeError` (assets not defined).

- [ ] **Step 3: Add imports to `assets.py`**

The file already imports `date`, `AssetExecutionContext`, `AutomationCondition`, `DagsterDltResource`, `DagsterDltTranslator`, `dlt_assets`, `DltResourceTranslatorData`, and `daily_partitions`. Add to the `auspex_lakehouse.bronze.dlt.sources` import block the names `snapshot_source`, `incremental_source`, `spacetrack_pipelines`, `login_session`, and add a new import:

```python
from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import SPACETRACK_API_POOL
```

- [ ] **Step 4: Append the space-track asset section to `assets.py`**

```python
# ---- space-track.org: one isolated pipeline + asset per class ----

# Staggered after SATCAT's 1700 UTC update; off-the-hour minutes (they serialize on
# the pool regardless, but staggering keeps the scheduler tidy).
_ST_SNAPSHOT_CRON = {"gp": "11 18 * * *", "satcat": "21 18 * * *", "boxscore": "31 18 * * *"}
_ST_INCREMENTAL_CRON = {"decay": "41 18 * * *", "cdm": "46 18 * * *", "tip": "51 18 * * *"}


class SpaceTrackDltTranslator(DagsterDltTranslator):
    def __init__(self, cron: str):
        self._cron = cron
        super().__init__()

    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_spacetrack_{data.resource.name}"),
            automation_condition=AutomationCondition.on_cron(self._cron),
        )


def _spacetrack_snapshot_assets(name: str):
    @dlt_assets(
        dlt_source=snapshot_source(name),                 # session=None at import
        dlt_pipeline=spacetrack_pipelines[name],
        name=f"spacetrack_{name}_bronze",
        group_name="spacetrack",
        dagster_dlt_translator=SpaceTrackDltTranslator(_ST_SNAPSHOT_CRON[name]),
        pool=SPACETRACK_API_POOL,
    )
    def _assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        session = login_session()                          # one login per run
        yield from dlt.run(
            context=context, dlt_source=snapshot_source(name, session=session)
        )

    return _assets


def _spacetrack_incremental_assets(name: str):
    @dlt_assets(
        dlt_source=incremental_source(name, start_date=date.today(), end_date=date.today()),
        dlt_pipeline=spacetrack_pipelines[name],
        name=f"spacetrack_{name}_bronze",
        group_name="spacetrack",
        partitions_def=daily_partitions,
        dagster_dlt_translator=SpaceTrackDltTranslator(_ST_INCREMENTAL_CRON[name]),
        pool=SPACETRACK_API_POOL,
    )
    def _assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        rng = context.partition_key_range
        session = login_session()                          # one login per run
        source = incremental_source(
            name,
            start_date=date.fromisoformat(rng.start),
            end_date=date.fromisoformat(rng.end),
            session=session,
        )
        yield from dlt.run(context=context, dlt_source=source)

    return _assets


spacetrack_gp_assets = _spacetrack_snapshot_assets("gp")
spacetrack_satcat_assets = _spacetrack_snapshot_assets("satcat")
spacetrack_boxscore_assets = _spacetrack_snapshot_assets("boxscore")
spacetrack_decay_assets = _spacetrack_incremental_assets("decay")
spacetrack_cdm_assets = _spacetrack_incremental_assets("cdm")
spacetrack_tip_assets = _spacetrack_incremental_assets("tip")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_spacetrack_assets.py -v`
Expected: PASS (4 tests). If `.op.pool` raises `AttributeError`, fall back to asserting the pool via `getattr(getattr(a, n), "op").pool` is identical — the installed `dagster_dlt` forwards `pool=` to `multi_asset` (verified), so `.op.pool` is the correct accessor.

- [ ] **Step 6: Lint and commit**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/assets.py tests/test_spacetrack_assets.py`

```bash
git add src/auspex_lakehouse/bronze/dlt/assets.py tests/test_spacetrack_assets.py
git commit -m "feat(bronze): add per-class space-track assets (pooled, scheduled)"
```

---

## Task 8: Definitions smoke test + full suite

**Files:**
- Modify: `tests/test_definitions.py`

**Interfaces:**
- Consumes: the full `Definitions` object; all six space-track assets.

- [ ] **Step 1: Add a smoke assertion to `tests/test_definitions.py`**

Append a test that the definitions load and include the space-track assets (this exercises that NO import-time HTTP/Delta happens — it would fail if login ran at import):

```python
def test_definitions_include_spacetrack_assets():
    from auspex_lakehouse.definitions import defs
    graph = defs.resolve_asset_graph()
    st_keys = {k.to_user_string() for k in graph.asset_keys_for_group("spacetrack")}
    assert len(st_keys) >= 6, f"Expected at least 6 spacetrack assets; got {st_keys}"
    assert "dlt_spacetrack_gp" in st_keys, f"Missing dlt_spacetrack_gp; got {st_keys}"
    assert "dlt_spacetrack_decay" in st_keys, f"Missing dlt_spacetrack_decay; got {st_keys}"
```

- [ ] **Step 2: Run the full suite**

Run: `uv run pytest -v`
Expected: PASS — all existing NASA tests plus the new space-track tests, no network calls, no credential requirement at import.

- [ ] **Step 3: Lint the whole change**

Run: `uv run ruff check src/ tests/`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add tests/test_definitions.py
git commit -m "test: smoke-check space-track assets load in definitions"
```

---

## Task 9: First live materialization (manual validation, requires credentials)

Not automated — a guarded first run to confirm end-to-end behavior against the live API and the bronze bucket. Do this after Tasks 1–8 are merged-ready.

- [ ] **Step 1: Confirm env**

Ensure `.env` has real `SPACETRACK_USERNAME` / `SPACETRACK_PASSWORD` and the bronze/MinIO vars (already used by NASA assets).

- [ ] **Step 2: Materialize one cheap snapshot class first**

Run: `uv run dagster asset materialize --select spacetrack_boxscore_assets -m auspex_lakehouse.definitions`
Expected: success; a `bronze/boxscore` Delta table appears. (BOXSCORE is the smallest and 1/day — verify the table, then proceed.)

- [ ] **Step 3: Materialize GP (largest snapshot) and confirm row count**

Run: `uv run dagster asset materialize --select spacetrack_gp_assets -m auspex_lakehouse.definitions`
Expected: success; `bronze/gp` row count matches the Task 1 probe (no floor RuntimeError). If it raises the row-cap RuntimeError, GP is being capped — implement explicit `limit`/`offset` paging in `_snapshot_resource` (out of current scope; open a follow-up).

- [ ] **Step 4: Materialize one incremental partition**

Run: `uv run dagster asset materialize --select spacetrack_decay_assets --partition 2026-06-27 -m auspex_lakehouse.definitions`
Expected: success; `bronze/decay` exists (may be empty for a quiet day — that is valid).

- [ ] **Step 5: Note any registry corrections**

If CDM/DECAY/TIP keys or class names needed adjustment vs. the probe, ensure the committed registries reflect reality. No further commit if already correct.

---

## Self-Review notes (author)

- **Spec coverage:** auth/session (Task 2), snapshot factory+registry+row-cap (Task 3), incremental factory+registry (Task 4), per-class sources+pipelines (Task 5), pool+config+env (Task 6), per-class pooled+scheduled assets (Task 7), no-import-HTTP smoke (Task 8), live verification of `⚠ verify-live` items (Tasks 1 & 9). All spec sections map to a task.
- **Failure isolation** (spec's key decision): realized by one pipeline + one asset per class in Tasks 5 & 7.
- **Type consistency:** registry tuple arities are fixed — snapshot = 6-tuple `(name, class, pk, segments, write_disposition, min_rows)`, incremental = 4-tuple `(name, class, pk, date_predicate)` — and every consumer (`SNAPSHOT_BY_NAME` unpack, `_snapshot_resource` signature, tests) matches those arities.
- **Open follow-up (documented, not silently dropped):** explicit GP/SATCAT pagination only if Task 1/9 reveals a server-side row cap.
