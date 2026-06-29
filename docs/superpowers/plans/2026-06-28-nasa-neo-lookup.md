# NASA NEO Lookup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich near-earth-object data by looking up each `neo_reference_id` from the `neows` feed against `GET /neo/rest/v1/neo/<id>` and landing the full payload in a `neo_lookup` bronze Delta table, fetching new/stale IDs only, fault-tolerantly, under the NASA 1000/hr budget.

**Architecture:** Split the NASA dlt sources into a per-endpoint `nasa/` package. A pure planner (`select_neo_work_ids`) decides which IDs to fetch (new + >30-day-stale, capped); a fault-tolerant fetcher (`fetch_neo_lookups`) calls the API (404→tombstone, 429→stop & defer, else raise); a thin Dagster `@asset` wires them, reading IDs from the `neows` Delta table and merge-writing via a dedicated dlt pipeline. A `nasa_api` concurrency pool (limit 1) serializes API access.

**Tech Stack:** Python 3.10+, Dagster 1.13.11, dagster-dlt, dlt[deltalake,s3] ≥1.28.1, deltalake, Polars, pytest, ruff, uv.

## Global Constraints

- Python target **3.10** (`requires-python = ">=3.10,<3.15"`); subscripted builtins (`list[str]`, `dict[str, datetime]`) are fine.
- Pin: `dagster==1.13.11`, `dlt[deltalake,s3]>=1.28.1`.
- Lint must pass: `uv run ruff check .` (rules `E`, `F`, `I` — imports sorted: stdlib / third-party / first-party, line-length **100**).
- Tests run with: `uv run pytest -q` (from repo root). CI provides dummy env (`NASA_API_KEY`, `MINIO_*`, `BRONZE_BUCKET_*`, `DESTINATION__FILESYSTEM__*`) — see [.github/workflows/ci.yml](.github/workflows/ci.yml).
- **No import-time HTTP or Delta/S3 access** — all network/Delta work happens inside resource generators or the asset body, never at module import (the `test_definitions_load` smoke test must keep passing without MinIO).
- **Public-name stability:** `nasa_api` and `nasa_pipeline` must remain importable from `auspex_lakehouse.bronze.dlt.sources` throughout.
- dlt API key is read via `dlt.secrets["nasa_api_key"]` (mapped from env `NASA_API_KEY`).
- If `git` is not initialized in this workspace, run `git init` once before the first commit (the project ships CI/gitleaks, so git is expected); otherwise the per-task commit steps apply as written.

---

## File Structure

**Created:**
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py` — assembles `nasa_api` source + `nasa_pipeline`; re-exports resources, config, neo-lookup names
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/_common.py` — `BASE_URL`, `iter_days()`, `nasa_api_key()`
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/config.py` — NASA budget constants
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/apod.py` — `apod` resource (relocated)
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/neows.py` — `neows` resource (relocated)
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py` — planner + fetcher + write resource + dedicated pipeline
- `tests/test_nasa_sources.py`, `tests/test_neo_lookup.py`, `tests/test_delta_helpers.py`, `tests/test_neo_lookup_asset.py`

**Modified:**
- `src/auspex_lakehouse/bronze/dlt/sources/__init__.py` — re-export new names
- `src/auspex_lakehouse/resources/delta.py` — add `delta_storage_options()`, `bronze_table_exists()`, `read_bronze_table()`
- `src/auspex_lakehouse/bronze/dlt/assets.py` — add `neo_lookup` asset + `_existing_lookup_index()`; refactor `apod_images` onto `read_bronze_table`
- `dagster.yaml` — add `concurrency.pools.nasa_api`

**Deleted:**
- `src/auspex_lakehouse/bronze/dlt/sources/nasa_api.py` — replaced by the `nasa/` package

---

## Task 1: Restructure NASA sources into a per-endpoint package

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/_common.py`
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/config.py`
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/apod.py`
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neows.py`
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`
- Delete: `src/auspex_lakehouse/bronze/dlt/sources/nasa_api.py`
- Test: `tests/test_nasa_sources.py`

**Interfaces:**
- Produces: `BASE_URL: str`, `iter_days(start_date, end_date) -> Iterator[date]`, `nasa_api_key() -> str` (in `_common`); `NASA_REFRESH_DAYS`, `NASA_MAX_LOOKUPS_PER_RUN`, `NASA_API_POOL` (in `config`); `apod`, `neows` dlt resources; `nasa_api` `@dlt.source`, `nasa_pipeline` `dlt.Pipeline` — all importable from `auspex_lakehouse.bronze.dlt.sources`.

- [ ] **Step 1: Write the failing regression test**

Create `tests/test_nasa_sources.py`:

```python
from datetime import date


def test_public_names_import_from_sources():
    from auspex_lakehouse.bronze.dlt.sources import nasa_api, nasa_pipeline

    assert nasa_pipeline.pipeline_name == "nasa_api"
    assert callable(nasa_api)


def test_nasa_source_exposes_apod_and_neows():
    from auspex_lakehouse.bronze.dlt.sources import nasa_api

    src = nasa_api(start_date=date(2026, 1, 1), end_date=date(2026, 1, 1))
    assert set(src.resources.keys()) == {"apod", "neows"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_nasa_sources.py -q`
Expected: PASS currently (names still resolve via the old module) — this test is the *regression guard*; it must keep passing after the move. Proceed to restructure and re-run.

- [ ] **Step 3: Create `nasa/_common.py`**

```python
from collections.abc import Iterator
from datetime import date, timedelta

import dlt

BASE_URL = "https://api.nasa.gov"


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yield each date in the inclusive [start_date, end_date] range."""
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)


def nasa_api_key() -> str:
    """NASA API key from dlt config (env NASA_API_KEY or .dlt/secrets.toml)."""
    return dlt.secrets["nasa_api_key"]
```

- [ ] **Step 4: Create `nasa/config.py`**

```python
"""NASA-provider budget constants.

The 1000 calls/hour limit is shared across all NASA endpoints, so these are
deliberately conservative. Other providers get their own constants.
"""

NASA_REFRESH_DAYS = 30           # re-fetch a NEO whose lookup is older than this
NASA_MAX_LOOKUPS_PER_RUN = 500   # secondary per-run guard (primary control is the pool + 429-handling)
NASA_API_POOL = "nasa_api"       # Dagster concurrency pool serializing NASA API access
```

- [ ] **Step 5: Create `nasa/apod.py`** (moved from `nasa_api.py`, now using shared helpers)

```python
from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


@dlt.resource(name="apod", write_disposition="merge", primary_key="date", table_format="delta")
def apod(start_date: date, end_date: date):
    api_key = nasa_api_key()
    for day in iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/planetary/apod",
            params={"api_key": api_key, "date": day.isoformat()},
        )
        resp.raise_for_status()
        yield resp.json()
```

- [ ] **Step 6: Create `nasa/neows.py`** (moved, using shared helpers)

```python
from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


@dlt.resource(
    name="neows",
    write_disposition="merge",
    primary_key=["date", "id"],
    table_format="delta",
)
def neows(start_date: date, end_date: date):
    api_key = nasa_api_key()
    for day in iter_days(start_date, end_date):
        resp = requests.get(
            f"{BASE_URL}/neo/rest/v1/feed",
            params={
                "api_key": api_key,
                "start_date": day.isoformat(),
                "end_date": day.isoformat(),
            },
        )
        resp.raise_for_status()
        # The feed nests asteroids under near_earth_objects keyed by date.
        # Flatten to one row per asteroid, tagged with its feed date.
        for feed_date, objects in resp.json()["near_earth_objects"].items():
            for obj in objects:
                yield {**obj, "date": feed_date}
```

- [ ] **Step 7: Create `nasa/__init__.py`** (the assembler)

```python
from datetime import date

import dlt

from auspex_lakehouse.bronze.dlt.sources.nasa.apod import apod
from auspex_lakehouse.bronze.dlt.sources.nasa.neows import neows


@dlt.source
def nasa_api(start_date: date, end_date: date):
    return [
        apod(start_date, end_date),
        neows(start_date, end_date),
    ]


nasa_pipeline = dlt.pipeline(
    pipeline_name="nasa_api",
    destination="filesystem",
    dataset_name="bronze",
)

__all__ = ["apod", "neows", "nasa_api", "nasa_pipeline"]
```

- [ ] **Step 8: Replace `sources/__init__.py`**

```python
from auspex_lakehouse.bronze.dlt.sources.nasa import nasa_api, nasa_pipeline

__all__ = ["nasa_api", "nasa_pipeline"]
```

- [ ] **Step 9: Delete the old module**

Run: `rm src/auspex_lakehouse/bronze/dlt/sources/nasa_api.py`

- [ ] **Step 10: Run lint + tests**

Run: `uv run ruff check . && uv run pytest tests/test_nasa_sources.py tests/test_definitions.py -q`
Expected: PASS (both restructure tests + the existing smoke test load cleanly).

- [ ] **Step 11: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources tests/test_nasa_sources.py
git commit -m "refactor(bronze): split NASA dlt sources into per-endpoint nasa/ package"
```

---

## Task 2: Shared bronze Delta helpers

**Files:**
- Modify: `src/auspex_lakehouse/resources/delta.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py` (refactor `apod_images`)
- Test: `tests/test_delta_helpers.py`

**Interfaces:**
- Produces: `delta_storage_options() -> dict`, `bronze_table_exists(name: str) -> bool`, `read_bronze_table(name: str) -> pl.DataFrame` in `auspex_lakehouse.resources.delta`.

- [ ] **Step 1: Write the failing test** (`delta_storage_options` is the unit-testable pure-ish piece — it reads env and builds a dict)

Create `tests/test_delta_helpers.py`:

```python
import importlib


def test_delta_storage_options_from_env(monkeypatch):
    monkeypatch.setenv("MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("AWS_REGION", "us-west-1")

    delta = importlib.import_module("auspex_lakehouse.resources.delta")
    opts = delta.delta_storage_options()

    assert opts["AWS_ACCESS_KEY_ID"] == "ak"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "sk"
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_REGION"] == "us-west-1"


def test_read_bronze_table_exists_callable():
    delta = importlib.import_module("auspex_lakehouse.resources.delta")
    assert callable(delta.read_bronze_table)
    assert callable(delta.bronze_table_exists)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_delta_helpers.py -q`
Expected: FAIL with `AttributeError: module ... has no attribute 'delta_storage_options'`.

- [ ] **Step 3: Add helpers to `resources/delta.py`**

Append below the existing `delta_io_manager` definition:

```python
import polars as pl
from deltalake import DeltaTable


def _bronze_table_uri(name: str) -> str:
    return f"{os.environ['BRONZE_BUCKET_URI']}/bronze/{name}"


def delta_storage_options() -> dict:
    """Raw storage-options dict for deltalake.DeltaTable against the MinIO bronze bucket."""
    return {
        "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
        "AWS_ENDPOINT_URL": os.environ["MINIO_ENDPOINT"],
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": os.environ.get("AWS_REGION", "us-west-1"),
    }


def bronze_table_exists(name: str) -> bool:
    """True if a Delta table exists at bronze/<name> (False on first run, before any write)."""
    return DeltaTable.is_deltatable(
        _bronze_table_uri(name), storage_options=delta_storage_options()
    )


def read_bronze_table(name: str) -> pl.DataFrame:
    """Open the bronze Delta table <name> as a Polars DataFrame.

    Raises if the table does not exist — callers that may run before the table's
    first write must guard with ``bronze_table_exists`` first.
    """
    dt = DeltaTable(_bronze_table_uri(name), storage_options=delta_storage_options())
    return pl.from_arrow(dt.to_pyarrow_table())
```

Add `import os` at the top if not already present (it is — `delta_io_manager` uses `os.getenv`).

- [ ] **Step 4: Refactor `apod_images` in `assets.py` to use the helper**

Replace the inline `DeltaTable(...)` block (the `dt = DeltaTable(...)` assignment through the `df = pl.from_arrow(...).filter(...)` lines) with:

```python
    df = read_bronze_table("apod").filter(pl.col("date") == partition_key)
```

Update `assets.py` imports: remove `from deltalake import DeltaTable`; add `from auspex_lakehouse.resources.delta import read_bronze_table`. Keep `import os`, `import boto3`, `import polars as pl`, `import requests` (still used by the image-download path).

- [ ] **Step 5: Run lint + tests**

Run: `uv run ruff check . && uv run pytest tests/test_delta_helpers.py tests/test_definitions.py -q`
Expected: PASS (helper unit test + smoke test; `apod_images` still imports cleanly — no import-time Delta access).

- [ ] **Step 6: Commit**

```bash
git add src/auspex_lakehouse/resources/delta.py src/auspex_lakehouse/bronze/dlt/assets.py tests/test_delta_helpers.py
git commit -m "refactor(bronze): centralize bronze Delta reads in resources.delta helpers"
```

---

## Task 3: NEO work-list planner (pure)

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py`
- Test: `tests/test_neo_lookup.py`

**Interfaces:**
- Produces: `NeoWorkPlan` dataclass with fields `selected: list[str]`, `new: list[str]`, `stale: list[str]`, `deferred_over_cap: list[str]`; `select_neo_work_ids(candidates: set[str], existing: dict[str, datetime], now: datetime, refresh_days: int, cap: int) -> NeoWorkPlan`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_neo_lookup.py`:

```python
from datetime import datetime, timedelta, timezone

from auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup import select_neo_work_ids

NOW = datetime(2026, 6, 28, tzinfo=timezone.utc)


def test_new_ids_selected_when_table_empty():
    plan = select_neo_work_ids({"a", "b"}, {}, NOW, 30, 500)
    assert set(plan.new) == {"a", "b"}
    assert plan.stale == []
    assert set(plan.selected) == {"a", "b"}
    assert plan.deferred_over_cap == []


def test_fresh_ids_skipped():
    existing = {"a": NOW - timedelta(days=10)}
    plan = select_neo_work_ids({"a"}, existing, NOW, 30, 500)
    assert plan.selected == []
    assert plan.new == []
    assert plan.stale == []


def test_stale_ids_refreshed():
    existing = {"a": NOW - timedelta(days=40)}
    plan = select_neo_work_ids({"a"}, existing, NOW, 30, 500)
    assert plan.stale == ["a"]
    assert plan.selected == ["a"]


def test_cap_prioritizes_new_and_defers_rest():
    candidates = {"n0", "n1", "n2", "old"}
    existing = {"old": NOW - timedelta(days=99)}  # stale
    plan = select_neo_work_ids(candidates, existing, NOW, 30, cap=2)
    assert plan.selected == ["n0", "n1"]            # new sorted, prioritized
    assert plan.deferred_over_cap == ["n2", "old"]  # remaining new + stale deferred
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_neo_lookup.py -q`
Expected: FAIL with `ModuleNotFoundError`/`ImportError` (no `neo_lookup` module yet).

- [ ] **Step 3: Create `neo_lookup.py` with the planner**

```python
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class NeoWorkPlan:
    selected: list[str]           # IDs to fetch this run (cap-bounded)
    new: list[str]                # candidates never looked up before
    stale: list[str]              # candidates whose lookup is older than the refresh window
    deferred_over_cap: list[str]  # selected-minus-cap, picked up next run


def select_neo_work_ids(
    candidates: set[str],
    existing: dict[str, datetime],
    now: datetime,
    refresh_days: int,
    cap: int,
) -> NeoWorkPlan:
    """Decide which neo_reference_ids to fetch: new first, then >refresh_days-stale,
    truncated to `cap`. Pure — no I/O."""
    new = sorted(candidates - existing.keys())
    stale = sorted(
        neo_id
        for neo_id in candidates & existing.keys()
        if now - existing[neo_id] > timedelta(days=refresh_days)
    )
    ordered = new + stale
    return NeoWorkPlan(
        selected=ordered[:cap],
        new=new,
        stale=stale,
        deferred_over_cap=ordered[cap:],
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_neo_lookup.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py tests/test_neo_lookup.py
git commit -m "feat(bronze): add NEO lookup work-list planner"
```

---

## Task 4: Fault-tolerant NEO fetcher

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py`
- Test: `tests/test_neo_lookup.py`

**Interfaces:**
- Consumes: `BASE_URL` from `nasa._common`.
- Produces: `FetchStats` dataclass with `fetched_ok: int`, `tombstoned: int`, `stopped_on_rate_limit: bool`, `deferred_on_stop: list[str]`; `fetch_neo_lookups(neo_ids: list[str], fetched_at: str, api_key: str) -> tuple[list[dict], FetchStats]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_neo_lookup.py`:

```python
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup as nl


def _resp(status, payload=None):
    r = Mock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.raise_for_status = Mock(
        side_effect=None if status < 400 else RuntimeError(f"http {status}")
    )
    return r


def test_fetch_ok_stamps_row(monkeypatch):
    monkeypatch.setattr(
        nl, "requests", Mock(get=Mock(return_value=_resp(200, {"neo_reference_id": "a", "name": "X"})))
    )
    rows, stats = nl.fetch_neo_lookups(["a"], "2026-06-28T00:00:00+00:00", "key")
    assert stats.fetched_ok == 1
    assert rows[0]["lookup_status"] == "ok"
    assert rows[0]["lookup_fetched_at"] == "2026-06-28T00:00:00+00:00"
    assert rows[0]["name"] == "X"


def test_fetch_404_writes_tombstone(monkeypatch):
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(return_value=_resp(404))))
    rows, stats = nl.fetch_neo_lookups(["dead"], "T", "key")
    assert stats.tombstoned == 1 and stats.fetched_ok == 0
    assert rows == [
        {"neo_reference_id": "dead", "lookup_fetched_at": "T", "lookup_status": "not_found"}
    ]


def test_fetch_429_stops_and_defers_tail(monkeypatch):
    seq = [_resp(200, {"neo_reference_id": "a"}), _resp(429), _resp(200, {"neo_reference_id": "c"})]
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(side_effect=seq)))
    rows, stats = nl.fetch_neo_lookups(["a", "b", "c"], "T", "key")
    assert stats.stopped_on_rate_limit is True
    assert stats.fetched_ok == 1
    assert stats.deferred_on_stop == ["b", "c"]
    assert [r["neo_reference_id"] for r in rows] == ["a"]


def test_fetch_other_error_raises(monkeypatch):
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(return_value=_resp(500))))
    with pytest.raises(RuntimeError):
        nl.fetch_neo_lookups(["x"], "T", "key")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_neo_lookup.py -k fetch -q`
Expected: FAIL (`fetch_neo_lookups`/`requests` not defined in the module).

- [ ] **Step 3: Add the fetcher to `neo_lookup.py`**

Add imports at the top of `neo_lookup.py` (keep `from dataclasses import dataclass` — extend it):

```python
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL
```

Append below the planner:

```python
@dataclass
class FetchStats:
    fetched_ok: int = 0
    tombstoned: int = 0                 # 404s recorded as tombstones
    stopped_on_rate_limit: bool = False  # hit 429; remaining IDs deferred
    deferred_on_stop: list[str] = field(default_factory=list)


def fetch_neo_lookups(
    neo_ids: list[str], fetched_at: str, api_key: str
) -> tuple[list[dict], FetchStats]:
    """Fetch each NEO lookup, tolerant per ID so one bad ID can't poison the batch:
    404 -> tombstone row (dedupe skips it until refresh); 429 -> commit progress and
    defer the remaining tail; any other non-2xx -> raise."""
    rows: list[dict] = []
    stats = FetchStats()
    for idx, neo_id in enumerate(neo_ids):
        resp = requests.get(
            f"{BASE_URL}/neo/rest/v1/neo/{neo_id}",
            params={"api_key": api_key},
        )
        if resp.status_code == 404:
            rows.append(
                {
                    "neo_reference_id": neo_id,
                    "lookup_fetched_at": fetched_at,
                    "lookup_status": "not_found",
                }
            )
            stats.tombstoned += 1
            continue
        if resp.status_code == 429:
            stats.stopped_on_rate_limit = True
            stats.deferred_on_stop = list(neo_ids[idx:])
            return rows, stats
        resp.raise_for_status()
        rows.append({**resp.json(), "lookup_fetched_at": fetched_at, "lookup_status": "ok"})
        stats.fetched_ok += 1
    return rows, stats
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_neo_lookup.py -q`
Expected: PASS (8 tests total: 4 planner + 4 fetcher).

- [ ] **Step 5: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py tests/test_neo_lookup.py
git commit -m "feat(bronze): add fault-tolerant NEO lookup fetcher (404 tombstone, 429 defer)"
```

---

## Task 5: NEO write resource + dedicated pipeline + exports

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`
- Test: `tests/test_nasa_sources.py`

**Interfaces:**
- Produces: `neo_lookup_rows(rows: list[dict])` dlt resource (name `neo_lookup`, merge on `neo_reference_id`, delta); `nasa_neo_lookup_pipeline` (pipeline_name `nasa_neo_lookup`, dataset `bronze`) — both importable from `auspex_lakehouse.bronze.dlt.sources`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_nasa_sources.py`:

```python
def test_neo_lookup_resource_and_pipeline_exported():
    from auspex_lakehouse.bronze.dlt.sources import (
        nasa_neo_lookup_pipeline,
        neo_lookup_rows,
    )

    assert nasa_neo_lookup_pipeline.pipeline_name == "nasa_neo_lookup"
    res = neo_lookup_rows([{"neo_reference_id": "a"}])
    assert res.name == "neo_lookup"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_nasa_sources.py::test_neo_lookup_resource_and_pipeline_exported -q`
Expected: FAIL with `ImportError` (names not exported yet).

- [ ] **Step 3: Add the write resource + pipeline to `neo_lookup.py`**

Add `import dlt` to the top of `neo_lookup.py` (alongside the existing imports), then append:

```python
@dlt.resource(
    name="neo_lookup",
    write_disposition="merge",
    primary_key="neo_reference_id",
    table_format="delta",
)
def neo_lookup_rows(rows: list[dict]):
    """Pass-through resource over already-fetched rows; dlt does the Delta merge
    and normalizes nested payload fields (orbital_data, close_approach_data, ...)
    into child tables."""
    yield from rows


nasa_neo_lookup_pipeline = dlt.pipeline(
    pipeline_name="nasa_neo_lookup",   # distinct working dir -> no collision with nasa_api
    destination="filesystem",
    dataset_name="bronze",             # same bronze dataset -> lands at bronze/neo_lookup
)
```

- [ ] **Step 4: Export from `nasa/__init__.py`**

Add the import and extend `__all__`:

```python
from auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup import (
    nasa_neo_lookup_pipeline,
    neo_lookup_rows,
)
```

Update `__all__` to:

```python
__all__ = ["apod", "neows", "nasa_api", "nasa_pipeline", "neo_lookup_rows", "nasa_neo_lookup_pipeline"]
```

- [ ] **Step 5: Re-export from `sources/__init__.py`**

```python
from auspex_lakehouse.bronze.dlt.sources.nasa import (
    nasa_api,
    nasa_neo_lookup_pipeline,
    nasa_pipeline,
    neo_lookup_rows,
)

__all__ = ["nasa_api", "nasa_pipeline", "neo_lookup_rows", "nasa_neo_lookup_pipeline"]
```

- [ ] **Step 6: Run lint + tests**

Run: `uv run ruff check . && uv run pytest tests/test_nasa_sources.py -q`
Expected: PASS (restructure + export tests).

- [ ] **Step 7: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources tests/test_nasa_sources.py
git commit -m "feat(bronze): add neo_lookup write resource and dedicated dlt pipeline"
```

---

## Task 6: NEO lookup asset (wires planner + fetcher + pipeline)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py`
- Test: `tests/test_neo_lookup_asset.py`

**Interfaces:**
- Consumes: `select_neo_work_ids`, `fetch_neo_lookups`, `neo_lookup_rows`, `nasa_neo_lookup_pipeline`, `NASA_REFRESH_DAYS`, `NASA_MAX_LOOKUPS_PER_RUN`, `NASA_API_POOL`, `read_bronze_table`, `bronze_table_exists`.
- Produces: a Dagster asset with key `neo_lookup`, partitioned by `daily_partitions`, depending on `AssetKey(["dlt_nasa_api_neows"])`, assigned to pool `nasa_api`.

- [ ] **Step 1: Write the failing wiring test**

Create `tests/test_neo_lookup_asset.py`:

```python
from dagster import AssetKey, AssetsDefinition


def test_neo_lookup_asset_wired_into_definitions():
    from auspex_lakehouse.definitions import defs

    graph = defs.get_asset_graph()
    key = AssetKey(["neo_lookup"])
    assert key in graph.get_all_asset_keys()
    # depends on the dlt neows bronze table
    assert AssetKey(["dlt_nasa_api_neows"]) in graph.get(key).parent_keys


def test_neo_lookup_asset_is_pooled():
    from auspex_lakehouse.definitions import defs

    ad = next(
        a
        for a in defs.assets
        if isinstance(a, AssetsDefinition) and AssetKey(["neo_lookup"]) in a.keys
    )
    assert ad.op.pool == "nasa_api"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_neo_lookup_asset.py -q`
Expected: FAIL (`neo_lookup` asset key not present in the graph).

- [ ] **Step 3: Add imports to `assets.py`**

Ensure these imports exist (merge with current ones; ruff will order them):

```python
import os
from datetime import date, datetime, timezone
from pathlib import PurePosixPath

import boto3
import dlt
import polars as pl
import requests
from dagster import AssetExecutionContext, AssetKey, AutomationCondition, asset
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from auspex_lakehouse.bronze.dlt.sources import (
    nasa_api,
    nasa_neo_lookup_pipeline,
    nasa_pipeline,
    neo_lookup_rows,
)
from auspex_lakehouse.bronze.dlt.sources.nasa.config import (
    NASA_API_POOL,
    NASA_MAX_LOOKUPS_PER_RUN,
    NASA_REFRESH_DAYS,
)
from auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup import (
    fetch_neo_lookups,
    select_neo_work_ids,
)
from auspex_lakehouse.partitions import daily_partitions
from auspex_lakehouse.resources.delta import bronze_table_exists, read_bronze_table
```

- [ ] **Step 4: Add the `_existing_lookup_index` helper and the asset**

Append to `assets.py`:

```python
def _existing_lookup_index() -> dict[str, datetime]:
    """Map neo_reference_id -> last lookup timestamp from the neo_lookup table.
    Empty on the first run, before the table exists.

    dlt infers the ISO-8601 `lookup_fetched_at` we write as a *timestamp* column,
    so Polars hands it back as a ``datetime`` (not the original string); be robust
    to either, and coerce naive timestamps to UTC so the staleness subtraction in
    ``select_neo_work_ids`` doesn't raise on naive/aware mixing. Keys are coerced
    to ``str`` so they compare equal to the str-coerced candidates."""
    if not bronze_table_exists("neo_lookup"):
        return {}
    df = read_bronze_table("neo_lookup").select(["neo_reference_id", "lookup_fetched_at"])
    index: dict[str, datetime] = {}
    for row in df.iter_rows(named=True):
        ts = row["lookup_fetched_at"]
        if ts is None:
            continue
        if not isinstance(ts, datetime):
            ts = datetime.fromisoformat(ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        index[str(row["neo_reference_id"])] = ts
    return index


@asset(
    name="neo_lookup",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_api_neows"])],
    automation_condition=AutomationCondition.eager(),
    pool=NASA_API_POOL,
)
def neo_lookup(context: AssetExecutionContext):
    partition_key = context.partition_key
    candidates = {
        str(neo_id)  # coerce so candidate IDs compare equal to str-keyed existing index
        for neo_id in read_bronze_table("neows")
        .filter(pl.col("date") == partition_key)
        .get_column("neo_reference_id")
        .to_list()
    }
    existing = _existing_lookup_index()
    now = datetime.now(timezone.utc)
    plan = select_neo_work_ids(
        candidates, existing, now, NASA_REFRESH_DAYS, NASA_MAX_LOOKUPS_PER_RUN
    )

    if not plan.selected:
        context.add_output_metadata({"candidates": len(candidates), "fetched_ok": 0})
        return

    rows, stats = fetch_neo_lookups(plan.selected, now.isoformat(), dlt.secrets["nasa_api_key"])
    if rows:
        nasa_neo_lookup_pipeline.run(neo_lookup_rows(rows))

    context.add_output_metadata(
        {
            "candidates": len(candidates),
            "new": len(plan.new),
            "stale": len(plan.stale),
            "fetched_ok": stats.fetched_ok,
            "tombstoned": stats.tombstoned,
            "deferred_over_cap": len(plan.deferred_over_cap),
            "stopped_on_rate_limit": stats.stopped_on_rate_limit,
            "deferred_on_stop": len(stats.deferred_on_stop),
        }
    )
```

- [ ] **Step 5: Run lint + the full test suite**

Run: `uv run ruff check . && uv run pytest -q`
Expected: PASS (wiring tests + pool test + all prior tests + smoke test). If `test_neo_lookup_asset_wired_into_definitions` fails on the dep, the neows asset key differs from `dlt_nasa_api_neows` — inspect with `uv run dagster definitions list` (or print `graph.get_all_asset_keys()`) and correct the `deps=` key.

- [ ] **Step 6: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/assets.py tests/test_neo_lookup_asset.py
git commit -m "feat(bronze): add neo_lookup asset (new+stale dedupe, capped, pooled)"
```

---

## Task 7: NASA concurrency pool in instance config

**Files:**
- Modify: `dagster.yaml`
- Test: `tests/test_neo_lookup_asset.py`

**Interfaces:**
- Produces: a `nasa_api` concurrency pool (limit 1, op granularity) in the Dagster instance config.

- [ ] **Step 1: Write the failing config test**

Append to `tests/test_neo_lookup_asset.py`:

```python
def test_dagster_yaml_defines_nasa_pool():
    import pathlib

    import yaml

    cfg = yaml.safe_load(pathlib.Path("dagster.yaml").read_text())
    pool = cfg["concurrency"]["pools"]["nasa_api"]
    assert pool["limit"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_neo_lookup_asset.py::test_dagster_yaml_defines_nasa_pool -q`
Expected: FAIL with `KeyError: 'concurrency'`.

- [ ] **Step 3: Add the pool to `dagster.yaml`**

Append below the existing `storage:` block:

```yaml
# Serialize NASA API access so concurrent runs can't both burn the shared
# 1000-calls/hour budget (the budget is one bucket; parallel streams only
# mutually 429 and double-spend). Other providers get their own pools.
concurrency:
  pools:
    nasa_api:
      limit: 1
      granularity: 'op'
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_neo_lookup_asset.py -q`
Expected: PASS.

- [ ] **Step 5: Full verification**

Run: `uv run ruff check . && uv run pytest -q`
Expected: PASS (entire suite).

- [ ] **Step 6: Commit**

```bash
git add dagster.yaml tests/test_neo_lookup_asset.py
git commit -m "feat(ops): add nasa_api concurrency pool to serialize NASA API access"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Component 1 (restructure) → Task 1. ✅
- Component 2 (planner + fetcher + write resource + pipeline) → Tasks 3, 4, 5. ✅
- Component 3 (asset + work-list) → Task 6. ✅
- Component 4 (Delta helpers + apod_images refactor) → Task 2. ✅
- Component 5 (concurrency pool) → Task 7. ✅
- Error-handling table (404/429/other/first-run/empty/cap) → covered by Task 4 tests + Task 6 first-run/empty guards. ✅

**Type consistency:** `NeoWorkPlan`/`FetchStats` field names and `select_neo_work_ids`/`fetch_neo_lookups` signatures are identical across Tasks 3–6 and the asset call site. `read_bronze_table`/`bronze_table_exists` names match between Task 2 and Task 6. Exports (`neo_lookup_rows`, `nasa_neo_lookup_pipeline`) match between Task 5 and Task 6 imports. ✅

**Placeholders:** none — every code step is complete.

**Known confirm-at-implementation items (flagged in steps, not blockers):**
- The `neows` dlt asset key `dlt_nasa_api_neows` (Task 6, Step 5 has the inspect-and-correct fallback).
- That the `neows` Delta table carries a `neo_reference_id` column (NASA feed objects include both `id` and `neo_reference_id`; if absent, key on `id` in Task 6, Step 4).
- `ad.op.pool` introspection (Task 6 test) reflects the Dagster 1.13 pool API; if the attribute differs, assert via `ad.op.tags["dagster/concurrency_key"] == "nasa_api"`.
