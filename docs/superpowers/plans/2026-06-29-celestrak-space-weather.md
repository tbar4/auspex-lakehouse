# CelesTrak Space Weather Bronze Ingestion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest CelesTrak's consolidated space-weather CSV as a single bronze Delta table (`celestrak_space_weather`) to provide atmospheric-drag driver inputs (F10.7 flux, Ap/Kp indices) for orbit propagation.

**Architecture:** A new `sources/celestrak/` provider package mirrors the established space-track pattern: a fetch/parse helper + a CSV-snapshot resource factory + a one-entry registry, wrapped by a single unpartitioned Dagster `@dlt_assets` that merges the whole file on `DATE` each run. A thin dbt view (`bronze_celestrak_space_weather`) exposes it, wired to the dlt asset via the source lineage bridge.

**Tech Stack:** Dagster + dagster-dlt + dlt (filesystem destination, Delta table format) + polars (CSV parse) + requests (fetch) + dbt-duckdb (bronze views over `delta_scan`).

**Spec:** `docs/superpowers/specs/2026-06-29-celestrak-space-weather-design.md`

## Global Constraints

Every task implicitly includes these (verbatim from the spec):

- **Table-naming convention `<source>_<dataset>`.** The physical table name `celestrak_space_weather` is one shared knob: the dlt `@dlt.resource` `name=`, the S3 folder `bronze/celestrak_space_weather`, the dbt source `name:`, and it drives the asset key as `dlt_{resource.name}` = `dlt_celestrak_space_weather` (**no doubled provider prefix**). dbt model is `bronze_celestrak_space_weather`.
- **NO `dagster.yaml` change.** `deploy/dagster.yaml` has only `default_limit: 1` and no named pools. Binding the asset to `pool="celestrak_api"` inherits that default automatically (exactly like `spacetrack_api`). Editing the YAML would regress working config.
- **Full-file CSV inference.** Parse with `polars.read_csv(infer_schema_length=None)` — the F10.7 81-day-average columns are blank in the earliest rows and would mis-type under the default 100-row inference.
- **dlt snake_cases column names.** Landed Delta/dbt columns are lowercased, dots stripped: `DATE`→`date`, `F10.7_OBS`→`f10_7_obs`, etc. The `primary_key="DATE"` hint normalizes the same way. Downstream (dbt, silver) uses the **normalized lowercase** names.
- **Merge on `DATE`.** `write_disposition="merge"`, `primary_key="DATE"`, `table_format="delta"`. Idempotent; forecast rows (`PRD`/`PRM`) firm into `OBS` in place.
- **Bronze = as-published, typed.** Land every column the file ships; defer all semantic casting/derivation to silver. `DATE` stays a raw `YYYY-MM-DD` string.
- **Dependencies:** `polars` resolves via the declared `polars-lts-cpu`. `requests` is imported but undeclared in `pyproject.toml` — a pre-existing practice (`assets.py`, `spacetrack/_common.py` already do this); do **not** add it unless explicitly asked.
- **Source CSV:** `https://celestrak.org/SpaceData/SW-All.csv`. Verified 31-column header (order): `DATE,BSRN,ND,KP1..KP8,KP_SUM,AP1..AP8,AP_AVG,CP,C9,ISN,F10.7_OBS,F10.7_ADJ,F10.7_DATA_TYPE,F10.7_OBS_CENTER81,F10.7_OBS_LAST81,F10.7_ADJ_CENTER81,F10.7_ADJ_LAST81`.

## File Structure

**Create:**
- `src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py` — `celestrak_source` factory + `celestrak_pipelines`; package exports.
- `src/auspex_lakehouse/bronze/dlt/sources/celestrak/config.py` — `CELESTRAK_API_POOL` constant.
- `src/auspex_lakehouse/bronze/dlt/sources/celestrak/_common.py` — `SW_ALL_URL` + `fetch_csv_rows`.
- `src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py` — `_csv_snapshot_resource` factory + `CELESTRAK_DATASETS` registry.
- `dbt/models/bronze/bronze_celestrak_space_weather.sql` — bronze view.
- `dbt/models/bronze/_bronze__celestrak__models.yml` — model-level docs/tests.
- `tests/test_celestrak_common.py`, `tests/test_celestrak_sources.py`, `tests/test_celestrak_assets.py`.

**Modify:**
- `src/auspex_lakehouse/bronze/dlt/sources/__init__.py` — re-export `celestrak_source`, `celestrak_pipelines`.
- `src/auspex_lakehouse/bronze/dlt/assets.py` — add `CelesTrakDltTranslator` + `celestrak_space_weather_assets`.
- `dbt/models/bronze/_bronze__sources.yml` — add the `celestrak_space_weather` source table.
- `src/auspex_lakehouse/transform/definitions.py` — add the `_SOURCE_ASSET_KEYS` entry.
- `tests/test_dbt_bronze.py`, `tests/test_definitions.py` — extend guarded sets / add a celestrak assertion.

**Branch:** `feat/celestrak` off `main`.

---

### Task 1: Fetch/parse helper + pool config

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py` (empty for now)
- Create: `src/auspex_lakehouse/bronze/dlt/sources/celestrak/config.py`
- Create: `src/auspex_lakehouse/bronze/dlt/sources/celestrak/_common.py`
- Test: `tests/test_celestrak_common.py`

**Interfaces:**
- Produces: `SW_ALL_URL: str`; `fetch_csv_rows(url: str) -> list[dict]` (typed row dicts, blanks → `None`, `DATE`/`F10.7_DATA_TYPE` as `str`); `CELESTRAK_API_POOL: str == "celestrak_api"`.

- [ ] **Step 1: Create the branch**

Run:
```bash
git checkout main && git checkout -b feat/celestrak
```

- [ ] **Step 2: Create the empty package init**

Create `src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py` with a single newline (it is replaced in Task 3; exists now so the package imports):

```python
```

- [ ] **Step 3: Write the failing test**

Create `tests/test_celestrak_common.py`:

```python
from unittest.mock import Mock

import auspex_lakehouse.bronze.dlt.sources.celestrak._common as c

# Real 31-column header + two rows. Row 1 (1957) leaves the F10.7 81-day-average
# columns BLANK on purpose: they have no full window yet. This is the case the
# default 100-row schema inference mis-types, so it pins the infer_schema_length=None fix.
_HEADER = (
    "DATE,BSRN,ND,KP1,KP2,KP3,KP4,KP5,KP6,KP7,KP8,KP_SUM,"
    "AP1,AP2,AP3,AP4,AP5,AP6,AP7,AP8,AP_AVG,CP,C9,ISN,"
    "F10.7_OBS,F10.7_ADJ,F10.7_DATA_TYPE,"
    "F10.7_OBS_CENTER81,F10.7_OBS_LAST81,F10.7_ADJ_CENTER81,F10.7_ADJ_LAST81"
)
_ROW_BLANK_81 = "1957-10-01,1700,19,43,40,30,20,37,23,43,37,273,32,27,15,7,22,9,32,22,21,1.1,5,334,269.3,269.8,OBS,,,,"
_ROW_FULL = "2026-06-28,2480,5,7,10,7,13,17,20,23,17,113,3,4,3,5,6,7,9,6,5,0.4,2,100,150.2,151.0,OBS,148.1,149.2,148.9,150.0"
CSV_BYTES = ("\n".join([_HEADER, _ROW_BLANK_81, _ROW_FULL]) + "\n").encode()


def _patch_get(monkeypatch, content):
    fake = Mock()
    fake.get.return_value = Mock(content=content, raise_for_status=Mock())
    monkeypatch.setattr(c, "requests", fake)
    return fake


def test_fetch_csv_rows_returns_raw_header_columns(monkeypatch):
    _patch_get(monkeypatch, CSV_BYTES)
    rows = c.fetch_csv_rows("http://example/SW-All.csv")
    assert len(rows) == 2
    # to_dicts() keys are the RAW (pre-dlt) headers — exactly 31, in the file's spelling.
    assert set(rows[0].keys()) == set(_HEADER.split(","))


def test_fetch_csv_rows_types_and_nulls(monkeypatch):
    _patch_get(monkeypatch, CSV_BYTES)
    rows = c.fetch_csv_rows("http://example/SW-All.csv")
    # DATE and the data-type flag stay strings; bronze keeps DATE raw.
    assert isinstance(rows[0]["DATE"], str) and rows[0]["DATE"] == "1957-10-01"
    assert rows[0]["F10.7_DATA_TYPE"] == "OBS"
    # Blank 81-day fields in the early row are null...
    assert rows[0]["F10.7_OBS_CENTER81"] is None
    # ...and the later row's value is a parsed float (full-file inference typed the column).
    assert rows[1]["F10.7_OBS_CENTER81"] == 148.1


def test_sw_all_url():
    assert c.SW_ALL_URL == "https://celestrak.org/SpaceData/SW-All.csv"


def test_pool_constant():
    from auspex_lakehouse.bronze.dlt.sources.celestrak.config import CELESTRAK_API_POOL
    assert CELESTRAK_API_POOL == "celestrak_api"
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `uv run pytest tests/test_celestrak_common.py -v`
Expected: FAIL — `ModuleNotFoundError`/`AttributeError` (no `_common`/`config` yet).

- [ ] **Step 5: Write `config.py`**

Create `src/auspex_lakehouse/bronze/dlt/sources/celestrak/config.py`:

```python
# src/auspex_lakehouse/bronze/dlt/sources/celestrak/config.py
"""CelesTrak provider constants.

CelesTrak SpaceData files are public static CSVs on a CDN — no auth, no real rate
limit. The pool is a convention (matches nasa_api / spacetrack_api) and inherits the
instance-wide default_limit of 1; we do NOT add a named pool to dagster.yaml.
"""

CELESTRAK_API_POOL = "celestrak_api"  # Dagster pool; inherits default_limit=1
```

- [ ] **Step 6: Write `_common.py`**

Create `src/auspex_lakehouse/bronze/dlt/sources/celestrak/_common.py`:

```python
# src/auspex_lakehouse/bronze/dlt/sources/celestrak/_common.py
import io

import polars as pl
import requests

SW_ALL_URL = "https://celestrak.org/SpaceData/SW-All.csv"


def fetch_csv_rows(url: str) -> list[dict]:
    """GET a CelesTrak SpaceData CSV and return typed row dicts.

    `infer_schema_length=None` scans the WHOLE file before typing — required here:
    the F10.7 81-day-average columns are blank for the first ~80 rows (Oct 1957,
    before a full window exists), and prediction rows carry blanks too. Under the
    default 100-row inference those all-null-early columns mis-type (or raise) when
    real values appear later. Full-file inference is cheap (~25k rows). Blank fields
    map to null; numeric columns type as numeric; DATE and F10.7_DATA_TYPE stay
    strings (bronze keeps DATE raw — silver casts to a date).
    """
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    df = pl.read_csv(io.BytesIO(resp.content), infer_schema_length=None)
    return df.to_dicts()
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `uv run pytest tests/test_celestrak_common.py -v`
Expected: PASS (4 tests).

- [ ] **Step 8: Lint**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/sources/celestrak/ tests/test_celestrak_common.py`
Expected: `All checks passed!`

- [ ] **Step 9: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/celestrak/ tests/test_celestrak_common.py
git commit -m "feat(celestrak): CSV fetch/parse helper + pool config"
```

---

### Task 2: CSV-snapshot resource factory + registry

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py`
- Test: `tests/test_celestrak_sources.py`

**Interfaces:**
- Consumes: `fetch_csv_rows`, `SW_ALL_URL` (Task 1).
- Produces: `_csv_snapshot_resource(name, url, primary_key, min_rows) -> dlt resource` (merge, delta, no-arg `_resource()`); `CELESTRAK_DATASETS: list[tuple]` with one entry `("celestrak_space_weather", SW_ALL_URL, "DATE", 20000)`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_celestrak_sources.py`:

```python
# tests/test_celestrak_sources.py
import pytest

import auspex_lakehouse.bronze.dlt.sources.celestrak.snapshot as snap


def test_csv_snapshot_resource_yields_each_row(monkeypatch):
    monkeypatch.setattr(snap, "fetch_csv_rows",
                        lambda url: [{"DATE": "2026-06-28"}, {"DATE": "2026-06-29"}])
    res = snap._csv_snapshot_resource("celestrak_space_weather", "http://x", "DATE", 0)
    rows = list(res())
    assert len(rows) == 2
    assert res.name == "celestrak_space_weather"


def test_csv_snapshot_resource_raises_below_floor(monkeypatch):
    monkeypatch.setattr(snap, "fetch_csv_rows", lambda url: [{"DATE": "2026-06-28"}])
    res = snap._csv_snapshot_resource("celestrak_space_weather", "http://x", "DATE", 10)
    # dlt wraps RuntimeError in ResourceExtractionError (subclass of Exception).
    with pytest.raises(Exception, match="suspected truncation"):
        list(res())


def test_celestrak_registry_shape():
    assert [e[0] for e in snap.CELESTRAK_DATASETS] == ["celestrak_space_weather"]
    name, url, pk, floor = snap.CELESTRAK_DATASETS[0]
    assert name == "celestrak_space_weather"
    assert url == snap.SW_ALL_URL
    assert pk == "DATE"
    assert floor and floor > 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_celestrak_sources.py -v`
Expected: FAIL — `ModuleNotFoundError` (no `snapshot.py`).

- [ ] **Step 3: Write `snapshot.py`**

Create `src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py`:

```python
# src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py
import dlt

from auspex_lakehouse.bronze.dlt.sources.celestrak._common import SW_ALL_URL, fetch_csv_rows


def _csv_snapshot_resource(name, url, primary_key, min_rows):
    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource():
        rows = fetch_csv_rows(url)
        if min_rows and len(rows) < min_rows:
            # Suspected truncated/short download — fail loudly rather than write a
            # gap-riddled drag-driver table. Floor is conservative (file only grows).
            raise RuntimeError(
                f"{name}: {len(rows)} rows < floor {min_rows}; suspected truncation"
            )
        yield from rows

    return _resource


CELESTRAK_DATASETS = [
    # (name, url, primary_key, min_rows)   name == physical bronze table name
    ("celestrak_space_weather", SW_ALL_URL, "DATE", 20000),
    # EOP slots in here later:
    # ("celestrak_earth_orientation_parameters", EOP_ALL_URL, "DATE", 15000),
]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_celestrak_sources.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/celestrak/snapshot.py tests/test_celestrak_sources.py
git commit -m "feat(celestrak): CSV-snapshot resource factory + registry"
```

---

### Task 3: Source factory, pipeline, and package re-exports

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py` (replace the empty stub)
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`
- Test: `tests/test_celestrak_sources.py` (append)

**Interfaces:**
- Consumes: `CELESTRAK_DATASETS`, `_csv_snapshot_resource` (Task 2).
- Produces: `celestrak_source(name) -> dlt source` (one resource named `name`); `celestrak_pipelines: dict[str, dlt.Pipeline]` keyed by table name, each `pipeline_name == name`, `dataset_name == "bronze"`. Both re-exported from `auspex_lakehouse.bronze.dlt.sources`.

- [ ] **Step 1: Write the failing test (append to `tests/test_celestrak_sources.py`)**

```python
def test_celestrak_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import celestrak_source
    src = celestrak_source("celestrak_space_weather")
    assert set(src.resources.keys()) == {"celestrak_space_weather"}


def test_celestrak_source_build_makes_no_http(monkeypatch):
    # Building a source must not fetch — the resource is lazy. Patch requests in
    # _common to explode if called; constructing the source must still succeed.
    import auspex_lakehouse.bronze.dlt.sources.celestrak._common as c
    from auspex_lakehouse.bronze.dlt.sources import celestrak_source

    def _boom(*a, **k):
        raise AssertionError("HTTP at build time")

    monkeypatch.setattr(c, "requests", type("R", (), {"get": staticmethod(_boom)}))
    celestrak_source("celestrak_space_weather")  # no iteration -> no fetch


def test_celestrak_pipelines_dict():
    from auspex_lakehouse.bronze.dlt.sources import celestrak_pipelines
    assert set(celestrak_pipelines) == {"celestrak_space_weather"}
    p = celestrak_pipelines["celestrak_space_weather"]
    assert p.pipeline_name == "celestrak_space_weather"  # name already carries celestrak_ prefix
    assert p.dataset_name == "bronze"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_celestrak_sources.py -v -k "source or pipelines"`
Expected: FAIL — `ImportError` (`celestrak_source` not exported).

- [ ] **Step 3: Write the celestrak package `__init__.py`**

Replace `src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py`:

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.celestrak.snapshot import (
    CELESTRAK_DATASETS,
    _csv_snapshot_resource,
)

DATASETS_BY_NAME = {e[0]: e for e in CELESTRAK_DATASETS}


@dlt.source
def celestrak_source(name):
    n, url, pk, floor = DATASETS_BY_NAME[name]
    return [_csv_snapshot_resource(n, url, pk, floor)()]


def _pipeline(name):
    return dlt.pipeline(
        pipeline_name=name,        # name already carries the celestrak_ prefix — no doubling
        destination="filesystem",
        dataset_name="bronze",     # tables land at bronze/<name> = bronze/celestrak_space_weather
    )


celestrak_pipelines = {name: _pipeline(name) for name in DATASETS_BY_NAME}

__all__ = ["celestrak_source", "celestrak_pipelines", "CELESTRAK_DATASETS"]
```

- [ ] **Step 4: Add the re-exports to `sources/__init__.py`**

In `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`, add an import block (after the spacetrack import) and extend `__all__`:

```python
from auspex_lakehouse.bronze.dlt.sources.celestrak import (
    celestrak_pipelines,
    celestrak_source,
)
```

Add to the `__all__` list:

```python
    "celestrak_source",
    "celestrak_pipelines",
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_celestrak_sources.py -v`
Expected: PASS (all source/pipeline tests + the Task 2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/celestrak/__init__.py src/auspex_lakehouse/bronze/dlt/sources/__init__.py tests/test_celestrak_sources.py
git commit -m "feat(celestrak): source factory, pipeline, package re-exports"
```

---

### Task 4: Dagster asset wiring

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py`
- Test: `tests/test_celestrak_assets.py`

**Interfaces:**
- Consumes: `celestrak_source`, `celestrak_pipelines` (Task 3), `CELESTRAK_API_POOL` (Task 1).
- Produces: module-level `celestrak_space_weather_assets` (`AssetsDefinition`), key `dlt_celestrak_space_weather`, group `celestrak`, unpartitioned, `op.pool == "celestrak_api"`, cron `30 5 * * *`. Discovered by `load_assets_from_package_module(bronze)`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_celestrak_assets.py`:

```python
from dagster import AssetKey, AssetsDefinition


def _load():
    import auspex_lakehouse.bronze.dlt.assets as a
    return a


def test_celestrak_asset_key():
    a = _load()
    assert a.celestrak_space_weather_assets.keys == {AssetKey("dlt_celestrak_space_weather")}


def test_celestrak_asset_exists_and_is_assets_def():
    a = _load()
    assert isinstance(a.celestrak_space_weather_assets, AssetsDefinition)


def test_celestrak_asset_unpartitioned():
    a = _load()
    assert a.celestrak_space_weather_assets.partitions_def is None


def test_celestrak_asset_uses_the_pool():
    a = _load()
    assert a.celestrak_space_weather_assets.op.pool == "celestrak_api"


def test_celestrak_asset_in_group():
    a = _load()
    for spec in a.celestrak_space_weather_assets.specs:
        assert spec.group_name == "celestrak"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_celestrak_assets.py -v`
Expected: FAIL — `AttributeError` (`celestrak_space_weather_assets` not defined).

- [ ] **Step 3: Add imports to `assets.py`**

In `src/auspex_lakehouse/bronze/dlt/assets.py`, extend the existing `from auspex_lakehouse.bronze.dlt.sources import (...)` block to include:

```python
    celestrak_pipelines,
    celestrak_source,
```

And add a new import near the other config imports:

```python
from auspex_lakehouse.bronze.dlt.sources.celestrak.config import CELESTRAK_API_POOL
```

- [ ] **Step 4: Add the translator + asset (append to `assets.py`)**

```python
# ---- CelesTrak: public CSV space-weather file; one snapshot-merge asset ----


class CelesTrakDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_{data.resource.name}"),  # resource name already = celestrak_space_weather
            automation_condition=AutomationCondition.on_cron("30 5 * * *"),
        )


@dlt_assets(
    dlt_source=celestrak_source("celestrak_space_weather"),
    dlt_pipeline=celestrak_pipelines["celestrak_space_weather"],
    name="celestrak_space_weather_bronze",
    group_name="celestrak",
    # NO partitions_def — whole-file current-state snapshot
    dagster_dlt_translator=CelesTrakDltTranslator(),
    pool=CELESTRAK_API_POOL,
)
def celestrak_space_weather_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    yield from dlt.run(
        context=context, dlt_source=celestrak_source("celestrak_space_weather")
    )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_celestrak_assets.py -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Confirm the code location still loads (definitions smoke)**

Run: `uv run pytest tests/test_definitions.py::test_definitions_load -v`
Expected: PASS — the new dlt asset loads; no dbt source yet, so no lineage edge, which is fine.

- [ ] **Step 7: Lint**

Run: `uv run ruff check src/auspex_lakehouse/bronze/dlt/assets.py tests/test_celestrak_assets.py`
Expected: `All checks passed!`

- [ ] **Step 8: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/assets.py tests/test_celestrak_assets.py
git commit -m "feat(celestrak): Dagster snapshot-merge asset (dlt_celestrak_space_weather)"
```

---

### Task 5: dbt model, source, model tests, and lineage bridge

**Files:**
- Create: `dbt/models/bronze/bronze_celestrak_space_weather.sql`
- Create: `dbt/models/bronze/_bronze__celestrak__models.yml`
- Modify: `dbt/models/bronze/_bronze__sources.yml`
- Modify: `src/auspex_lakehouse/transform/definitions.py`
- Modify: `tests/test_dbt_bronze.py`, `tests/test_definitions.py`

**Interfaces:**
- Consumes: the dlt asset key `dlt_celestrak_space_weather` (Task 4).
- Produces: dbt model `bronze_celestrak_space_weather` (asset key `["bronze_celestrak_space_weather"]`) with a lineage edge to `dlt_celestrak_space_weather`.

- [ ] **Step 1: Add the source table to `_bronze__sources.yml`**

Under `tables:` in `dbt/models/bronze/_bronze__sources.yml`, add:

```yaml
      - name: celestrak_space_weather
        description: "CelesTrak consolidated daily space weather — one row per date (F10.7 solar flux observed/adjusted + 81-day averages, 3-hourly & daily Ap/Kp, sunspot number; F10.7_DATA_TYPE flags OBS/INT/PRD/PRM). Drag-driver inputs for orbit propagation (celestrak.org/SpaceData/SW-All.csv)."
        meta: {dagster: {asset_key: ["dlt_celestrak_space_weather"]}}
```

- [ ] **Step 2: Create the bronze view**

Create `dbt/models/bronze/bronze_celestrak_space_weather.sql`:

```sql
{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'celestrak_space_weather') }}
```

- [ ] **Step 3: Create the model docs/tests yml**

Create `dbt/models/bronze/_bronze__celestrak__models.yml`. Column is the **normalized** lowercase `date`:

```yaml
version: 2
models:
  - name: bronze_celestrak_space_weather
    description: "CelesTrak daily space weather (drag drivers: F10.7 flux + Ap/Kp). One row per date; column names snake_cased by dlt."
    columns:
      - name: date
        description: "Calendar date (YYYY-MM-DD, raw string from the source)."
        data_tests: [not_null, unique]
```

- [ ] **Step 4: Add the lineage-bridge entry**

In `src/auspex_lakehouse/transform/definitions.py`, add to the `_SOURCE_ASSET_KEYS` dict (after the space-track entries):

```python
    "celestrak_space_weather": AssetKey(["dlt_celestrak_space_weather"]),
```

- [ ] **Step 5: Extend `tests/test_dbt_bronze.py`**

In `test_20_bronze_assets_with_lineage`, add `"celestrak_space_weather"` to the `expected` set comprehension's table list (so it expects `bronze_celestrak_space_weather`), and add one lineage assertion alongside the existing samples:

```python
    assert (
        AssetKey(["dlt_celestrak_space_weather"])
        in ag.get(AssetKey(["bronze_celestrak_space_weather"])).parent_keys
    )
```

- [ ] **Step 6: Add a definitions assertion in `tests/test_definitions.py`**

Append:

```python
def test_definitions_include_celestrak_asset():
    from auspex_lakehouse.definitions import defs
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.asset_keys_for_group("celestrak")}
    assert "dlt_celestrak_space_weather" in keys, f"got {keys}"
```

- [ ] **Step 7: Regenerate the dbt manifest and run the affected tests**

The `conftest.py` session fixture runs `dbt parse`, but regenerate manually first to surface any dbt error directly:

Run:
```bash
cd dbt && DBT_PROFILES_DIR="$PWD" uv run dbt parse && cd ..
```
Expected: `... Wrote ... manifest ...` with no errors (the new source + model + yml parse).

Run: `uv run pytest tests/test_dbt_bronze.py tests/test_definitions.py -v`
Expected: PASS — `bronze_celestrak_space_weather` present; lineage edge to `dlt_celestrak_space_weather` confirmed; celestrak group assertion passes.

- [ ] **Step 8: Run the full suite + lint**

Run: `uv run pytest -q`
Expected: PASS (all prior tests + the new celestrak tests).

Run: `uv run ruff check src/ tests/`
Expected: `All checks passed!`

- [ ] **Step 9: Commit**

```bash
git add dbt/models/bronze/bronze_celestrak_space_weather.sql dbt/models/bronze/_bronze__celestrak__models.yml dbt/models/bronze/_bronze__sources.yml src/auspex_lakehouse/transform/definitions.py tests/test_dbt_bronze.py tests/test_definitions.py
git commit -m "feat(celestrak): bronze dbt model + source + lineage bridge"
```

---

### Task 6: Live end-to-end verification (manual, optional gate)

**Files:** none (operational check against MinIO + live CelesTrak).

**Interfaces:** consumes the full pipeline from Tasks 1–5.

> Requires MinIO env (`MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`/`BRONZE_BUCKET_URI`) and outbound network to celestrak.org. Skip in pure-unit CI; run once locally to confirm the table actually lands.

- [ ] **Step 1: Materialize the asset**

In `dg dev` (or via the Dagster UI), materialize `dlt_celestrak_space_weather`. Expected: run succeeds; >20 000 rows (no floor error).

- [ ] **Step 2: Confirm the Delta table is readable and columns are normalized**

Run:
```bash
uv run python -c "
from auspex_lakehouse.resources.delta import read_bronze_table
df = read_bronze_table('celestrak_space_weather')
print(df.shape)
print([c for c in df.columns if c in ('date','f10_7_obs','f10_7_data_type','kp_sum','ap_avg')])
print(df.select(['date','f10_7_obs','f10_7_data_type']).head(3))
"
```
Expected: row count > 20 000; the snake_cased columns (`date`, `f10_7_obs`, `f10_7_data_type`, `kp_sum`, `ap_avg`) are present; `date` values look like `YYYY-MM-DD`.

- [ ] **Step 3: Confirm idempotency**

Re-materialize the asset. Expected: row count is stable (merge-on-`date` upserts, no duplication).

- [ ] **Step 4: Build the dbt model against live data**

Run:
```bash
cd dbt && DBT_PROFILES_DIR="$PWD" uv run dbt build --select bronze_celestrak_space_weather && cd ..
```
Expected: the view builds and the `not_null`/`unique` test on `date` passes. (If DuckDB rejects the bare `date` identifier as a keyword, quote it in the model yml per the spec note and re-run.)

---

## Post-implementation

After Task 5 (all unit tests green), the work is mergeable. Task 6 is a live smoke check best run before relying on the data. Use `superpowers:finishing-a-development-branch` to open the PR for `feat/celestrak`.
