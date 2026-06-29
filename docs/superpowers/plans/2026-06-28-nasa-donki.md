# NASA DONKI Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest all 11 NASA DONKI space-weather endpoints as bronze Delta tables via a single resource factory + config registry, grouped as a dedicated `donki` dlt assets group on its own `nasa_donki` pipeline, serialized under the `nasa_api` concurrency pool.

**Architecture:** A `_donki_resource(name, path, primary_key, extra_params)` factory builds a `merge`-on-ID dlt resource per endpoint; a `DONKI_ENDPOINTS` registry lists the 11; `donki_source` assembles them; a `@dlt_assets` group (`pool="nasa_api"`, daily-partitioned, `on_cron` 07:00) runs them on the dedicated `nasa_donki_pipeline`. Builds on the `nasa/` package and `nasa_api` pool merged in PR #2.

**Tech Stack:** Python 3.10+, Dagster 1.13.11, dagster-dlt, dlt[deltalake,s3] ≥1.28.1, Polars, pytest, ruff, uv.

## Global Constraints

- Python target **3.10**; subscripted builtins (`list[str]`) fine.
- Lint must pass: `uv run ruff check .` (rules `E`, `F`, `I`; imports sorted stdlib / third-party / first-party; line-length **100**).
- Tests: `uv run pytest -q` from repo root. If `uv run` attempts a dependency sync that fails, the venv is already populated — retry with `uv run --no-sync ...`.
- **No import-time HTTP or Delta/S3 access** — network only inside resource generators; `nasa_api_key()` is called inside the generator body, not at construction; `test_definitions.py` must keep loading without MinIO.
- **Public-name stability:** `nasa_api`, `nasa_pipeline`, `neo_lookup_rows`, `nasa_neo_lookup_pipeline` must remain importable from `auspex_lakehouse.bronze.dlt.sources`; this plan **adds** `donki_source` and `nasa_donki_pipeline` to that surface.
- API key via `nasa_api_key()` (from `nasa._common`), which reads `dlt.secrets["nasa_api_key"]`.
- Every DONKI endpoint uses `write_disposition="merge"`, `table_format="delta"`, keyed on its natural ID. Endpoint registry (verbatim — name, path, primary_key, extra_params):
  - `("cme", "CME", "activityID", None)`
  - `("cme_analysis", "CMEAnalysis", ["associatedCMEID", "time21_5"], None)`
  - `("gst", "GST", "gstID", None)`
  - `("ips", "IPS", "activityID", None)`
  - `("flr", "FLR", "flrID", None)`
  - `("sep", "SEP", "sepID", None)`
  - `("mpc", "MPC", "mpcID", None)`
  - `("rbe", "RBE", "rbeID", None)`
  - `("hss", "HSS", "hssID", None)`
  - `("wsa_enlil_simulations", "WSAEnlilSimulations", "simulationID", None)`
  - `("notifications", "notifications", "messageID", {"type": "all"})`
- Append this commit trailer (own line, after a blank line): `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

**Created:**
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/donki.py` — factory, registry, `donki_source`, `nasa_donki_pipeline`
- `tests/test_donki.py` — factory/registry/source/export unit tests
- `tests/test_donki_asset.py` — assets-group wiring tests

**Modified:**
- `src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py` — export `donki_source`, `nasa_donki_pipeline`
- `src/auspex_lakehouse/bronze/dlt/sources/__init__.py` — re-export the two new names
- `src/auspex_lakehouse/bronze/dlt/assets.py` — `DonkiDltTranslator` + `donki_assets`

---

## Task 1: DONKI factory, registry, source, pipeline + exports

**Files:**
- Create: `src/auspex_lakehouse/bronze/dlt/sources/nasa/donki.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/__init__.py`
- Test: `tests/test_donki.py`

**Interfaces:**
- Consumes: `BASE_URL`, `iter_days`, `nasa_api_key` from `auspex_lakehouse.bronze.dlt.sources.nasa._common`.
- Produces: `_donki_resource(name, endpoint_path, primary_key, extra_params=None) -> DltResource`; `DONKI_ENDPOINTS: list[tuple]`; `donki_source(start_date, end_date) -> DltSource`; `nasa_donki_pipeline` (pipeline_name `nasa_donki`, dataset `bronze`) — the last two importable from `auspex_lakehouse.bronze.dlt.sources`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_donki.py`:

```python
from datetime import date
from unittest.mock import Mock

import auspex_lakehouse.bronze.dlt.sources.nasa.donki as donki
from auspex_lakehouse.bronze.dlt.sources.nasa.donki import (
    DONKI_ENDPOINTS,
    _donki_resource,
    donki_source,
    nasa_donki_pipeline,
)


def _resp(payload):
    r = Mock()
    r.json.return_value = payload
    r.raise_for_status = Mock()
    return r


def test_resource_yields_list_items(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    monkeypatch.setattr(donki, "requests", Mock(get=Mock(return_value=_resp([{"activityID": "a"}, {"activityID": "b"}]))))
    res = _donki_resource("cme", "CME", "activityID")
    assert list(res(date(2024, 5, 1), date(2024, 5, 1))) == [{"activityID": "a"}, {"activityID": "b"}]


def test_resource_tolerates_non_list_body(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    monkeypatch.setattr(donki, "requests", Mock(get=Mock(return_value=_resp({"error": "no data"}))))
    res = _donki_resource("gst", "GST", "gstID")
    assert list(res(date(2024, 5, 1), date(2024, 5, 1))) == []


def test_resource_builds_request_with_extra_params(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    captured = {}

    def fake_get(url, params=None):
        captured["url"], captured["params"] = url, params
        return _resp([])

    monkeypatch.setattr(donki, "requests", Mock(get=fake_get))
    res = _donki_resource("notifications", "notifications", "messageID", {"type": "all"})
    list(res(date(2024, 5, 1), date(2024, 5, 1)))
    assert captured["url"].endswith("/DONKI/notifications")
    assert captured["params"] == {
        "api_key": "key", "startDate": "2024-05-01", "endDate": "2024-05-01", "type": "all",
    }


def test_resource_metadata_composite_key():
    res = _donki_resource("cme_analysis", "CMEAnalysis", ["associatedCMEID", "time21_5"])
    assert res.name == "cme_analysis"
    assert res.write_disposition == "merge"
    ts = res.compute_table_schema()
    assert ts["table_format"] == "delta"
    pk_cols = {c for c, v in ts["columns"].items() if v.get("primary_key")}
    assert pk_cols == {"associatedCMEID", "time21_5"}


def test_registry_has_11_unique_endpoints():
    assert len(DONKI_ENDPOINTS) == 11
    names = [name for (name, _p, _k, _e) in DONKI_ENDPOINTS]
    assert len(set(names)) == 11
    assert all(pk for (_n, _p, pk, _e) in DONKI_ENDPOINTS)
    notif = next(e for e in DONKI_ENDPOINTS if e[0] == "notifications")
    assert notif[1] == "notifications" and notif[2] == "messageID" and notif[3] == {"type": "all"}


def test_source_exposes_all_11_resources():
    src = donki_source(start_date=date(2024, 5, 1), end_date=date(2024, 5, 1))
    assert set(src.resources.keys()) == {
        "cme", "cme_analysis", "gst", "ips", "flr", "sep",
        "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
    }


def test_exports_from_sources_package():
    from auspex_lakehouse.bronze.dlt.sources import donki_source as ds, nasa_donki_pipeline as p
    assert p.pipeline_name == "nasa_donki"
    assert callable(ds)


def test_pipeline_name():
    assert nasa_donki_pipeline.pipeline_name == "nasa_donki"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest -q tests/test_donki.py`
Expected: FAIL with `ModuleNotFoundError: ...nasa.donki` (module not created yet).

- [ ] **Step 3: Create `nasa/donki.py`**

```python
from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


def _donki_resource(name, endpoint_path, primary_key, extra_params=None):
    """Build a merge-on-ID dlt resource for one DONKI endpoint. Bulk list query
    per partition-day; tolerates empty/non-list bodies without writing junk."""

    @dlt.resource(name=name, write_disposition="merge", primary_key=primary_key, table_format="delta")
    def _resource(start_date: date, end_date: date):
        api_key = nasa_api_key()
        for day in iter_days(start_date, end_date):
            params = {
                "api_key": api_key,
                "startDate": day.isoformat(),
                "endDate": day.isoformat(),
                **(extra_params or {}),
            }
            resp = requests.get(f"{BASE_URL}/DONKI/{endpoint_path}", params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                yield from data

    return _resource


# (resource_name, endpoint_path, primary_key, extra_params)
DONKI_ENDPOINTS = [
    ("cme",                   "CME",                 "activityID",                     None),
    ("cme_analysis",          "CMEAnalysis",         ["associatedCMEID", "time21_5"],  None),
    ("gst",                   "GST",                 "gstID",                          None),
    ("ips",                   "IPS",                 "activityID",                     None),
    ("flr",                   "FLR",                 "flrID",                          None),
    ("sep",                   "SEP",                 "sepID",                          None),
    ("mpc",                   "MPC",                 "mpcID",                          None),
    ("rbe",                   "RBE",                 "rbeID",                          None),
    ("hss",                   "HSS",                 "hssID",                          None),
    ("wsa_enlil_simulations", "WSAEnlilSimulations", "simulationID",                   None),
    ("notifications",         "notifications",       "messageID",                      {"type": "all"}),
]


@dlt.source
def donki_source(start_date: date, end_date: date):
    return [
        _donki_resource(name, path, pk, extra)(start_date, end_date)
        for (name, path, pk, extra) in DONKI_ENDPOINTS
    ]


nasa_donki_pipeline = dlt.pipeline(
    pipeline_name="nasa_donki",   # distinct working dir → no collision with nasa_api / nasa_neo_lookup
    destination="filesystem",
    dataset_name="bronze",        # tables land at bronze/<resource_name>
)
```

- [ ] **Step 4: Wire exports through `nasa/__init__.py`**

Add the import (ruff-I sorted among the existing `nasa.*` imports) and extend `__all__`:

```python
from auspex_lakehouse.bronze.dlt.sources.nasa.donki import donki_source, nasa_donki_pipeline
```

Append `"donki_source"` and `"nasa_donki_pipeline"` to the module's `__all__` list.

- [ ] **Step 5: Re-export from `sources/__init__.py`**

```python
from auspex_lakehouse.bronze.dlt.sources.nasa import (
    donki_source,
    nasa_api,
    nasa_neo_lookup_pipeline,
    nasa_pipeline,
    nasa_donki_pipeline,
    neo_lookup_rows,
)

__all__ = [
    "nasa_api",
    "nasa_pipeline",
    "neo_lookup_rows",
    "nasa_neo_lookup_pipeline",
    "donki_source",
    "nasa_donki_pipeline",
]
```

(Run `uv run ruff check .` — it will canonicalize the import order; accept its sort.)

- [ ] **Step 6: Run lint + tests**

Run: `uv run ruff check . && uv run pytest -q tests/test_donki.py tests/test_definitions.py`
Expected: PASS (8 donki tests + the smoke test; no import-time HTTP — `nasa_api_key` is only called when a resource is iterated).

- [ ] **Step 7: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/sources/nasa/donki.py \
        src/auspex_lakehouse/bronze/dlt/sources/nasa/__init__.py \
        src/auspex_lakehouse/bronze/dlt/sources/__init__.py \
        tests/test_donki.py
git commit -m "feat(bronze): add DONKI source (11-endpoint factory + registry)"
```

---

## Task 2: DONKI assets group (pooled, scheduled)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py`
- Test: `tests/test_donki_asset.py`

**Interfaces:**
- Consumes: `donki_source`, `nasa_donki_pipeline` from `auspex_lakehouse.bronze.dlt.sources`; existing `DagsterDltTranslator`, `DltResourceTranslatorData`, `DagsterDltResource`, `dlt_assets`, `AssetExecutionContext`, `AutomationCondition`, `daily_partitions`, `date` (all already imported in `assets.py`).
- Produces: a `donki_assets` `@dlt_assets` group yielding 11 asset keys `dlt_nasa_donki_<resource_name>` in group `donki`, partitioned by `daily_partitions`, on `op.pool == "nasa_api"`, scheduled `on_cron("0 7 * * *")`.

- [ ] **Step 1: Write the failing wiring tests**

Create `tests/test_donki_asset.py`:

```python
from dagster import AssetKey, AssetsDefinition

DONKI_KEYS = [
    "dlt_nasa_donki_cme",
    "dlt_nasa_donki_cme_analysis",
    "dlt_nasa_donki_gst",
    "dlt_nasa_donki_ips",
    "dlt_nasa_donki_flr",
    "dlt_nasa_donki_sep",
    "dlt_nasa_donki_mpc",
    "dlt_nasa_donki_rbe",
    "dlt_nasa_donki_hss",
    "dlt_nasa_donki_wsa_enlil_simulations",
    "dlt_nasa_donki_notifications",
]


def test_all_11_donki_assets_present():
    from auspex_lakehouse.definitions import defs

    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    missing = [k for k in DONKI_KEYS if k not in keys]
    assert not missing, f"missing DONKI asset keys: {missing}"


def test_donki_assets_pooled_and_grouped():
    from auspex_lakehouse.definitions import defs

    cme = AssetKey(["dlt_nasa_donki_cme"])
    ad = next(a for a in defs.assets if isinstance(a, AssetsDefinition) and cme in a.keys)
    assert ad.op.pool == "nasa_api"
    assert ad.group_names_by_key[cme] == "donki"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest -q tests/test_donki_asset.py`
Expected: FAIL — `dlt_nasa_donki_*` keys absent from the graph (asset not defined yet).

- [ ] **Step 3: Add imports to `assets.py`**

Extend the existing `from auspex_lakehouse.bronze.dlt.sources import (...)` block to also import `donki_source` and `nasa_donki_pipeline` (ruff-I will order them):

```python
from auspex_lakehouse.bronze.dlt.sources import (
    donki_source,
    nasa_api,
    nasa_neo_lookup_pipeline,
    nasa_pipeline,
    nasa_donki_pipeline,
    neo_lookup_rows,
)
```

All other names used below (`DagsterDltTranslator`, `DltResourceTranslatorData`, `dlt_assets`, `DagsterDltResource`, `AssetExecutionContext`, `AutomationCondition`, `daily_partitions`, `date`) are already imported in `assets.py` — do not re-add them.

- [ ] **Step 4: Add the translator and the assets group**

Append to `assets.py`:

```python
class DonkiDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            automation_condition=AutomationCondition.on_cron("0 7 * * *"),
        )


@dlt_assets(
    dlt_source=donki_source(start_date=date.today(), end_date=date.today()),
    dlt_pipeline=nasa_donki_pipeline,
    name="nasa_donki_bronze",
    group_name="donki",
    partitions_def=daily_partitions,
    dagster_dlt_translator=DonkiDltTranslator(),
    pool="nasa_api",  # serialize DONKI runs against neo_lookup on the shared NASA budget
)
def donki_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    rng = context.partition_key_range
    source = donki_source(
        start_date=date.fromisoformat(rng.start),
        end_date=date.fromisoformat(rng.end),
    )
    yield from dlt.run(context=context, dlt_source=source)
```

- [ ] **Step 5: Run lint + the full suite**

Run: `uv run ruff check . && uv run pytest -q`
Expected: PASS (the 2 wiring tests + all prior tests + smoke test). If `test_all_11_donki_assets_present` fails, print the actual keys with
`uv run --no-sync python -c "from auspex_lakehouse.definitions import defs; print(sorted(k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()))"`
and reconcile the resource names → key names (`dlt_<pipeline_name>_<resource_name>`).

- [ ] **Step 6: Commit**

```bash
git add src/auspex_lakehouse/bronze/dlt/assets.py tests/test_donki_asset.py
git commit -m "feat(bronze): add DONKI dlt_assets group (pooled, on_cron, daily)"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Factory + registry (Component 1) → Task 1 (Steps 3). ✅
- Source + dedicated pipeline (Component 2) → Task 1 (Step 3) + exports (Steps 4–5). ✅
- Assets group + pool + scheduling (Component 3) → Task 2. ✅
- Endpoint registry (all 11 keys) → Global Constraints + Task 1 Step 3, asserted in `test_registry_has_11_unique_endpoints`. ✅
- Error handling (empty/non-list/429) → factory `isinstance(data, list)` guard + `raise_for_status`; `test_resource_tolerates_non_list_body` covers the non-list path. The 429/all-or-nothing behavior is inherent to dlt extract and not separately unit-testable (integration). ✅
- CMEAnalysis composite key → registry + `test_resource_metadata_composite_key`. ✅

**Type/name consistency:** `_donki_resource` signature, `DONKI_ENDPOINTS` tuple shape, `donki_source`/`nasa_donki_pipeline` names, and the `dlt_nasa_donki_<name>` asset keys are consistent across Tasks 1–2 and the tests. Verified empirically before authoring: `dlt_assets` accepts `pool=` and surfaces `op.pool`; `defs.resolve_asset_graph()` (not `get_asset_graph()`) is the correct API in Dagster 1.13.11; a factory-built resource iterates via `list(res(...))` and exposes `name`/`write_disposition`/`compute_table_schema()`.

**Placeholders:** none.

**Known confirm-at-implementation items (not blockers):**
- `ad.group_names_by_key[key]` reflects the AssetsDefinition group API; if it differs, read the group via `defs.resolve_asset_graph().get(key).group_name`.
- The asset-body run path (`dlt.run` over the live source) is integration-verified only (needs MinIO/NASA), like the existing `nasa_api_assets`.
