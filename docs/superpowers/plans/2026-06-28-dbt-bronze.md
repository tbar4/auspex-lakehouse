# dbt Bronze Staging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold a dbt-duckdb project inside the Dagster code location and build a bronze staging layer — 14 `view` models (one per top-level dlt Delta table) that drop dlt system columns, exposed as Dagster assets with lineage from the dlt bronze assets.

**Architecture:** dbt-duckdb; DuckDB reads Delta from MinIO via `delta_scan` (sources use `external_location`). Bronze models are `select * exclude (_dlt_id, _dlt_load_id)` views. `dagster-dbt` (`DbtProject` + `@dbt_assets` + a custom translator) wires the dbt sources to the existing dlt asset keys.

**Tech Stack:** dbt-core 1.11.11, dbt-duckdb 1.10.1, duckdb 1.5.4, dagster 1.13.11, dagster-dbt, dbt_utils; uv, pytest, ruff.

## Global Constraints

- Worktree: `/Users/tbarnes/projects/python/auspex-lakehouse-dbt` (branch `feat/dbt-bronze`, off `origin/main` @ `47910cb`). Run from there; use `uv run --no-sync ...`.
- **MinIO creds for Tasks 1–2** (dbt hitting real data): before dbt commands that read MinIO, load env: `set -a; source /Users/tbarnes/projects/python/auspex-lakehouse/.env; set +a`. (Task 3's `dbt parse` + Dagster graph load do NOT need MinIO.)
- Lint: `uv run --no-sync ruff check .` (E, F, I; line-length 100) — applies to Python only.
- Python tests: `uv run --no-sync pytest -q`.
- Materialization is **`view`** for all bronze models. Models are `select * exclude (_dlt_id, _dlt_load_id)` (dlt system columns are exactly `_dlt_id`, `_dlt_load_id` — spike-confirmed).
- **Scope:** build all 14 models + wiring; only `apod`/`neows` have data on MinIO and are validated end-to-end. The other 12 parse/compile but their sources don't exist yet (their dlt assets haven't run) — accepted (**fail-until-data**: a bronze view errors until its source has ≥1 row).
- **Proven config (verified against real MinIO during planning):** dbt-duckdb profile `secrets:` block with `type: s3`, `endpoint` derived from `MINIO_ENDPOINT` (host only), `url_style: path`, `use_ssl` from the scheme; source `external_location: "delta_scan('s3://auspex-lakehouse/bronze/{name}')"`.
- The 14 tables and their dlt-normalized primary keys (deterministic; apod/neows confirmed live):
  `apod`=`date`; `neows`=`date,id`; `neo_lookup`=`neo_reference_id`; `cme`=`activity_id`; `cme_analysis`=`associated_cmeid,time21_5`; `gst`=`gst_id`; `ips`=`activity_id`; `flr`=`flr_id`; `sep`=`sep_id`; `mpc`=`mpc_id`; `rbe`=`rbe_id`; `hss`=`hss_id`; `wsa_enlil_simulations`=`simulation_id`; `notifications`=`message_id`.
- The source → dlt asset-key mapping (non-uniform): `apod→dlt_nasa_api_apod`, `neows→dlt_nasa_api_neows`, `neo_lookup→neo_lookup`, and every DONKI table `<t>→dlt_nasa_donki_<t>`.
- Append this commit trailer (own line, after a blank line): `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

**Created:**
- `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/packages.yml`
- `dbt/models/bronze/_bronze__sources.yml`, `dbt/models/bronze/_bronze__models.yml`
- `dbt/models/bronze/bronze_<table>.sql` × 14
- `src/auspex_lakehouse/transform/__init__.py`, `src/auspex_lakehouse/transform/definitions.py` (DbtProject, translator, @dbt_assets)
- `tests/test_dbt_bronze.py` (Dagster wiring smoke test)

**Modified:**
- `pyproject.toml` (add `dbt-duckdb`, `duckdb`)
- `src/auspex_lakehouse/definitions.py` (add dbt assets + DbtCliResource)
- `Dockerfile_user_code` (COPY dbt/ + `dbt deps`/`dbt parse` at build)

---

## Task 1: dbt project scaffold + deps + sources (validated read)

**Files:** Create `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/packages.yml`, `dbt/models/bronze/_bronze__sources.yml`; Modify `pyproject.toml`.

**Interfaces:**
- Produces: a parseable dbt project named `auspex_lakehouse` whose `bronze` source group resolves the 14 dlt Delta tables via `external_location`, readable through dbt-duckdb over MinIO.

- [ ] **Step 1: Add deps**

In `pyproject.toml` `dependencies`, add `"dbt-duckdb>=1.10.1"` and `"duckdb>=1.5.4"`. Run `uv sync` (in the worktree; the polars-lts-cpu pin makes sync work — if it tries an unwanted resolution, `uv add dbt-duckdb duckdb` instead).

- [ ] **Step 2: Create `dbt/dbt_project.yml`**

```yaml
name: auspex_lakehouse
version: "1.0.0"
profile: auspex_lakehouse
model-paths: ["models"]

models:
  auspex_lakehouse:
    bronze:
      +materialized: view
```

- [ ] **Step 3: Create `dbt/profiles.yml`** (proven against MinIO)

```yaml
auspex_lakehouse:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH', '/tmp/auspex_dbt.duckdb') }}"
      extensions:
        - httpfs
        - delta
      secrets:
        - type: s3
          key_id: "{{ env_var('MINIO_ACCESS_KEY', 'unset') }}"
          secret: "{{ env_var('MINIO_SECRET_KEY', 'unset') }}"
          endpoint: "{{ env_var('MINIO_ENDPOINT', 'http://unset') | replace('https://','') | replace('http://','') | replace('/','') }}"
          url_style: path
          use_ssl: "{{ 'true' if 'https' in env_var('MINIO_ENDPOINT', 'http://unset') else 'false' }}"
```

**`env_var` defaults are required** (verified): `dbt parse` evaluates these and
fails on a missing var even though it never connects. Defaults let parse/manifest
generation run cred-free (Docker build, CI, tests); only actual reads
(`dbt build`/`show`) need real creds, and fail at connection if absent.

- [ ] **Step 4: Create `dbt/packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

- [ ] **Step 5: Create `dbt/models/bronze/_bronze__sources.yml`**

```yaml
version: 2
sources:
  - name: bronze
    schema: bronze
    meta:
      external_location: "delta_scan('s3://auspex-lakehouse/bronze/{name}')"
    tables:
      - name: apod
        meta: {dagster: {asset_key: ["dlt_nasa_api_apod"]}}
      - name: neows
        meta: {dagster: {asset_key: ["dlt_nasa_api_neows"]}}
      - name: neo_lookup
        meta: {dagster: {asset_key: ["neo_lookup"]}}
      - name: cme
        meta: {dagster: {asset_key: ["dlt_nasa_donki_cme"]}}
      - name: cme_analysis
        meta: {dagster: {asset_key: ["dlt_nasa_donki_cme_analysis"]}}
      - name: gst
        meta: {dagster: {asset_key: ["dlt_nasa_donki_gst"]}}
      - name: ips
        meta: {dagster: {asset_key: ["dlt_nasa_donki_ips"]}}
      - name: flr
        meta: {dagster: {asset_key: ["dlt_nasa_donki_flr"]}}
      - name: sep
        meta: {dagster: {asset_key: ["dlt_nasa_donki_sep"]}}
      - name: mpc
        meta: {dagster: {asset_key: ["dlt_nasa_donki_mpc"]}}
      - name: rbe
        meta: {dagster: {asset_key: ["dlt_nasa_donki_rbe"]}}
      - name: hss
        meta: {dagster: {asset_key: ["dlt_nasa_donki_hss"]}}
      - name: wsa_enlil_simulations
        meta: {dagster: {asset_key: ["dlt_nasa_donki_wsa_enlil_simulations"]}}
      - name: notifications
        meta: {dagster: {asset_key: ["dlt_nasa_donki_notifications"]}}
```

(The `meta.dagster.asset_key` is consumed by the Task 3 translator; harmless to dbt.)

- [ ] **Step 6: Install packages and validate the read against MinIO**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt/dbt
set -a; source /Users/tbarnes/projects/python/auspex-lakehouse/.env; set +a
DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt deps
DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt parse
DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt show --inline "select count(*) as n from {{ source('bronze','apod') }}" --limit 1
```
Expected: `dbt parse` succeeds (project + 14 sources found); `dbt show` prints `n = 179` (apod read end-to-end via the source `external_location`). This is the Task gate — the read path through dbt is proven.

- [ ] **Step 7: Commit**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt
printf 'dbt_packages/\ntarget/\nlogs/\n' >> dbt/.gitignore
git add pyproject.toml uv.lock dbt/dbt_project.yml dbt/profiles.yml dbt/packages.yml dbt/models/bronze/_bronze__sources.yml dbt/.gitignore
git commit -m "feat(dbt): scaffold dbt-duckdb project + 14 bronze sources over MinIO Delta"
```

---

## Task 2: 14 bronze view models + tests

**Files:** Create `dbt/models/bronze/bronze_<table>.sql` × 14, `dbt/models/bronze/_bronze__models.yml`.

**Interfaces:**
- Consumes: the `bronze` sources from Task 1.
- Produces: 14 `view` models named `bronze_<table>`, each `select * exclude (_dlt_id, _dlt_load_id) from {{ source('bronze','<table>') }}`, with `not_null`/`unique` PK tests.

- [ ] **Step 1: Create the 14 model files**

Each `dbt/models/bronze/bronze_<table>.sql` is identical except the source name. For every table in `[apod, neows, neo_lookup, cme, cme_analysis, gst, ips, flr, sep, mpc, rbe, hss, wsa_enlil_simulations, notifications]`:

```sql
-- dbt/models/bronze/bronze_<table>.sql
{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', '<table>') }}
```

- [ ] **Step 2: Create `dbt/models/bronze/_bronze__models.yml`** (PK tests; composite via dbt_utils)

```yaml
version: 2
models:
  - name: bronze_apod
    columns: [{name: date, data_tests: [not_null, unique]}]
  - name: bronze_neows
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [date, id]
    columns: [{name: date, data_tests: [not_null]}, {name: id, data_tests: [not_null]}]
  - name: bronze_neo_lookup
    columns: [{name: neo_reference_id, data_tests: [not_null, unique]}]
  - name: bronze_cme
    columns: [{name: activity_id, data_tests: [not_null, unique]}]
  - name: bronze_cme_analysis
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [associated_cmeid, time21_5]
    columns: [{name: associated_cmeid, data_tests: [not_null]}]
  - name: bronze_gst
    columns: [{name: gst_id, data_tests: [not_null, unique]}]
  - name: bronze_ips
    columns: [{name: activity_id, data_tests: [not_null, unique]}]
  - name: bronze_flr
    columns: [{name: flr_id, data_tests: [not_null, unique]}]
  - name: bronze_sep
    columns: [{name: sep_id, data_tests: [not_null, unique]}]
  - name: bronze_mpc
    columns: [{name: mpc_id, data_tests: [not_null, unique]}]
  - name: bronze_rbe
    columns: [{name: rbe_id, data_tests: [not_null, unique]}]
  - name: bronze_hss
    columns: [{name: hss_id, data_tests: [not_null, unique]}]
  - name: bronze_wsa_enlil_simulations
    columns: [{name: simulation_id, data_tests: [not_null, unique]}]
  - name: bronze_notifications
    columns: [{name: message_id, data_tests: [not_null, unique]}]
```

- [ ] **Step 2b: Run test to verify parse fails first**

Run (from `dbt/`): `DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt parse` BEFORE writing models — expect parse to fail (models referenced by `_bronze__models.yml` don't exist). Then after Step 1+2, parse passes. (Order: write `_bronze__models.yml`, parse → FAIL "model bronze_apod not found"; write the 14 models, parse → PASS.)

- [ ] **Step 3: Validate the two real models build + test against MinIO**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt/dbt
set -a; source /Users/tbarnes/projects/python/auspex-lakehouse/.env; set +a
DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt build --select bronze_apod bronze_neows
DBT_PROFILES_DIR=. uv run --no-sync --project .. dbt parse
```
Expected: `dbt build` creates the two views and runs their tests (apod: not_null+unique on `date`; neows: composite unique + not_nulls) — **all pass** against the 179/843 real rows. `dbt parse` succeeds for all 14. The other 12 are NOT built (their sources don't exist — fail-until-data; do not select them).

- [ ] **Step 4: Commit**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt
git add dbt/models/bronze/
git commit -m "feat(dbt): add 14 bronze view models with PK tests"
```

---

## Task 3: Dagster integration (assets, lineage, image, smoke test)

**Files:** Create `src/auspex_lakehouse/transform/__init__.py`, `src/auspex_lakehouse/transform/definitions.py`, `tests/test_dbt_bronze.py`; Modify `src/auspex_lakehouse/definitions.py`, `Dockerfile_user_code`.

**Interfaces:**
- Consumes: the dbt project from Tasks 1–2; the dlt asset keys listed in Global Constraints.
- Produces: 14 dbt assets keyed `bronze_<table>` in group `dbt_bronze`, each depending on its dlt source asset; a `DbtCliResource` wired into `Definitions`.

- [ ] **Step 1a: Create `tests/conftest.py`** (manifest must exist before ANY test imports `defs`)

`src/auspex_lakehouse/definitions.py` now imports the dbt assets, which read the
dbt manifest at import time. The EXISTING `tests/test_definitions.py` also imports
`defs`, so the manifest must be generated session-wide before any test — hence a
`conftest.py` fixture, not one inside `test_dbt_bronze.py`. Use the venv's `dbt`
via `sys.executable -m`/`uv run` so it works regardless of PATH:

```python
import os
import subprocess
import sys
from pathlib import Path

import pytest

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


@pytest.fixture(scope="session", autouse=True)
def _dbt_manifest():
    # dagster-dbt builds the asset graph from target/manifest.json; generate it
    # once per session. `dbt parse` needs no MinIO (profile env_vars have defaults).
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)}
    base = [sys.executable, "-m", "dbt.cli.main"]
    subprocess.run(base + ["deps"], cwd=DBT_DIR, check=True, env=env)
    subprocess.run(base + ["parse"], cwd=DBT_DIR, check=True, env=env)
```

(`dbt deps` hits the dbt hub once per session for `dbt_utils` — acceptable; CI has
network. If offline runs are needed later, vendor `dbt_packages/`.)

- [ ] **Step 1b: Write the failing smoke test** (`tests/test_dbt_bronze.py`)

```python
from dagster import AssetKey


def test_14_bronze_assets_with_lineage():
    from auspex_lakehouse.definitions import defs

    ag = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in ag.get_all_asset_keys()}
    expected = {f"bronze_{t}" for t in [
        "apod", "neows", "neo_lookup", "cme", "cme_analysis", "gst", "ips",
        "flr", "sep", "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
    ]}
    assert expected <= keys, f"missing: {expected - keys}"
    # lineage: a sample of the non-uniform source->dlt-key mapping
    assert AssetKey(["dlt_nasa_api_neows"]) in ag.get(AssetKey(["bronze_neows"])).parent_keys
    assert AssetKey(["dlt_nasa_donki_cme"]) in ag.get(AssetKey(["bronze_cme"])).parent_keys
    assert AssetKey(["neo_lookup"]) in ag.get(AssetKey(["bronze_neo_lookup"])).parent_keys
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt && uv run --no-sync pytest -q tests/test_dbt_bronze.py`
Expected: FAIL — `bronze_*` keys absent (no dbt assets wired yet). (The fixture's `dbt deps`/`parse` should succeed; if `dbt` isn't on PATH, prefix with `uv run --no-sync dbt ...` inside the fixture.)

- [ ] **Step 3: Create `src/auspex_lakehouse/transform/definitions.py`**

```python
import os
from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=os.getenv(
        "DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[3] / "dbt")
    )
)
dbt_project.prepare_if_dev()

_SOURCE_ASSET_KEYS = {
    "apod": AssetKey(["dlt_nasa_api_apod"]),
    "neows": AssetKey(["dlt_nasa_api_neows"]),
    "neo_lookup": AssetKey(["neo_lookup"]),
    **{
        t: AssetKey([f"dlt_nasa_donki_{t}"])
        for t in ["cme", "cme_analysis", "gst", "ips", "flr", "sep", "mpc",
                  "rbe", "hss", "wsa_enlil_simulations", "notifications"]
    },
}


class BronzeDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            mapped = _SOURCE_ASSET_KEYS.get(dbt_resource_props["name"])
            if mapped is not None:
                return mapped
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return "dbt_bronze"
        return super().get_group_name(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=BronzeDbtTranslator())
def dbt_bronze_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

- [ ] **Step 4: Create `src/auspex_lakehouse/transform/__init__.py`**

```python
from auspex_lakehouse.transform.definitions import (
    BronzeDbtTranslator,
    dbt_bronze_assets,
    dbt_project,
)

__all__ = ["dbt_bronze_assets", "dbt_project", "BronzeDbtTranslator"]
```

- [ ] **Step 5: Wire into `src/auspex_lakehouse/definitions.py`**

Add imports and register the dbt assets + resource:

```python
from dagster_dbt import DbtCliResource

from auspex_lakehouse.transform import dbt_bronze_assets, dbt_project
```

In `Definitions(...)`: add `dbt_bronze_assets` to `assets=[...]`, and add `"dbt": DbtCliResource(project_dir=dbt_project)` to `resources={...}` (alongside the existing `"dlt"`).

- [ ] **Step 6: Run the smoke test**

Run: `cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt && uv run --no-sync ruff check . && uv run --no-sync pytest -q`
Expected: PASS — 14 `bronze_*` assets present; `bronze_neows`→`dlt_nasa_api_neows`, `bronze_cme`→`dlt_nasa_donki_cme`, `bronze_neo_lookup`→`neo_lookup` lineage confirmed; existing tests still green. If `get_asset_key`/`resource_type` prop shape differs, inspect `dbt_resource_props` keys and adjust (dagster-dbt passes the dbt manifest node dict).

- [ ] **Step 7: Update `Dockerfile_user_code`**

After the source COPY, add the dbt project and bake its manifest into the image so the code location loads without a dev parse:

```dockerfile
COPY dbt/ ./dbt/
RUN cd dbt && DBT_PROFILES_DIR=. dbt deps && DBT_PROFILES_DIR=. dbt parse
ENV DBT_PROJECT_DIR=/app/dbt
```
(Adjust the workdir/paths to match the existing Dockerfile; `dbt parse` here needs no MinIO.)

- [ ] **Step 8: Commit**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse-dbt
git add src/auspex_lakehouse/transform/ src/auspex_lakehouse/definitions.py tests/test_dbt_bronze.py Dockerfile_user_code
git commit -m "feat(dbt): wire bronze dbt assets into Dagster with dlt lineage"
```

---

## Self-Review (completed by plan author)

**Spec coverage:** Engine/profile/spike (Component 1) → Task 1 (proven live during planning). Project structure (Component 2) → Tasks 1+3. Sources + non-uniform asset-key mapping (Component 3) → Task 1 `_sources.yml` + Task 3 translator. 14 view models, drop `_dlt_*` (Component 4) → Task 2. Dagster integration (Component 5) → Task 3. Testing (PK tests, parse gate, smoke) → Tasks 2+3. Scope (all 14, validate 2) + fail-until-data → Global Constraints + Task 2 Step 3. ✅

**Verified live during planning (not assumptions):** dbt-duckdb `secrets:` profile reads `apod` (179) over MinIO; source `external_location` with `{name}` resolves; `select * exclude (_dlt_id,_dlt_load_id)` drops the dlt cols; dlt-normalized PK names computed deterministically; `_dlt_id`/`_dlt_load_id` are the system columns.

**Placeholders:** none — every config/model/test is concrete.

**Known confirm-at-implementation items (not blockers):**
- `DagsterDbtTranslator.get_asset_key` receives `dbt_resource_props` (the manifest node dict with `resource_type`/`name`); confirm key names on first run (Task 3 Step 6 has the inspect-and-adjust note).
- `dbt_project.manifest_path` + `prepare_if_dev()` is the dagster-dbt manifest pattern; the smoke-test fixture generates the manifest via `dbt deps`/`parse`.
- The `Dockerfile_user_code` workdir/paths must match the existing file's structure.
