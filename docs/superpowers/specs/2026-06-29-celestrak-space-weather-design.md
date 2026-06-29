# CelesTrak Space Weather — Bronze Source Design

**Date:** 2026-06-29
**Status:** Reviewed (findings from adversarial review folded in); pending implementation plan
**Builds on:** the provider-package + factory/registry pattern and the per-provider
concurrency-pool pattern established by the **space-track** work
(`2026-06-28-spacetrack-design.md`), and the **`<source>_<dataset>` table-naming
convention** from `2026-06-29-bronze-descriptive-names-design.md`. CelesTrak is the
**third provider** (after NASA and space-track) and the **first flat-file (CSV) source** —
there is no query API, no auth, and effectively no rate limit, so it is the simplest
provider to date.

**Naming convention (followed throughout).** Per the descriptive-names design, the
**physical table name is one shared knob**: it is the dlt `@dlt.resource` `name=`, the S3
folder `bronze/<table>`, the dbt source `name:`, the `read_bronze_table()` argument, and
it drives the asset key as `dlt_<table>` (translator emits `dlt_{resource.name}`, **no
doubled provider prefix**). This source's one table is therefore **`celestrak_space_weather`**
→ asset key `dlt_celestrak_space_weather`, dbt model `bronze_celestrak_space_weather`.
"space_weather" is naturally uncountable, so it stays singular (like `boxscore`).

**Sequencing.** Independent of the descriptive-names *rename* PR: this is a greenfield
source that simply adopts the target convention directly. The clean `dlt_{resource.name}`
translator and the additive `_SOURCE_ASSET_KEYS` entry are self-consistent whether they
land before or after that rename.

> **Verification status.** The CelesTrak facts below were verified **live** on
> 2026-06-29 (see *Verified facts*): both CSV URLs return `200`, the exact column header,
> one-row-per-date semantics back to 1957-10-01, and the `F10.7_DATA_TYPE` observed/predicted
> flag. The Dagster/dlt wiring reuses mechanisms already verified in the space-track spec
> (`pool=` kwarg, lazy resources, `merge`+composite-key+delta on the filesystem
> destination, unpartitioned `@dlt_assets`). Nothing here is `⚠ verify-live`.

## Goal

Ingest CelesTrak's consolidated **space weather** file as a single bronze Delta table
(`celestrak_space_weather`) to provide the **atmospheric-drag driver inputs** (F10.7 solar radio
flux and Ap/Kp geomagnetic indices) that orbit propagators / density models
(NRLMSISE-00, JB2008) require. Neither space-track nor SSC supplies these, so this closes
the **drag-driver gap** identified in the SDA data-source review.

This spec covers **only the CelesTrak path**. The deliberate decision to use CelesTrak
rather than NOAA SWPC — and the criteria under which we would later add specific NOAA
endpoints — is captured in *Decision: CelesTrak vs NOAA SWPC* below.

## Decision: CelesTrak vs NOAA SWPC

**Decision: ingest CelesTrak now; defer NOAA SWPC.**

The two are **not independent sources of truth.** CelesTrak's space-weather file is a
curated **re-publication** of the same authoritative upstream measurements NOAA exposes:

| Quantity | Upstream producer | Exposed by NOAA SWPC | Packaged by CelesTrak |
|---|---|---|---|
| Kp / Ap geomagnetic indices | GFZ Potsdam | yes (per-endpoint) | yes (one file) |
| F10.7 solar radio flux | Canadian Space Weather (Penticton) | yes | yes |
| Near-term predictions | NOAA SWPC (45-day), NASA (monthly) | yes | yes (flagged `PRD`/`PRM`) |
| Sunspot number | SIDC | — | yes |

So choosing CelesTrak vs NOAA is choosing **a cleaned, consolidated, model-ready package**
vs **the rawer, real-time, multi-endpoint origin** — not different data.

**Why CelesTrak for the drag-driver use case:**

1. **Purpose-built for propagation.** One file delivers exactly the inputs NRLMSISE-00 /
   JB2008 consume (F10.7 observed + adjusted + 81-day centered/last averages; daily and
   3-hourly Ap/Kp) at the cadence those models use. Atmospheric models ingest daily /
   3-hourly indices, so NOAA's per-minute granularity buys nothing here.
2. **Quality-controlled & gap-filled.** Interpolated values are flagged (`INT`); the
   schema is flat and stable. Raw NOAA products are spread across endpoints with their
   own schemas/units that we would have to reconcile and de-gap ourselves.
3. **Trivial ingest in our architecture.** One **snapshot-merge** source keyed on `DATE`,
   no auth, no pagination — vs several NOAA `rest_api`/incremental sources we would then
   have to re-consolidate.

**When we would add NOAA SWPC (deferred, à la carte):** only if the RL formulation needs
something CelesTrak does not package — **real-time nowcasting, forecast horizons
(3-day/27-day), storm alerts, or raw solar-wind / GOES X-ray streams** (i.e. the agent
reacts to space-weather events as they happen rather than propagating with historical
drag inputs). At that point we add **only the one or two endpoints actually used** as
their own provider package (`sources/swpc/`, likely via dlt's `rest_api` source), **not**
the whole NOAA catalog. See *Out of Scope*.

## Constraints & Principles

- **No auth.** The file is a public static download on CelesTrak's CDN. No login, no key.
- **No meaningful rate limit.** CelesTrak refreshes the file a few times per day; a single
  daily pull is courteous and sufficient. We still bind the asset to a limit-1 provider
  pool (`celestrak_api`) to match the established per-provider convention and to keep
  backfills from running the asset concurrently with itself.
- **Whole-file snapshot, merged on `DATE`.** The file is small (~25k rows, one per day
  since 1957-10-01, growing by one row/day). Each run pulls the whole file and **upserts
  on `DATE`**. Merge (not replace) is chosen so that:
  - **forecast rows firm into observations in place** — a future date first appears as
    `PRD`/`PRM`, and later runs overwrite that same `DATE` row once it becomes `OBS`;
  - we retain the option to later switch the daily pull to the smaller
    `SW-Last5Years.csv` for bandwidth **without losing pre-window history** in bronze
    (full history stays; only recent rows are re-upserted).
- **Bronze = as-published, typed (not re-derived).** Land every column the file ships,
  typed by a single full-file parse (numeric → numeric, blank → null), exactly as the JSON
  sources land dlt-inferred types. `DATE` is kept as its raw `YYYY-MM-DD` string; **all
  semantic casting/derivation (date parsing, unit work, OBS/PRD filtering) is deferred to
  silver.** No values are computed or dropped at bronze.
- **dlt normalizes column names to snake_case.** The landed Delta/dbt columns are lowercased
  with dots stripped: `DATE`→`date`, `F10.7_OBS`→`f10_7_obs`, `F10.7_DATA_TYPE`→
  `f10_7_data_type`, `KP_SUM`→`kp_sum`, etc. The `primary_key="DATE"` hint is normalized the
  same way (proven by space-track's `primary_key="NORAD_CAT_ID"` → `norad_cat_id`), so the
  merge key resolves correctly. Downstream references (dbt tests, silver) must use the
  **normalized lowercase** names.
- **Row-count floor guard.** As with space-track snapshots, a conservative `min_rows`
  floor turns a silently truncated/short download into a loud failure rather than a
  corrupted catalog.
- **One source, one asset, unpartitioned.** This is a current-state snapshot (the file
  *is* the full history every time), so there is no date partition — same shape as
  space-track `gp`/`satcat`.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Scope | **CelesTrak `SW-All.csv`** → one bronze table `celestrak_space_weather`. (EOP and NOAA SWPC deferred — see *Out of Scope*.) |
| Provider package | New `sources/celestrak/` (third provider; first CSV/flat-file source) |
| Auth | None |
| File | `https://celestrak.org/SpaceData/SW-All.csv` (full history + near-term predictions) |
| Cadence model | **Snapshot-merge** on `DATE` (whole-file pull each run) |
| Parsing | `requests.get` → `polars.read_csv(infer_schema_length=None)` → `to_dicts()` (full-file type inference, blanks → null) |
| Code structure | One CSV-snapshot factory + a `CELESTRAK_DATASETS` registry (EOP slots in as a one-line entry later) |
| Grouping | One `@dlt_assets`, group `celestrak`, one pipeline |
| Budget control | `celestrak_api` pool (limit 1) — convention, not a hard need |
| Scheduling | `AutomationCondition.on_cron("30 5 * * *")` (after the overnight GFZ/NOAA updates; tunable) |
| Bronze model | `dbt/models/bronze/bronze_celestrak_space_weather.sql` (view) + source entry + `_SOURCE_ASSET_KEYS` lineage entry |
| Lineage bridge | Add `celestrak_space_weather → dlt_celestrak_space_weather` to `_SOURCE_ASSET_KEYS` (redundant with the sources.yml `meta` fallback; add for consistency) |
| Base branch | `feat/celestrak` off `main` |

## Verified facts (live, 2026-06-29)

1. **Both URLs return `200`:** `https://celestrak.org/SpaceData/SW-All.csv` (full history)
   and `https://celestrak.org/SpaceData/SW-Last5Years.csv` (rolling 5-year subset).
2. **Exact header (31 columns), in order:**
   `DATE,BSRN,ND,KP1,KP2,KP3,KP4,KP5,KP6,KP7,KP8,KP_SUM,AP1,AP2,AP3,AP4,AP5,AP6,AP7,AP8,AP_AVG,CP,C9,ISN,F10.7_OBS,F10.7_ADJ,F10.7_DATA_TYPE,F10.7_OBS_CENTER81,F10.7_OBS_LAST81,F10.7_ADJ_CENTER81,F10.7_ADJ_LAST81`
3. **One row per calendar date**, `DATE` in ISO-8601 `YYYY-MM-DD`, first row `1957-10-01`.
4. **Drag-driver columns present:** F10.7 — `F10.7_OBS`, `F10.7_ADJ`, and the 81-day
   averages `F10.7_{OBS,ADJ}_{CENTER81,LAST81}`; geomagnetic — `KP1..KP8`/`KP_SUM`,
   `AP1..AP8`/`AP_AVG`, plus `CP`/`C9`.
5. **Observed vs predicted flag:** `F10.7_DATA_TYPE` ∈ {`OBS` observed, `INT` CelesTrak
   linear interpolation, `PRD` 45-day predicted, `PRM` monthly predicted}. This makes the
   merge-on-`DATE` "forecast firms into observed" behavior a first-class, queryable fact.
6. **Cited sources:** GFZ Potsdam (Kp/Ap), Canadian Space Weather/Penticton (F10.7),
   NOAA SWPC (Kp forecast + 45-day predictions), SIDC (sunspots), NASA (monthly
   predictions) — confirming the provenance argument above.

## Fetch & parse helper (`sources/celestrak/_common.py`)

```python
import io

import polars as pl
import requests

SW_ALL_URL = "https://celestrak.org/SpaceData/SW-All.csv"


def fetch_csv_rows(url: str) -> list[dict]:
    """GET a CelesTrak SpaceData CSV and return typed row dicts.

    `infer_schema_length=None` scans the WHOLE file before typing — required here:
    the F10.7 81-day-average columns are blank for the first ~80 rows (Oct 1957,
    before a full window exists), and prediction rows carry blanks too. With the
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

## Component 1 — CSV-snapshot factory + registry (`sources/celestrak/snapshot.py`)

```python
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
            # gap-riddled drag-driver table. Floor is conservative (the file only
            # ever grows); tune if it ever false-fires.
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

- **Merge on `DATE`** — the only natural key; one row per date. Re-pulling the whole file
  upserts every row idempotently and overwrites prediction rows as they become observed.
- **Floor 20 000** — the file already holds ~25k rows (1957→present); it never legitimately
  drops below 20k. A short body (CDN hiccup, partial transfer) raises instead of writing.

## Component 2 — Source + pipeline (`sources/celestrak/__init__.py`)

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

Re-export `celestrak_source`, `celestrak_pipelines`, and `CELESTRAK_DATASETS` from
`sources/__init__.py` (matching the space-track re-exports). The resource is lazy, so
building a source makes **no HTTP call** — safe at import / decoration time. Unlike
space-track there is **no session to thread** (no auth), so the source needs no
runtime-rebuild dance; we still rebuild it in the asset body for symmetry with the
existing assets (still zero HTTP at build).

## Component 3 — Asset, pooling, scheduling (`dlt/assets.py`)

```python
# add to assets.py's existing imports:
from auspex_lakehouse.bronze.dlt.sources import celestrak_pipelines, celestrak_source
from auspex_lakehouse.bronze.dlt.sources.celestrak.config import CELESTRAK_API_POOL


class CelesTrakDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_{data.resource.name}"),   # resource name already = celestrak_space_weather
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

The translator emits `dlt_{resource.name}` — i.e. `dlt_celestrak_space_weather`, **no
doubled prefix** (the `celestrak_` already lives in the resource/table name), matching the
descriptive-names convention. The asset is **unpartitioned** (like `gp`/`satcat`) and
bound to the `celestrak_api` pool.

## Component 4 — Infra wiring

- **`config.py`** (`sources/celestrak/config.py`):

  ```python
  CELESTRAK_API_POOL = "celestrak_api"  # convention; CelesTrak is a static file, no hard limit
  ```

- **`deploy/dagster.yaml` — NO CHANGE.** (Corrected after adversarial review.) The real
  config has **no named pools** — only:

  ```yaml
  concurrency:
    pools:
      default_limit: 1
      granularity: 'op'
  ```

  and a comment noting a named pool's limit *cannot* be set here (it must be set via
  `dagster instance concurrency set <pool> <n>` on the deployed instance). `nasa_api` /
  `spacetrack_api` exist **only** as `pool="..."` labels in `assets.py`; they inherit
  `default_limit: 1` automatically. So binding the new asset to `pool="celestrak_api"` is
  **sufficient on its own** — it inherits the default limit of 1, exactly like the existing
  providers. Editing `dagster.yaml` would delete the working `default_limit` and insert a
  block the instance schema rejects. Raise the celestrak limit later only if needed, via the
  CLI on the deployed instance.

- **No `.env` changes** — CelesTrak needs no credentials.

- **`requests` dependency** — the fetch helper imports `requests`, which is **not** declared
  in `pyproject.toml` `[dependencies]` (present only transitively via boto3/dlt). This is a
  pre-existing practice — `assets.py` and `spacetrack/_common.py` already import it
  undeclared — so it is not a new break; optionally declare `requests` explicitly while
  here. `polars` resolves via the declared `polars-lts-cpu` (same import name).

## Component 5 — dbt bronze model + lineage bridge

> **Note (current code state).** As of this writing the bronze descriptive-names *rename*
> is approved-but-unimplemented, so the live `_SOURCE_ASSET_KEYS` and `_bronze__sources.yml`
> still use the **old** short names (`gp`, `cme`, …). CelesTrak adds its entry in the
> **new** convention regardless; it does not depend on that rename landing first.

- **`dbt/models/bronze/bronze_celestrak_space_weather.sql`** — thin view, identical shape
  to the other bronze models:

  ```sql
  {{ config(materialized='view') }}
  select * exclude (_dlt_id, _dlt_load_id)
  from {{ source('bronze', 'celestrak_space_weather') }}
  ```

- **`_bronze__sources.yml`** — add the source table, with a description in the same style
  as the recently-added NASA/space-track descriptions:

  ```yaml
  - name: celestrak_space_weather
    description: "CelesTrak consolidated daily space weather — one row per date (F10.7 solar flux observed/adjusted + 81-day averages, 3-hourly & daily Ap/Kp, sunspot number; F10.7_DATA_TYPE flags OBS/INT/PRD/PRM). Drag-driver inputs for orbit propagation (celestrak.org/SpaceData/SW-All.csv)."
    meta: {dagster: {asset_key: ["dlt_celestrak_space_weather"]}}
  ```

- **`transform/definitions.py::_SOURCE_ASSET_KEYS`** — add, to match every existing source
  row:

  ```python
  "celestrak_space_weather": AssetKey(["dlt_celestrak_space_weather"]),
  ```

  Resolution order (verified against installed `dagster_dbt`):
  `BronzeDbtTranslator.get_asset_key` checks `_SOURCE_ASSET_KEYS` **first**, else falls back
  to the default fn, which **does** read `meta.dagster.asset_key`. So with **both** the
  sources.yml `meta` (above) **and** this dict entry set to the same key, they are
  **redundant**, not load-bearing — if this entry were omitted, the identical `meta`
  fallback would still carry the lineage edge. Add it anyway for consistency with the other
  rows and to keep the dict the single obvious place to read provider→asset wiring.

- Model-level docs/tests (`_bronze__celestrak__models.yml`) follow the convention from the
  bronze-dbt-docs work: at minimum a `not_null`/`unique` test on the **normalized** column
  `date` (lowercase — see column normalization above). Note `date` is a DuckDB type keyword,
  so the generated test predicate must quote it (`"date"`).

## Error Handling

| Situation | Behavior |
|-----------|----------|
| HTTP non-200 / network error | `raise_for_status()` fails the run loudly; no auto-retry — re-materialize |
| **Short / truncated download** (below floor) | `min_rows` guard **raises** rather than writing a gap-riddled table |
| Blank prediction fields | `polars.read_csv` maps to null; landed as null in bronze |
| Forecast row (`PRD`/`PRM`/`INT`) later becomes `OBS` | Merge-on-`DATE` upserts the same row in place; `F10.7_DATA_TYPE` reflects the firmer value |
| Whole-file re-pull | Idempotent — every row upserts on `DATE`, no duplication |

## Testing

(All mocked — no live HTTP; monkeypatch `fetch_csv_rows`.)

- **Factory:** a list of row dicts → one yielded row each; a below-floor list **raises**;
  resource has the right `name`, `write_disposition="merge"`, `primary_key="DATE"`,
  `table_format="delta"`.
- **Parsing helper:** given CSV bytes with the real header + a blank field, returns dicts
  with the blank as `None` and `DATE`/`F10.7_DATA_TYPE` as strings. Include a fixture row
  whose 81-day-average columns are blank to lock in the `infer_schema_length=None` fix.
  (Asserts the **raw** 31-header set from `polars.read_csv`; this is pre-dlt, so it validates
  the source header, **not** the snake_cased landed column names.)
- **Registry:** exactly one entry named `celestrak_space_weather`; URL is the `SW-All.csv`
  URL; key is `DATE`; floor present.
- **Source/pipeline:** `celestrak_source("celestrak_space_weather")` exposes one resource
  of the right name; constructing it makes **no HTTP call** (monkeypatch `requests.get` to
  raise if called at build); `celestrak_pipelines["celestrak_space_weather"]` exists with
  `pipeline_name == "celestrak_space_weather"`.
- **Wiring:** the module-level asset def exists; key `dlt_celestrak_space_weather` in
  group `celestrak`; **unpartitioned**; op `pool == "celestrak_api"`.
- **Lineage bridge:** `_SOURCE_ASSET_KEYS["celestrak_space_weather"]` ==
  `AssetKey(["dlt_celestrak_space_weather"])` — guards the silent lineage-drop failure mode.
- **dbt:** the `celestrak_space_weather` source resolves and `bronze_celestrak_space_weather`
  compiles; the dbt source asset key (via `BronzeDbtTranslator`) equals the dlt asset key.
  `test_dbt_bronze.py` uses a subset check so it won't break, but add
  `bronze_celestrak_space_weather` + the `dlt_celestrak_space_weather` lineage edge to its
  guarded set for real coverage.
- **Smoke:** `test_definitions_load` still loads (no import-time HTTP/Delta).

## Out of Scope

- **NOAA SWPC endpoints** — deferred. Add only specific endpoints (real-time solar wind,
  planetary-K forecast, storm alerts, GOES X-ray) **if** the RL formulation needs live
  nowcasting/forecasts/alerts, as a separate `sources/swpc/` provider (likely dlt
  `rest_api`). Criteria stated in *Decision: CelesTrak vs NOAA SWPC*.
- **CelesTrak EOP** (`EOP-All.csv`, Earth-orientation parameters for high-precision
  ITRF↔GCRF frame transforms) — a one-line `CELESTRAK_DATASETS` addition
  (`celestrak_earth_orientation_parameters`) when a precise-OD workflow needs it; not
  required for drag drivers.
- **`SW-Last5Years.csv` swap** — the merge design already supports moving the daily pull
  to the smaller file later for bandwidth; not needed at current volume.
- **Silver-layer drag modeling** — feeding F10.7/Ap into NRLMSISE-00 / JB2008, joining to
  the space-track GP catalog for propagation. Separate spec.
- **JPL Horizons** (precise ephemerides — the other gap-closer) — its own sibling spec.
