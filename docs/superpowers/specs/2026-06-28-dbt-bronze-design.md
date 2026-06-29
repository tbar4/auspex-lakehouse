# dbt Bronze Staging (views over dlt Delta tables) — Design

**Date:** 2026-06-28
**Status:** Approved (after adversarial review), pending implementation plan
**Base:** `origin/main` @ `47910cb` (has NEO + DONKI dlt bronze tables merged).

## Goal

Scaffold a dbt project inside the Dagster code location and build a **bronze
staging layer** as **view**-materialized dbt models, one per top-level dlt Delta
table. The views become Dagster assets with lineage from the dlt bronze assets,
give us dbt tests on the raw grain, and a clean column-standardized surface for a
future silver layer — without duplicating the Delta storage.

## What this layer IS and ISN'T (accepted after adversarial review)

dbt-duckdb's catalog is a single, effectively per-run `.duckdb` file, and DuckDB
is single-writer. Therefore these `view` models are a **lineage / transformation
/ test layer**, NOT a durable queryable store:

- ✅ Dagster assets + lineage from the dlt tables, dbt tests, standardized SQL
  surface for silver.
- ❌ You cannot `SELECT * FROM bronze_neows` outside a dbt run — the view exists
  only in that run's ephemeral DuckDB catalog. **The durable bronze artifact
  remains the dlt Delta table.**

This is the deliberate trade-off (chosen over `external`, which would duplicate
storage, and over a persistent engine like Trino). Silver, when it comes, will
write back to Delta via dbt-duckdb `external` materialization; bronze stays views.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Engine / adapter | **dbt-duckdb** (DuckDB reads Delta from MinIO via `delta_scan`) |
| Materialization | **`view`** (lineage/transform/test layer; Delta is the durable store) |
| Scope | **Parent tables only** (~14); nested dlt child tables deferred to silver |
| dbt project location | repo-root **`dbt/`** (dbt-CLI-friendly; added to the user-code image) |
| Model naming | **`bronze_<table>`** |
| `_dlt_*` columns | **dropped** in the bronze views |
| Dagster wiring | `DbtProject` + `@dbt_assets`; a translator maps dbt sources → dlt asset keys |

## Verified building blocks (spike, controller)

`duckdb 1.5.4` loads `httpfs` + `delta`; `delta_scan` reads a real Delta table
locally; a MinIO-style S3 secret (`ENDPOINT`, `URL_STYLE 'path'`, `USE_SSL
false`) is accepted. **Not yet verified:** an end-to-end `delta_scan` against the
real homelab MinIO (unreachable from the design environment). See Component 1.

## Component 1 — Engine, profile, and the de-risking spike (FIRST task)

Add deps `dbt-duckdb` + `duckdb`. The dbt-duckdb profile (committed
`profiles.yml`, non-secret) loads the `httpfs` + `delta` extensions and a MinIO
S3 secret built from existing env (`MINIO_*`), with `url_style=path`,
`use_ssl=false`, endpoint host derived from `MINIO_ENDPOINT`. The DuckDB catalog
path comes from an env var (e.g. `DBT_DUCKDB_PATH`, default a tmp file).

Sources resolve through dbt-duckdb's `external_location`:
`delta_scan('s3://auspex-lakehouse/bronze/<table>')`.

**The first implementation task is a spike, not models.** It must, against the
real MinIO: (a) confirm `delta_scan('s3://.../bronze/neows')` returns rows with
the configured profile; (b) `DESCRIBE` that table to capture the **dlt-normalized
column names** (dlt snake-cases identifiers — `activityID`→`activity_id`,
`gstID`→`gst_id`, `neo_reference_id` stays, etc.), which the models and tests
must use. Do NOT author 14 models until one source reads end-to-end and its real
column names are known. If MinIO custom-endpoint Delta reads fail in DuckDB,
stop and escalate — the engine choice is reconsidered, not worked around.

## Component 2 — dbt project structure

```
dbt/
  dbt_project.yml          # name auspex_lakehouse; models/bronze → materialized: view
  profiles.yml             # duckdb target; httpfs+delta; MinIO secret from env
  packages.yml             # dbt_utils (for composite-key test)
  models/bronze/
    _bronze__sources.yml   # 14 sources w/ external_location + dagster asset-key meta
    bronze_apod.sql        # ... one view model per parent table
    bronze_neows.sql
    ... (14 total)
    _bronze__models.yml    # column docs + not_null/unique tests per model
```

`Dockerfile_user_code` is updated to `COPY dbt/ ./dbt/` so the project ships in
the code-location image.

## Component 3 — Sources and the source→dlt-asset-key mapping

14 sources under a `bronze` source group, each with
`external_location: "delta_scan('s3://auspex-lakehouse/bronze/{name}')"` and a
`meta` carrying its upstream Dagster asset key. The mapping is **not uniform**
(apod/neows from the `nasa_api` dlt source, DONKI from `nasa_donki`, neo_lookup a
plain asset):

| source (table) | upstream Dagster asset key |
|---|---|
| `apod` | `dlt_nasa_api_apod` |
| `neows` | `dlt_nasa_api_neows` |
| `neo_lookup` | `neo_lookup` |
| `cme` | `dlt_nasa_donki_cme` |
| `cme_analysis` | `dlt_nasa_donki_cme_analysis` |
| `gst` | `dlt_nasa_donki_gst` |
| `ips` | `dlt_nasa_donki_ips` |
| `flr` | `dlt_nasa_donki_flr` |
| `sep` | `dlt_nasa_donki_sep` |
| `mpc` | `dlt_nasa_donki_mpc` |
| `rbe` | `dlt_nasa_donki_rbe` |
| `hss` | `dlt_nasa_donki_hss` |
| `wsa_enlil_simulations` | `dlt_nasa_donki_wsa_enlil_simulations` |
| `notifications` | `dlt_nasa_donki_notifications` |

The custom `DagsterDbtTranslator.get_asset_key` reads each source's
`meta.dagster.asset_key` to wire the dependency (with a dict fallback in the
translator if meta-based mapping proves unreliable — to be confirmed in impl).

## Component 4 — Bronze view models

One thin `view` per parent table — a faithful pass-through that drops only the
dlt system columns, using DuckDB's `EXCLUDE`. This is uniform across all 14
(DRY), needs no per-table column capture, and lets new source columns flow
through (the right default for a raw bronze layer; column pinning is a silver
concern):

```sql
-- models/bronze/bronze_neows.sql
{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'neows') }}
```

(Spike-confirmed: the dlt system columns are exactly `_dlt_id` and
`_dlt_load_id`.)

## Component 5 — Dagster integration

A new `auspex_lakehouse/transform/` module: a `DbtProject` pointing at `dbt/`, a
`@dbt_assets` over the bronze selection, and the source-mapping
`DagsterDbtTranslator`. Wired into `definitions.py` (add the dbt assets +
`DbtCliResource`). Bronze view assets land in a `dbt_bronze` group and depend on
their dlt upstreams via the Component 3 mapping.

## Testing

- **dbt tests:** `not_null` + `unique` on each table's primary key. Composite key
  (`cme_analysis` = `associated_cmeid` + `time21_5`, normalized names TBD by
  spike) uses `dbt_utils.unique_combination_of_columns`.
- **CI gate:** `dbt parse` (no DB connection) + `dbt compile` if it resolves
  external sources without MinIO; otherwise parse-only. dbt deps installed in CI.
- **Dagster smoke:** `definitions` loads with the 14 `bronze_*` assets present
  and each depending on the correct dlt upstream key — no MinIO needed (graph
  construction only).
- The actual `delta_scan`-over-MinIO read is integration-verified (the
  Component 1 spike + first real run), not in unit CI.

## Scope & validation (post-spike)

The spike ran against real MinIO: **`apod` (179 rows) and `neows` (843 rows) read
end-to-end; the other 12 source tables do not exist yet** (`neo_lookup` + all
DONKI assets have never materialized on the homelab). Decision: **build all 14
bronze models + the Dagster wiring now, but only `apod`/`neows` are validated
against real data**; the other 12 are authored, parse/compile-clean, and light up
once their dlt assets first run. They are mechanical copies of the proven
`select * exclude` pattern.

## Risks / accepted trade-offs

- **F1 (accepted):** views are not durably queryable (per-run DuckDB catalog);
  Delta tables remain the durable bronze. Silver will use `external` → Delta.
- **F2 (resolved by spike):** Delta-on-MinIO reads work in DuckDB (apod/neows
  proven); the MinIO secret config is `endpoint=<host>`, `url_style=path`,
  `use_ssl=true`.
- **F4 (accepted — fail-until-data):** DuckDB's `delta_scan` errors on a Delta
  table with no data files ("No files in log segment"). A source that is missing
  or has zero rows (sparse DONKI endpoints can yield no events) makes its bronze
  view **error until the source has ≥1 row, then self-heal**. No clean DuckDB-side
  guard (reading parquet directly breaks merge semantics); documented as a known
  limitation, not worked around.
- **F3 (minor):** a view re-scans its Delta source on every downstream reference
  (no caching) — negligible for bronze, revisit when silver fans out.

## Out of Scope

- Silver / gold models; nested dlt child tables (silver unnests those).
- Persisting the DuckDB catalog / running bronze as `external` Delta.
- Running dbt against real MinIO in CI (CI parses/compiles only).
- A persistent SQL engine (Trino/Spark) over the lakehouse.
