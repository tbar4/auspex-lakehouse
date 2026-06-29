# Bronze dbt documentation + space-track models — design

**Date:** 2026-06-29
**Status:** approved (design), pending implementation plan

## Goal

Fill out the bronze-layer dbt documentation so the generated dbt docs site (deployed
to GitHub Pages) is genuinely useful: every table and every top-level column described,
a project-specific landing page replacing dbt's generic overview, and the space-track
data brought into the dbt project so the catalog covers the full intended dataset —
not just the NASA half.

## Context (what exists today)

- 14 NASA bronze models, each a `select * exclude (_dlt_id, _dlt_load_id)` view over a
  dlt-loaded Delta table at `s3://auspex-lakehouse/bronze/{name}`:
  - `apod`, `neows`, `neo_lookup`, and 11 DONKI endpoints (`cme`, `cme_analysis`,
    `gst`, `ips`, `flr`, `sep`, `mpc`, `rbe`, `hss`, `wsa_enlil_simulations`,
    `notifications`).
- Sources defined in `dbt/models/bronze/_bronze__sources.yml` as a single `bronze`
  source with `external_location: "delta_scan('s3://auspex-lakehouse/bronze/{name}')"`
  and per-table `meta.dagster.asset_key` for lineage.
- Tests + (sparse) column entries in `dbt/models/bronze/_bronze__models.yml`.
- **space-track** is fully wired in Dagster (asset keys `dlt_spacetrack_{gp,satcat,
  boxscore,decay,cdm,tip}`, pipelines land in the same `bronze` dataset/bucket) but has
  **no dbt models, sources, or docs**.
- Docs build/deploy: `.github/workflows/dbt-docs.yml` runs
  `dbt docs generate --static --empty-catalog` and publishes `static_index.html`.

## Decisions (locked)

- **Scope:** NASA models **plus** add the six space-track bronze models, then document
  everything.
- **Column depth:** document **all top-level columns** for every table.
- **Field source:** public API docs (space-track.org `#/api`, api.nasa.gov) + the
  PK/field definitions already in the repo. **No live API calls, no warehouse.**
- **File layout:** split the models YAML by provider; keep sources unified.

## Components

### 1. Six space-track bronze models

Mirror the NASA pattern exactly. New files in `dbt/models/bronze/`:

- `bronze_gp.sql`, `bronze_satcat.sql`, `bronze_boxscore.sql`, `bronze_decay.sql`,
  `bronze_cdm.sql`, `bronze_tip.sql`
- Each body: `{{ config(materialized='view') }}` then
  `select * exclude (_dlt_id, _dlt_load_id) from {{ source('bronze', '<name>') }}`.

Source entries added to the existing single `bronze` source in
`_bronze__sources.yml` (one source definition is required; the templated
`external_location` already resolves `{name}` for these). Each table gets
`meta: {dagster: {asset_key: ["dlt_spacetrack_<name>"]}}`.

No name collisions between NASA and space-track table names.

### 2. Tests (mirror the dlt `primary_key` definitions — the authoritative keys)

| model | test |
|-------|------|
| `bronze_gp` | `norad_cat_id` not_null + unique |
| `bronze_satcat` | `norad_cat_id` not_null + unique |
| `bronze_decay` | `dbt_utils.unique_combination_of_columns [norad_cat_id, msg_epoch, precedence]` + not_null on each |
| `bronze_cdm` | `cdm_id` not_null + unique |
| `bronze_tip` | `dbt_utils.unique_combination_of_columns [norad_cat_id, msg_epoch]` + not_null on each |
| `bronze_boxscore` | `replace` table, no dlt PK → not_null on the country column only (no unique claim without real data) |

### 3. Full column documentation, provider-split

- Split `_bronze__models.yml` into `_bronze__nasa__models.yml` and
  `_bronze__spacetrack__models.yml`. (dbt allows models across multiple YAML files.)
  Sources stay in the single `_bronze__sources.yml`.
- Every table: a `description:`.
- Every **top-level** column: a `description:`.
- **Normalization rule** applied to every documented name: dlt snake-cases source
  field names (`NORAD_CAT_ID` → `norad_cat_id`, `activityID` → `activity_id`). Nested
  arrays/objects become dlt **child tables** (e.g. neows `close_approach_data`,
  neo_lookup `orbital_data` / `close_approach_data`) — documented in the parent
  table's `description`, not as columns (child tables are not modeled here).
- **Shared doc blocks:** `_bronze__docs.md` holds `{% docs %}` blocks for the
  space-track identity columns repeated across tables (`norad_cat_id`, `object_name`,
  `object_id`, `object_type`), referenced via `description: '{{ doc("...") }}'` to
  stay DRY.

#### Field-list sources per table (for the implementation plan)

- **NASA** (api.nasa.gov): APOD planetary endpoint; NeoWs `/feed` asteroid object;
  NEO lookup `/neo/{id}` (+ the `lookup_fetched_at` / `lookup_status` columns the
  pipeline injects, and the `not_found` tombstone rows); DONKI CME / CMEAnalysis /
  GST / IPS / FLR / SEP / MPC / RBE / HSS / WSAEnlilSimulations / notifications.
- **space-track** (space-track.org `#/api` model defs): `gp`, `satcat`, `boxscore`,
  `decay`, `cdm` (`cdm_public` class), `tip`.

### 4. Top-level README / overview

`dbt/models/overview.md` defining `{% docs __overview__ %}` — replaces dbt's generic
landing page. Content:

- Project purpose (auspex lakehouse: space-domain-awareness + space-weather data).
- Lakehouse flow: dlt extract → Delta tables on S3 (`bronze/{table}`) → dbt bronze
  views → (future silver/gold).
- The two providers, each with a link to its API docs
  (https://api.nasa.gov/, https://www.space-track.org/documentation#/api) and a one-line
  description of what each table covers.
- Naming / layer conventions (`bronze_*` views, `_dlt_*` columns excluded, dlt
  snake_case normalization, nested → child tables).
- How to navigate the catalog (sources vs models, lineage to Dagster asset keys).

### 5. Test update

`tests/test_dbt_bronze.py`: extend the expected set to include the six space-track
`bronze_*` models and add a lineage sample (`dlt_spacetrack_gp` → `bronze_gp`). Rename
`test_14_bronze_assets_with_lineage` to reflect the new count (20).

## Verification

- `uv run --no-sync dbt deps` then `dbt parse` — clean parse.
- `dbt docs generate --static --empty-catalog` (the exact CI command) — builds the
  static site without a warehouse.
- `dbt compile` — the six new models resolve against the `bronze` source.
- `uv run pytest tests/test_dbt_bronze.py` — passes with the updated expectations.

## Out of scope / call-outs

- No silver/gold modeling; bronze pass-through only.
- Column lists are derived from the public API docs + repo definitions; the *exact* set
  of dlt child tables only fully materializes against real data, so child-table mentions
  are documented from the API schema and flagged as such where uncertain.
- No live space-track or NASA calls; no warehouse connection (matches the
  `--empty-catalog` docs build).
