# Bronze model descriptive rename — design

**Date:** 2026-06-29
**Status:** Approved (design); pending implementation plan

## Goal

Rename every bronze model so the name carries its provider prefix (`nasa_` /
`space_track_`) and the full spelling of the entity instead of an acronym
(`cme` → `coronal_mass_ejections`). The rename spans the full chain: dlt
resource names (which drive the physical Delta table path on S3), the Dagster
asset keys, the Python references, and the dbt sources/models — so the
descriptive name is consistent end to end.

## Decisions (locked)

- **DONKI datasets** carry a `nasa_donki_` sub-API tag (preserves which NASA
  sub-API the table comes from). APOD and NeoWs need no sub-API tag — the
  entity name already disambiguates — so they get a plain `nasa_` prefix.
- **dbt model names** keep the `bronze_` layer prefix (`bronze_<table>`).
- **Dagster asset keys** are renamed to match: `dlt_<table>`.
- **Existing S3 data** is handled by **re-ingest**, not physical folder-move
  (decision revised after adversarial review — see "Migration" below). dlt
  cleanly creates the new-named Delta tables; merge-on-primary-key makes
  re-ingest idempotent.
- **`apod_images`** (adjacent plain asset + S3 prefix) is renamed too. Its
  accumulated image blobs are plain objects (not a Delta table), so they get a
  simple one-off S3 prefix-copy to the new prefix.
- **Pluralization:** plural for entities (`coronal_mass_ejections`), but
  naturally-singular / proper names kept as-is (`satellite_catalog`,
  `boxscore`, `wsa_enlil_simulations`).

## Name mapping

The physical table name is one shared knob: it is the dlt `@dlt.resource`
`name=`, the dbt source `name:`, the `read_bronze_table()`/`bronze_table_exists()`
argument, and the S3 folder `bronze/<table>`. The dbt model is `bronze_<table>`;
the asset key is `dlt_<table>`.

### NASA — planetary / NeoWs (plain `nasa_`)

| Old | New table |
|---|---|
| `apod` | `nasa_astronomy_picture_of_the_day` |
| `neows` | `nasa_near_earth_object_feed` |
| `neo_lookup` | `nasa_near_earth_object_lookups` |

### NASA — DONKI (`nasa_donki_`)

| Old | New table |
|---|---|
| `cme` | `nasa_donki_coronal_mass_ejections` |
| `cme_analysis` | `nasa_donki_coronal_mass_ejection_analyses` |
| `gst` | `nasa_donki_geomagnetic_storms` |
| `ips` | `nasa_donki_interplanetary_shocks` |
| `flr` | `nasa_donki_solar_flares` |
| `sep` | `nasa_donki_solar_energetic_particles` |
| `mpc` | `nasa_donki_magnetopause_crossings` |
| `rbe` | `nasa_donki_radiation_belt_enhancements` |
| `hss` | `nasa_donki_high_speed_streams` |
| `wsa_enlil_simulations` | `nasa_donki_wsa_enlil_simulations` |
| `notifications` | `nasa_donki_notifications` |

### Space-Track (`space_track_`)

| Old | New table |
|---|---|
| `gp` | `space_track_general_perturbations` |
| `satcat` | `space_track_satellite_catalog` |
| `boxscore` | `space_track_boxscore` |
| `decay` | `space_track_decays` |
| `cdm` | `space_track_conjunction_data_messages` |
| `tip` | `space_track_tracking_and_impact_predictions` |

### Adjacent asset

| Old | New |
|---|---|
| `apod_images` (asset name + `bronze/apod_images/` S3 prefix) | `nasa_astronomy_picture_of_the_day_images` |

## Changes by layer

### 1. dlt resource names (drives the S3 path)

- `sources/nasa/apod.py` — `@dlt.resource(name=...)` and the `apod` symbol
  exported/imported as a resource. The resource `name=` becomes the new table
  name; the Python symbol may stay short or be renamed for readability (plan
  decides, but imports must stay consistent).
- `sources/nasa/neows.py` — same for `neows`.
- `sources/nasa/neo_lookup.py` — `neo_lookup_rows` resource `name=`.
- `sources/nasa/donki.py` — the first column of every `DONKI_ENDPOINTS` tuple.
- `sources/spacetrack/snapshot.py` — first column of `SNAPSHOT_CLASSES`.
- `sources/spacetrack/incremental.py` — first column of `INCREMENTAL_CLASSES`.

For Space-Track the short name is **overloaded** as: the dlt resource/table
name, the `spacetrack_pipelines[name]` dict key, the `_ST_SNAPSHOT_CRON` /
`_ST_INCREMENTAL_CRON` dict keys, the argument passed into
`_spacetrack_snapshot_assets(...)` / `_spacetrack_incremental_assets(...)`, and
(via `_pipeline`) the `pipeline_name=f"spacetrack_{name}"` suffix. **Decision:**
push the long name through all of them in lockstep (no short-key/long-name split
to drift). Consequence: the local dlt working-dir `pipeline_name` becomes
verbose (e.g. `spacetrack_space_track_general_perturbations`) — cosmetic only
(names a local dir; state restores from the destination regardless).

### 2. Translators (clean asset keys)

Override the asset key to `dlt_{data.resource.name}` so there is no doubled
prefix:

- `NasaDltTranslator` — add a `key=AssetKey(f"dlt_{data.resource.name}")`
  override (currently relies on the default `dlt_nasa_api_<resource>`).
- `DonkiDltTranslator` — same override.
- `SpaceTrackDltTranslator` — change its existing
  `key=AssetKey(f"dlt_spacetrack_{data.resource.name}")` to
  `key=AssetKey(f"dlt_{data.resource.name}")` (the `space_track_` is now in the
  resource name itself).

### 3. Python references (`assets.py`)

- `apod_images` asset: rename `name="apod_images"` →
  `name="nasa_astronomy_picture_of_the_day_images"`; update its
  `deps=[AssetKey(["dlt_nasa_api_apod"])]` →
  `[AssetKey(["dlt_nasa_astronomy_picture_of_the_day"])]`; update
  `read_bronze_table("apod")` → new name; update the
  `bronze/apod_images/...` object-key prefix → new prefix.
- `neo_lookup` plain `@asset`: rename `name="neo_lookup"` →
  `name="nasa_near_earth_object_lookups"`; update its
  `deps=[AssetKey(["dlt_nasa_api_neows"])]` →
  `[AssetKey(["dlt_nasa_near_earth_object_feed"])]`; update
  `read_bronze_table("neows")`, `bronze_table_exists("neo_lookup")`, and
  `read_bronze_table("neo_lookup")` to the new names.
- `_existing_lookup_index()` — its `bronze_table_exists`/`read_bronze_table`
  calls use the new lookups table name.

### 4. dbt↔dlt lineage bridge (`transform/definitions.py`) — LOAD-BEARING

`src/auspex_lakehouse/transform/definitions.py` holds `_SOURCE_ASSET_KEYS`, a
dict mapping each **dbt source name** (= table name) → the **dlt asset key** it
should depend on. Both sides of every entry change:

- the dict keys become the new table names (`apod` → `nasa_astronomy_picture_of_the_day`, etc.);
- the NASA/DONKI values become the new `dlt_<table>` keys (e.g.
  `AssetKey(["dlt_nasa_donki_coronal_mass_ejections"])`);
- the `neo_lookup` entry's value becomes `AssetKey(["nasa_near_earth_object_lookups"])`
  (the plain asset's key — no `dlt_` prefix);
- the dict-comprehension over the DONKI list updates its `f"dlt_nasa_donki_{t}"`
  pattern + the `t` list to the new names.

If this file is missed, the dbt models still load but lose their lineage edge to
the dlt assets — a silent break that no import error surfaces.

### 5. dbt

> Note: the 14 dbt bronze models / sources are all NASA + DONKI. Space-Track has
> no dbt models or `_bronze__sources.yml` entries yet, so the Space-Track rename
> is dlt + asset-key only — there is nothing to change under `dbt/` for it.

- Rename all 14 `dbt/models/bronze/bronze_<old>.sql` → `bronze_<new>.sql` and
  update each `source('bronze', '<old>')` → `source('bronze', '<new>')`.
- `_bronze__sources.yml` — each table `name:` → new table name, and each
  `dagster.asset_key` → `["dlt_<new>"]` (for `neo_lookup` the key is
  `["nasa_near_earth_object_lookups"]`, the plain asset's key — no `dlt_`).
- `_bronze__models.yml` — each `- name: bronze_<old>` → `bronze_<new>`.

### 6. Tests (8 files)

The rename touches far more than `test_dbt_bronze.py`. Affected:

- `tests/test_dbt_bronze.py` — expected `bronze_<t>` set + 3 lineage assertions
  (`dlt_nasa_api_neows` → `dlt_nasa_near_earth_object_feed`; `dlt_nasa_donki_cme`
  → `dlt_nasa_donki_coronal_mass_ejections`; `neo_lookup` →
  `nasa_near_earth_object_lookups`).
- `tests/test_donki_asset.py` — the `dlt_nasa_donki_<x>` key list + the
  `cme` key references.
- `tests/test_neo_lookup_asset.py` — `AssetKey(["neo_lookup"])` →
  `["nasa_near_earth_object_lookups"]`; `dlt_nasa_api_neows` parent →
  `dlt_nasa_near_earth_object_feed`.
- `tests/test_spacetrack_assets.py` — the six `dlt_spacetrack_<x>` keys →
  `dlt_space_track_<x>` (new prefix + expanded names).
- `tests/test_definitions.py` — `dlt_spacetrack_gp` / `dlt_spacetrack_decay`
  assertions → new keys; update the comment about the translator convention.
- `tests/test_donki.py` — `_donki_resource(...)`/`DONKI_ENDPOINTS` resource-name
  assertions (`"cme"`, `"gst"`, …, `res.name == "cme_analysis"`, the full
  `src.resources.keys()` set).
- `tests/test_nasa_sources.py` — `src.resources.keys() == {"apod","neows"}` and
  `res.name == "neo_lookup"`.
- `tests/test_spacetrack_sources.py` — resource-name assertions, the
  `SNAPSHOT_CLASSES`/`INCREMENTAL_CLASSES` first-column lists, the
  `spacetrack_pipelines` key set, and `pipeline_name == "spacetrack_gp"` →
  the new long-name equivalents.

**Not affected** (verified): `tests/test_delta_helpers.py` (only callable
checks), `tests/test_neo_lookup.py` (pure fetch/select logic; `neo_reference_id`
is a column), `tests/test_spacetrack_common.py` (`"gp"`/`"boxscore"` there are
the Space-Track **API class** in the query URL, not table names — unchanged).

### 7. Docs (cosmetic)

- `README.md` and `.env.example` mention the `apod_images` asset; update the
  references to the new asset name.
- Historical specs/plans under `docs/superpowers/{specs,plans}/2026-06-28-*`
  are a record of past work — left as-is.

### 8. Migration: re-ingest (decision revised after adversarial review)

Physical S3 folder-move was rejected as too risky: naive globbing collides
`cme` / `cme_analysis` / the unrelated `apod_images` prefix; long new names can
make dlt **hash-truncate child-table names** (so moved child files orphan); the
first post-rename load may append-not-merge; and the shared `bronze/_dlt_*`
metadata is one bad delete from corrupting all 8 pipelines. Containers also have
**no persistent dlt volume** — state restores from the destination on every
deploy — so there is no local state to preserve.

Instead, **re-ingest** (all tables are API-sourced; merge-on-PK is idempotent,
so re-running cannot duplicate rows):

- **`replace` (`boxscore`):** rename + one run — full overwrite, strictly clean.
- **Snapshot-merge (`gp`, `satcat`):** rename + one run repopulates the current
  snapshot.
- **Incremental/partitioned merge** (`apod`, `neows`, `neo_lookup`, all DONKI,
  `decay`, `cdm`, `tip`): rename + backfill the partition range. Cost is
  re-fetching history against rate-limited APIs (NASA 1000/hr, Space-Track),
  bounded and idempotent.
- **`apod_images` blobs:** a one-off plain S3 prefix-copy
  `bronze/apod_images/` → `bronze/nasa_astronomy_picture_of_the_day_images/`
  (plain objects, not Delta — no glob/child/state hazards), or re-materialize
  the asset over history. Prefix-copy preferred (avoids re-downloading images).
- Leave `bronze/_dlt_loads`, `bronze/_dlt_pipeline_state`, `bronze/_dlt_version`
  **untouched**. Old-named Delta folders + stale schema entries are harmless
  cruft; delete the old `bronze/<old>/` folders manually once the new tables are
  confirmed populated.

## Out of scope

- No schema/column changes; models remain `select * exclude(_dlt_id,
  _dlt_load_id)` views.
- No silver/gold layer (none exists yet).
- Space-Track has no dbt layer yet (dlt + asset-key changes only).

## Verification

- `dbt parse` / `dbt compile` succeeds with renamed sources and models.
- `pytest` (full suite — all 8 affected files) passes against the new names.
- Dagster definitions load (`defs.resolve_asset_graph()`); all expected
  `bronze_<new>` keys present; lineage edges (dbt source → dlt asset) intact via
  the updated `_SOURCE_ASSET_KEYS`.
- After a re-ingest run, new `bronze/<new>/` Delta tables are readable via
  `read_bronze_table`; spot-check row counts are sane.
