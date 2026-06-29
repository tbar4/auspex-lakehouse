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
- **Existing S3 data** is migrated: Delta table folders are moved from the old
  path to the new path (parent + dlt child tables), then old prefixes deleted.
- **`apod_images`** (adjacent plain asset + S3 prefix) is renamed too.
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

For Space-Track the long name is also the key used to look up
`spacetrack_pipelines[name]`, the `_ST_SNAPSHOT_CRON` / `_ST_INCREMENTAL_CRON`
dict keys, and the argument passed into `_spacetrack_snapshot_assets(...)` /
`_spacetrack_incremental_assets(...)`. All of these move to the long name in
lockstep so the wiring still resolves.

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

### 4. dbt

> Note: the 14 dbt bronze models / sources are all NASA + DONKI. Space-Track has
> no dbt models or `_bronze__sources.yml` entries yet, so the Space-Track rename
> is dlt + asset-key only — there is nothing to change under `dbt/` for it.

- Rename all 14 `dbt/models/bronze/bronze_<old>.sql` → `bronze_<new>.sql` and
  update each `source('bronze', '<old>')` → `source('bronze', '<new>')`.
- `_bronze__sources.yml` — each table `name:` → new table name, and each
  `dagster.asset_key` → `["dlt_<new>"]` (for `neo_lookup` the key is
  `["nasa_near_earth_object_lookups"]`, the plain asset's key — no `dlt_`).
- `_bronze__models.yml` — each `- name: bronze_<old>` → `bronze_<new>`.

### 5. Tests

- `tests/test_dbt_bronze.py` — update the expected `bronze_<t>` set to the new
  table names and the three lineage assertions (`dlt_nasa_api_neows` →
  `dlt_nasa_near_earth_object_feed`; `dlt_nasa_donki_cme` →
  `dlt_nasa_donki_coronal_mass_ejections`; `neo_lookup` →
  `nasa_near_earth_object_lookups`).

### 6. S3 migration (one-off)

A standalone boto3 script that, for each `(old, new)` pair:

1. Lists objects under `bronze/<old>/` and copies each to `bronze/<new>/`
   (Delta uses table-relative paths, so a straight prefix copy is valid).
2. Repeats for dlt child tables — any prefix matching `bronze/<old>__*` →
   `bronze/<new>__*` (e.g. `neo_lookup__close_approach_data`).
3. Deletes the old prefixes after the copy is verified.
4. Same treatment for `bronze/apod_images/` →
   `bronze/nasa_astronomy_picture_of_the_day_images/`.

dlt adopts the moved Delta tables on the next merge run (the renamed resource
opens the existing Delta table at the new path and merges into it). The old
`_dlt_*` metadata folders are left untouched and are harmless.

## Out of scope

- dlt **pipeline names** (`pipeline_name=...`) stay as-is — they only name
  local working dirs, not tables.
- No schema/column changes; models remain `select * exclude(_dlt_id,
  _dlt_load_id)` views.
- No silver/gold layer (none exists yet).

## Verification

- `dbt parse` / `dbt compile` succeeds with the renamed sources and models.
- `pytest tests/test_dbt_bronze.py` passes against the new names.
- The Dagster definitions load (`defs.resolve_asset_graph()`), all expected
  `bronze_<new>` keys present, lineage edges intact.
- Spot-check S3: new `bronze/<new>/` folders readable via `read_bronze_table`,
  old folders gone after migration.
