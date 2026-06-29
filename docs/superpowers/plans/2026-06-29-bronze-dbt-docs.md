# Bronze dbt Documentation + Space-Track Models Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fully document the bronze dbt layer (every table + every top-level column), replace dbt's generic docs landing page with a project README, and bring the six space-track tables into the dbt project so the catalog covers the full intended dataset.

**Architecture:** Space-track bronze models mirror the existing NASA pattern exactly — `select * exclude (_dlt_id, _dlt_load_id)` views over dlt-loaded Delta tables at `s3://auspex-lakehouse/bronze/{name}`. Documentation lives in dbt schema YAML (`description:` on tables and columns), shared column docs in a `{% docs %}` block file, and the landing page in a `{% docs __overview__ %}` block. Source→Dagster lineage is wired through the hardcoded `_SOURCE_ASSET_KEYS` dict in `transform/definitions.py` (NOT the YAML `meta` alone).

**Tech Stack:** dbt-duckdb (delta + httpfs extensions), dbt_utils, dlt (snake_case naming), Dagster + dagster_dbt, pytest.

## Global Constraints

- dbt model bodies are exactly: `{{ config(materialized='view') }}` then `select * exclude (_dlt_id, _dlt_load_id)` then `from {{ source('bronze', '<name>') }}`. (DuckDB `EXCLUDE` syntax; matches every existing model.)
- Sources stay in ONE `bronze` source block in `dbt/models/bronze/_bronze__sources.yml` (dbt forbids a duplicate source name). The templated `external_location: "delta_scan('s3://auspex-lakehouse/bronze/{name}')"` resolves `{name}` per table — confirmed correct for space-track (`bucket_url=s3://auspex-lakehouse`, `dataset_name=bronze`).
- Column names are dlt-snake_case-normalized. VERIFIED via dlt's own `snake_case` `NamingConvention`: `NORAD_CAT_ID→norad_cat_id`, `CDM_ID→cdm_id`, `MSG_EPOCH→msg_epoch`, `PRECEDENCE→precedence`, `COUNTRY→country`, `activityID→activity_id`, `associatedCMEID→associated_cmeid`, `gstID→gst_id`, `time21_5→time21_5`. Use these exact strings.
- The dbt manifest (`dbt/target/manifest.json`) is **gitignored and ephemeral**. After any model/source change you MUST run `dbt parse` to regenerate it before `pytest tests/test_dbt_bronze.py` (which reads the manifest via the Dagster asset graph).
- dbt commands run from the `dbt/` directory with `DBT_PROFILES_DIR=.`. Use `uv run --no-sync dbt ...`. Run `dbt deps` once before the first `dbt parse`.
- Nested API arrays become dlt **child tables** (separate tables, not parent columns); nested objects flatten into parent columns with a `__` separator. Document child tables in the parent table's `description`; document only the parent (top-level) columns as `columns:`.
- Space-track table names and NASA table names do not collide.
- Field lists are sourced from the public API docs (https://api.nasa.gov/, https://www.space-track.org/documentation#/api) + repo PK definitions. No live API calls, no warehouse connection. Docs build with `--empty-catalog`, so column docs are NOT schema-validated — accuracy is for usefulness, not to pass a check.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `dbt/models/bronze/bronze_{gp,satcat,boxscore,decay,cdm,tip}.sql` | New space-track pass-through view models (6 files) |
| `dbt/models/bronze/_bronze__sources.yml` | Single `bronze` source — add 6 space-track tables; add table descriptions (all 20) |
| `dbt/models/bronze/_bronze__nasa__models.yml` | NASA model tests + descriptions (the existing 14, moved here) |
| `dbt/models/bronze/_bronze__spacetrack__models.yml` | Space-track model tests + descriptions (6 new) |
| `dbt/models/bronze/_bronze__docs.md` | Shared `{% docs %}` blocks for repeated space-track identity columns |
| `dbt/models/overview.md` | `{% docs __overview__ %}` — project landing page |
| `src/auspex_lakehouse/transform/definitions.py:12` | Add 6 space-track entries to `_SOURCE_ASSET_KEYS` |
| `tests/test_dbt_bronze.py` | Expect 20 bronze models + space-track lineage sample |

The existing `dbt/models/bronze/_bronze__models.yml` is renamed/split into the two `_bronze__*__models.yml` files and deleted.

---

## Task 1: Scaffold space-track into the dbt project (models, sources, lineage, tests)

**Files:**
- Create: `dbt/models/bronze/bronze_gp.sql`, `bronze_satcat.sql`, `bronze_boxscore.sql`, `bronze_decay.sql`, `bronze_cdm.sql`, `bronze_tip.sql`
- Create: `dbt/models/bronze/_bronze__nasa__models.yml`, `dbt/models/bronze/_bronze__spacetrack__models.yml`
- Delete: `dbt/models/bronze/_bronze__models.yml`
- Modify: `dbt/models/bronze/_bronze__sources.yml` (add 6 source tables)
- Modify: `src/auspex_lakehouse/transform/definitions.py` (add 6 dict entries)
- Test: `tests/test_dbt_bronze.py`

**Interfaces:**
- Produces: 6 dbt models named `bronze_gp/satcat/boxscore/decay/cdm/tip`; source tables `gp/satcat/boxscore/decay/cdm/tip`; asset-graph lineage `dlt_spacetrack_<name>` → `bronze_<name>`.
- Consumes (existing): the 14 NASA model entries currently in `_bronze__models.yml` (tests only — see below).

- [ ] **Step 1: Write the failing test** — replace the body of `tests/test_dbt_bronze.py`:

```python
from dagster import AssetKey


def test_20_bronze_assets_with_lineage():
    from auspex_lakehouse.definitions import defs

    ag = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in ag.get_all_asset_keys()}
    expected = {
        f"bronze_{t}"
        for t in [
            # NASA
            "apod", "neows", "neo_lookup", "cme", "cme_analysis", "gst", "ips",
            "flr", "sep", "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
            # space-track
            "gp", "satcat", "boxscore", "decay", "cdm", "tip",
        ]
    }
    assert expected <= keys, f"missing: {expected - keys}"
    # lineage: a sample of the non-uniform source->dlt-key mapping
    assert AssetKey(["dlt_nasa_api_neows"]) in ag.get(AssetKey(["bronze_neows"])).parent_keys
    assert AssetKey(["dlt_nasa_donki_cme"]) in ag.get(AssetKey(["bronze_cme"])).parent_keys
    assert AssetKey(["neo_lookup"]) in ag.get(AssetKey(["bronze_neo_lookup"])).parent_keys
    assert AssetKey(["dlt_spacetrack_gp"]) in ag.get(AssetKey(["bronze_gp"])).parent_keys
    assert AssetKey(["dlt_spacetrack_cdm"]) in ag.get(AssetKey(["bronze_cdm"])).parent_keys
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/tbarnes/projects/python/auspex-lakehouse && uv run pytest tests/test_dbt_bronze.py -v`
Expected: FAIL — `bronze_gp` etc. missing from `keys` (models don't exist yet), and/or `KeyError`/`None` on `ag.get(AssetKey(["bronze_gp"]))`.

- [ ] **Step 3: Create the 6 space-track SQL models**

Each file `dbt/models/bronze/bronze_<name>.sql` (substitute `<name>` = gp, satcat, boxscore, decay, cdm, tip):

```sql
{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', '<name>') }}
```

- [ ] **Step 4: Add the 6 space-track tables to `_bronze__sources.yml`**

Append under `tables:` (after the existing `notifications` entry, keeping the file's single `bronze` source):

```yaml
      - name: gp
        meta: {dagster: {asset_key: ["dlt_spacetrack_gp"]}}
      - name: satcat
        meta: {dagster: {asset_key: ["dlt_spacetrack_satcat"]}}
      - name: boxscore
        meta: {dagster: {asset_key: ["dlt_spacetrack_boxscore"]}}
      - name: decay
        meta: {dagster: {asset_key: ["dlt_spacetrack_decay"]}}
      - name: cdm
        meta: {dagster: {asset_key: ["dlt_spacetrack_cdm"]}}
      - name: tip
        meta: {dagster: {asset_key: ["dlt_spacetrack_tip"]}}
```

- [ ] **Step 5: Add the 6 entries to `_SOURCE_ASSET_KEYS`** in `src/auspex_lakehouse/transform/definitions.py`

Inside the `_SOURCE_ASSET_KEYS` dict literal, after the DONKI dict-comprehension block (before the closing `}`), add:

```python
    "gp": AssetKey(["dlt_spacetrack_gp"]),
    "satcat": AssetKey(["dlt_spacetrack_satcat"]),
    "boxscore": AssetKey(["dlt_spacetrack_boxscore"]),
    "decay": AssetKey(["dlt_spacetrack_decay"]),
    "cdm": AssetKey(["dlt_spacetrack_cdm"]),
    "tip": AssetKey(["dlt_spacetrack_tip"]),
```

- [ ] **Step 6: Split the models YAML — create `_bronze__nasa__models.yml`**

Move the existing 14 model entries verbatim from `_bronze__models.yml` into a new file `dbt/models/bronze/_bronze__nasa__models.yml`:

```yaml
version: 2
models:
  - name: bronze_apod
    columns: [{name: date, data_tests: [not_null, unique]}]
  - name: bronze_neows
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [date, id]
    columns: [{name: date, data_tests: [not_null]}, {name: id, data_tests: [not_null]}]
  - name: bronze_neo_lookup
    columns: [{name: neo_reference_id, data_tests: [not_null, unique]}]
  - name: bronze_cme
    columns: [{name: activity_id, data_tests: [not_null, unique]}]
  - name: bronze_cme_analysis
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [associated_cmeid, time21_5]
    columns: [{name: associated_cmeid, data_tests: [not_null]}, {name: time21_5, data_tests: [not_null]}]
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

- [ ] **Step 7: Create `_bronze__spacetrack__models.yml`** with the 6 new models and their tests

```yaml
version: 2
models:
  - name: bronze_gp
    columns: [{name: norad_cat_id, data_tests: [not_null, unique]}]
  - name: bronze_satcat
    columns: [{name: norad_cat_id, data_tests: [not_null, unique]}]
  - name: bronze_boxscore
    columns: [{name: country, data_tests: [not_null]}]
  - name: bronze_decay
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [norad_cat_id, msg_epoch, precedence]
    columns:
      - {name: norad_cat_id, data_tests: [not_null]}
      - {name: msg_epoch, data_tests: [not_null]}
      - {name: precedence, data_tests: [not_null]}
  - name: bronze_cdm
    columns: [{name: cdm_id, data_tests: [not_null, unique]}]
  - name: bronze_tip
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [norad_cat_id, msg_epoch]
    columns:
      - {name: norad_cat_id, data_tests: [not_null]}
      - {name: msg_epoch, data_tests: [not_null]}
```

- [ ] **Step 8: Delete the old combined file**

```bash
rm dbt/models/bronze/_bronze__models.yml
```

- [ ] **Step 9: Regenerate the manifest, then run the test**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse/dbt
DBT_PROFILES_DIR=. uv run --no-sync dbt deps
DBT_PROFILES_DIR=. uv run --no-sync dbt parse
cd /Users/tbarnes/projects/python/auspex-lakehouse
uv run pytest tests/test_dbt_bronze.py -v
```
Expected: `dbt parse` writes a manifest with 20 models, no parse errors; pytest PASSES.

- [ ] **Step 10: Commit**

```bash
git add dbt/models/bronze/ src/auspex_lakehouse/transform/definitions.py tests/test_dbt_bronze.py
git commit -m "feat(dbt): add space-track bronze models with dlt lineage

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Document the NASA tables and columns

**Files:**
- Modify: `dbt/models/bronze/_bronze__nasa__models.yml` (add `description:` to each model + every top-level column)
- Modify: `dbt/models/bronze/_bronze__sources.yml` (add `description:` to the 14 NASA source tables)

**Interfaces:**
- Consumes: the model/source entries created in Task 1.
- Produces: rendered table + column descriptions in the docs catalog for all NASA tables.

Note: descriptions are added alongside the existing `data_tests`. Keep every existing test exactly as-is; only add `description:` keys and expand the compact `columns: [...]` inline lists into block form where columns gain descriptions.

- [ ] **Step 1: Add the NASA source-table descriptions** to `_bronze__sources.yml`

For each NASA table, add a `description:` line. Use these (one line each):

```yaml
      - name: apod
        description: "NASA Astronomy Picture of the Day — one record per calendar date (api.nasa.gov /planetary/apod)."
        meta: {dagster: {asset_key: ["dlt_nasa_api_apod"]}}
      - name: neows
        description: "Near-Earth Object Web Service daily feed — one row per asteroid per close-approach feed date (api.nasa.gov /neo/rest/v1/feed)."
        meta: {dagster: {asset_key: ["dlt_nasa_api_neows"]}}
      - name: neo_lookup
        description: "Per-object NEO lookups enriched with orbital data; 'not_found' rows are 404 tombstones (api.nasa.gov /neo/rest/v1/neo/{id})."
        meta: {dagster: {asset_key: ["neo_lookup"]}}
      - name: cme
        description: "DONKI Coronal Mass Ejection events (api.nasa.gov /DONKI/CME)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_cme"]}}
      - name: cme_analysis
        description: "DONKI CME Analysis — kinematic measurements per CME (api.nasa.gov /DONKI/CMEAnalysis)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_cme_analysis"]}}
      - name: gst
        description: "DONKI Geomagnetic Storm events (api.nasa.gov /DONKI/GST)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_gst"]}}
      - name: ips
        description: "DONKI Interplanetary Shock events (api.nasa.gov /DONKI/IPS)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_ips"]}}
      - name: flr
        description: "DONKI Solar Flare events (api.nasa.gov /DONKI/FLR)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_flr"]}}
      - name: sep
        description: "DONKI Solar Energetic Particle events (api.nasa.gov /DONKI/SEP)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_sep"]}}
      - name: mpc
        description: "DONKI Magnetopause Crossing events (api.nasa.gov /DONKI/MPC)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_mpc"]}}
      - name: rbe
        description: "DONKI Radiation Belt Enhancement events (api.nasa.gov /DONKI/RBE)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_rbe"]}}
      - name: hss
        description: "DONKI High Speed Stream events (api.nasa.gov /DONKI/HSS)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_hss"]}}
      - name: wsa_enlil_simulations
        description: "DONKI WSA-Enlil heliospheric model simulations (api.nasa.gov /DONKI/WSAEnlilSimulations)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_wsa_enlil_simulations"]}}
      - name: notifications
        description: "DONKI space-weather notifications/reports of all types (api.nasa.gov /DONKI/notifications)."
        meta: {dagster: {asset_key: ["dlt_nasa_donki_notifications"]}}
```

- [ ] **Step 2: Rewrite `_bronze__nasa__models.yml`** with table + column descriptions

Replace the file with the following (tests preserved verbatim; descriptions added). Child tables noted in each model's `description`:

```yaml
version: 2
models:
  - name: bronze_apod
    description: >
      Astronomy Picture of the Day — one row per calendar date. Pass-through view
      over the apod Delta table (dlt internal columns excluded).
    columns:
      - {name: date, description: "Publication date (YYYY-MM-DD). Primary key.", data_tests: [not_null, unique]}
      - {name: title, description: "Title of the day's image or video."}
      - {name: explanation, description: "NASA's written explanation of the image."}
      - {name: url, description: "URL of the standard-resolution image or video embed."}
      - {name: hdurl, description: "URL of the high-definition image (null for some entries / videos)."}
      - {name: media_type, description: "Asset type: 'image' or 'video'."}
      - {name: service_version, description: "APOD API service version (e.g. 'v1')."}
      - {name: copyright, description: "Copyright holder when the asset is not public domain (null otherwise)."}
  - name: bronze_neows
    description: >
      Near-Earth Object daily feed — one row per asteroid per feed date.
      Child table: close_approach_data (per close-approach event: epoch, relative
      velocity, miss distance, orbiting body). Estimated-diameter bounds are
      flattened into estimated_diameter__* columns.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [date, id]
    columns:
      - {name: date, description: "Feed date (YYYY-MM-DD) on which the object was reported close-approaching. Primary key with id.", data_tests: [not_null]}
      - {name: id, description: "NeoWs object id. Primary key with date.", data_tests: [not_null]}
      - {name: neo_reference_id, description: "Stable NEO reference id; join key to bronze_neo_lookup."}
      - {name: name, description: "Object designation / name."}
      - {name: nasa_jpl_url, description: "Link to the object's JPL Small-Body Database page."}
      - {name: absolute_magnitude_h, description: "Absolute magnitude (H) of the object."}
      - {name: is_potentially_hazardous_asteroid, description: "True if classified as a Potentially Hazardous Asteroid."}
      - {name: is_sentry_object, description: "True if present on the JPL Sentry impact-risk list."}
  - name: bronze_neo_lookup
    description: >
      Per-object NEO lookups enriched with orbital data. lookup_status='not_found'
      rows are 404 tombstones (only neo_reference_id, lookup_fetched_at, lookup_status
      populated). Child tables: close_approach_data, orbital_data (orbit_class nested).
    columns:
      - {name: neo_reference_id, description: "Stable NEO reference id. Primary key.", data_tests: [not_null, unique]}
      - {name: lookup_fetched_at, description: "ISO-8601 timestamp this lookup row was fetched (injected by the pipeline)."}
      - {name: lookup_status, description: "'ok' for a successful lookup; 'not_found' for a 404 tombstone row."}
      - {name: id, description: "NeoWs object id."}
      - {name: name, description: "Object designation / name."}
      - {name: designation, description: "Provisional or permanent designation."}
      - {name: nasa_jpl_url, description: "Link to the object's JPL Small-Body Database page."}
      - {name: absolute_magnitude_h, description: "Absolute magnitude (H) of the object."}
      - {name: is_potentially_hazardous_asteroid, description: "True if classified as a Potentially Hazardous Asteroid."}
      - {name: is_sentry_object, description: "True if present on the JPL Sentry impact-risk list."}
  - name: bronze_cme
    description: >
      DONKI Coronal Mass Ejection events. Child tables: cme_analyses, instruments,
      linked_events.
    columns:
      - {name: activity_id, description: "Unique CME activity identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: catalog, description: "Source catalog (e.g. 'M2M_CATALOG')."}
      - {name: start_time, description: "CME start time (ISO-8601 UTC)."}
      - {name: source_location, description: "Heliographic source location on the Sun."}
      - {name: active_region_num, description: "Associated NOAA active region number (null if none)."}
      - {name: note, description: "Analyst note."}
      - {name: submission_time, description: "Time the record was submitted to DONKI."}
      - {name: version_id, description: "Record version number."}
      - {name: link, description: "URL to the DONKI web record."}
  - name: bronze_cme_analysis
    description: >
      DONKI CME Analysis — kinematic fit per CME. Child table: enlil_list
      (associated WSA-Enlil simulation links).
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [associated_cmeid, time21_5]
    columns:
      - {name: associated_cmeid, description: "Activity id of the CME this analysis describes. Primary key with time21_5.", data_tests: [not_null]}
      - {name: time21_5, description: "Time the CME leading edge reached 21.5 solar radii (ISO-8601). Primary key with associated_cmeid.", data_tests: [not_null]}
      - {name: latitude, description: "Source latitude (degrees)."}
      - {name: longitude, description: "Source longitude (degrees)."}
      - {name: half_angle, description: "Half-angular width of the CME cone (degrees)."}
      - {name: speed, description: "CME radial speed (km/s)."}
      - {name: type, description: "CME speed/type classification (S, C, O, R, ER)."}
      - {name: is_most_accurate, description: "True if this is the preferred analysis for the CME."}
      - {name: note, description: "Analyst note."}
      - {name: catalog, description: "Source catalog."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the analysis was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_gst
    description: "DONKI Geomagnetic Storm events. Child tables: all_kp_index, linked_events."
    columns:
      - {name: gst_id, description: "Unique geomagnetic-storm identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: start_time, description: "Storm start time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_ips
    description: "DONKI Interplanetary Shock events. Child tables: instruments, linked_events."
    columns:
      - {name: activity_id, description: "Unique IPS activity identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: catalog, description: "Source catalog."}
      - {name: location, description: "Observation location (e.g. spacecraft / region)."}
      - {name: event_time, description: "Shock observation time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_flr
    description: "DONKI Solar Flare events. Child tables: instruments, linked_events."
    columns:
      - {name: flr_id, description: "Unique solar-flare identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: catalog, description: "Source catalog."}
      - {name: begin_time, description: "Flare begin time (ISO-8601 UTC)."}
      - {name: peak_time, description: "Flare peak time (ISO-8601 UTC)."}
      - {name: end_time, description: "Flare end time (ISO-8601 UTC; null if ongoing/unknown)."}
      - {name: class_type, description: "GOES soft X-ray flare class (e.g. 'M1.5', 'X2.0')."}
      - {name: source_location, description: "Heliographic source location on the Sun."}
      - {name: active_region_num, description: "Associated NOAA active region number (null if none)."}
      - {name: note, description: "Analyst note."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_sep
    description: "DONKI Solar Energetic Particle events. Child tables: instruments, linked_events."
    columns:
      - {name: sep_id, description: "Unique SEP identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: event_time, description: "SEP onset time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_mpc
    description: "DONKI Magnetopause Crossing events. Child tables: instruments, linked_events."
    columns:
      - {name: mpc_id, description: "Unique magnetopause-crossing identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: event_time, description: "Crossing time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_rbe
    description: "DONKI Radiation Belt Enhancement events. Child tables: instruments, linked_events."
    columns:
      - {name: rbe_id, description: "Unique radiation-belt-enhancement identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: event_time, description: "Enhancement onset time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_hss
    description: "DONKI High Speed Stream events. Child tables: instruments, linked_events."
    columns:
      - {name: hss_id, description: "Unique high-speed-stream identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: event_time, description: "Stream onset time (ISO-8601 UTC)."}
      - {name: link, description: "URL to the DONKI web record."}
      - {name: submission_time, description: "Time the record was submitted."}
      - {name: version_id, description: "Record version number."}
  - name: bronze_wsa_enlil_simulations
    description: >
      DONKI WSA-Enlil heliospheric model runs. Child tables: cme_inputs, impact_list.
    columns:
      - {name: simulation_id, description: "Unique simulation identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: model_completion_time, description: "Time the model run completed (ISO-8601 UTC)."}
      - {name: au, description: "Heliocentric distance of the model domain boundary (astronomical units)."}
      - {name: estimated_shock_arrival_time, description: "Estimated shock arrival time at Earth (ISO-8601; null if no Earth impact)."}
      - {name: estimated_duration, description: "Estimated disturbance duration (hours)."}
      - {name: rmin_re, description: "Minimum modeled magnetopause standoff distance (Earth radii)."}
      - {name: kp_18, description: "Predicted Kp index at +18h."}
      - {name: kp_90, description: "Predicted Kp index at +90h."}
      - {name: kp_135, description: "Predicted Kp index at +135h."}
      - {name: kp_180, description: "Predicted Kp index at +180h."}
      - {name: is_earth_gb, description: "True if Earth is glancing-blow impacted."}
      - {name: link, description: "URL to the DONKI web record."}
  - name: bronze_notifications
    description: "DONKI space-weather notifications/reports of all message types."
    columns:
      - {name: message_id, description: "Unique notification identifier. Primary key.", data_tests: [not_null, unique]}
      - {name: message_type, description: "Notification type (e.g. CME, FLR, GST, Report)."}
      - {name: message_url, description: "URL to the full notification on the DONKI site."}
      - {name: message_issue_time, description: "Time the notification was issued (ISO-8601 UTC)."}
      - {name: message_body, description: "Full notification text."}
```

- [ ] **Step 3: Verify the docs parse and build**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse/dbt
DBT_PROFILES_DIR=. uv run --no-sync dbt parse
DBT_PROFILES_DIR=. uv run --no-sync dbt docs generate --static --empty-catalog
```
Expected: clean parse (no duplicate-column / YAML errors); `target/static_index.html` regenerated. Open it and confirm NASA tables show table + column descriptions.

- [ ] **Step 4: Commit**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse
git add dbt/models/bronze/_bronze__nasa__models.yml dbt/models/bronze/_bronze__sources.yml
git commit -m "docs(dbt): describe NASA bronze tables and columns

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Document the space-track tables and columns

**Files:**
- Create: `dbt/models/bronze/_bronze__docs.md` (shared doc blocks)
- Modify: `dbt/models/bronze/_bronze__spacetrack__models.yml` (add descriptions; reference shared blocks)
- Modify: `dbt/models/bronze/_bronze__sources.yml` (add `description:` to the 6 space-track source tables)

**Interfaces:**
- Consumes: the space-track model/source entries from Task 1.
- Produces: rendered descriptions for all space-track tables/columns; reusable `norad_cat_id` / `object_name` / `object_id` / `object_type` doc blocks.

- [ ] **Step 1: Create `_bronze__docs.md`** with shared column doc blocks

```markdown
{% docs st_norad_cat_id %}
Satellite Catalog Number (NORAD ID) — the unique integer identifier assigned by US
Space Command to each tracked on-orbit object.
{% enddocs %}

{% docs st_object_name %}
Common name of the on-orbit object (satellite name or catalog designation).
{% enddocs %}

{% docs st_object_id %}
International Designator (COSPAR ID), e.g. '1998-067A' — launch year, launch number
of the year, and piece.
{% enddocs %}

{% docs st_object_type %}
Object classification: PAYLOAD, ROCKET BODY, DEBRIS, or UNKNOWN.
{% enddocs %}
```

- [ ] **Step 2: Add the space-track source-table descriptions** to `_bronze__sources.yml`

Add a `description:` to each of the 6 space-track source entries created in Task 1:

```yaml
      - name: gp
        description: "space-track General Perturbations — latest orbital element set (OMM/TLE) per catalogued object (space-track.org class 'gp')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_gp"]}}
      - name: satcat
        description: "space-track Satellite Catalog — current metadata for every catalogued object (space-track.org class 'satcat')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_satcat"]}}
      - name: boxscore
        description: "space-track Boxscore — on-orbit and decayed object counts by country/organization (space-track.org class 'boxscore')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_boxscore"]}}
      - name: decay
        description: "space-track Decay messages — predicted and observed re-entry/decay events (space-track.org class 'decay')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_decay"]}}
      - name: cdm
        description: "space-track public Conjunction Data Messages — predicted close approaches between objects (space-track.org class 'cdm_public')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_cdm"]}}
      - name: tip
        description: "space-track Tracking and Impact Prediction messages for decaying objects (space-track.org class 'tip')."
        meta: {dagster: {asset_key: ["dlt_spacetrack_tip"]}}
```

- [ ] **Step 3: Rewrite `_bronze__spacetrack__models.yml`** with table + column descriptions

Tests preserved verbatim; descriptions added; shared columns reference the doc blocks via `{{ doc(...) }}`:

```yaml
version: 2
models:
  - name: bronze_gp
    description: >
      Latest General Perturbations element set (OMM/TLE) per catalogued object —
      one row per NORAD ID. Source class 'gp'.
    columns:
      - {name: norad_cat_id, description: '{{ doc("st_norad_cat_id") }}', data_tests: [not_null, unique]}
      - {name: object_name, description: '{{ doc("st_object_name") }}'}
      - {name: object_id, description: '{{ doc("st_object_id") }}'}
      - {name: object_type, description: '{{ doc("st_object_type") }}'}
      - {name: classification_type, description: "Data classification: U (unclassified), C, or S."}
      - {name: rcs_size, description: "Radar cross-section size bucket: SMALL, MEDIUM, or LARGE."}
      - {name: country_code, description: "Owner/operator country or organization code."}
      - {name: launch_date, description: "Launch date (YYYY-MM-DD)."}
      - {name: site, description: "Launch site code."}
      - {name: decay_date, description: "Decay/re-entry date if decayed (null if on-orbit)."}
      - {name: epoch, description: "Epoch of the element set (ISO-8601 UTC)."}
      - {name: mean_motion, description: "Mean motion (revolutions per day)."}
      - {name: eccentricity, description: "Orbital eccentricity (dimensionless)."}
      - {name: inclination, description: "Orbital inclination (degrees)."}
      - {name: ra_of_asc_node, description: "Right ascension of the ascending node (degrees)."}
      - {name: arg_of_pericenter, description: "Argument of pericenter (degrees)."}
      - {name: mean_anomaly, description: "Mean anomaly at epoch (degrees)."}
      - {name: ephemeris_type, description: "Ephemeris type (0 for SGP4)."}
      - {name: element_set_no, description: "Element set number."}
      - {name: rev_at_epoch, description: "Revolution number at epoch."}
      - {name: bstar, description: "SGP4 B* drag term (1/earth radii)."}
      - {name: mean_motion_dot, description: "First derivative of mean motion (rev/day^2)."}
      - {name: mean_motion_ddot, description: "Second derivative of mean motion (rev/day^3)."}
      - {name: semimajor_axis, description: "Semi-major axis (km)."}
      - {name: period, description: "Orbital period (minutes)."}
      - {name: apoapsis, description: "Apoapsis altitude (km)."}
      - {name: periapsis, description: "Periapsis altitude (km)."}
      - {name: gp_id, description: "space-track internal GP record id."}
      - {name: tle_line0, description: "TLE line 0 (object name line)."}
      - {name: tle_line1, description: "TLE line 1."}
      - {name: tle_line2, description: "TLE line 2."}
      - {name: creation_date, description: "Time the element set was created (ISO-8601 UTC)."}
  - name: bronze_satcat
    description: >
      Satellite Catalog — current metadata for every catalogued object, one row per
      NORAD ID. Source class 'satcat' (CURRENT='Y').
    columns:
      - {name: norad_cat_id, description: '{{ doc("st_norad_cat_id") }}', data_tests: [not_null, unique]}
      - {name: object_id, description: '{{ doc("st_object_id") }}'}
      - {name: object_name, description: '{{ doc("st_object_name") }}'}
      - {name: object_type, description: '{{ doc("st_object_type") }}'}
      - {name: intldes, description: "International designator (legacy field; same value as object_id)."}
      - {name: satname, description: "Satellite name (legacy field; same value as object_name)."}
      - {name: country, description: "Owner/operator country or organization code."}
      - {name: launch, description: "Launch date (YYYY-MM-DD)."}
      - {name: site, description: "Launch site code."}
      - {name: decay, description: "Decay/re-entry date if decayed (null if on-orbit)."}
      - {name: period, description: "Orbital period (minutes)."}
      - {name: inclination, description: "Orbital inclination (degrees)."}
      - {name: apogee, description: "Apogee altitude (km)."}
      - {name: perigee, description: "Perigee altitude (km)."}
      - {name: rcs_size, description: "Radar cross-section size bucket: SMALL, MEDIUM, or LARGE."}
      - {name: rcsvalue, description: "Numeric radar cross-section value (m^2) where available."}
      - {name: launch_year, description: "Year of launch."}
      - {name: launch_num, description: "Launch number within the year."}
      - {name: launch_piece, description: "Piece designator within the launch."}
      - {name: current, description: "'Y' if this is the current catalog record."}
      - {name: object_number, description: "Object number (same as norad_cat_id where present)."}
      - {name: comment, description: "Catalog comment."}
      - {name: file, description: "space-track internal file/batch id."}
  - name: bronze_boxscore
    description: >
      Boxscore — counts of on-orbit and decayed objects by country/organization.
      Full-replace snapshot (no per-row history). Source class 'boxscore'.
    columns:
      - {name: country, description: "Country/organization name or 'ALL' for the global total. Identity column.", data_tests: [not_null]}
      - {name: spadoc_cd, description: "SPADOC country/organization code."}
      - {name: orbital_tba, description: "On-orbit objects to-be-assigned a catalog entry."}
      - {name: orbital_payload_count, description: "On-orbit payload count."}
      - {name: orbital_rocket_body_count, description: "On-orbit rocket-body count."}
      - {name: orbital_debris_count, description: "On-orbit debris count."}
      - {name: orbital_total_count, description: "Total on-orbit object count."}
      - {name: decayed_payload_count, description: "Decayed payload count."}
      - {name: decayed_rocket_body_count, description: "Decayed rocket-body count."}
      - {name: decayed_debris_count, description: "Decayed debris count."}
      - {name: decayed_total_count, description: "Total decayed object count."}
      - {name: country_total, description: "Grand total of objects attributed to the country/organization."}
  - name: bronze_decay
    description: >
      Decay messages — predicted and observed re-entry events. One row per
      (norad_cat_id, msg_epoch, precedence). Source class 'decay'.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [norad_cat_id, msg_epoch, precedence]
    columns:
      - {name: norad_cat_id, description: '{{ doc("st_norad_cat_id") }}', data_tests: [not_null]}
      - {name: msg_epoch, description: "Timestamp the decay message was generated (ISO-8601 UTC). Part of the primary key.", data_tests: [not_null]}
      - {name: precedence, description: "Message precedence/priority; disambiguates messages sharing a norad_cat_id and msg_epoch. Part of the primary key.", data_tests: [not_null]}
      - {name: object_number, description: "Object number (same as norad_cat_id)."}
      - {name: object_name, description: '{{ doc("st_object_name") }}'}
      - {name: intldes, description: "International designator (COSPAR ID)."}
      - {name: rcs, description: "Radar cross-section value (m^2) where available."}
      - {name: rcs_size, description: "Radar cross-section size bucket."}
      - {name: country, description: "Owner/operator country or organization code."}
      - {name: decay_epoch, description: "Predicted or observed decay/re-entry time (ISO-8601 UTC)."}
      - {name: source, description: "Source of the decay message."}
      - {name: msg_type, description: "Message type (e.g. prediction vs observation)."}
  - name: bronze_cdm
    description: >
      Public Conjunction Data Messages — predicted close approaches between two
      objects. One row per CDM. Source class 'cdm_public'.
    columns:
      - {name: cdm_id, description: "Unique conjunction-data-message id. Primary key.", data_tests: [not_null, unique]}
      - {name: created, description: "Time the CDM was created (ISO-8601 UTC)."}
      - {name: emergency_reportable, description: "'Y' if the conjunction meets emergency-reporting criteria."}
      - {name: tca, description: "Time of closest approach (ISO-8601 UTC)."}
      - {name: min_rng, description: "Minimum range between the two objects at TCA (km)."}
      - {name: pc, description: "Probability of collision (dimensionless)."}
      - {name: sat_1_id, description: "NORAD catalog id of the primary object."}
      - {name: sat_1_name, description: "Name of the primary object."}
      - {name: sat1_object_type, description: "Object type of the primary object."}
      - {name: sat1_rcs, description: "Radar cross-section size of the primary object."}
      - {name: sat_1_excl_vol, description: "Exclusion-volume radius of the primary object (km)."}
      - {name: sat_2_id, description: "NORAD catalog id of the secondary object."}
      - {name: sat_2_name, description: "Name of the secondary object."}
      - {name: sat2_object_type, description: "Object type of the secondary object."}
      - {name: sat2_rcs, description: "Radar cross-section size of the secondary object."}
      - {name: sat_2_excl_vol, description: "Exclusion-volume radius of the secondary object (km)."}
  - name: bronze_tip
    description: >
      Tracking and Impact Prediction messages for decaying objects. One row per
      (norad_cat_id, msg_epoch). Source class 'tip'.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [norad_cat_id, msg_epoch]
    columns:
      - {name: norad_cat_id, description: '{{ doc("st_norad_cat_id") }}', data_tests: [not_null]}
      - {name: msg_epoch, description: "Timestamp the TIP message was generated (ISO-8601 UTC). Part of the primary key.", data_tests: [not_null]}
      - {name: insert_epoch, description: "Timestamp the message was inserted into space-track (ISO-8601 UTC)."}
      - {name: decay_epoch, description: "Predicted decay/impact time (ISO-8601 UTC)."}
      - {name: window, description: "Prediction window half-width (minutes)."}
      - {name: rev, description: "Revolution number at predicted decay."}
      - {name: direction, description: "Ascending/descending pass direction at predicted decay."}
      - {name: lat, description: "Predicted impact latitude (degrees)."}
      - {name: lon, description: "Predicted impact longitude (degrees)."}
      - {name: incl, description: "Orbital inclination (degrees)."}
      - {name: next_report, description: "Hours until the next TIP report is expected."}
      - {name: id, description: "space-track internal TIP record id."}
      - {name: high_interest, description: "'Y' if the object is flagged high-interest."}
      - {name: object_number, description: "Object number (same as norad_cat_id)."}
```

- [ ] **Step 4: Verify the docs parse and build**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse/dbt
DBT_PROFILES_DIR=. uv run --no-sync dbt parse
DBT_PROFILES_DIR=. uv run --no-sync dbt docs generate --static --empty-catalog
```
Expected: clean parse (the `{{ doc(...) }}` refs resolve against `_bronze__docs.md`); `static_index.html` regenerated. Confirm space-track tables show descriptions and the shared `norad_cat_id` description renders identically across gp/satcat/decay/tip.

- [ ] **Step 5: Commit**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse
git add dbt/models/bronze/_bronze__spacetrack__models.yml dbt/models/bronze/_bronze__docs.md dbt/models/bronze/_bronze__sources.yml
git commit -m "docs(dbt): describe space-track bronze tables and columns

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Project README / docs landing page

**Files:**
- Create: `dbt/models/overview.md` (`{% docs __overview__ %}` block)

**Interfaces:**
- Consumes: nothing (standalone doc block).
- Produces: replaces dbt's generic docs landing page with a project overview.

dbt searches all `model-paths` for `.md` doc blocks, so `dbt/models/overview.md` is picked up automatically; the special block name `__overview__` overrides the catalog landing page.

- [ ] **Step 1: Create `dbt/models/overview.md`**

```markdown
{% docs __overview__ %}

# Auspex Lakehouse — Bronze Layer

This catalog documents the **bronze** layer of the Auspex lakehouse: a space-domain
awareness and space-weather data warehouse built from public NASA and US Space Force
(space-track.org) APIs.

## How data flows

```
External APIs ──(dlt extract)──▶ Delta tables on S3 ──(dbt views)──▶ bronze_* models
                                  s3://auspex-lakehouse/bronze/{table}
```

- **Extract:** [dlt](https://dlthub.com) pipelines (orchestrated by Dagster) pull from
  each API and write **Delta Lake** tables to object storage under
  `s3://auspex-lakehouse/bronze/`.
- **Model:** each `bronze_<table>` here is a thin dbt **view** —
  `select * exclude (_dlt_id, _dlt_load_id)` over its Delta table. Bronze preserves the
  source shape; it does not clean or reshape data.
- **Lineage:** every bronze model traces back to its Dagster dlt asset
  (`dlt_nasa_*` / `dlt_spacetrack_*`) via the source `meta.dagster.asset_key`.

## Conventions

- **Naming:** dlt normalizes source field names to `snake_case`
  (`NORAD_CAT_ID` → `norad_cat_id`, `activityID` → `activity_id`).
- **Nested data:** nested JSON arrays are split into dlt **child tables** (documented
  in each parent table's description); nested objects are flattened into the parent with
  a `__` separator.
- **dlt bookkeeping columns** (`_dlt_id`, `_dlt_load_id`) are excluded from every model.

## Data sources

### NASA — [api.nasa.gov](https://api.nasa.gov/)

- **APOD** — Astronomy Picture of the Day (`bronze_apod`).
- **NeoWs** — Near-Earth Object feed and per-object lookups
  (`bronze_neows`, `bronze_neo_lookup`).
- **DONKI** — Space Weather Database Of Notifications, Knowledge, Information:
  CME, CME analysis, geomagnetic storms, interplanetary shocks, solar flares,
  solar energetic particles, magnetopause crossings, radiation-belt enhancements,
  high-speed streams, WSA-Enlil simulations, and notifications
  (`bronze_cme`, `bronze_cme_analysis`, `bronze_gst`, `bronze_ips`, `bronze_flr`,
  `bronze_sep`, `bronze_mpc`, `bronze_rbe`, `bronze_hss`,
  `bronze_wsa_enlil_simulations`, `bronze_notifications`).

### space-track.org — [API documentation](https://www.space-track.org/documentation#/api)

- **gp** — latest orbital element sets (OMM/TLE) per object (`bronze_gp`).
- **satcat** — satellite catalog metadata (`bronze_satcat`).
- **boxscore** — object counts by country/organization (`bronze_boxscore`).
- **decay** — re-entry/decay messages (`bronze_decay`).
- **cdm** — public conjunction data messages (`bronze_cdm`).
- **tip** — tracking and impact predictions (`bronze_tip`).

## Navigating this catalog

- **Sources** (left nav → Sources) are the raw Delta tables; **Models** are the
  `bronze_*` views built on them.
- Use the **lineage graph** (bottom-right icon on any model) to see the dlt asset → Delta
  table → bronze view chain.

{% enddocs %}
```

- [ ] **Step 2: Verify the overview renders**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse/dbt
DBT_PROFILES_DIR=. uv run --no-sync dbt parse
DBT_PROFILES_DIR=. uv run --no-sync dbt docs generate --static --empty-catalog
```
Expected: clean parse; open `target/static_index.html` and confirm the landing page shows the "Auspex Lakehouse — Bronze Layer" overview instead of dbt's generic boilerplate.

- [ ] **Step 3: Full verification sweep**

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse
uv run pytest tests/test_dbt_bronze.py -v
```
Expected: PASS (20 models + lineage).

- [ ] **Step 4: Commit**

```bash
git add dbt/models/overview.md
git commit -m "docs(dbt): add project README overview for dbt docs landing page

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (run after all tasks)

```bash
cd /Users/tbarnes/projects/python/auspex-lakehouse/dbt
DBT_PROFILES_DIR=. uv run --no-sync dbt deps
DBT_PROFILES_DIR=. uv run --no-sync dbt parse          # 20 models, clean
DBT_PROFILES_DIR=. uv run --no-sync dbt compile        # all models resolve
DBT_PROFILES_DIR=. uv run --no-sync dbt docs generate --static --empty-catalog
cd /Users/tbarnes/projects/python/auspex-lakehouse
uv run pytest tests/test_dbt_bronze.py -v              # green
```

Manually open `dbt/target/static_index.html` and confirm: custom overview landing page; every table and column described; space-track `norad_cat_id` description shared across models; lineage graphs intact.
