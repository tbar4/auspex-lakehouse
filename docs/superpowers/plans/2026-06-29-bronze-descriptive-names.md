# Bronze Descriptive Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename every bronze table/asset/model from short acronyms to descriptive, provider-prefixed names across dlt, Dagster, and dbt — keeping the test suite green after each provider group.

**Architecture:** A pure coordinated rename. The dlt `@dlt.resource(name=...)` is the single knob driving the physical S3 Delta path, the dbt source name, and the `read_bronze_table()` argument; the dbt model is `bronze_<table>`; the Dagster asset key is `dlt_<table>` (set by overriding each dlt translator). Work proceeds in three provider-group tasks (NASA planetary/NeoWs, NASA DONKI, Space-Track) so the full pytest suite is green at every commit, then docs, then a runtime re-ingest runbook.

**Tech Stack:** Python 3, dlt 1.28.1, deltalake 0.18.0, dagster + dagster-dlt 0.29.11 + dagster-dbt, dbt (DuckDB delta_scan), MinIO/S3, pytest.

## Global Constraints

- **Canonical name mapping (old → new table).** This is the single source of truth; every layer uses it.

  NASA planetary / NeoWs (plain `nasa_`):
  - `apod` → `nasa_astronomy_picture_of_the_day`
  - `neows` → `nasa_near_earth_object_feed`
  - `neo_lookup` → `nasa_near_earth_object_lookups`

  NASA DONKI (`nasa_donki_`):
  - `cme` → `nasa_donki_coronal_mass_ejections`
  - `cme_analysis` → `nasa_donki_coronal_mass_ejection_analyses`
  - `gst` → `nasa_donki_geomagnetic_storms`
  - `ips` → `nasa_donki_interplanetary_shocks`
  - `flr` → `nasa_donki_solar_flares`
  - `sep` → `nasa_donki_solar_energetic_particles`
  - `mpc` → `nasa_donki_magnetopause_crossings`
  - `rbe` → `nasa_donki_radiation_belt_enhancements`
  - `hss` → `nasa_donki_high_speed_streams`
  - `wsa_enlil_simulations` → `nasa_donki_wsa_enlil_simulations`
  - `notifications` → `nasa_donki_notifications`

  Space-Track (`space_track_`):
  - `gp` → `space_track_general_perturbations`
  - `satcat` → `space_track_satellite_catalog`
  - `boxscore` → `space_track_boxscore`
  - `decay` → `space_track_decays`
  - `cdm` → `space_track_conjunction_data_messages`
  - `tip` → `space_track_tracking_and_impact_predictions`

  Adjacent asset: `apod_images` → `nasa_astronomy_picture_of_the_day_images` (asset name + `bronze/apod_images/` S3 prefix).

- **dbt model name** = `bronze_<new table>` (keep the `bronze_` layer prefix).
- **Dagster asset key** = `dlt_<new table>` for all dlt assets (e.g. `dlt_nasa_donki_coronal_mass_ejections`, `dlt_space_track_general_perturbations`). The two plain `@asset`s use their bare name as the key: `nasa_near_earth_object_lookups` and `nasa_astronomy_picture_of_the_day_images` (no `dlt_`).
- **Do NOT change:** dlt `cls` / Space-Track API class strings (the 2nd column of `SNAPSHOT_CLASSES`/`INCREMENTAL_CLASSES` and the URL class in `query_class`) — `"gp"`, `"satcat"`, `"boxscore"`, `"decay"`, `"cdm_public"`, `"tip"` stay; they are API identifiers, not table names. Likewise leave column names (`neo_reference_id`, `time21_5`, etc.) untouched.
- **Migration is re-ingest, not file-move** — no S3 Delta surgery in this plan; see Task 5.
- **macOS sed:** use `sed -i ''` (BSD). Commands below assume zsh on darwin.
- **Commit cadence:** one commit per task. Run the FULL suite (`uv run pytest -q`) before each commit; it must be green.

---

### Task 1: Rename NASA planetary / NeoWs chain (apod, neows, neo_lookup, apod_images)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/apod.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neows.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/neo_lookup.py`
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py` (NasaDltTranslator, apod_images asset, neo_lookup asset, _existing_lookup_index)
- Modify: `src/auspex_lakehouse/transform/definitions.py` (3 `_SOURCE_ASSET_KEYS` entries)
- Rename+Modify: `dbt/models/bronze/bronze_apod.sql`, `bronze_neows.sql`, `bronze_neo_lookup.sql`
- Modify: `dbt/models/bronze/_bronze__sources.yml` (3 entries), `_bronze__models.yml` (3 entries)
- Test: `tests/test_nasa_sources.py`, `tests/test_neo_lookup_asset.py`, `tests/test_dbt_bronze.py`

**Interfaces:**
- Produces: dlt resource/table names `nasa_astronomy_picture_of_the_day`, `nasa_near_earth_object_feed`, `nasa_near_earth_object_lookups`; asset keys `dlt_nasa_astronomy_picture_of_the_day`, `dlt_nasa_near_earth_object_feed`, plain asset keys `nasa_near_earth_object_lookups` and `nasa_astronomy_picture_of_the_day_images`. Tasks 2–3 and the lineage bridge reuse the `dlt_<table>` convention.

- [ ] **Step 1: Update the failing tests first (red)**

`tests/test_nasa_sources.py` line 15: change
```python
    assert set(src.resources.keys()) == {"apod", "neows"}
```
to
```python
    assert set(src.resources.keys()) == {
        "nasa_astronomy_picture_of_the_day",
        "nasa_near_earth_object_feed",
    }
```
`tests/test_nasa_sources.py` line 26: change `assert res.name == "neo_lookup"` to
```python
    assert res.name == "nasa_near_earth_object_lookups"
```

`tests/test_neo_lookup_asset.py`: replace both `AssetKey(["neo_lookup"])` (lines 8 and 20) with `AssetKey(["nasa_near_earth_object_lookups"])`, and line 11 `AssetKey(["dlt_nasa_api_neows"])` with `AssetKey(["dlt_nasa_near_earth_object_feed"])`.

`tests/test_dbt_bronze.py`: in the `for t in [...]` list, replace the first three entries `"apod", "neows", "neo_lookup"` with `"nasa_astronomy_picture_of_the_day", "nasa_near_earth_object_feed", "nasa_near_earth_object_lookups"` (leave the 11 DONKI entries unchanged for now). Then update the two NASA lineage assertions:
```python
    assert AssetKey(["dlt_nasa_near_earth_object_feed"]) in ag.get(AssetKey(["bronze_nasa_near_earth_object_feed"])).parent_keys
    assert AssetKey(["nasa_near_earth_object_lookups"]) in ag.get(AssetKey(["bronze_nasa_near_earth_object_lookups"])).parent_keys
```
(Leave the `dlt_nasa_donki_cme` / `bronze_cme` assertion unchanged for now.)

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `uv run pytest tests/test_nasa_sources.py tests/test_neo_lookup_asset.py tests/test_dbt_bronze.py -q`
Expected: FAIL (resource names / asset keys not yet renamed).

- [ ] **Step 3: Rename the dlt resource names**

`apod.py` — change `name="apod"` to `name="nasa_astronomy_picture_of_the_day"`:
```python
@dlt.resource(name="nasa_astronomy_picture_of_the_day", write_disposition="merge", primary_key="date", table_format="delta")
def apod(start_date: date, end_date: date):
```
`neows.py` — change `name="neows"` to `name="nasa_near_earth_object_feed"`:
```python
@dlt.resource(
    name="nasa_near_earth_object_feed",
    write_disposition="merge",
    primary_key=["date", "id"],
    table_format="delta",
)
def neows(start_date: date, end_date: date):
```
`neo_lookup.py` — change `name="neo_lookup"` on `neo_lookup_rows` to `name="nasa_near_earth_object_lookups"`:
```python
@dlt.resource(
    name="nasa_near_earth_object_lookups",
    write_disposition="merge",
    primary_key="neo_reference_id",
    table_format="delta",
)
def neo_lookup_rows(rows: list[dict]):
```
(Keep the Python symbol names `apod`, `neows`, `neo_lookup_rows` and all pipeline names as-is.)

- [ ] **Step 4: Override the NASA translator key + rename the plain assets**

In `src/auspex_lakehouse/bronze/dlt/assets.py`, update `NasaDltTranslator` to emit clean keys:
```python
class NasaDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_{data.resource.name}"),
            automation_condition=AutomationCondition.on_cron("0 6 * * *"),
        )
```
`apod_images` asset — rename + repoint dep + table read + S3 prefix:
```python
@asset(
    name="nasa_astronomy_picture_of_the_day_images",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_astronomy_picture_of_the_day"])],
    automation_condition=AutomationCondition.eager(),
)
def apod_images(context: AssetExecutionContext):
    partition_key = context.partition_key

    df = read_bronze_table("nasa_astronomy_picture_of_the_day").filter(pl.col("date") == partition_key)
```
…and in the same function change the object key:
```python
        object_key = f"bronze/nasa_astronomy_picture_of_the_day_images/{partition_key}_{filename}"
```
`_existing_lookup_index()` — change both table-name strings:
```python
    if not bronze_table_exists("nasa_near_earth_object_lookups"):
        return {}
    df = read_bronze_table("nasa_near_earth_object_lookups").select(["neo_reference_id", "lookup_fetched_at"])
```
`neo_lookup` asset — rename + repoint dep + table read:
```python
@asset(
    name="nasa_near_earth_object_lookups",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_near_earth_object_feed"])],
    automation_condition=AutomationCondition.eager(),
    pool=NASA_API_POOL,
)
def neo_lookup(context: AssetExecutionContext):
    partition_key = context.partition_key
    # neows table is guaranteed to exist by the dlt_nasa_near_earth_object_feed dep above.
    candidates = {
        str(neo_id)
        for neo_id in read_bronze_table("nasa_near_earth_object_feed")
        .filter(pl.col("date") == partition_key)
        .get_column("neo_reference_id")
        .to_list()
    }
```
(Keep the Python function name `neo_lookup` and the `nasa_neo_lookup_pipeline` import/usage as-is.)

- [ ] **Step 5: Update the lineage bridge (3 NASA entries)**

In `src/auspex_lakehouse/transform/definitions.py`, change the first three `_SOURCE_ASSET_KEYS` entries:
```python
_SOURCE_ASSET_KEYS = {
    "nasa_astronomy_picture_of_the_day": AssetKey(["dlt_nasa_astronomy_picture_of_the_day"]),
    "nasa_near_earth_object_feed": AssetKey(["dlt_nasa_near_earth_object_feed"]),
    "nasa_near_earth_object_lookups": AssetKey(["nasa_near_earth_object_lookups"]),
    **{
        t: AssetKey([f"dlt_nasa_donki_{t}"])
        for t in [ ... ]   # DO NOT EDIT in Task 1 — the existing 11-name DONKI list stays verbatim
    },
}
```
Only the three NASA `_SOURCE_ASSET_KEYS` entries change in this task. The DONKI dict-comprehension (the `f"dlt_nasa_donki_{t}"` block and its 11-name list) is left exactly as it is in the file — Task 2 rewrites it.

- [ ] **Step 6: Rename the 3 dbt model files + their source() refs**

Run:
```bash
cd dbt/models/bronze
git mv bronze_apod.sql bronze_nasa_astronomy_picture_of_the_day.sql
git mv bronze_neows.sql bronze_nasa_near_earth_object_feed.sql
git mv bronze_neo_lookup.sql bronze_nasa_near_earth_object_lookups.sql
sed -i '' "s/source('bronze', 'apod')/source('bronze', 'nasa_astronomy_picture_of_the_day')/" bronze_nasa_astronomy_picture_of_the_day.sql
sed -i '' "s/source('bronze', 'neows')/source('bronze', 'nasa_near_earth_object_feed')/" bronze_nasa_near_earth_object_feed.sql
sed -i '' "s/source('bronze', 'neo_lookup')/source('bronze', 'nasa_near_earth_object_lookups')/" bronze_nasa_near_earth_object_lookups.sql
cd -
```

- [ ] **Step 7: Update the dbt sources + models yml (3 entries each)**

In `dbt/models/bronze/_bronze__sources.yml`, replace the apod/neows/neo_lookup table blocks:
```yaml
      - name: nasa_astronomy_picture_of_the_day
        meta: {dagster: {asset_key: ["dlt_nasa_astronomy_picture_of_the_day"]}}
      - name: nasa_near_earth_object_feed
        meta: {dagster: {asset_key: ["dlt_nasa_near_earth_object_feed"]}}
      - name: nasa_near_earth_object_lookups
        meta: {dagster: {asset_key: ["nasa_near_earth_object_lookups"]}}
```
In `dbt/models/bronze/_bronze__models.yml`, rename the three model headers (keep their `columns`/`data_tests` bodies):
- `- name: bronze_apod` → `- name: bronze_nasa_astronomy_picture_of_the_day`
- `- name: bronze_neows` → `- name: bronze_nasa_near_earth_object_feed`
- `- name: bronze_neo_lookup` → `- name: bronze_nasa_near_earth_object_lookups`

- [ ] **Step 8: Run the full suite (green)**

Run: `uv run pytest -q`
Expected: PASS (DONKI + Space-Track still on old names and internally consistent).

If `test_dbt_bronze.py` needs a dbt manifest, generate it first: `cd dbt && uv run dbt parse && cd -`, then re-run.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor(bronze): descriptive rename of NASA planetary/NeoWs tables

apod->nasa_astronomy_picture_of_the_day, neows->nasa_near_earth_object_feed,
neo_lookup->nasa_near_earth_object_lookups, apod_images->..._images.
dlt resources, asset keys (dlt_<table>), plain assets, lineage bridge, dbt
sources/models, and tests renamed in lockstep.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Rename NASA DONKI chain (11 tables)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/nasa/donki.py` (`DONKI_ENDPOINTS`)
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py` (`DonkiDltTranslator`)
- Modify: `src/auspex_lakehouse/transform/definitions.py` (DONKI comprehension)
- Rename+Modify: 11 `dbt/models/bronze/bronze_<donki>.sql`
- Modify: `dbt/models/bronze/_bronze__sources.yml` (11 entries), `_bronze__models.yml` (11 entries)
- Test: `tests/test_donki.py`, `tests/test_donki_asset.py`, `tests/test_dbt_bronze.py`

**Interfaces:**
- Consumes: `dlt_<table>` key convention + `DonkiDltTranslator` key-override pattern from Task 1.
- Produces: table/asset names `nasa_donki_<entity>` for all 11 DONKI resources; asset keys `dlt_nasa_donki_<entity>`.

- [ ] **Step 1: Update the failing tests first (red)**

`tests/test_donki_asset.py` — replace the `DONKI_KEYS` list (lines 3–15) with:
```python
DONKI_KEYS = [
    "dlt_nasa_donki_coronal_mass_ejections",
    "dlt_nasa_donki_coronal_mass_ejection_analyses",
    "dlt_nasa_donki_geomagnetic_storms",
    "dlt_nasa_donki_interplanetary_shocks",
    "dlt_nasa_donki_solar_flares",
    "dlt_nasa_donki_solar_energetic_particles",
    "dlt_nasa_donki_magnetopause_crossings",
    "dlt_nasa_donki_radiation_belt_enhancements",
    "dlt_nasa_donki_high_speed_streams",
    "dlt_nasa_donki_wsa_enlil_simulations",
    "dlt_nasa_donki_notifications",
]
```
…and replace both occurrences of `AssetKey(["dlt_nasa_donki_cme"])` (lines 29 and 35) with `AssetKey(["dlt_nasa_donki_coronal_mass_ejections"])`.

`tests/test_donki.py` — update the resource-name assertions to the new names. Change the `_donki_resource(...)` first arguments and `res.name`/`DONKI_ENDPOINTS` checks:
- `_donki_resource("cme", "CME", "activityID")` → `_donki_resource("nasa_donki_coronal_mass_ejections", "CME", "activityID")`
- `_donki_resource("gst", "GST", "gstID")` → `_donki_resource("nasa_donki_geomagnetic_storms", "GST", "gstID")`
- `_donki_resource("notifications", "notifications", "messageID", {"type": "all"})` → first arg `"nasa_donki_notifications"`
- `_donki_resource("cme_analysis", "CMEAnalysis", ["associatedCMEID", "time21_5"])` → first arg `"nasa_donki_coronal_mass_ejection_analyses"`; and `assert res.name == "cme_analysis"` → `== "nasa_donki_coronal_mass_ejection_analyses"`
- the `notif = next(e for e in DONKI_ENDPOINTS if e[0] == "notifications")` → `== "nasa_donki_notifications"` (the `notif[1] == "notifications"` endpoint-path check stays — that's the API path, unchanged)
- the `src.resources.keys()` set (lines 72–75) → the 11 new names:
```python
    assert set(src.resources.keys()) == {
        "nasa_donki_coronal_mass_ejections", "nasa_donki_coronal_mass_ejection_analyses",
        "nasa_donki_geomagnetic_storms", "nasa_donki_interplanetary_shocks",
        "nasa_donki_solar_flares", "nasa_donki_solar_energetic_particles",
        "nasa_donki_magnetopause_crossings", "nasa_donki_radiation_belt_enhancements",
        "nasa_donki_high_speed_streams", "nasa_donki_wsa_enlil_simulations",
        "nasa_donki_notifications",
    }
```

`tests/test_dbt_bronze.py` — in the `for t in [...]` list replace the 11 DONKI entries (`"cme"` … `"notifications"`) with the 11 `nasa_donki_<entity>` names, and update the DONKI lineage assertion:
```python
    assert AssetKey(["dlt_nasa_donki_coronal_mass_ejections"]) in ag.get(AssetKey(["bronze_nasa_donki_coronal_mass_ejections"])).parent_keys
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `uv run pytest tests/test_donki.py tests/test_donki_asset.py tests/test_dbt_bronze.py -q`
Expected: FAIL.

- [ ] **Step 3: Rename `DONKI_ENDPOINTS` resource names**

In `src/auspex_lakehouse/bronze/dlt/sources/nasa/donki.py`, replace the `DONKI_ENDPOINTS` list (the first tuple column is the resource/table name; the 2nd column is the API endpoint path — leave it unchanged):
```python
DONKI_ENDPOINTS = [
    ("nasa_donki_coronal_mass_ejections",         "CME",                 "activityID",                     None),
    ("nasa_donki_coronal_mass_ejection_analyses", "CMEAnalysis",         ["associatedCMEID", "time21_5"],  None),
    ("nasa_donki_geomagnetic_storms",             "GST",                 "gstID",                          None),
    ("nasa_donki_interplanetary_shocks",          "IPS",                 "activityID",                     None),
    ("nasa_donki_solar_flares",                   "FLR",                 "flrID",                          None),
    ("nasa_donki_solar_energetic_particles",      "SEP",                 "sepID",                          None),
    ("nasa_donki_magnetopause_crossings",         "MPC",                 "mpcID",                          None),
    ("nasa_donki_radiation_belt_enhancements",    "RBE",                 "rbeID",                          None),
    ("nasa_donki_high_speed_streams",             "HSS",                 "hssID",                          None),
    ("nasa_donki_wsa_enlil_simulations",          "WSAEnlilSimulations", "simulationID",                   None),
    ("nasa_donki_notifications",                  "notifications",       "messageID",                  {"type": "all"}),
]
```

- [ ] **Step 4: Override the DONKI translator key**

In `assets.py`, update `DonkiDltTranslator`:
```python
class DonkiDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_{data.resource.name}"),
            automation_condition=AutomationCondition.on_cron("0 7 * * *"),
        )
```

- [ ] **Step 5: Update the lineage bridge (DONKI comprehension)**

In `transform/definitions.py`, change the DONKI comprehension so the dict key is the new table name and the value is `dlt_<table>`:
```python
    **{
        t: AssetKey([f"dlt_{t}"])
        for t in [
            "nasa_donki_coronal_mass_ejections",
            "nasa_donki_coronal_mass_ejection_analyses",
            "nasa_donki_geomagnetic_storms",
            "nasa_donki_interplanetary_shocks",
            "nasa_donki_solar_flares",
            "nasa_donki_solar_energetic_particles",
            "nasa_donki_magnetopause_crossings",
            "nasa_donki_radiation_belt_enhancements",
            "nasa_donki_high_speed_streams",
            "nasa_donki_wsa_enlil_simulations",
            "nasa_donki_notifications",
        ]
    },
```

- [ ] **Step 6: Rename the 11 dbt model files + source() refs**

Run:
```bash
cd dbt/models/bronze
declare -A MAP=(
  [cme]=nasa_donki_coronal_mass_ejections
  [cme_analysis]=nasa_donki_coronal_mass_ejection_analyses
  [gst]=nasa_donki_geomagnetic_storms
  [ips]=nasa_donki_interplanetary_shocks
  [flr]=nasa_donki_solar_flares
  [sep]=nasa_donki_solar_energetic_particles
  [mpc]=nasa_donki_magnetopause_crossings
  [rbe]=nasa_donki_radiation_belt_enhancements
  [hss]=nasa_donki_high_speed_streams
  [wsa_enlil_simulations]=nasa_donki_wsa_enlil_simulations
  [notifications]=nasa_donki_notifications
)
for old in "${(@k)MAP}"; do
  new=${MAP[$old]}
  git mv "bronze_${old}.sql" "bronze_${new}.sql"
  sed -i '' "s/source('bronze', '${old}')/source('bronze', '${new}')/" "bronze_${new}.sql"
done
cd -
```
> Note: `${(@k)MAP}` is zsh associative-array key expansion. The `source()` patterns are exact (include the closing quote), so `cme` will not match `cme_analysis`.

- [ ] **Step 7: Update the dbt sources + models yml (11 entries each)**

In `_bronze__sources.yml`, replace the 11 DONKI table blocks so each is `name: <new>` with `asset_key: ["dlt_<new>"]`, e.g.:
```yaml
      - name: nasa_donki_coronal_mass_ejections
        meta: {dagster: {asset_key: ["dlt_nasa_donki_coronal_mass_ejections"]}}
      - name: nasa_donki_coronal_mass_ejection_analyses
        meta: {dagster: {asset_key: ["dlt_nasa_donki_coronal_mass_ejection_analyses"]}}
      - name: nasa_donki_geomagnetic_storms
        meta: {dagster: {asset_key: ["dlt_nasa_donki_geomagnetic_storms"]}}
      - name: nasa_donki_interplanetary_shocks
        meta: {dagster: {asset_key: ["dlt_nasa_donki_interplanetary_shocks"]}}
      - name: nasa_donki_solar_flares
        meta: {dagster: {asset_key: ["dlt_nasa_donki_solar_flares"]}}
      - name: nasa_donki_solar_energetic_particles
        meta: {dagster: {asset_key: ["dlt_nasa_donki_solar_energetic_particles"]}}
      - name: nasa_donki_magnetopause_crossings
        meta: {dagster: {asset_key: ["dlt_nasa_donki_magnetopause_crossings"]}}
      - name: nasa_donki_radiation_belt_enhancements
        meta: {dagster: {asset_key: ["dlt_nasa_donki_radiation_belt_enhancements"]}}
      - name: nasa_donki_high_speed_streams
        meta: {dagster: {asset_key: ["dlt_nasa_donki_high_speed_streams"]}}
      - name: nasa_donki_wsa_enlil_simulations
        meta: {dagster: {asset_key: ["dlt_nasa_donki_wsa_enlil_simulations"]}}
      - name: nasa_donki_notifications
        meta: {dagster: {asset_key: ["dlt_nasa_donki_notifications"]}}
```
In `_bronze__models.yml`, rename the 11 DONKI `- name: bronze_<old>` headers to `bronze_<new>`, keeping each body (`columns`/`data_tests`) unchanged. The mapping is the same as Step 6 with a `bronze_` prefix (e.g. `bronze_cme` → `bronze_nasa_donki_coronal_mass_ejections`, `bronze_cme_analysis` → `bronze_nasa_donki_coronal_mass_ejection_analyses`, etc.).

- [ ] **Step 8: Run the full suite (green)**

Run: `cd dbt && uv run dbt parse && cd - && uv run pytest -q`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor(bronze): descriptive rename of NASA DONKI tables

11 DONKI resources -> nasa_donki_<entity>; dlt resources, asset keys, lineage
bridge, dbt sources/models, and tests renamed in lockstep.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Rename Space-Track chain (6 tables)

**Files:**
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/snapshot.py` (`SNAPSHOT_CLASSES`)
- Modify: `src/auspex_lakehouse/bronze/dlt/sources/spacetrack/incremental.py` (`INCREMENTAL_CLASSES`)
- Modify: `src/auspex_lakehouse/bronze/dlt/assets.py` (cron dicts, factory calls, `SpaceTrackDltTranslator`)
- Test: `tests/test_spacetrack_sources.py`, `tests/test_spacetrack_assets.py`, `tests/test_definitions.py`

**Interfaces:**
- Consumes: `dlt_<table>` key convention from Tasks 1–2.
- Produces: table/asset names `space_track_<entity>`; asset keys `dlt_space_track_<entity>`. No dbt changes (Space-Track has no dbt models yet).

- [ ] **Step 1: Update the failing tests first (red)**

`tests/test_spacetrack_assets.py` — replace the `expected` set (lines 21–28) and the docstring on line 10:
```python
    """All six space-track assets must produce provider-scoped dlt_space_track_<name> keys."""
```
```python
    expected = {
        AssetKey("dlt_space_track_general_perturbations"),
        AssetKey("dlt_space_track_satellite_catalog"),
        AssetKey("dlt_space_track_boxscore"),
        AssetKey("dlt_space_track_decays"),
        AssetKey("dlt_space_track_conjunction_data_messages"),
        AssetKey("dlt_space_track_tracking_and_impact_predictions"),
    }
```
(The `a.spacetrack_gp_assets` Python attribute names stay — only the key strings change.)

`tests/test_definitions.py` — lines 20, 23, 24:
```python
    # SpaceTrackDltTranslator overrides the default dlt key to dlt_space_track_<name>.
    st_keys = {k.to_user_string() for k in graph.asset_keys_for_group("spacetrack")}
    assert len(st_keys) >= 6, f"Expected at least 6 spacetrack assets; got {st_keys}"
    assert "dlt_space_track_general_perturbations" in st_keys, f"Missing gp key; got {st_keys}"
    assert "dlt_space_track_decays" in st_keys, f"Missing decay key; got {st_keys}"
```

`tests/test_spacetrack_sources.py` — update every **resource-name** position (the 1st arg to `_snapshot_resource`/`_incremental_resource`, `res.name`, the `SNAPSHOT_CLASSES`/`INCREMENTAL_CLASSES` first-column lists, the `resources.keys()` sets, the `spacetrack_pipelines` key set, and `pipeline_name`). Leave the 2nd positional arg (the API `cls`: `"gp"`, `"decay"`, `"tip"`) unchanged. Concretely:
- line 14: `snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", ...)` → first arg `"space_track_general_perturbations"` (2nd stays `"gp"`)
- line 18: `assert res.name == "gp"` → `== "space_track_general_perturbations"`
- line 23: `snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", (), "merge", 0)` → first arg `"space_track_general_perturbations"`
- line 29: same → first arg `"space_track_general_perturbations"`
- line 37: `["gp", "satcat", "boxscore"]` → `["space_track_general_perturbations", "space_track_satellite_catalog", "space_track_boxscore"]`
- lines 38–41: `by["gp"]`→`by["space_track_general_perturbations"]`, `by["satcat"]`→`by["space_track_satellite_catalog"]`, `by["boxscore"]`→`by["space_track_boxscore"]`
- line 52: `inc._incremental_resource("decay", "decay", ...)` → first arg `"space_track_decays"` (2nd stays `"decay"`)
- line 60: `assert res.name == "decay"` → `== "space_track_decays"`
- line 65: `inc._incremental_resource("tip", "tip", ...)` → first arg `"space_track_tracking_and_impact_predictions"`
- line 71: `["decay", "cdm", "tip"]` → `["space_track_decays", "space_track_conjunction_data_messages", "space_track_tracking_and_impact_predictions"]`
- lines 72–75: `by["cdm"]`→`by["space_track_conjunction_data_messages"]` (the `== "cdm_public"` value stays), `by["decay"]`→`by["space_track_decays"]`, `by["tip"]`→`by["space_track_tracking_and_impact_predictions"]`
- line 81: `snapshot_source("gp")` → `snapshot_source("space_track_general_perturbations")`; line 82 `{"gp"}` → `{"space_track_general_perturbations"}`
- line 87: `incremental_source("decay", ...)` → `incremental_source("space_track_decays", ...)`; line 88 `{"decay"}` → `{"space_track_decays"}`
- line 93: the set → `{"space_track_general_perturbations", "space_track_satellite_catalog", "space_track_boxscore", "space_track_decays", "space_track_conjunction_data_messages", "space_track_tracking_and_impact_predictions"}`
- line 94: `spacetrack_pipelines["gp"].pipeline_name == "spacetrack_gp"` → `spacetrack_pipelines["space_track_general_perturbations"].pipeline_name == "spacetrack_space_track_general_perturbations"`
- line 95: `spacetrack_pipelines["decay"].dataset_name` → `spacetrack_pipelines["space_track_decays"].dataset_name`

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `uv run pytest tests/test_spacetrack_sources.py tests/test_spacetrack_assets.py tests/test_definitions.py -q`
Expected: FAIL.

- [ ] **Step 3: Rename `SNAPSHOT_CLASSES` resource names**

In `snapshot.py`, replace the first tuple column (resource/table name) with the long name; leave the 2nd column (API `cls`) short:
```python
SNAPSHOT_CLASSES = [
    ("space_track_general_perturbations", "gp",       "NORAD_CAT_ID",
     ("orderby", "NORAD_CAT_ID"),                         "merge",   10000),
    ("space_track_satellite_catalog",     "satcat",   "NORAD_CAT_ID",
     ("CURRENT", "Y", "orderby", "NORAD_CAT_ID"),         "merge",   10000),
    ("space_track_boxscore",              "boxscore", None,
     (),                                                   "replace", None),
]
```

- [ ] **Step 4: Rename `INCREMENTAL_CLASSES` resource names**

In `incremental.py`, same treatment (1st column long, 2nd column `cls` stays):
```python
INCREMENTAL_CLASSES = [
    ("space_track_decays",                         "decay",      ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("space_track_conjunction_data_messages",      "cdm_public", "CDM_ID",                                    "CREATED"),
    ("space_track_tracking_and_impact_predictions","tip",        ["NORAD_CAT_ID", "MSG_EPOCH"],               "INSERT_EPOCH"),
]
```
> Note: the `cdm` API class is `"cdm_public"` (already its real value in the 2nd column) — keep it.

- [ ] **Step 5: Update cron dicts, factory calls, and the Space-Track translator**

In `assets.py`, re-key the cron dicts to the long names:
```python
_ST_SNAPSHOT_CRON = {
    "space_track_general_perturbations": "11 18 * * *",
    "space_track_satellite_catalog": "21 18 * * *",
    "space_track_boxscore": "31 18 * * *",
}
_ST_INCREMENTAL_CRON = {
    "space_track_decays": "41 18 * * *",
    "space_track_conjunction_data_messages": "46 18 * * *",
    "space_track_tracking_and_impact_predictions": "51 18 * * *",
}
```
Change the translator key from `dlt_spacetrack_<name>` to `dlt_<name>`:
```python
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey(f"dlt_{data.resource.name}"),
            automation_condition=AutomationCondition.on_cron(self._cron),
        )
```
Update the six factory calls (keep the Python variable names):
```python
spacetrack_gp_assets = _spacetrack_snapshot_assets("space_track_general_perturbations")
spacetrack_satcat_assets = _spacetrack_snapshot_assets("space_track_satellite_catalog")
spacetrack_boxscore_assets = _spacetrack_snapshot_assets("space_track_boxscore")
spacetrack_decay_assets = _spacetrack_incremental_assets("space_track_decays")
spacetrack_cdm_assets = _spacetrack_incremental_assets("space_track_conjunction_data_messages")
spacetrack_tip_assets = _spacetrack_incremental_assets("space_track_tracking_and_impact_predictions")
```

- [ ] **Step 6: Run the full suite (green)**

Run: `uv run pytest -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(bronze): descriptive rename of Space-Track tables

gp/satcat/boxscore/decay/cdm/tip -> space_track_<entity>; resource names,
pipeline keys, cron keys, factory args, and asset keys (dlt_space_track_<x>,
replacing dlt_spacetrack_<x>) renamed in lockstep. Tests updated.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Update docs (cosmetic)

**Files:**
- Modify: `README.md`
- Modify: `.env.example`

**Interfaces:** none (prose only).

- [ ] **Step 1: Update README + .env.example apod_images references**

Find the references:
```bash
git grep -nP 'apod_images' -- README.md .env.example
```
In each, change `apod_images` to `nasa_astronomy_picture_of_the_day_images` (asset name), preserving surrounding prose. (The `MINIO_*` / `BRONZE_*` env var names themselves do not change.)

- [ ] **Step 2: Verify no stale short names remain in repo source**

Run:
```bash
git grep -nP '(\bdlt_nasa_api_|\bdlt_nasa_donki_(cme|gst|ips|flr|sep|mpc|rbe|hss)\b|\bdlt_spacetrack_|apod_images|source\(.bronze., .(cme|apod|neows|neo_lookup|gp|satcat|decay|cdm|tip)\b)' -- ':!dbt/target' ':!docs/superpowers'
```
Expected: no output. (Investigate any hit before committing.)

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: update apod_images references to new asset name

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Data migration runbook — re-ingest (operational; run against live infra)

> This task runs against the deployed Dagster/MinIO instance, not the test suite. It is **not** a code change. Execute it after Tasks 1–4 are deployed (Watchtower auto-updates the user-code image). Re-ingest is idempotent on primary key, so re-running is safe.

**Files:** none (runbook).

- [ ] **Step 1: Confirm the new code is deployed**

In the Dagster UI, confirm the asset graph shows the new keys (e.g. `dlt_nasa_donki_coronal_mass_ejections`, `dlt_space_track_general_perturbations`, `bronze_nasa_astronomy_picture_of_the_day`) and the old keys are gone.

- [ ] **Step 2: Re-ingest the Delta tables into their new paths**

Trigger a run / backfill for each renamed dlt asset so dlt creates the new-named Delta tables at `bronze/<new>/`:
- Snapshot + replace tables (`space_track_*`, both NASA APOD/NeoWs feeds): a single materialization of the latest partition repopulates them.
- Incremental/partitioned tables (DONKI, `decay`, `cdm`, `tip`, `nasa_near_earth_object_*`): backfill the partition range you care about. History is re-fetchable from the source APIs; merge-on-PK guarantees no duplicate rows.
- `nasa_near_earth_object_lookups` and `nasa_astronomy_picture_of_the_day_images` are downstream eager assets — they materialize after their upstream feed lands.

- [ ] **Step 3: Prefix-copy the existing APOD image blobs**

The image blobs are plain S3 objects (not a Delta table), so a simple copy is safe. Run a one-off (adjust the MinIO alias/endpoint to your environment):
```bash
# mc (MinIO client) example — copies old image prefix to the new one
mc cp --recursive "myminio/auspex-lakehouse/bronze/apod_images/" \
                  "myminio/auspex-lakehouse/bronze/nasa_astronomy_picture_of_the_day_images/"
```
(Alternatively re-materialize the `nasa_astronomy_picture_of_the_day_images` asset over history to re-download from NASA.)

- [ ] **Step 4: Verify the new tables, then delete the old folders**

- In a Dagster shell or notebook: `read_bronze_table("nasa_donki_coronal_mass_ejections")` (and a couple of others) returns sane row counts.
- Leave `bronze/_dlt_loads`, `bronze/_dlt_pipeline_state`, `bronze/_dlt_version` **untouched**.
- Once the new tables are confirmed populated, delete the orphaned old folders manually (e.g. `mc rm --recursive --force "myminio/auspex-lakehouse/bronze/cme/"` etc.) — taking care to remove only the exact old table dirs (`cme/` and its `cme__*` children), never `cme_analysis/` or the shared `_dlt_*` folders.

---

## Notes for the executor

- **Order matters:** Tasks 1→2→3 each leave the suite green; do not start a task before the previous one is committed.
- **`uv run`** is the project's runner; if the repo uses a different invocation (`poetry run`, bare `pytest`), substitute consistently.
- **dbt manifest:** `test_dbt_bronze.py` resolves the Dagster↔dbt graph, which needs an up-to-date dbt manifest. If a task's test fails to see renamed models, run `cd dbt && uv run dbt parse` to regenerate `target/manifest.json`, then re-run pytest.
- **Keep Python symbol names** (`apod`, `neows`, `neo_lookup`, `neo_lookup_rows`, `spacetrack_gp_assets`, etc.) and **pipeline_name** values as-is unless a step says otherwise — only the dlt `name=` resource strings, asset keys, dbt names, and lookup-dict keys change.
