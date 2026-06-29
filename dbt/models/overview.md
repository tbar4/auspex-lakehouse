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
- **Lineage:** every bronze model traces back to its upstream Dagster dlt asset
  (`dlt_nasa_*`, `dlt_spacetrack_*`, or `neo_lookup`) through the dbt source's asset-key mapping.

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
