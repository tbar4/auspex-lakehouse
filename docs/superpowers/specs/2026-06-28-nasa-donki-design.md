# NASA DONKI Endpoints — Bronze Source Design

**Date:** 2026-06-28
**Status:** Approved, pending implementation plan
**Builds on:** the `nasa/` source package and `nasa_api` concurrency pool from the NEO-lookup work (merged to `main` in PR #2).

## Goal

Ingest all 11 DONKI (Space Weather Database Of Notifications, Knowledge,
Information) endpoints as bronze Delta tables. Each endpoint is a uniform
`GET https://api.nasa.gov/DONKI/<ENDPOINT>?startDate&endDate&api_key` call
returning a list of space-weather events, merged on the event's natural ID.

## Constraints & Principles

- **NASA API budget: 1000 calls/hour, shared across all NASA endpoints.** DONKI
  is cheap — exactly **one call per endpoint per partition-day** (11 calls/day) —
  but a parallel backfill must still be bounded. DONKI joins the existing
  `nasa_api` concurrency pool (limit 1) so all NASA API access is serialized.
- **Uniform endpoints → one factory.** The 11 endpoints differ only by path,
  primary key, and (for 1 of them) a fixed extra query param. A single resource
  factory + a config registry keeps the layer concise.
- **Bronze = raw.** Land the full event payload; dlt normalizes nested arrays
  (`cmeAnalyses`, `linkedEvents`, `allKpIndex`, `instruments`,
  `sentNotifications`) into child tables. Flattening/curation is a silver concern.
- **Keep Dagster layers clean and concise.**

## Design Decisions

| Decision | Choice |
|----------|--------|
| Scope | **All 11 DONKI endpoints** |
| Code structure | **Factory + config registry** (one `donki.py`, not 11 files) |
| Grouping | **Own `donki` dlt source + `@dlt_assets` (group `donki`) + dedicated `nasa_donki` pipeline** |
| Budget control | **Join the `nasa_api` pool (limit 1)** — `@dlt_assets` accepts `pool=` (verified) |
| Per-event write | **`merge` on the event's natural ID, Delta format** |
| Scheduling | **Daily partitions + `on_cron` staggered from apod/neows (07:00)** via a `DonkiDltTranslator` |
| Base branch | `feat/nasa-donki` off `main` (post PR #2 merge) |

## Endpoint registry (verified against live responses)

| Resource name | Path | Merge key | Extra params |
|---|---|---|---|
| `cme` | `CME` | `activityID` | — |
| `cme_analysis` | `CMEAnalysis` | `["associatedCMEID", "time21_5"]` ⚠️ | — |
| `gst` | `GST` | `gstID` | — |
| `ips` | `IPS` | `activityID` | — |
| `flr` | `FLR` | `flrID` | — |
| `sep` | `SEP` | `sepID` | — |
| `mpc` | `MPC` | `mpcID` | — |
| `rbe` | `RBE` | `rbeID` | — |
| `hss` | `HSS` | `hssID` | — |
| `wsa_enlil_simulations` | `WSAEnlilSimulations` | `simulationID` | — |
| `notifications` | `notifications` | `messageID` | `type=all` |

Tables land at `bronze/<resource_name>`; asset keys are `dlt_nasa_donki_<resource_name>`.

## Component 1 — Factory + registry (`sources/nasa/donki.py`)

```python
from datetime import date

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL, iter_days, nasa_api_key


def _donki_resource(name, endpoint_path, primary_key, extra_params=None):
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
            if isinstance(data, list):  # DONKI returns a list of events; tolerate empty/non-list
                yield from data
    return _resource


DONKI_ENDPOINTS = [
    # (resource_name, endpoint_path, primary_key, extra_params)
    ("cme",                   "CME",                 "activityID",                      None),
    ("cme_analysis",          "CMEAnalysis",         ["associatedCMEID", "time21_5"],   None),
    ("gst",                   "GST",                 "gstID",                           None),
    ("ips",                   "IPS",                 "activityID",                      None),
    ("flr",                   "FLR",                 "flrID",                           None),
    ("sep",                   "SEP",                 "sepID",                           None),
    ("mpc",                   "MPC",                 "mpcID",                           None),
    ("rbe",                   "RBE",                 "rbeID",                           None),
    ("hss",                   "HSS",                 "hssID",                           None),
    ("wsa_enlil_simulations", "WSAEnlilSimulations", "simulationID",                    None),
    ("notifications",         "notifications",       "messageID",                       {"type": "all"}),
]
```

**Why no per-ID fault tolerance / dedupe (unlike NEO lookup):** DONKI endpoints
are *bulk list* queries, not per-ID lookups. A `429`/`5xx` simply fails the run
via `raise_for_status()` and Dagster retries; there is no poison-pill-ID risk and
no need to read prior state. The `isinstance(data, list)` guard tolerates empty
days (`[]`) and any non-list error body without writing junk rows.

## Component 2 — Source + pipeline (`sources/nasa/donki.py`)

```python
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

`donki_source` and `nasa_donki_pipeline` are re-exported from
`auspex_lakehouse.bronze.dlt.sources` for the asset to import.

## Component 3 — Assets group + pooling + scheduling (`dlt/assets.py`)

A `@dlt_assets` group mirroring `nasa_api_assets`, but on the dedicated pipeline,
in the `donki` group, **assigned to the `nasa_api` pool**, and scheduled via a
`DonkiDltTranslator` that staggers DONKI 1 hour after apod/neows:

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
    pool="nasa_api",   # serialize against neo_lookup and other NASA API ops
)
def donki_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    rng = context.partition_key_range
    source = donki_source(
        start_date=date.fromisoformat(rng.start),
        end_date=date.fromisoformat(rng.end),
    )
    yield from dlt.run(context=context, dlt_source=source)
```

This follows the established `nasa_api_assets` runtime pattern (re-build the
source for the actual partition range, then `dlt.run`). The `pool="nasa_api"`
binding is the new ingredient — verified available on `dagster_dlt.dlt_assets` —
giving an "≤1 NASA API op in flight" guarantee across apod/neows/neo_lookup/DONKI
during backfills.

## CMEAnalysis caveat (accepted)

`CMEAnalysis` has no natural unique ID: `associatedCMEID` repeats (multiple
analyses per CME). We merge on the composite `["associatedCMEID", "time21_5"]`,
which is *usually* unique but not guaranteed — two analyses sharing both values
would upsert-collide (one overwrites the other). It also overlaps the
`cme__cme_analyses` child table that the `cme` resource already produces. This is
accepted for bronze; silver should treat `cme_analysis` as best-effort and prefer
the CME-nested analyses where exactness matters. Documented so it isn't mistaken
for a clean key.

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Empty day (`[]`) | Resource yields nothing; no rows written; no error |
| Non-list error body (200) | `isinstance(data, list)` guard skips it; yields nothing |
| `429` / `5xx` | `raise_for_status()` fails the run; Dagster retries (low risk — DONKI is 1 call/endpoint/day and pool-serialized) |
| Backfill fan-out | Bounded by the `nasa_api` pool (limit 1); DONKI runs serialize |

## Testing

- **Factory** (`_donki_resource`, mocked `requests`): list → one row per element;
  `[]`/non-list → no rows; `extra_params` merged into the query; the produced
  resource has the right `name`, `write_disposition="merge"`, `primary_key`,
  `table_format="delta"`.
- **Registry:** exactly 11 entries; resource names unique; every primary key
  present; `notifications` carries `type=all`.
- **Source:** `donki_source(...)` exposes all 11 resources by name.
- **Wiring:** `donki_assets` produces 11 asset keys `dlt_nasa_donki_<name>` in
  group `donki`, partitioned by `daily_partitions`, with `op.pool == "nasa_api"`.
- **Smoke:** `test_definitions_load` still loads (no import-time HTTP/Delta).

## Out of Scope

- Retrofitting apod/neows into the `nasa_api` pool (now that `dlt_assets` supports
  `pool`) — a small optional cleanup, negligible volume, deferred.
- The per-provider hourly rate-budget scheduler (still future).
- Silver-layer modeling of DONKI events; cross-endpoint `linkedEvents` graph.
- Other providers (space-track.org, spaceflightnews, thespacedevs).
