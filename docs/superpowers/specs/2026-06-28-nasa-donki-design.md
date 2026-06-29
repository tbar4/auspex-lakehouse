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
  is cheap in steady state — exactly **one call per endpoint per partition-day**
  (11 calls/day). DONKI joins the existing `nasa_api` concurrency pool (limit 1)
  so NASA API access is serialized. **The pool bounds *concurrency*, not *rate*:**
  it prevents parallel double-spend, but a *continuous full-history backfill*
  (~180 partitions × 11 calls ≈ 2000 calls back-to-back) can still run at
  ~1000–1300 calls/hr and trip the budget. See *Resilience & backfill* below —
  the mitigation is to backfill in small date-range batches, not a code change.
- **Uniform endpoints → one factory.** The 11 endpoints differ only by path,
  primary key, and (for 1 of them) a fixed extra query param. A single resource
  factory + a config registry keeps the layer concise.
- **Bronze = raw.** Land the full event payload; dlt normalizes nested arrays
  (`cmeAnalyses`, `linkedEvents`, `allKpIndex`, `instruments`,
  `sentNotifications`, and deeper ones like `cmeAnalyses__enlilList`) into child
  tables. Expect this to produce **many Delta tables** — plausibly ~40–60 across
  the 11 endpoints (parent + child). That is the intended bronze=raw outcome;
  silver consolidates. Flattening/curation is a silver concern.
- **No refresh.** Each partition is fetched once (cron tick); DONKI rows carry
  `versionId`/`submissionTime`, so a record updated or late-submitted after its
  partition ran is not re-captured. Acceptable for bronze; revisit if downstream
  needs the latest version.
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
are *bulk list* queries, not per-ID lookups, so there is no poison-pill-ID risk
and no need to read prior state. A `429`/`5xx` fails the run loudly via
`raise_for_status()` (a *visible* failed partition you can re-materialize — there
is **no automatic retry**; `dlt_assets` does not accept `retry_policy`, and
instance run-retries have no backoff so they wouldn't help a rate-limit). The
`isinstance(data, list)` guard tolerates empty days (`[]`); note it also silently
skips a non-list 200 body (an unusual error response would look like an empty day
— acceptable, low risk).

**All-or-nothing across the 11 endpoints (accepted):** all 11 resources run in
one `donki_source` / one `dlt_assets` op. dlt extracts the whole source then
loads atomically, so if one endpoint raises (e.g. a 429 on the 6th call), nothing
commits and the partition fails — the other 10 (successful) calls are wasted and
a re-run re-fetches all 11. This is the simplicity trade-off vs. NEO's per-ID
fault tolerance; it is fine given DONKI's low steady-state volume and pool
serialization, but a backfill that trips the budget will fail whole partitions.

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
during backfills. **Addendum (final-review):** `nasa_api_assets` (apod/neows) was also
assigned `pool=NASA_API_POOL` alongside this feature, so ALL four NASA Dagster ops are
pooled and shared `bronze/_dlt_*` writes are fully serialized with no gaps.

## CMEAnalysis caveat (accepted)

`CMEAnalysis` has no single natural unique ID (`associatedCMEID` repeats — many
analyses per CME). We merge on the composite `["associatedCMEID", "time21_5"]`.
On a live 138-row sample (May 2024) this composite had **0 null `time21_5` and 0
collisions**, so it is sound in practice; it is *theoretically* possible (not
observed) for two analyses to share both values and upsert-collide. `cme_analysis`
also overlaps the `cme__cme_analyses` child table the `cme` resource already
produces. Accepted for bronze; silver should prefer the CME-nested analyses where
exactness matters. Documented so the composite key isn't mistaken for a
guaranteed-unique one.

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Empty day (`[]`) | Resource yields nothing; no rows written; no error |
| Non-list 200 body | `isinstance(data, list)` guard skips it silently (looks like an empty day) |
| `429` / `5xx` | `raise_for_status()` fails the partition loudly; **no auto-retry** — re-materialize to recover. Atomic across all 11 endpoints (see above) |
| Backfill *concurrency* | Bounded by the `nasa_api` pool (limit 1); DONKI runs serialize against neo_lookup |
| Backfill *rate* | NOT bounded by the pool — see *Resilience & backfill* |

## Resilience & backfill

Steady-state daily runs (one partition, ~11 calls) are far under budget and will
not 429. The risk is a **full-history backfill**: pool serialization caps
concurrency but not request *rate*, so running ~180 partitions back-to-back
(~1000–1300 calls/hr, plus neo_lookup/apod/neows on the same budget) can trip
1000/hr and fail partitions — and there is no automatic retry.

**Mitigation (operational, no code):** backfill DONKI in **small date-range
batches** (e.g. a week or two at a time, or with limited run concurrency) rather
than launching the whole history at once; re-run any partitions that 429. The
real fix — an hourly per-provider rate-budget scheduler that drains within
budget — remains out of scope. This is the same "backfill gently" guidance the
NEO-lookup work landed with.

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

- ~~Retrofitting apod/neows into the `nasa_api` pool~~ — **Done** (completed in the
  final-review fix wave; `nasa_api_assets` now carries `pool=NASA_API_POOL`). See Component 3 addendum above.
- The per-provider hourly rate-budget scheduler (still future).
- Silver-layer modeling of DONKI events; cross-endpoint `linkedEvents` graph.
- Other providers (space-track.org, spaceflightnews, thespacedevs).
