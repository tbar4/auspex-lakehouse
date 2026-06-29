# Space-Track.org Endpoints — Bronze Source Design

**Date:** 2026-06-28
**Status:** Approved, pending implementation plan
**Builds on:** the provider-package + factory/registry pattern from the NASA NEO-lookup
and DONKI work, and the per-provider concurrency-pool pattern (`nasa_api`). This is
the **first non-NASA provider**, so it introduces the first cookie-session auth.

## Goal

Ingest six current-state space-track.org data classes as bronze Delta tables:
**GP** (latest SGP4 orbital element sets), **SATCAT** (satellite catalog metadata),
**BOXSCORE** (catalog accounting by country/type), **DECAY** (reentry predictions),
**CDM** (conjunction data messages), and **TIP** (tracking & impact predictions).

Unlike NASA (a simple `api_key` query param), space-track requires a **cookie-session
login**, enforces **much stricter rate limits**, and most of these classes return the
**current catalog** rather than a queryable per-date feed. The design adapts the
established patterns to those three differences.

## Constraints & Principles

- **Auth is mandatory and cookie-based.** `POST /ajaxauth/login` with
  `identity`/`password` form fields sets a session cookie reused for all queries.
  There is no API-key alternative. **space-track returns HTTP 200 even on bad
  credentials**, with a failure body — login must inspect the body, not just the
  status. Log in **once per run** and reuse the session for all classes (login is
  itself throttled).
- **Strict, asymmetric rate limits.** Overall **<30 requests/min and <300/hour**;
  **GP queryable ~1/hour** (randomize the minute); **SATCAT updated once/day after
  1700 UTC**; CDM has its own per-event limits. Steady state here is tiny
  (~1 login + 6 queries/day), so limits only matter during backfill of the
  incremental classes — see *Resilience & backfill*.
- **Most classes are current-state, not a date feed.** GP returns the latest elset
  per object *now*; SATCAT returns the catalog *now*. You cannot query these
  "as of" a past date without GP_HISTORY (out of scope). Therefore these are
  modeled as **unpartitioned snapshot pulls** merged on a natural key, not as
  date-partitioned feeds. DECAY/CDM/TIP *are* accumulating event logs with usable
  date predicates, so they are modeled as **date-windowed incremental** pulls on
  `daily_partitions`. (Per-class cadence was the explicit design choice.)
- **Bronze = raw.** Land full payloads; dlt normalizes any nested arrays into child
  tables. Flattening/curation is a silver concern.
- **Separate provider budget.** space-track gets its own `spacetrack_api`
  concurrency pool (limit 1), independent of `nasa_api`.
- **Keep Dagster layers clean and concise** — factory + registry, mirroring DONKI.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Scope | **GP, SATCAT, BOXSCORE, DECAY, CDM, TIP** (GP_HISTORY deferred to its own spec) |
| Provider package | New `sources/spacetrack/` (first non-NASA provider) |
| Auth | Cookie session via `requests.Session`; creds from `dlt.secrets["spacetrack_username"/"spacetrack_password"]` (env `SPACETRACK_USERNAME`/`SPACETRACK_PASSWORD`) |
| Login placement | **In the asset body, once per run** — NOT in the source function (which runs at import) |
| Cadence model | **Per class:** snapshot-merge for GP/SATCAT/BOXSCORE; date-windowed incremental for DECAY/CDM/TIP |
| Code structure | **Two factories + two registries** (`snapshot.py`, `incremental.py`) |
| Grouping | Two `@dlt_assets` (group `spacetrack`), two pipelines — split because the two cadence families need different `partitions_def` |
| Budget control | New `spacetrack_api` pool (limit 1), both `@dlt_assets` bound to it |
| Scheduling | After SATCAT's 1700 UTC update: snapshot `on_cron("17 18 * * *")`, incremental `on_cron("47 18 * * *")` |
| Base branch | `feat/spacetrack` off `main` |

## Authentication & query helpers (`sources/spacetrack/_common.py`)

```python
import dlt
import requests  # stdlib requests for cookie-session persistence

BASE_URL = "https://www.space-track.org"


def spacetrack_credentials() -> tuple[str, str]:
    return dlt.secrets["spacetrack_username"], dlt.secrets["spacetrack_password"]


def login_session() -> requests.Session:
    """Authenticate and return a cookie-bearing session.

    space-track returns HTTP 200 even on bad credentials (with a failure body),
    so a 200 is NOT sufficient — verify the body before trusting the session.
    """
    username, password = spacetrack_credentials()
    session = requests.Session()
    resp = session.post(
        f"{BASE_URL}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.text or ""
    # Success body is empty/"{}"; failure body contains a "Login" failure marker.
    if "Failed" in body or "login" in body.lower():
        raise RuntimeError("space-track login failed (check SPACETRACK_USERNAME/PASSWORD)")
    return session


def query_class(session: requests.Session, cls: str, *segments: str):
    """GET /basicspacedata/query/class/<cls>/<segments>/format/json -> parsed JSON.

    `segments` are predicate path parts already URL-safe, e.g.
    ("CURRENT", "Y", "orderby", "NORAD_CAT_ID") or ("MSG_EPOCH", "2026-06-28--2026-06-29").
    """
    path = "/".join(segments)
    sep = "/" if path else ""
    url = f"{BASE_URL}/basicspacedata/query/class/{cls}{sep}{path}/format/json"
    resp = session.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def iter_days(start_date, end_date):
    """Yield each date in the inclusive [start, end] range (matches nasa/_common)."""
    from datetime import timedelta
    day = start_date
    while day <= end_date:
        yield day
        day += timedelta(days=1)
```

> The exact login-failure detection string is verified against a live failed login
> during implementation; the body check is the load-bearing part, not the literal.

## Component 1 — Snapshot factory + registry (`sources/spacetrack/snapshot.py`)

Current-state classes. Each run pulls the whole class and upserts on its natural key
(BOXSCORE has no stable row key, so it is fully replaced each run).

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import query_class


def _snapshot_resource(name, cls, primary_key, segments, write_disposition="merge"):
    @dlt.resource(
        name=name,
        write_disposition=write_disposition,
        primary_key=primary_key,        # None for replace tables
        table_format="delta",
    )
    def _resource(session):
        data = query_class(session, cls, *segments)
        if isinstance(data, list):      # tolerate empty/non-list bodies like DONKI
            yield from data
    return _resource


SNAPSHOT_CLASSES = [
    # (resource_name, class, primary_key, query_segments, write_disposition)
    ("gp",       "gp",       "NORAD_CAT_ID", ("orderby", "NORAD_CAT_ID"),              "merge"),
    ("satcat",   "satcat",   "NORAD_CAT_ID", ("CURRENT", "Y", "orderby", "NORAD_CAT_ID"), "merge"),
    ("boxscore", "boxscore", None,           (),                                       "replace"),
]
```

- **GP** returns the single latest elset per on-orbit object (~30k rows); merge on
  `NORAD_CAT_ID` keeps the table at one row per object, updated each run.
- **SATCAT** `CURRENT/Y` returns the current catalog (incl. decayed objects);
  merge on `NORAD_CAT_ID`.
- **BOXSCORE** is a small aggregate (~hundreds of rows) with no stable per-row key,
  so it is **replaced** wholesale each run.

## Component 2 — Incremental factory + registry (`sources/spacetrack/incremental.py`)

Accumulating event logs with usable date predicates. Each daily partition queries a
one-day window and merges on the record's natural key.

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import iter_days, query_class


def _incremental_resource(name, cls, primary_key, date_predicate):
    @dlt.resource(
        name=name,
        write_disposition="merge",
        primary_key=primary_key,
        table_format="delta",
    )
    def _resource(session, start_date, end_date):
        from datetime import timedelta
        for day in iter_days(start_date, end_date):
            window = f"{day.isoformat()}--{(day + timedelta(days=1)).isoformat()}"
            data = query_class(session, cls, date_predicate, window)
            if isinstance(data, list):
                yield from data
    return _resource


INCREMENTAL_CLASSES = [
    # (resource_name, class, primary_key, date_predicate)
    ("decay", "decay",      ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("cdm",   "cdm_public", "CDM_ID",                                    "CREATED"),
    ("tip",   "tip",        ["NORAD_CAT_ID", "MSG_EPOCH"],               "INSERT_EPOCH"),
]
```

- The day window uses space-track's inclusive range operator `start--end`. Because
  writes are **merge on the natural key**, a record landing on a window boundary in
  two adjacent partitions is a harmless idempotent upsert.
- **CDM** uses the **`cdm_public`** class (the publicly queryable conjunction class),
  keyed on the unique `CDM_ID`.

## Merge-key caveat (accepted, verified during implementation)

`DECAY` and `TIP` have **no documented single unique predicate**, so they use
composite keys (`[NORAD_CAT_ID, MSG_EPOCH, PRECEDENCE]` and `[NORAD_CAT_ID,
MSG_EPOCH]`). These are validated against live responses during implementation; if a
composite still collides, the fallback is to add another discriminating predicate.
This mirrors the DONKI `CMEAnalysis` composite-key caveat — documented so the keys
aren't mistaken for guaranteed-unique. `GP`/`SATCAT` (`NORAD_CAT_ID`) and `CDM`
(`CDM_ID`) are genuinely unique.

## Component 3 — Sources + pipelines (`sources/spacetrack/__init__.py`)

Each source receives an **already-authenticated session** (login is done in the asset
body, not here — see below). Resources are lazy generators, so constructing a source
makes **no HTTP calls**.

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental import (
    INCREMENTAL_CLASSES, _incremental_resource,
)
from auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot import (
    SNAPSHOT_CLASSES, _snapshot_resource,
)


@dlt.source
def spacetrack_snapshot_source(session=None):
    return [
        _snapshot_resource(name, cls, pk, segs, wd)(session)
        for (name, cls, pk, segs, wd) in SNAPSHOT_CLASSES
    ]


@dlt.source
def spacetrack_incremental_source(start_date, end_date, session=None):
    return [
        _incremental_resource(name, cls, pk, pred)(session, start_date, end_date)
        for (name, cls, pk, pred) in INCREMENTAL_CLASSES
    ]


spacetrack_snapshot_pipeline = dlt.pipeline(
    pipeline_name="spacetrack_snapshot",   # distinct working dir
    destination="filesystem",
    dataset_name="bronze",                 # tables land at bronze/<resource_name>
)
spacetrack_incremental_pipeline = dlt.pipeline(
    pipeline_name="spacetrack_incremental",
    destination="filesystem",
    dataset_name="bronze",
)
```

`session=None` is a safe placeholder for the import-time source construction the
`@dlt_assets` decorator performs (resources are never iterated then, so the `None`
session is never used). All four names are re-exported from `sources/__init__.py`.

## Component 4 — Assets, pooling, scheduling (`dlt/assets.py`)

Two `@dlt_assets` groups, both in group `spacetrack`, both bound to the
`spacetrack_api` pool, **logging in once in the asset body** and rebuilding the source
with the real session.

```python
class SpaceTrackDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            automation_condition=AutomationCondition.on_cron("17 18 * * *"),
        )


class SpaceTrackIncrementalDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            automation_condition=AutomationCondition.on_cron("47 18 * * *"),
        )


@dlt_assets(
    dlt_source=spacetrack_snapshot_source(),     # session=None at import (no HTTP)
    dlt_pipeline=spacetrack_snapshot_pipeline,
    name="spacetrack_snapshot_bronze",
    group_name="spacetrack",
    # NO partitions_def — these are current-state snapshots
    dagster_dlt_translator=SpaceTrackDltTranslator(),
    pool="spacetrack_api",
)
def spacetrack_snapshot_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    session = login_session()                    # one login per run
    source = spacetrack_snapshot_source(session=session)
    yield from dlt.run(context=context, dlt_source=source)


@dlt_assets(
    dlt_source=spacetrack_incremental_source(
        start_date=date.today(), end_date=date.today(),
    ),
    dlt_pipeline=spacetrack_incremental_pipeline,
    name="spacetrack_incremental_bronze",
    group_name="spacetrack",
    partitions_def=daily_partitions,
    dagster_dlt_translator=SpaceTrackIncrementalDltTranslator(),
    pool="spacetrack_api",
)
def spacetrack_incremental_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    rng = context.partition_key_range
    session = login_session()                    # one login per run
    source = spacetrack_incremental_source(
        start_date=date.fromisoformat(rng.start),
        end_date=date.fromisoformat(rng.end),
        session=session,
    )
    yield from dlt.run(context=context, dlt_source=source)
```

The snapshot group is **unpartitioned**: `on_cron` triggers a daily materialization
that merges/replaces the current catalog. The incremental group follows the
established `nasa_api_assets` runtime pattern (rebuild the source for the actual
partition range). The `pool="spacetrack_api"` binding (verified available on
`dagster_dlt.dlt_assets`) guarantees ≤1 space-track op in flight, so the two groups
never log in or query simultaneously.

## Component 5 — Infra wiring

- **`dagster.yaml`** — add a second pool alongside `nasa_api`:

  ```yaml
  concurrency:
    pools:
      nasa_api:
        limit: 1
        granularity: 'op'
      spacetrack_api:        # separate provider, separate budget
        limit: 1
        granularity: 'op'
  ```

- **`.env.example`** — add:

  ```
  # ---- space-track.org (consumed by dlt as dlt.secrets["spacetrack_username"/"_password"]) ----
  SPACETRACK_USERNAME=your_spacetrack_username
  SPACETRACK_PASSWORD=your_spacetrack_password
  ```

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Bad credentials | `login_session()` raises (200 + failure body is detected); run fails before any query |
| Empty window/class (`[]`) | Resource yields nothing; no rows written; no error |
| Non-list 200 body | `isinstance(data, list)` guard skips it silently (looks like an empty result) |
| Boundary-overlapping incremental record | Idempotent merge upsert (no duplication) |
| `401` / `429` / `5xx` on a query | `raise_for_status()` fails the partition loudly; **no auto-retry** — re-materialize. Atomic per source (all-or-nothing across that source's classes) |
| Backfill *concurrency* | Bounded by the `spacetrack_api` pool (limit 1) |
| Backfill *rate* | NOT bounded by the pool — see below |

## Resilience & backfill

Steady-state daily runs are ~1 login + 3 queries per source (≈8 requests/day total),
far under <30/min and <300/hour, and within GP's 1/hr and SATCAT's daily limits
(hence the post-1700-UTC schedule). The snapshot classes are **not** backfillable
(no historical "as of" query without GP_HISTORY). Only DECAY/CDM/TIP backfill over
date partitions; running many partitions back-to-back can approach <300/hr.

**Mitigation (operational, no code):** backfill incremental classes in **small
date-range batches** (a week or two at a time, or with limited run concurrency) and
re-run any partitions that 429 — the same "backfill gently" guidance the NEO-lookup
and DONKI work landed with. A per-provider hourly rate-budget scheduler remains out
of scope. Note CDM history retention is server-side limited; early partitions may
legitimately return `[]`.

## Testing

(All mocked — no live HTTP; monkeypatch `login_session`/`query_class`.)

- **Snapshot factory:** list → one row per element; `[]`/non-list → no rows; produced
  resource has the right `name`, `write_disposition`, `primary_key`,
  `table_format="delta"`; `boxscore` is `write_disposition="replace"` with no key.
- **Incremental factory:** builds the correct `start--end+1` window per day across a
  multi-day range and passes the right date predicate to `query_class`; list → rows;
  `[]`/non-list → none; merge attrs correct.
- **Registries:** exactly 3 snapshot + 3 incremental; resource names unique; every
  primary key present (or `None` only for boxscore); `cdm` uses class `cdm_public`;
  each incremental entry has a date predicate.
- **Auth:** `login_session` POSTs `identity`/`password`, raises on a failure body,
  returns the session on success; `query_class` builds the correct
  `/basicspacedata/query/class/.../format/json` URL.
- **Sources:** `spacetrack_snapshot_source()` exposes `{gp, satcat, boxscore}`;
  `spacetrack_incremental_source(...)` exposes `{decay, cdm, tip}`; constructing
  either with `session=None` makes no HTTP call.
- **Wiring:** asset keys follow dlt's `dlt_<pipeline_name>_<resource>` convention —
  `dlt_spacetrack_snapshot_{gp,satcat,boxscore}` and
  `dlt_spacetrack_incremental_{decay,cdm,tip}` — all in group `spacetrack`; snapshot
  group has no partitions, incremental uses `daily_partitions`; both ops have
  `pool == "spacetrack_api"`.
- **Smoke:** `test_definitions_load` still loads (no import-time HTTP/Delta).

## Out of Scope

- **GP_HISTORY** (138M+ rows; space-track allows querying it once per lifetime) — its
  own spec and one-time backfill design.
- Silver-layer modeling (orbital-element curation, conjunction graphs, cross-class joins).
- The per-provider hourly rate-budget scheduler (still future).
- Retrofitting NASA assets into pools; other providers (spaceflightnews, thespacedevs).
