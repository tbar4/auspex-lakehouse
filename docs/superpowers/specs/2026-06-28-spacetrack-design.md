# Space-Track.org Endpoints — Bronze Source Design

**Date:** 2026-06-28
**Status:** Approved (post adversarial review), pending implementation plan
**Builds on:** the provider-package + factory/registry pattern from the NASA NEO-lookup
and DONKI work, and the per-provider concurrency-pool pattern (`nasa_api`). This is
the **first non-NASA provider**, so it introduces the first cookie-session auth.

> **Verification status.** The Dagster/dlt wiring claims below were verified against
> the *installed* packages (`dagster_dlt` 0.29.11, `dagster` 1.13.11, `dlt` 1.28.1) —
> see *Verified mechanisms*. The space-track API details (exact class names, predicate
> lists, merge keys, login body, row limits) are **NOT** verifiable from the public
> docs page, which omits the model definitions. They are flagged **⚠ verify-live** and
> must be confirmed against the authenticated API during implementation.

## Goal

Ingest six current-state space-track.org data classes as bronze Delta tables:
**GP** (latest SGP4 orbital element sets), **SATCAT** (satellite catalog metadata),
**BOXSCORE** (catalog accounting by country/type), **DECAY** (reentry predictions),
**CDM** (conjunction data messages), and **TIP** (tracking & impact predictions).

Unlike NASA (a simple `api_key` query param), space-track requires a **cookie-session
login**, enforces **strict, asymmetric rate limits**, and most of these classes return
the **current catalog** rather than a queryable per-date feed. The design adapts the
established patterns to those three differences.

## Constraints & Principles

- **Auth is mandatory and cookie-based.** `POST /ajaxauth/login` with
  `identity`/`password` form fields sets a session cookie reused for all queries.
  There is no API-key alternative. Log in **once per run** and reuse the session.
- **Strict, per-class rate limits** (from live docs):

  | Class | Limit | Notes |
  |---|---|---|
  | GP | **1 / hour** | randomize the minute |
  | SATCAT | **1 / day** | updated after 1700 UTC |
  | BOXSCORE | **1 / day** | |
  | DECAY | **1 / day** | docs recommend `/MSG_EPOCH/>now-1/` |
  | CDM | **3 / day** (or 1/hr for a specific event) | |
  | TIP | **1 / hour** | (every 10 min if reentry <12 h) |
  | Overall | **<30 req/min, <300 req/hour** | across all queries |

  Steady state here is tiny (per-class: 1 login + 1 query/day), so limits only bite
  during backfill of the incremental classes and on **failed re-runs** — see
  *Failure isolation* and *Resilience & backfill*.
- **Most classes are current-state, not a date feed.** GP returns the latest elset
  per object *now*; SATCAT returns the catalog *now*. You cannot query these "as of"
  a past date without GP_HISTORY (out of scope). Therefore GP/SATCAT/BOXSCORE are
  modeled as **unpartitioned snapshot pulls** merged on a natural key. DECAY/CDM/TIP
  *are* accumulating event logs with usable date predicates, so they are **date-windowed
  incremental** pulls on `daily_partitions`.
- **One asset per class (failure isolation).** Because SATCAT/BOXSCORE/DECAY are
  **1/day** and GP is **1/hr**, a transient failure must not force re-querying a
  *different* daily-limited class. Each class therefore gets its **own pipeline and
  own `@dlt_assets`**, so a failure (and re-materialization) is scoped to that one
  class and its own limit. (This is the key divergence from DONKI's single
  all-or-nothing source; the daily limits make all-or-nothing too costly here.)
- **Bronze = raw.** Land full payloads; dlt normalizes nested arrays into child tables.
- **Separate provider budget.** space-track gets its own `spacetrack_api` pool (limit 1),
  independent of `nasa_api`.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Scope | **GP, SATCAT, BOXSCORE, DECAY, CDM, TIP** (GP_HISTORY deferred to its own spec) |
| Provider package | New `sources/spacetrack/` (first non-NASA provider) |
| Auth | Cookie session via `requests.Session`; creds from `dlt.secrets["spacetrack_username"/"spacetrack_password"]` (env `SPACETRACK_USERNAME`/`SPACETRACK_PASSWORD`) |
| Login placement | **In each asset body, once per run** — NOT in the source function (which runs at import) |
| Cadence model | Snapshot-merge for GP/SATCAT/BOXSCORE; date-windowed incremental for DECAY/CDM/TIP |
| Code structure | Two factories + two registries (`snapshot.py`, `incremental.py`); per-class pipelines + assets generated from the registries |
| Grouping | **Six** `@dlt_assets` (one per class), all group `spacetrack`, **six** pipelines |
| Budget control | `spacetrack_api` pool (limit 1); every `@dlt_assets` bound to it |
| Scheduling | After SATCAT's 1700 UTC update; staggered off-the-hour minutes (see *Assets*) |
| Base branch | `feat/spacetrack` off `main` |

## Verified mechanisms (against installed source)

These underpin the wiring and were confirmed by reading the installed packages:

1. **`pool=` is a direct named kwarg** on `dagster_dlt.dlt_assets` (`asset_decorator.py:57-69`), forwarded to `multi_asset`. `pool="spacetrack_api"` is valid.
2. **`@dlt_assets` does NOT iterate resource generators at import** — `build_dlt_asset_specs` reads only static specs (name, write_disposition, primary_key, docstring). So passing `session=None` at decoration time is safe; data is pulled only at `dlt.run`.
3. **Runtime source rebuild is supported** — `DagsterDltResource.run` prefers an explicitly passed `dlt_source` (`resource.py:208-209`); the existing `nasa_api_assets` already relies on this.
4. **`replace`+delta and `merge`+composite-key+delta both work** on the filesystem destination (`factory.py`; `neows.py` already does composite merge+delta).
5. **Unpartitioned `@dlt_assets` is valid** — `partitions_def` defaults to `None`.

## Authentication & query helpers (`sources/spacetrack/_common.py`)

```python
import dlt
import requests  # stdlib requests for cookie-session persistence

BASE_URL = "https://www.space-track.org"


def spacetrack_credentials() -> tuple[str, str]:
    return dlt.secrets["spacetrack_username"], dlt.secrets["spacetrack_password"]


def login_session() -> requests.Session:
    """Authenticate and return a cookie-bearing session.

    ⚠ verify-live: space-track is reported to return HTTP 200 even on bad
    credentials, and the docs do NOT specify the success/failure body. So we do
    NOT rely on a brittle body string-match. Instead, after the login POST we make
    one trivial authenticated probe and treat login as failed if it is rejected.
    """
    username, password = spacetrack_credentials()
    session = requests.Session()
    resp = session.post(
        f"{BASE_URL}/ajaxauth/login",
        data={"identity": username, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    # Probe: a tiny authenticated query. Unauthenticated access redirects to the
    # login page / 401s; a JSON list back means the cookie is good.
    probe = session.get(
        f"{BASE_URL}/basicspacedata/query/class/boxscore/limit/1/format/json",
        timeout=60,
    )
    if probe.status_code != 200 or not _looks_like_json_payload(probe):
        raise RuntimeError(
            "space-track login failed (check SPACETRACK_USERNAME/PASSWORD)"
        )
    return session


def _looks_like_json_payload(resp) -> bool:
    """True if the response body parses as JSON (list/dict) rather than an HTML
    login redirect. ⚠ verify-live against a real failed login during impl."""
    try:
        resp.json()
        return True
    except ValueError:
        return False


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

## Component 1 — Snapshot factory + registry (`sources/spacetrack/snapshot.py`)

Current-state classes; each run pulls the whole class and upserts on its natural key
(BOXSCORE has no stable row key → full replace). A **row-count floor** guards against
silent server-side truncation/row-capping (see *Error Handling, row-cap*).

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import query_class


def _snapshot_resource(name, cls, primary_key, segments, write_disposition, min_rows):
    @dlt.resource(
        name=name,
        write_disposition=write_disposition,
        primary_key=primary_key,        # None for replace tables
        table_format="delta",
    )
    def _resource(session):
        data = query_class(session, cls, *segments)
        if not isinstance(data, list):  # tolerate empty/non-list bodies
            return
        if min_rows and len(data) < min_rows:
            # Suspected truncation/implicit limit — fail loudly rather than
            # silently writing a short catalog. Floor is conservative (catalog
            # never legitimately shrinks below it); tune if it ever false-fires.
            raise RuntimeError(
                f"{name}: {len(data)} rows < floor {min_rows}; suspected row-cap"
            )
        yield from data
    return _resource


SNAPSHOT_CLASSES = [
    # (name, class, primary_key, segments, write_disposition, min_rows)
    ("gp",       "gp",       "NORAD_CAT_ID", ("orderby", "NORAD_CAT_ID"),                  "merge",   10000),
    ("satcat",   "satcat",   "NORAD_CAT_ID", ("CURRENT", "Y", "orderby", "NORAD_CAT_ID"), "merge",   10000),
    ("boxscore", "boxscore", None,           (),                                          "replace", None),
]
```

- **GP** returns the latest elset per on-orbit object (~30k); merge on `NORAD_CAT_ID`.
- **SATCAT** `CURRENT/Y` returns the current catalog (incl. decayed); merge on `NORAD_CAT_ID`. ⚠ verify-live: `CURRENT/Y` segment.
- **BOXSCORE** is a small aggregate (~hundreds of rows), no stable per-row key → **replace** each run, no floor.

## Component 2 — Incremental factory + registry (`sources/spacetrack/incremental.py`)

Accumulating event logs with date predicates; each daily partition queries a one-day
window and merges on the record's natural key.

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
    # (name, class, primary_key, date_predicate)   ⚠ verify-live: classes, keys, predicates
    ("decay", "decay",      ["NORAD_CAT_ID", "MSG_EPOCH", "PRECEDENCE"], "MSG_EPOCH"),
    ("cdm",   "cdm_public", "CDM_ID",                                    "CREATED"),
    ("tip",   "tip",        ["NORAD_CAT_ID", "MSG_EPOCH"],               "INSERT_EPOCH"),
]
```

- Day windows use space-track's inclusive `start--end` range (confirmed in docs). Because
  writes are **merge on the natural key**, a record on a window boundary in two adjacent
  partitions is a harmless idempotent upsert.
- `MSG_EPOCH` (decay) and `INSERT_EPOCH` (tip) predicates are confirmed by doc examples;
  the docs also recommend exactly the steady-state incremental pull this design uses.

## Merge-key caveat (⚠ verify-live)

The public docs do **not** publish model keys. `GP`/`SATCAT` (`NORAD_CAT_ID`) are
almost certainly unique; `CDM` is assumed keyed on `CDM_ID` (and the class may be
`cdm` vs `cdm_public`). `DECAY` and `TIP` have **no documented single unique
predicate**, so they use composite keys (`[NORAD_CAT_ID, MSG_EPOCH, PRECEDENCE]`,
`[NORAD_CAT_ID, MSG_EPOCH]`). All of these are confirmed against live responses in the
implementation plan's first step; if a composite still collides, add another
discriminating predicate. (Same spirit as the DONKI `CMEAnalysis` caveat — documented
so the keys aren't mistaken for guaranteed-unique.)

## Component 3 — Per-class sources + pipelines (`sources/spacetrack/__init__.py`)

One single-resource source + one pipeline **per class** (distinct pipeline names →
distinct working dirs, no collision). Sources receive an already-authenticated session
(login happens in the asset body — see Component 4). Resources are lazy, so building a
source makes **no HTTP calls**.

```python
import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental import (
    INCREMENTAL_CLASSES, _incremental_resource,
)
from auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot import (
    SNAPSHOT_CLASSES, _snapshot_resource,
)

SNAPSHOT_BY_NAME = {e[0]: e for e in SNAPSHOT_CLASSES}
INCREMENTAL_BY_NAME = {e[0]: e for e in INCREMENTAL_CLASSES}


@dlt.source
def snapshot_source(name, session=None):
    n, cls, pk, segs, wd, floor = SNAPSHOT_BY_NAME[name]
    return [_snapshot_resource(n, cls, pk, segs, wd, floor)(session)]


@dlt.source
def incremental_source(name, start_date, end_date, session=None):
    n, cls, pk, pred = INCREMENTAL_BY_NAME[name]
    return [_incremental_resource(n, cls, pk, pred)(session, start_date, end_date)]


def _pipeline(name):
    return dlt.pipeline(
        pipeline_name=f"spacetrack_{name}",   # distinct working dir per class
        destination="filesystem",
        dataset_name="bronze",                # tables land at bronze/<name>
    )


spacetrack_pipelines = {
    name: _pipeline(name)
    for name in list(SNAPSHOT_BY_NAME) + list(INCREMENTAL_BY_NAME)
}
```

All of `snapshot_source`, `incremental_source`, `spacetrack_pipelines`, the registries,
and `login_session` are re-exported from `sources/__init__.py`. Pipelines live here (not
in `assets.py`) so `assets.py` never needs `import dlt` (which would shadow the
`DagsterDltResource` param named `dlt`).

## Component 4 — Per-class assets, pooling, scheduling (`dlt/assets.py`)

Two small asset-builder helpers generate one `@dlt_assets` per registry entry. Each
**logs in once in its own body** and rebuilds its single-resource source with the real
session. Assets are bound to module-level names so `load_assets_from_package_module`
discovers them.

```python
class SpaceTrackDltTranslator(DagsterDltTranslator):
    def __init__(self, cron):
        self._cron = cron
        super().__init__()

    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            automation_condition=AutomationCondition.on_cron(self._cron),
        )


# Staggered after SATCAT's 1700 UTC update, off-the-hour minutes, GP/SATCAT/BOXSCORE
# spread so they don't all fire on the same minute (they serialize on the pool anyway).
_SNAPSHOT_CRON = {"gp": "11 18 * * *", "satcat": "21 18 * * *", "boxscore": "31 18 * * *"}
_INCREMENTAL_CRON = {"decay": "41 18 * * *", "cdm": "46 18 * * *", "tip": "51 18 * * *"}


def _snapshot_assets(name):
    pipeline = spacetrack_pipelines[name]

    @dlt_assets(
        dlt_source=snapshot_source(name),                 # session=None at import
        dlt_pipeline=pipeline,
        name=f"spacetrack_{name}_bronze",
        group_name="spacetrack",
        # NO partitions_def — current-state snapshot
        dagster_dlt_translator=SpaceTrackDltTranslator(_SNAPSHOT_CRON[name]),
        pool=SPACETRACK_API_POOL,
    )
    def _assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        session = login_session()                          # one login per run
        yield from dlt.run(
            context=context, dlt_source=snapshot_source(name, session=session)
        )

    return _assets


def _incremental_assets(name):
    pipeline = spacetrack_pipelines[name]

    @dlt_assets(
        dlt_source=incremental_source(name, start_date=date.today(), end_date=date.today()),
        dlt_pipeline=pipeline,
        name=f"spacetrack_{name}_bronze",
        group_name="spacetrack",
        partitions_def=daily_partitions,
        dagster_dlt_translator=SpaceTrackDltTranslator(_INCREMENTAL_CRON[name]),
        pool=SPACETRACK_API_POOL,
    )
    def _assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        rng = context.partition_key_range
        session = login_session()                          # one login per run
        source = incremental_source(
            name,
            start_date=date.fromisoformat(rng.start),
            end_date=date.fromisoformat(rng.end),
            session=session,
        )
        yield from dlt.run(context=context, dlt_source=source)

    return _assets


spacetrack_gp_assets       = _snapshot_assets("gp")
spacetrack_satcat_assets   = _snapshot_assets("satcat")
spacetrack_boxscore_assets = _snapshot_assets("boxscore")
spacetrack_decay_assets    = _incremental_assets("decay")
spacetrack_cdm_assets      = _incremental_assets("cdm")
spacetrack_tip_assets      = _incremental_assets("tip")
```

Asset keys follow dlt's convention `dlt_<pipeline_name>_<resource>`, e.g.
`dlt_spacetrack_gp_gp`, `dlt_spacetrack_decay_decay`. (The pipeline-name = resource-name
per class makes the key read doubled; acceptable, or set a distinct pipeline name if the
doubling bothers — cosmetic only.) The `pool="spacetrack_api"` binding (verified) means
≤1 space-track op in flight, so the six assets never log in or query simultaneously.

> **Login count:** six assets × one login per run = up to 6 logins/day in steady state
> (serialized by the pool). Backfilling an incremental class logs in once **per
> partition run**, so logins scale with run count — backfill gently (below).

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

- **`config.py`** — `SPACETRACK_API_POOL = "spacetrack_api"` plus the rate-limit table
  above as documenting constants.

- **`.env.example`** — add:

  ```
  # ---- space-track.org (consumed by dlt as dlt.secrets["spacetrack_username"/"_password"]) ----
  SPACETRACK_USERNAME=your_spacetrack_username
  SPACETRACK_PASSWORD=your_spacetrack_password
  ```

## Error Handling

| Situation | Behavior |
|-----------|----------|
| Bad credentials | `login_session()` probe fails → raises before any class query |
| Empty window/class (`[]`) | Resource yields nothing; no rows; no error |
| Non-list 200 body | Guard skips it (looks like an empty result) |
| **Suspected row-cap** (snapshot below floor) | `min_rows` guard **raises** — fail loudly rather than write a truncated catalog |
| Boundary-overlapping incremental record | Idempotent merge upsert (no duplication) |
| `401` / `429` / `5xx` on a query | `raise_for_status()` fails that class's partition loudly; **no auto-retry** — re-materialize. Scoped to one class (per-class isolation), so it never burns another class's daily budget |

## Failure isolation (why per-class)

With one asset per class, a failure on (say) SATCAT fails only `spacetrack_satcat_assets`
and only consumes SATCAT's 1/day budget; GP and BOXSCORE are untouched. Re-materialize
the failed class once its own limit allows (GP: next hour; SATCAT/BOXSCORE/DECAY: next
day; CDM: up to 3/day; TIP: next hour). This is the concrete payoff of splitting the
classes rather than grouping them into one all-or-nothing source.

## Resilience & backfill

Steady-state daily runs are ~1 login + 1 query per class — far under <30/min, <300/hr,
and within every per-class limit (hence the post-1700-UTC schedule). The snapshot
classes are **not** backfillable (no historical "as of" query without GP_HISTORY). Only
DECAY/CDM/TIP backfill over date partitions, each independently.

**Mitigation (operational, no code):** backfill an incremental class in **small
date-range batches** with limited run concurrency, and re-run any partitions that 429 —
remember each partition run performs its own login, so a wide backfill is many logins;
keep it gentle. CDM history retention is server-side limited, so early partitions may
legitimately return `[]`. A per-provider hourly rate-budget scheduler remains out of
scope.

## Testing

(All mocked — no live HTTP; monkeypatch `login_session`/`query_class`.)

- **Snapshot factory:** list → one row per element; `[]`/non-list → no rows; below-floor
  list → **raises**; resource has the right `name`, `write_disposition`, `primary_key`,
  `table_format="delta"`; `boxscore` is `replace` with no key and no floor.
- **Incremental factory:** builds the correct `start--end+1` window per day across a
  multi-day range and passes the right date predicate; list → rows; `[]`/non-list → none;
  merge attrs correct.
- **Registries:** exactly 3 snapshot + 3 incremental; names unique; primary keys present
  (or `None` only for boxscore); `cdm` uses class `cdm_public`; each incremental entry
  has a date predicate.
- **Auth:** `login_session` POSTs `identity`/`password`, raises when the probe is
  rejected, returns the session on a JSON probe; `query_class` builds the correct URL.
- **Sources/pipelines:** `snapshot_source("gp")` / `incremental_source("decay", ...)`
  each expose one resource of the right name; constructing with `session=None` makes no
  HTTP call; `spacetrack_pipelines` has 6 entries with distinct names.
- **Wiring:** the six module-level asset defs exist; keys `dlt_spacetrack_<name>_<name>`
  in group `spacetrack`; snapshot assets unpartitioned, incremental use `daily_partitions`;
  every op has `pool == "spacetrack_api"`.
- **Smoke:** `test_definitions_load` still loads (no import-time HTTP/Delta).

## First implementation step — live API verification (⚠ required)

Before/while building, with the account, confirm against the authenticated API:
class names (`cdm` vs `cdm_public`; `gp`, `satcat`, `decay`, `tip`, `boxscore`),
that a no-`limit` query returns the **full** result set (set `min_rows` floors from the
observed counts; add explicit `limit`/`offset` paging only if a cap is found),
the unique keys (esp. `CDM_ID`; DECAY/TIP composites), the `CURRENT/Y` SATCAT segment,
and the real failed-login response shape. Fold any corrections back into the registries.

## Out of Scope

- **GP_HISTORY** (138M+ rows; queryable once per lifetime) — its own spec + one-time backfill.
- Silver-layer modeling (orbital-element curation, conjunction graphs, cross-class joins).
- The per-provider hourly rate-budget scheduler (still future).
- Retrofitting NASA assets into pools; other providers (spaceflightnews, thespacedevs).
