# NASA NEO Lookup — Bronze Source Design

**Date:** 2026-06-28
**Status:** Approved (revised after adversarial review), pending implementation plan

## Goal

Add a new bronze source that enriches near-earth-object data. For each
`neo_reference_id` surfaced by the existing `neows` feed extract, call the NASA
NEO lookup endpoint and land the full per-asteroid payload in a `neo_lookup`
bronze Delta table.

```
GET https://api.nasa.gov/neo/rest/v1/neo/<neo_reference_id>?api_key=<NASA_API_KEY>
```

Base URL reuses the existing `BASE_URL = "https://api.nasa.gov"`.

## Constraints & Principles

- **NASA API budget: 1000 calls/hour, shared across all NASA endpoints.** Orbital
  data is nearly static, so we fetch each asteroid once and refresh only
  periodically.
- **Budget is bounded by run concurrency, not a per-run count** (see Component 5
  and the adversarial-review note below). The per-run cap is a secondary guard.
- **Per-provider budgets.** The 1000/hr limit, the cap, and the concurrency tag
  are NASA-specific. Future providers (space-track.org, spaceflightnews,
  thespacedevs) have their own limits and get their own controls.
- **Idempotent + dedupe-driven + fault-tolerant per ID**, so a single bad ID
  cannot poison a partition or waste the budget, and so this slots cleanly into a
  future per-provider rate-budget scheduler (out of scope here).
- **Keep Dagster layers clean and concise** — small, single-purpose modules with
  well-defined interfaces.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Which IDs to fetch | **New + periodic refresh** (new IDs always; existing IDs re-fetched once their lookup is older than the refresh window) |
| Where IDs come from | **Read the bronze `neows` Delta table** (data dependency on the dlt neows asset) |
| Budget enforcement | **429-handling + hourly dedupe (primary) + concurrency pool + per-run cap (secondary)** |
| Per-ID failures | **Tolerant**: 404 → tombstone; 429 → stop & defer; else raise |
| Payload shape | **Land the full nested payload** (bronze = raw); dlt normalizes nested fields into child tables; flattening deferred to silver |
| Code organization | **One module per endpoint** under a `nasa/` package |

**Defaults:** refresh window **30 days**; NASA per-run cap **500 lookups/run**;
`nasa_api` pool limit **1** (instance config).

## Adversarial-review changes (v1 → v2)

This revision addresses the review of the v1 spec:

- **C1 — the per-run cap did not protect the budget.** A daily partition's
  candidate set is one day of the feed (~10–30 objects), so a 500/run cap never
  triggers in steady state; the real risk is `eager` cascading into *parallel*
  partition runs during a backfill, and the aggregate across NASA endpoints —
  neither bounded by a per-run count. **The real ceiling is the API's 429 plus
  the hourly-resume-with-dedupe loop; a `nasa_api` concurrency pool (limit 1,
  Component 5) serializes access** so runs can't double-spend the shared bucket.
  The per-run cap is kept only as a cheap secondary guard.
- **C2 — direct reuse of `nasa_pipeline` risked working-dir/state collisions**
  (no `pipelines_dir` is set, so dlt defaults to `~/.dlt/pipelines/nasa_api`, the
  same object bound into the apod/neows `@dlt_assets`). **The lookup now uses a
  dedicated pipeline** (`pipeline_name="nasa_neo_lookup"`, same `dataset_name="bronze"`).
- **C3 — `raise_for_status()` created poison-pill partitions and wasted budget.**
  **Fetching is now per-ID fault-tolerant** and progress is committed even on a
  partial run (Component 3 / Component 4).

## Component 1 — Source module restructure

Split the single `sources/nasa_api.py` into a `nasa/` package so each endpoint is
a small, focused file with shared helpers factored out.

```
src/auspex_lakehouse/bronze/dlt/sources/
  __init__.py            # re-exports nasa_api, nasa_pipeline, neo_lookup_rows, nasa_neo_lookup_pipeline
  nasa/
    __init__.py          # assembles nasa_api source + nasa_pipeline; exports resources, config, neo-lookup domain
    _common.py           # BASE_URL, _iter_days(), api_key access
    config.py            # NASA budget constants
    apod.py              # apod resource (moved verbatim)
    neows.py             # neows resource (moved verbatim)
    neo_lookup.py        # NEW: neo-lookup domain (planner + fetcher + write resource + pipeline)
```

**Public-name stability:** `nasa_api` and `nasa_pipeline` remain importable from
`auspex_lakehouse.bronze.dlt.sources` exactly as today, so `dlt/assets.py` and
`definitions.py` need no import changes. `__init__.py` **additionally** exports
`neo_lookup_rows` and `nasa_neo_lookup_pipeline` for the new asset.
`apod`/`neows` behavior is unchanged — they are only relocated.

`nasa/config.py` holds the NASA-provider budget knobs:

```python
NASA_REFRESH_DAYS = 30           # re-fetch a NEO whose lookup is older than this
NASA_MAX_LOOKUPS_PER_RUN = 500   # secondary per-run guard (primary control is the pool + 429-handling)
NASA_API_POOL = "nasa_api"       # concurrency pool serializing NASA API access
```

## Component 2 — NEO-lookup domain module (`nasa/neo_lookup.py`)

Holds all NEO-lookup logic in one focused file. Three pieces:

**(a) Pure planner** — no I/O, unit-testable in isolation:

```python
@dataclass
class NeoWorkPlan:
    selected: list[str]          # IDs to fetch this run (cap-bounded)
    new: list[str]
    stale: list[str]
    deferred_over_cap: list[str] # selected-minus-cap, picked up next run

def select_neo_work_ids(
    candidates: set[str],
    existing: dict[str, datetime],   # neo_reference_id -> last lookup_fetched_at
    now: datetime,
    refresh_days: int,
    cap: int,
) -> NeoWorkPlan:
    new   = sorted(candidates - existing.keys())
    stale = sorted(i for i in candidates & existing.keys()
                   if now - existing[i] > timedelta(days=refresh_days))
    ordered = new + stale                       # new prioritized over refresh
    return NeoWorkPlan(selected=ordered[:cap], new=new, stale=stale,
                       deferred_over_cap=ordered[cap:])
```

**(b) Fault-tolerant fetcher** — pure I/O, returns rows + stats; never lets one
ID abort the batch:

```python
@dataclass
class FetchStats:
    fetched_ok: int
    tombstoned: int               # 404s recorded as tombstones
    stopped_on_rate_limit: bool   # hit 429; remaining IDs deferred
    deferred_on_stop: list[str]

def fetch_neo_lookups(neo_ids: list[str], fetched_at: str) -> tuple[list[dict], FetchStats]:
    rows, ok, tomb = [], 0, 0
    for idx, neo_id in enumerate(neo_ids):
        resp = requests.get(f"{BASE_URL}/neo/rest/v1/neo/{neo_id}",
                            params={"api_key": dlt.secrets["nasa_api_key"]})
        if resp.status_code == 404:
            rows.append({"neo_reference_id": neo_id, "lookup_fetched_at": fetched_at,
                         "lookup_status": "not_found"})
            tomb += 1
            continue
        if resp.status_code == 429:   # budget exhausted mid-run: commit progress, defer rest
            return rows, FetchStats(ok, tomb, True, neo_ids[idx:])
        resp.raise_for_status()       # any other non-2xx is a real error -> fail loudly
        rows.append({**resp.json(), "lookup_fetched_at": fetched_at, "lookup_status": "ok"})
        ok += 1
    return rows, FetchStats(ok, tomb, False, [])
```

**(c) Write resource + dedicated pipeline** — a trivial pass-through resource
over already-fetched rows lets dlt do the Delta **merge** and the nested-payload
**normalization** (into `neo_lookup__close_approach_data`, etc.):

```python
@dlt.resource(name="neo_lookup", write_disposition="merge",
              primary_key="neo_reference_id", table_format="delta")
def neo_lookup_rows(rows: list[dict]):
    yield from rows

nasa_neo_lookup_pipeline = dlt.pipeline(
    pipeline_name="nasa_neo_lookup",   # distinct working dir -> no collision with nasa_api (C2)
    destination="filesystem",
    dataset_name="bronze",             # same bronze dataset -> lands at bronze/neo_lookup
)
```

- **Tombstones** (`lookup_status="not_found"`) merge in as sparse rows so dedupe
  skips them until the refresh window re-tries them — a permanently-dead ID never
  re-poisons the partition (C3).
- Committing the rows collected *before* a 429 preserves paid-for progress; the
  deferred tail returns to the work list next run (C3).

## Component 3 — Work-list + asset (`dlt/assets.py`)

A thin Dagster `@asset` orchestrates; all NEO logic lives in Component 2.

```python
@asset(
    name="neo_lookup",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_api_neows"])],         # mirrors apod_images -> dlt_nasa_api_apod
    automation_condition=AutomationCondition.eager(),
    pool="nasa_api",                                 # serialized against the NASA budget (C1)
)
def neo_lookup_asset(context: AssetExecutionContext):
    pk = context.partition_key
    candidates = set(
        read_bronze_table("neows")
        .filter(pl.col("date") == pk)
        .get_column("neo_reference_id").to_list()
    )
    existing = _existing_lookup_index()              # {} if neo_lookup table absent (M1)
    now = datetime.now(timezone.utc)
    plan = select_neo_work_ids(candidates, existing, now,
                               NASA_REFRESH_DAYS, NASA_MAX_LOOKUPS_PER_RUN)
    if not plan.selected:
        context.add_output_metadata({"candidates": len(candidates), "fetched": 0})
        return
    rows, stats = fetch_neo_lookups(plan.selected, now.isoformat())
    if rows:
        nasa_neo_lookup_pipeline.run(neo_lookup_rows(rows))
    context.add_output_metadata({
        "candidates": len(candidates), "new": len(plan.new), "stale": len(plan.stale),
        "fetched_ok": stats.fetched_ok, "tombstoned": stats.tombstoned,
        "deferred_over_cap": len(plan.deferred_over_cap),
        "stopped_on_rate_limit": stats.stopped_on_rate_limit,
        "deferred_on_stop": len(stats.deferred_on_stop),
    })
```

`_existing_lookup_index()` reads `read_bronze_table("neo_lookup")` (empty if the
table doesn't exist yet) and parses `lookup_fetched_at` to tz-aware datetimes —
**timestamps are parsed, not string-compared** (M3).

**Why a plain `@asset` (not `@dlt_assets`):** matches the existing `apod_images`
pattern, yields a clean `neo_lookup` asset key, makes the neows dependency a
one-line `deps=`, and — critically — keeps the fetch loop in our hands so the
per-ID fault tolerance (C3) isn't fighting dlt's all-or-nothing extract.

## Component 4 — Shared Delta helper (`resources/delta.py`)

Factor the inline MinIO/`DeltaTable` storage-options block out of `apod_images`
into reusable helpers, with **explicit missing-table handling** (M1):

```python
def delta_storage_options() -> dict: ...
def bronze_table_exists(name: str) -> bool:
    return DeltaTable.is_deltatable(f"{BRONZE_URI}/bronze/{name}", storage_options=...)
def read_bronze_table(name: str) -> pl.DataFrame:
    """Open a bronze Delta table as Polars; raises if absent — callers guard via bronze_table_exists."""
```

- `apod_images` is refactored to use `read_bronze_table("apod")` (behavior
  unchanged).
- `_existing_lookup_index()` checks `bronze_table_exists("neo_lookup")` first and
  returns `{}` on the first run.
- The existing `delta_io_manager` in this file is left as-is.

## Component 5 — Budget control via a concurrency pool (`dagster.yaml`)

Dagster's documented mechanism for assets hitting a rate-limited API is a
**concurrency pool** (`pool=` on the asset + a per-pool limit in deployment
settings), which bounds in-progress op executions *across all runs* — exactly the
backfill-fan-out risk. The high-volume NASA consumer (`neo_lookup`) joins a
`nasa_api` pool; future heavy NASA endpoints join the same pool so they share the
single 1000/hr bucket.

```yaml
# dagster.yaml — add alongside the existing `storage:` block
concurrency:
  pools:
    nasa_api:
      limit: 1          # serialize NASA API access: the budget is one shared
      granularity: 'op' # bucket, so parallel streams only mutually 429 and double-spend
```

The asset declares `@asset(..., pool="nasa_api")`.

**What actually bounds the 1000/hr spend** is the interaction of three things, in
order of importance:
1. **The API's own 429** (Component 2's fetcher rides up to the limit, then stops
   and defers) — this is the real ceiling.
2. **The hourly cadence + dedupe** — each run resumes the deferred/new work; the
   initial backfill drains over successive hours without redoing fetched IDs.
3. **The pool (limit 1)** — serializes access so two runs can't both burn the
   shared bucket at once, and prevents dlt pipeline-state races.

The per-run cap (`NASA_MAX_LOOKUPS_PER_RUN`) is a secondary politeness guard that
leaves headroom for other NASA endpoints sharing the same hour. `apod`/`neows`
are **not** pooled — each makes ≤1 call per partition, negligible against the
budget. The full hourly-token rate-budget scheduler across all NASA endpoints
remains a separate future spec.

## Data Flow

```
neows feed (dlt) ─► bronze/neows (Delta)
                        │  distinct neo_reference_id where date == partition (Component 3)
                        ▼
        select_neo_work_ids (pure)  ◄── existing index from bronze/neo_lookup (Component 4, {} if absent)
                        │  new + stale, cap-bounded
                        ▼
        fetch_neo_lookups (per-ID tolerant: 404→tombstone, 429→stop&defer)
                        │  rows (+ tombstones)
                        ▼
        neo_lookup_rows ─► nasa_neo_lookup_pipeline ─► bronze/neo_lookup (Delta, merge on neo_reference_id)
                                                           └─ nested → bronze/neo_lookup__close_approach_data, ...
```

## Error Handling (summary)

| Situation | Behavior |
|-----------|----------|
| ID returns 404 | Tombstone row written; dedupe skips it until refresh window; never re-poisons the partition |
| Budget hit (429) mid-run | Commit rows already fetched; defer remaining IDs to next run; surface `stopped_on_rate_limit` |
| Other non-2xx | `raise_for_status()` → run fails loudly (genuine error) |
| `neo_lookup` table absent (first run) | `_existing_lookup_index()` returns `{}`; all candidates are *new* |
| Empty work list | Asset short-circuits; zero-count metadata; no API calls |
| Cap reached | Excess IDs reported via `deferred_over_cap`; fetched next run |
| Backfill fan-out | Serialized by the `nasa_api` concurrency pool (limit 1, Component 5); spend drains over successive hourly runs |

## Testing

- `select_neo_work_ids`: new vs. stale vs. cap classification and new-before-stale
  ordering — pure, no network/Delta.
- `fetch_neo_lookups`: 404→tombstone, 429→stop+defer-tail, 200→stamped row,
  500→raises (mock `requests`).
- `read_bronze_table`/`bronze_table_exists`: missing table → guarded empty.
- Restructure regression: `nasa_api`/`nasa_pipeline` still import from
  `auspex_lakehouse.bronze.dlt.sources`; `apod`/`neows` output unchanged; the
  existing `test_definitions_load` smoke test still passes (no import-time
  Delta/HTTP — work-list runs only in the asset body) (m3).

## Out of Scope

- The per-provider hourly-token rate-budget scheduler (staggered crons across all
  NASA endpoints under the 1000/hr ceiling) — its own future spec.
- Other providers: space-track.org, spaceflightnews, thespacedevs — separate
  specs, separate budgets.
- Silver-layer modeling/flattening of the nested lookup payload.
```

