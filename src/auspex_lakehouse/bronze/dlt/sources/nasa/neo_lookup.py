import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import dlt
from dlt.sources.helpers import requests

from auspex_lakehouse.bronze.dlt.sources.nasa._common import BASE_URL


@dataclass
class NeoWorkPlan:
    selected: list[str]           # IDs to fetch this run (cap-bounded)
    new: list[str]                # candidates never looked up before
    stale: list[str]              # candidates whose lookup is older than the refresh window
    deferred_over_cap: list[str]  # selected-minus-cap, picked up next run


def select_neo_work_ids(
    candidates: set[str],
    existing: dict[str, datetime],
    now: datetime,
    refresh_days: int,
    cap: int,
) -> NeoWorkPlan:
    """Decide which neo_reference_ids to fetch: new first, then >refresh_days-stale,
    truncated to `cap`. Pure — no I/O."""
    new = sorted(candidates - existing.keys())
    stale = sorted(
        neo_id
        for neo_id in candidates & existing.keys()
        if now - existing[neo_id] > timedelta(days=refresh_days)
    )
    ordered = new + stale
    return NeoWorkPlan(
        selected=ordered[:cap],
        new=new,
        stale=stale,
        deferred_over_cap=ordered[cap:],
    )


@dataclass
class FetchStats:
    fetched_ok: int = 0
    tombstoned: int = 0                 # 404s recorded as tombstones
    stopped_on_rate_limit: bool = False  # hit 429; remaining IDs deferred
    deferred_on_stop: list[str] = field(default_factory=list)


def fetch_neo_lookups(
    neo_ids: list[str], fetched_at: str, api_key: str
) -> tuple[list[dict], FetchStats]:
    """Fetch each NEO lookup, tolerant per ID so one bad ID can't poison the batch:
    404 -> tombstone row (dedupe skips it until refresh); 429 -> commit progress and
    defer the remaining tail; any other non-2xx -> raise."""
    rows: list[dict] = []
    stats = FetchStats()
    for idx, neo_id in enumerate(neo_ids):
        resp = requests.get(
            f"{BASE_URL}/neo/rest/v1/neo/{neo_id}",
            params={"api_key": api_key},
        )
        if resp.status_code == 404:
            rows.append(
                {
                    "neo_reference_id": neo_id,
                    "lookup_fetched_at": fetched_at,
                    "lookup_status": "not_found",
                }
            )
            stats.tombstoned += 1
            continue
        if resp.status_code == 429:
            stats.stopped_on_rate_limit = True
            stats.deferred_on_stop = list(neo_ids[idx:])
            return rows, stats
        resp.raise_for_status()
        rows.append({**resp.json(), "lookup_fetched_at": fetched_at, "lookup_status": "ok"})
        stats.fetched_ok += 1
    return rows, stats


@dlt.resource(
    name="nasa_near_earth_object_lookups",
    write_disposition="merge",
    primary_key="neo_reference_id",
    table_format="delta",
)
def neo_lookup_rows(rows: list[dict]):
    """Pass-through resource over already-fetched rows; dlt does the Delta merge
    and normalizes nested payload fields (orbital_data, close_approach_data, ...)
    into child tables."""
    yield from rows


def build_neo_lookup_pipeline(pipelines_dir: str | None = None):
    """Build the NEO-lookup dlt pipeline.

    `pipelines_dir` isolates dlt's local working directory. The asset passes a fresh
    per-run dir (see load_neo_lookups) so a load package left partially written by an
    interrupted run can never be resumed — and crash — on the next run. dlt otherwise
    reuses one persistent working dir and re-tries pending packages across runs, which
    is what failed in production (FileNotFoundError retrying a stale .reference job).
    Merge correctness is destination-side (the Delta primary key), so the local working
    dir is disposable.
    """
    return dlt.pipeline(
        pipeline_name="nasa_neo_lookup",   # distinct working dir -> no collision with nasa_api
        destination="filesystem",
        dataset_name="bronze",             # same bronze dataset -> lands at bronze/neo_lookup
        pipelines_dir=pipelines_dir,
    )


def load_neo_lookups(rows: list[dict]) -> None:
    """Merge fetched NEO rows into the bronze Delta table via a disposable dlt working dir.

    Each call gets its own temp working dir, removed afterward (success or failure), so no
    partial/pending load package survives to poison a later run. Re-running is idempotent:
    rows merge by neo_reference_id at the destination, so a discarded partial load is simply
    re-fetched and re-merged next run.
    """
    pipelines_dir = tempfile.mkdtemp(prefix="dlt-nasa_neo_lookup-")
    try:
        build_neo_lookup_pipeline(pipelines_dir).run(neo_lookup_rows(rows))
    finally:
        shutil.rmtree(pipelines_dir, ignore_errors=True)


# Module-level default instance kept for the public export contract; the asset loads via
# load_neo_lookups (isolated per-run dir) rather than this shared-dir singleton.
nasa_neo_lookup_pipeline = build_neo_lookup_pipeline()
