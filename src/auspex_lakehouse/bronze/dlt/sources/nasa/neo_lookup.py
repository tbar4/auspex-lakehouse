from dataclasses import dataclass, field
from datetime import datetime, timedelta

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
