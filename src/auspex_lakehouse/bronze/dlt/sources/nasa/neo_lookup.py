from dataclasses import dataclass
from datetime import datetime, timedelta


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
