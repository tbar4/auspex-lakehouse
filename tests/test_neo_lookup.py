from datetime import datetime, timedelta, timezone

from auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup import select_neo_work_ids

NOW = datetime(2026, 6, 28, tzinfo=timezone.utc)


def test_new_ids_selected_when_table_empty():
    plan = select_neo_work_ids({"a", "b"}, {}, NOW, 30, 500)
    assert set(plan.new) == {"a", "b"}
    assert plan.stale == []
    assert set(plan.selected) == {"a", "b"}
    assert plan.deferred_over_cap == []


def test_fresh_ids_skipped():
    existing = {"a": NOW - timedelta(days=10)}
    plan = select_neo_work_ids({"a"}, existing, NOW, 30, 500)
    assert plan.selected == []
    assert plan.new == []
    assert plan.stale == []


def test_stale_ids_refreshed():
    existing = {"a": NOW - timedelta(days=40)}
    plan = select_neo_work_ids({"a"}, existing, NOW, 30, 500)
    assert plan.stale == ["a"]
    assert plan.selected == ["a"]


def test_cap_prioritizes_new_and_defers_rest():
    candidates = {"n0", "n1", "n2", "old"}
    existing = {"old": NOW - timedelta(days=99)}  # stale
    plan = select_neo_work_ids(candidates, existing, NOW, 30, cap=2)
    assert plan.selected == ["n0", "n1"]            # new sorted, prioritized
    assert plan.deferred_over_cap == ["n2", "old"]  # remaining new + stale deferred
