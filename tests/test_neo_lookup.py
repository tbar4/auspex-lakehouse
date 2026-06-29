from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup as nl
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


def _resp(status, payload=None):
    r = Mock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.raise_for_status = Mock(
        side_effect=None if status < 400 else RuntimeError(f"http {status}")
    )
    return r


def test_fetch_ok_stamps_row(monkeypatch):
    monkeypatch.setattr(
        nl,
        "requests",
        Mock(
            get=Mock(
                return_value=_resp(200, {"neo_reference_id": "a", "name": "X"})
            )
        ),
    )
    rows, stats = nl.fetch_neo_lookups(["a"], "2026-06-28T00:00:00+00:00", "key")
    assert stats.fetched_ok == 1
    assert rows[0]["lookup_status"] == "ok"
    assert rows[0]["lookup_fetched_at"] == "2026-06-28T00:00:00+00:00"
    assert rows[0]["name"] == "X"


def test_fetch_404_writes_tombstone(monkeypatch):
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(return_value=_resp(404))))
    rows, stats = nl.fetch_neo_lookups(["dead"], "T", "key")
    assert stats.tombstoned == 1 and stats.fetched_ok == 0
    assert rows == [
        {"neo_reference_id": "dead", "lookup_fetched_at": "T", "lookup_status": "not_found"}
    ]


def test_fetch_429_stops_and_defers_tail(monkeypatch):
    seq = [_resp(200, {"neo_reference_id": "a"}), _resp(429), _resp(200, {"neo_reference_id": "c"})]
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(side_effect=seq)))
    rows, stats = nl.fetch_neo_lookups(["a", "b", "c"], "T", "key")
    assert stats.stopped_on_rate_limit is True
    assert stats.fetched_ok == 1
    assert stats.deferred_on_stop == ["b", "c"]
    assert [r["neo_reference_id"] for r in rows] == ["a"]


def test_fetch_other_error_raises(monkeypatch):
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(return_value=_resp(500))))
    with pytest.raises(RuntimeError):
        nl.fetch_neo_lookups(["x"], "T", "key")


def test_age_equal_to_refresh_window_is_not_stale():
    existing = {"a": NOW - timedelta(days=30)}
    plan = select_neo_work_ids({"a"}, existing, NOW, 30, 500)
    assert plan.stale == []
    assert plan.selected == []


def test_fetch_404_continues_to_next_id(monkeypatch):
    seq = [_resp(404), _resp(200, {"neo_reference_id": "live"})]
    monkeypatch.setattr(nl, "requests", Mock(get=Mock(side_effect=seq)))
    rows, stats = nl.fetch_neo_lookups(["dead", "live"], "T", "key")
    assert len(rows) == 2
    assert stats.tombstoned == 1
    assert stats.fetched_ok == 1
    assert rows[0]["lookup_status"] == "not_found"
    assert rows[1]["lookup_status"] == "ok"
