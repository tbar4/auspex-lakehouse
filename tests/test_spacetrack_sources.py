# tests/test_spacetrack_sources.py
from datetime import date
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental as inc
import auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot as snap


def test_snapshot_resource_yields_each_row(monkeypatch):
    monkeypatch.setattr(snap, "query_class",
                        lambda session, *seg: [{"NORAD_CAT_ID": 1}, {"NORAD_CAT_ID": 2}])
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID",
                                  ("orderby", "NORAD_CAT_ID"), "merge", 0)
    rows = list(res(session=Mock()))
    assert len(rows) == 2
    assert res.name == "gp"


def test_snapshot_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: None)
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", (), "merge", 0)
    assert list(res(session=Mock())) == []


def test_snapshot_resource_raises_below_floor(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: [{"x": 1}])
    res = snap._snapshot_resource("gp", "gp", "NORAD_CAT_ID", (), "merge", 10)
    # dlt wraps RuntimeError in ResourceExtractionError (PipeException -> DltException -> Exception)
    with pytest.raises(Exception, match="suspected row-cap"):
        list(res(session=Mock()))


def test_snapshot_registry_shape():
    by = {e[0]: e for e in snap.SNAPSHOT_CLASSES}
    assert [e[0] for e in snap.SNAPSHOT_CLASSES] == ["gp", "satcat", "boxscore"]
    assert by["gp"][4] == "merge" and by["gp"][2] == "NORAD_CAT_ID"
    assert by["satcat"][4] == "merge" and by["satcat"][2] == "NORAD_CAT_ID"
    assert by["boxscore"][4] == "replace" and by["boxscore"][2] is None
    assert by["boxscore"][5] is None  # no row-count floor for the small aggregate


def test_incremental_resource_windows_each_day(monkeypatch):
    calls = []

    def fake_query(session, cls, predicate, window):
        calls.append((cls, predicate, window))
        return [{"id": window}]

    monkeypatch.setattr(inc, "query_class", fake_query)
    res = inc._incremental_resource("decay", "decay", ["NORAD_CAT_ID"], "MSG_EPOCH")
    rows = list(res(Mock(), date(2026, 1, 1), date(2026, 1, 2)))

    assert calls == [
        ("decay", "MSG_EPOCH", "2026-01-01--2026-01-02"),
        ("decay", "MSG_EPOCH", "2026-01-02--2026-01-03"),
    ]
    assert len(rows) == 2
    assert res.name == "decay"


def test_incremental_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(inc, "query_class", lambda *a: None)
    res = inc._incremental_resource("tip", "tip", ["NORAD_CAT_ID"], "INSERT_EPOCH")
    assert list(res(Mock(), date(2026, 1, 1), date(2026, 1, 1))) == []


def test_incremental_registry_shape():
    by = {e[0]: e for e in inc.INCREMENTAL_CLASSES}
    assert [e[0] for e in inc.INCREMENTAL_CLASSES] == ["decay", "cdm", "tip"]
    assert by["cdm"][1] == "cdm_public"
    assert by["cdm"][2] == "CDM_ID"
    assert by["decay"][3] == "MSG_EPOCH"
    assert by["tip"][3] == "INSERT_EPOCH"
    assert all(len(e) == 4 for e in inc.INCREMENTAL_CLASSES)
