# tests/test_spacetrack_sources.py
from unittest.mock import Mock

import pytest

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
