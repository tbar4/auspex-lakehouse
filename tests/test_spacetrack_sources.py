# tests/test_spacetrack_sources.py
from datetime import date
from unittest.mock import Mock

import pytest

import auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental as inc
import auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot as snap


def test_snapshot_resource_yields_each_row(monkeypatch):
    monkeypatch.setattr(snap, "query_class",
                        lambda session, *seg: [{"NORAD_CAT_ID": 1}, {"NORAD_CAT_ID": 2}])
    res = snap._snapshot_resource("space_track_general_perturbations", "gp", "NORAD_CAT_ID",
                                  ("orderby", "NORAD_CAT_ID"), "merge", 0)
    rows = list(res(session=Mock()))
    assert len(rows) == 2
    assert res.name == "space_track_general_perturbations"


def test_snapshot_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: None)
    res = snap._snapshot_resource(
        "space_track_general_perturbations", "gp", "NORAD_CAT_ID", (), "merge", 0
    )
    assert list(res(session=Mock())) == []


def test_snapshot_resource_raises_below_floor(monkeypatch):
    monkeypatch.setattr(snap, "query_class", lambda session, *seg: [{"x": 1}])
    res = snap._snapshot_resource(
        "space_track_general_perturbations", "gp", "NORAD_CAT_ID", (), "merge", 10
    )
    # dlt wraps RuntimeError in ResourceExtractionError (PipeException -> DltException -> Exception)
    with pytest.raises(Exception, match="suspected row-cap"):
        list(res(session=Mock()))


def test_snapshot_registry_shape():
    by = {e[0]: e for e in snap.SNAPSHOT_CLASSES}
    assert [e[0] for e in snap.SNAPSHOT_CLASSES] == [
        "space_track_general_perturbations",
        "space_track_satellite_catalog",
        "space_track_boxscore",
    ]
    assert (
        by["space_track_general_perturbations"][4] == "merge"
        and by["space_track_general_perturbations"][2] == "NORAD_CAT_ID"
    )
    assert (
        by["space_track_satellite_catalog"][4] == "merge"
        and by["space_track_satellite_catalog"][2] == "NORAD_CAT_ID"
    )
    assert by["space_track_boxscore"][4] == "replace" and by["space_track_boxscore"][2] is None
    assert by["space_track_boxscore"][5] is None  # no row-count floor for the small aggregate


def test_incremental_resource_windows_each_day(monkeypatch):
    calls = []

    def fake_query(session, cls, predicate, window):
        calls.append((cls, predicate, window))
        return [{"NORAD_CAT_ID": 1, "id": window}]

    monkeypatch.setattr(inc, "query_class", fake_query)
    res = inc._incremental_resource("space_track_decays", "decay", ["NORAD_CAT_ID"], "MSG_EPOCH")
    rows = list(res(Mock(), date(2026, 1, 1), date(2026, 1, 2)))

    assert calls == [
        ("decay", "MSG_EPOCH", "2026-01-01--2026-01-02"),
        ("decay", "MSG_EPOCH", "2026-01-02--2026-01-03"),
    ]
    assert len(rows) == 2
    assert res.name == "space_track_decays"


def test_incremental_resource_drops_rows_missing_primary_key(monkeypatch, caplog):
    # TIP (and other merge resources) fail terminally in dlt normalize if a row is
    # missing a primary-key column: get_row_hash over the key subset raises KeyError
    # (or the delta/duckdb load rejects the null key). Such rows can't participate in
    # the merge, so the resource must drop them — and say so — rather than crash.
    def fake_query(session, cls, predicate, window):
        return [
            {"NORAD_CAT_ID": 1, "MSG_EPOCH": "2026-01-01"},   # keep
            {"MSG_EPOCH": "2026-01-01"},                      # drop: NORAD_CAT_ID absent
            {"NORAD_CAT_ID": None, "MSG_EPOCH": "2026-01-01"},  # drop: null key
            {"NORAD_CAT_ID": "  ", "MSG_EPOCH": "2026-01-01"},  # drop: blank key
            {"NORAD_CAT_ID": 2},                             # drop: MSG_EPOCH absent
        ]

    monkeypatch.setattr(inc, "query_class", fake_query)
    res = inc._incremental_resource(
        "space_track_tracking_and_impact_predictions", "tip",
        ["NORAD_CAT_ID", "MSG_EPOCH"], "INSERT_EPOCH",
    )
    import logging
    with caplog.at_level(logging.WARNING):
        rows = list(res(Mock(), date(2026, 1, 1), date(2026, 1, 1)))

    assert rows == [{"NORAD_CAT_ID": 1, "MSG_EPOCH": "2026-01-01"}]
    assert "dropped 4" in caplog.text and "tip" in caplog.text


def test_incremental_resource_non_list_yields_nothing(monkeypatch):
    monkeypatch.setattr(inc, "query_class", lambda *a: None)
    res = inc._incremental_resource(
        "space_track_tracking_and_impact_predictions", "tip", ["NORAD_CAT_ID"], "INSERT_EPOCH"
    )
    assert list(res(Mock(), date(2026, 1, 1), date(2026, 1, 1))) == []


def test_incremental_registry_shape():
    by = {e[0]: e for e in inc.INCREMENTAL_CLASSES}
    assert [e[0] for e in inc.INCREMENTAL_CLASSES] == [
        "space_track_decays",
        "space_track_conjunction_data_messages",
        "space_track_tracking_and_impact_predictions",
    ]
    assert by["space_track_conjunction_data_messages"][1] == "cdm_public"
    assert by["space_track_conjunction_data_messages"][2] == "CDM_ID"
    assert by["space_track_decays"][3] == "MSG_EPOCH"
    assert by["space_track_tracking_and_impact_predictions"][3] == "INSERT_EPOCH"
    assert all(len(e) == 4 for e in inc.INCREMENTAL_CLASSES)


def test_snapshot_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import snapshot_source
    src = snapshot_source("space_track_general_perturbations")  # session=None -> no HTTP
    assert set(src.resources.keys()) == {"space_track_general_perturbations"}


def test_incremental_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import incremental_source
    src = incremental_source(
        "space_track_decays", start_date=date(2026, 1, 1), end_date=date(2026, 1, 1)
    )
    assert set(src.resources.keys()) == {"space_track_decays"}


def test_pipelines_dict_has_all_six():
    from auspex_lakehouse.bronze.dlt.sources import spacetrack_pipelines
    assert set(spacetrack_pipelines) == {
        "space_track_general_perturbations",
        "space_track_satellite_catalog",
        "space_track_boxscore",
        "space_track_decays",
        "space_track_conjunction_data_messages",
        "space_track_tracking_and_impact_predictions",
    }
    assert (
        spacetrack_pipelines["space_track_general_perturbations"].pipeline_name
        == "spacetrack_space_track_general_perturbations"
    )
    assert spacetrack_pipelines["space_track_decays"].dataset_name == "bronze"


def test_pool_constant():
    from auspex_lakehouse.bronze.dlt.sources.spacetrack.config import SPACETRACK_API_POOL
    assert SPACETRACK_API_POOL == "spacetrack_api"
