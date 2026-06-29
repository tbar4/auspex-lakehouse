# tests/test_celestrak_sources.py
import pytest

import auspex_lakehouse.bronze.dlt.sources.celestrak.snapshot as snap


def test_csv_snapshot_resource_yields_each_row(monkeypatch):
    monkeypatch.setattr(snap, "fetch_csv_rows",
                        lambda url: [{"DATE": "2026-06-28"}, {"DATE": "2026-06-29"}])
    res = snap._csv_snapshot_resource("celestrak_space_weather", "http://x", "DATE", 0)
    rows = list(res())
    assert len(rows) == 2
    assert res.name == "celestrak_space_weather"


def test_csv_snapshot_resource_raises_below_floor(monkeypatch):
    monkeypatch.setattr(snap, "fetch_csv_rows", lambda url: [{"DATE": "2026-06-28"}])
    res = snap._csv_snapshot_resource("celestrak_space_weather", "http://x", "DATE", 10)
    # dlt wraps RuntimeError in ResourceExtractionError (subclass of Exception).
    with pytest.raises(Exception, match="suspected truncation"):
        list(res())


def test_celestrak_registry_shape():
    assert [e[0] for e in snap.CELESTRAK_DATASETS] == ["celestrak_space_weather"]
    name, url, pk, floor = snap.CELESTRAK_DATASETS[0]
    assert name == "celestrak_space_weather"
    assert url == snap.SW_ALL_URL
    assert pk == "DATE"
    assert floor and floor > 0
