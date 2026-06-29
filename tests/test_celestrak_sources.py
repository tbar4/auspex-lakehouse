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


def test_celestrak_source_exposes_one_named_resource():
    from auspex_lakehouse.bronze.dlt.sources import celestrak_source
    src = celestrak_source("celestrak_space_weather")
    assert set(src.resources.keys()) == {"celestrak_space_weather"}


def test_celestrak_source_build_makes_no_http(monkeypatch):
    # Building a source must not fetch — the resource is lazy. Patch requests in
    # _common to explode if called; constructing the source must still succeed.
    import auspex_lakehouse.bronze.dlt.sources.celestrak._common as c
    from auspex_lakehouse.bronze.dlt.sources import celestrak_source

    def _boom(*a, **k):
        raise AssertionError("HTTP at build time")

    monkeypatch.setattr(c, "requests", type("R", (), {"get": staticmethod(_boom)}))
    celestrak_source("celestrak_space_weather")  # no iteration -> no fetch


def test_celestrak_pipelines_dict():
    from auspex_lakehouse.bronze.dlt.sources import celestrak_pipelines
    assert set(celestrak_pipelines) == {"celestrak_space_weather"}
    p = celestrak_pipelines["celestrak_space_weather"]
    assert p.pipeline_name == "celestrak_space_weather"  # name already carries celestrak_ prefix
    assert p.dataset_name == "bronze"
