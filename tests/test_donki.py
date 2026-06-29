from datetime import date
from unittest.mock import Mock

import auspex_lakehouse.bronze.dlt.sources.nasa.donki as donki
from auspex_lakehouse.bronze.dlt.sources.nasa.donki import (
    DONKI_ENDPOINTS,
    _donki_resource,
    donki_source,
    nasa_donki_pipeline,
)


def _resp(payload):
    r = Mock()
    r.json.return_value = payload
    r.raise_for_status = Mock()
    return r


def test_resource_yields_list_items(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    payload = [{"activityID": "a"}, {"activityID": "b"}]
    monkeypatch.setattr(donki, "requests", Mock(get=Mock(return_value=_resp(payload))))
    res = _donki_resource("cme", "CME", "activityID")
    assert list(res(date(2024, 5, 1), date(2024, 5, 1))) == payload


def test_resource_tolerates_non_list_body(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    monkeypatch.setattr(donki, "requests", Mock(get=Mock(return_value=_resp({"error": "no data"}))))
    res = _donki_resource("gst", "GST", "gstID")
    assert list(res(date(2024, 5, 1), date(2024, 5, 1))) == []


def test_resource_builds_request_with_extra_params(monkeypatch):
    monkeypatch.setattr(donki, "nasa_api_key", lambda: "key")
    captured = {}

    def fake_get(url, params=None):
        captured["url"], captured["params"] = url, params
        return _resp([])

    monkeypatch.setattr(donki, "requests", Mock(get=fake_get))
    res = _donki_resource("notifications", "notifications", "messageID", {"type": "all"})
    list(res(date(2024, 5, 1), date(2024, 5, 1)))
    assert captured["url"].endswith("/DONKI/notifications")
    assert captured["params"] == {
        "api_key": "key", "startDate": "2024-05-01", "endDate": "2024-05-01", "type": "all",
    }


def test_resource_metadata_composite_key():
    res = _donki_resource("cme_analysis", "CMEAnalysis", ["associatedCMEID", "time21_5"])
    assert res.name == "cme_analysis"
    assert res.write_disposition == "merge"
    ts = res.compute_table_schema()
    assert ts["table_format"] == "delta"
    pk_cols = {c for c, v in ts["columns"].items() if v.get("primary_key")}
    assert pk_cols == {"associatedCMEID", "time21_5"}


def test_registry_has_11_unique_endpoints():
    assert len(DONKI_ENDPOINTS) == 11
    names = [name for (name, _p, _k, _e) in DONKI_ENDPOINTS]
    assert len(set(names)) == 11
    assert all(pk for (_n, _p, pk, _e) in DONKI_ENDPOINTS)
    notif = next(e for e in DONKI_ENDPOINTS if e[0] == "notifications")
    assert notif[1] == "notifications" and notif[2] == "messageID" and notif[3] == {"type": "all"}


def test_source_exposes_all_11_resources():
    src = donki_source(start_date=date(2024, 5, 1), end_date=date(2024, 5, 1))
    assert set(src.resources.keys()) == {
        "cme", "cme_analysis", "gst", "ips", "flr", "sep",
        "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
    }


def test_exports_from_sources_package():
    from auspex_lakehouse.bronze.dlt.sources import donki_source as ds
    from auspex_lakehouse.bronze.dlt.sources import nasa_donki_pipeline as p

    assert p.pipeline_name == "nasa_donki"
    assert callable(ds)


def test_pipeline_name():
    assert nasa_donki_pipeline.pipeline_name == "nasa_donki"
