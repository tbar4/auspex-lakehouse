from unittest.mock import Mock

import auspex_lakehouse.bronze.dlt.sources.celestrak._common as c

# Real 31-column header + two rows. Row 1 (1957) leaves the F10.7 81-day-average
# columns BLANK on purpose: they have no full window yet. This is the case the
# default 100-row schema inference mis-types, so it pins the infer_schema_length=None fix.
_HEADER = (
    "DATE,BSRN,ND,KP1,KP2,KP3,KP4,KP5,KP6,KP7,KP8,KP_SUM,"
    "AP1,AP2,AP3,AP4,AP5,AP6,AP7,AP8,AP_AVG,CP,C9,ISN,"
    "F10.7_OBS,F10.7_ADJ,F10.7_DATA_TYPE,"
    "F10.7_OBS_CENTER81,F10.7_OBS_LAST81,F10.7_ADJ_CENTER81,F10.7_ADJ_LAST81"
)
_ROW_BLANK_81 = "1957-10-01,1700,19,43,40,30,20,37,23,43,37,273,32,27,15,7,22,9,32,22,21,1.1,5,334,269.3,269.8,OBS,,,,"  # noqa: E501
_ROW_FULL = "2026-06-28,2480,5,7,10,7,13,17,20,23,17,113,3,4,3,5,6,7,9,6,5,0.4,2,100,150.2,151.0,OBS,148.1,149.2,148.9,150.0"  # noqa: E501
CSV_BYTES = ("\n".join([_HEADER, _ROW_BLANK_81, _ROW_FULL]) + "\n").encode()


def _patch_get(monkeypatch, content):
    fake = Mock()
    fake.get.return_value = Mock(content=content, raise_for_status=Mock())
    monkeypatch.setattr(c, "requests", fake)
    return fake


def test_fetch_csv_rows_returns_raw_header_columns(monkeypatch):
    _patch_get(monkeypatch, CSV_BYTES)
    rows = c.fetch_csv_rows("http://example/SW-All.csv")
    assert len(rows) == 2
    # to_dicts() keys are the RAW (pre-dlt) headers — exactly 31, in the file's spelling.
    assert set(rows[0].keys()) == set(_HEADER.split(","))


def test_fetch_csv_rows_types_and_nulls(monkeypatch):
    _patch_get(monkeypatch, CSV_BYTES)
    rows = c.fetch_csv_rows("http://example/SW-All.csv")
    # DATE and the data-type flag stay strings; bronze keeps DATE raw.
    assert isinstance(rows[0]["DATE"], str) and rows[0]["DATE"] == "1957-10-01"
    assert rows[0]["F10.7_DATA_TYPE"] == "OBS"
    # Blank 81-day fields in the early row are null...
    assert rows[0]["F10.7_OBS_CENTER81"] is None
    # ...and the later row's value is a parsed float (full-file inference typed the column).
    assert rows[1]["F10.7_OBS_CENTER81"] == 148.1


def test_sw_all_url():
    assert c.SW_ALL_URL == "https://celestrak.org/SpaceData/SW-All.csv"


def test_pool_constant():
    from auspex_lakehouse.bronze.dlt.sources.celestrak.config import CELESTRAK_API_POOL
    assert CELESTRAK_API_POOL == "celestrak_api"
