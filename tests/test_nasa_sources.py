from datetime import date


def test_public_names_import_from_sources():
    from auspex_lakehouse.bronze.dlt.sources import nasa_api, nasa_pipeline

    assert nasa_pipeline.pipeline_name == "nasa_api"
    assert callable(nasa_api)


def test_nasa_source_exposes_apod_and_neows():
    from auspex_lakehouse.bronze.dlt.sources import nasa_api

    src = nasa_api(start_date=date(2026, 1, 1), end_date=date(2026, 1, 1))
    assert set(src.resources.keys()) == {"apod", "neows"}
