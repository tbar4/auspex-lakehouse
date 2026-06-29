from dagster import AssetKey, AssetsDefinition


def _load():
    import auspex_lakehouse.bronze.dlt.assets as a
    return a


def test_celestrak_asset_key():
    a = _load()
    assert a.celestrak_space_weather_assets.keys == {AssetKey("dlt_celestrak_space_weather")}


def test_celestrak_asset_exists_and_is_assets_def():
    a = _load()
    assert isinstance(a.celestrak_space_weather_assets, AssetsDefinition)


def test_celestrak_asset_unpartitioned():
    a = _load()
    assert a.celestrak_space_weather_assets.partitions_def is None


def test_celestrak_asset_uses_the_pool():
    a = _load()
    assert a.celestrak_space_weather_assets.op.pool == "celestrak_api"


def test_celestrak_asset_in_group():
    a = _load()
    for spec in a.celestrak_space_weather_assets.specs:
        assert spec.group_name == "celestrak"
