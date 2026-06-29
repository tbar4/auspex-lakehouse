from dagster import AssetKey, AssetsDefinition


def test_neo_lookup_asset_wired_into_definitions():
    from auspex_lakehouse.definitions import defs

    graph = defs.resolve_asset_graph()
    key = AssetKey(["neo_lookup"])
    assert key in graph.get_all_asset_keys()
    # depends on the dlt neows bronze table
    assert AssetKey(["dlt_nasa_api_neows"]) in graph.get(key).parent_keys


def test_neo_lookup_asset_is_pooled():
    from auspex_lakehouse.definitions import defs

    ad = next(
        a
        for a in defs.assets
        if isinstance(a, AssetsDefinition) and AssetKey(["neo_lookup"]) in a.keys
    )
    assert ad.op.pool == "nasa_api"


def test_dagster_yaml_defines_pool_default():
    import pathlib

    import yaml

    # The instance config lives at deploy/dagster.yaml and sets an instance-wide
    # pool default (named pools aren't valid here); each pool (nasa_api,
    # spacetrack_api) inherits this default_limit unless raised via the UI/CLI.
    cfg = yaml.safe_load(pathlib.Path("deploy/dagster.yaml").read_text())
    pools = cfg["concurrency"]["pools"]
    assert pools["default_limit"] == 1
