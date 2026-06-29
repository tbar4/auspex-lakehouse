from dagster import AssetsDefinition


def _load():
    import auspex_lakehouse.bronze.dlt.assets as a
    return a


def test_six_spacetrack_assets_exist():
    a = _load()
    names = [
        "spacetrack_gp_assets", "spacetrack_satcat_assets", "spacetrack_boxscore_assets",
        "spacetrack_decay_assets", "spacetrack_cdm_assets", "spacetrack_tip_assets",
    ]
    for n in names:
        assert isinstance(getattr(a, n), AssetsDefinition), n


def test_snapshot_assets_unpartitioned_incremental_partitioned():
    a = _load()
    assert a.spacetrack_gp_assets.partitions_def is None
    assert a.spacetrack_boxscore_assets.partitions_def is None
    assert a.spacetrack_decay_assets.partitions_def is not None


def test_all_spacetrack_assets_use_the_pool():
    a = _load()
    for n in ["spacetrack_gp_assets", "spacetrack_satcat_assets",
              "spacetrack_boxscore_assets", "spacetrack_decay_assets",
              "spacetrack_cdm_assets", "spacetrack_tip_assets"]:
        assert getattr(a, n).op.pool == "spacetrack_api", n


def test_spacetrack_assets_in_group():
    a = _load()
    for spec in a.spacetrack_gp_assets.specs:
        assert spec.group_name == "spacetrack"
