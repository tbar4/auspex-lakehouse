from dagster import AssetKey, AssetsDefinition, MaterializeResult, materialize
from dagster._core.storage.tags import BACKFILL_ID_TAG


def _load():
    import auspex_lakehouse.bronze.dlt.assets as a
    return a


def test_spacetrack_asset_keys():
    """All six space-track assets must produce provider-scoped dlt_space_track_<name> keys."""
    a = _load()
    asset_defs = [
        a.spacetrack_gp_assets,
        a.spacetrack_satcat_assets,
        a.spacetrack_boxscore_assets,
        a.spacetrack_decay_assets,
        a.spacetrack_cdm_assets,
        a.spacetrack_tip_assets,
    ]
    all_keys = {key for ad in asset_defs for key in ad.keys}
    expected = {
        AssetKey("dlt_space_track_general_perturbations"),
        AssetKey("dlt_space_track_satellite_catalog"),
        AssetKey("dlt_space_track_boxscore"),
        AssetKey("dlt_space_track_decays"),
        AssetKey("dlt_space_track_conjunction_data_messages"),
        AssetKey("dlt_space_track_tracking_and_impact_predictions"),
    }
    assert all_keys == expected, f"Unexpected asset keys: {all_keys}"


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


class _FakeDltResource:
    """Stand-in for DagsterDltResource: records the host choice and emits the asset's keys."""

    def __init__(self, recorder):
        self._recorder = recorder

    def run(self, context, dlt_source):
        import auspex_lakehouse.bronze.dlt.sources.spacetrack._common as c

        self._recorder.append(c._use_test_host())
        for key in context.assets_def.keys:
            yield MaterializeResult(asset_key=key)


def _materialize_host(monkeypatch, asset_attr, tags, *, partition_key=None):
    """Materialize a space-track asset with network stubbed; return [host_was_test_host]."""
    a = _load()
    monkeypatch.delenv("SPACETRACK_USE_TEST_HOST", raising=False)
    monkeypatch.setattr(a, "login_session", lambda: object())
    monkeypatch.setattr(a, "incremental_source", lambda *args, **kwargs: None)
    monkeypatch.setattr(a, "snapshot_source", lambda *args, **kwargs: None)
    recorder = []
    result = materialize(
        [getattr(a, asset_attr)],
        partition_key=partition_key,
        tags=tags,
        resources={"dlt": _FakeDltResource(recorder)},
    )
    assert result.success
    return recorder


def test_backfill_run_routes_incremental_to_test_host(monkeypatch):
    # An incremental (partitioned) backfill run hits the unlimited test host, no env var set.
    recorder = _materialize_host(
        monkeypatch, "spacetrack_decay_assets", {BACKFILL_ID_TAG: "abc123"},
        partition_key="2026-03-11",
    )
    assert recorder == [True]


def test_backfill_run_routes_snapshot_to_test_host(monkeypatch):
    # Snapshot (unpartitioned) assets ride along in mixed-selection backfills too.
    recorder = _materialize_host(
        monkeypatch, "spacetrack_boxscore_assets", {BACKFILL_ID_TAG: "abc123"},
    )
    assert recorder == [True]


def test_non_backfill_run_uses_prod_host(monkeypatch):
    # An ordinary (cron/manual) run is unaffected and stays on prod.
    assert _materialize_host(
        monkeypatch, "spacetrack_decay_assets", {}, partition_key="2026-03-11"
    ) == [False]
    assert _materialize_host(monkeypatch, "spacetrack_boxscore_assets", {}) == [False]
