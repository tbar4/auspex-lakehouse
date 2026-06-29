from dagster import AssetKey


def test_20_bronze_assets_with_lineage():
    from auspex_lakehouse.definitions import defs

    ag = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in ag.get_all_asset_keys()}
    expected = {
        f"bronze_{t}"
        for t in [
            # NASA
            "apod", "neows", "neo_lookup", "cme", "cme_analysis", "gst", "ips",
            "flr", "sep", "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
            # space-track
            "gp", "satcat", "boxscore", "decay", "cdm", "tip",
        ]
    }
    assert expected <= keys, f"missing: {expected - keys}"
    # lineage: a sample of the non-uniform source->dlt-key mapping
    assert AssetKey(["dlt_nasa_api_neows"]) in ag.get(AssetKey(["bronze_neows"])).parent_keys
    assert AssetKey(["dlt_nasa_donki_cme"]) in ag.get(AssetKey(["bronze_cme"])).parent_keys
    assert AssetKey(["neo_lookup"]) in ag.get(AssetKey(["bronze_neo_lookup"])).parent_keys
    assert AssetKey(["dlt_spacetrack_gp"]) in ag.get(AssetKey(["bronze_gp"])).parent_keys
    assert AssetKey(["dlt_spacetrack_cdm"]) in ag.get(AssetKey(["bronze_cdm"])).parent_keys
