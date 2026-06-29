from dagster import AssetKey


def test_20_bronze_assets_with_lineage():
    from auspex_lakehouse.definitions import defs

    ag = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in ag.get_all_asset_keys()}
    expected = {
        f"bronze_{t}"
        for t in [
            # NASA
            "nasa_astronomy_picture_of_the_day", "nasa_near_earth_object_feed", "nasa_near_earth_object_lookups", "cme", "cme_analysis", "gst", "ips",
            "flr", "sep", "mpc", "rbe", "hss", "wsa_enlil_simulations", "notifications",
            # space-track
            "gp", "satcat", "boxscore", "decay", "cdm", "tip",
        ]
    }
    assert expected <= keys, f"missing: {expected - keys}"
    # lineage: a sample of the non-uniform source->dlt-key mapping
    assert AssetKey(["dlt_nasa_near_earth_object_feed"]) in ag.get(AssetKey(["bronze_nasa_near_earth_object_feed"])).parent_keys
    assert AssetKey(["dlt_nasa_donki_cme"]) in ag.get(AssetKey(["bronze_cme"])).parent_keys
    assert AssetKey(["nasa_near_earth_object_lookups"]) in ag.get(AssetKey(["bronze_nasa_near_earth_object_lookups"])).parent_keys
    assert AssetKey(["dlt_spacetrack_gp"]) in ag.get(AssetKey(["bronze_gp"])).parent_keys
    assert AssetKey(["dlt_spacetrack_cdm"]) in ag.get(AssetKey(["bronze_cdm"])).parent_keys
