from dagster import AssetKey


def test_20_bronze_assets_with_lineage():
    from auspex_lakehouse.definitions import defs

    ag = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in ag.get_all_asset_keys()}
    expected = {
        f"bronze_{t}"
        for t in [
            # NASA
            "nasa_astronomy_picture_of_the_day",
            "nasa_near_earth_object_feed",
            "nasa_near_earth_object_lookups",
            "nasa_donki_coronal_mass_ejections", "nasa_donki_coronal_mass_ejection_analyses",
            "nasa_donki_geomagnetic_storms", "nasa_donki_interplanetary_shocks",
            "nasa_donki_solar_flares", "nasa_donki_solar_energetic_particles",
            "nasa_donki_magnetopause_crossings", "nasa_donki_radiation_belt_enhancements",
            "nasa_donki_high_speed_streams", "nasa_donki_wsa_enlil_simulations",
            "nasa_donki_notifications",
            # space-track
            "space_track_general_perturbations",
            "space_track_satellite_catalog",
            "space_track_boxscore",
            "space_track_decays",
            "space_track_conjunction_data_messages",
            "space_track_tracking_and_impact_predictions",
            # celestrak
            "celestrak_space_weather",
        ]
    }
    assert expected <= keys, f"missing: {expected - keys}"
    # lineage: a sample of the non-uniform source->dlt-key mapping
    assert AssetKey(["dlt_nasa_near_earth_object_feed"]) in ag.get(
        AssetKey(["bronze_nasa_near_earth_object_feed"])
    ).parent_keys
    assert AssetKey(["dlt_nasa_donki_coronal_mass_ejections"]) in ag.get(
        AssetKey(["bronze_nasa_donki_coronal_mass_ejections"])
    ).parent_keys
    assert AssetKey(["nasa_near_earth_object_lookups"]) in ag.get(
        AssetKey(["bronze_nasa_near_earth_object_lookups"])
    ).parent_keys
    assert AssetKey(["dlt_space_track_general_perturbations"]) in ag.get(
        AssetKey(["bronze_space_track_general_perturbations"])
    ).parent_keys
    assert AssetKey(["dlt_space_track_conjunction_data_messages"]) in ag.get(
        AssetKey(["bronze_space_track_conjunction_data_messages"])
    ).parent_keys
    assert (
        AssetKey(["dlt_celestrak_space_weather"])
        in ag.get(AssetKey(["bronze_celestrak_space_weather"])).parent_keys
    )
