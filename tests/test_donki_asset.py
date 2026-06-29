from dagster import AssetKey, AssetsDefinition

DONKI_KEYS = [
    "dlt_nasa_donki_coronal_mass_ejections",
    "dlt_nasa_donki_coronal_mass_ejection_analyses",
    "dlt_nasa_donki_geomagnetic_storms",
    "dlt_nasa_donki_interplanetary_shocks",
    "dlt_nasa_donki_solar_flares",
    "dlt_nasa_donki_solar_energetic_particles",
    "dlt_nasa_donki_magnetopause_crossings",
    "dlt_nasa_donki_radiation_belt_enhancements",
    "dlt_nasa_donki_high_speed_streams",
    "dlt_nasa_donki_wsa_enlil_simulations",
    "dlt_nasa_donki_notifications",
]


def test_all_11_donki_assets_present():
    from auspex_lakehouse.definitions import defs

    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    missing = [k for k in DONKI_KEYS if k not in keys]
    assert not missing, f"missing DONKI asset keys: {missing}"


def test_donki_assets_pooled_and_grouped():
    from auspex_lakehouse.definitions import defs

    cme = AssetKey(["dlt_nasa_donki_coronal_mass_ejections"])
    ad = next(a for a in defs.assets if isinstance(a, AssetsDefinition) and cme in a.keys)
    assert ad.op.pool == "nasa_api"
    assert ad.group_names_by_key[cme] == "donki"

    # Fix 4: guard the on_cron schedule via DonkiDltTranslator
    cme_spec = next(
        s for s in ad.specs if s.key == AssetKey(["dlt_nasa_donki_coronal_mass_ejections"])
    )
    assert cme_spec.automation_condition is not None

    # Fix 5: assert pool and group for all 11 DONKI assets
    for k in DONKI_KEYS:
        assert ad.op.pool == "nasa_api"
        assert ad.group_names_by_key[AssetKey([k])] == "donki"
