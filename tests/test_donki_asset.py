from dagster import AssetKey, AssetsDefinition

DONKI_KEYS = [
    "dlt_nasa_donki_cme",
    "dlt_nasa_donki_cme_analysis",
    "dlt_nasa_donki_gst",
    "dlt_nasa_donki_ips",
    "dlt_nasa_donki_flr",
    "dlt_nasa_donki_sep",
    "dlt_nasa_donki_mpc",
    "dlt_nasa_donki_rbe",
    "dlt_nasa_donki_hss",
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

    cme = AssetKey(["dlt_nasa_donki_cme"])
    ad = next(a for a in defs.assets if isinstance(a, AssetsDefinition) and cme in a.keys)
    assert ad.op.pool == "nasa_api"
    assert ad.group_names_by_key[cme] == "donki"
