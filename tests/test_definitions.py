"""Smoke test: the Dagster code location imports and builds without error.

Importing the module constructs the ``Definitions`` object, which validates
asset keys, resource wiring, and sensor targets. This is a cheap CI gate that
catches the most common "the code location won't load" failures before deploy.
"""

from dagster import Definitions


def test_definitions_load():
    from auspex_lakehouse.definitions import defs

    assert isinstance(defs, Definitions)


def test_definitions_include_spacetrack_assets():
    from auspex_lakehouse.definitions import defs
    graph = defs.resolve_asset_graph()
    # SpaceTrackDltTranslator overrides the default dlt key to dlt_space_track_<name>.
    st_keys = {k.to_user_string() for k in graph.asset_keys_for_group("spacetrack")}
    assert len(st_keys) >= 6, f"Expected at least 6 spacetrack assets; got {st_keys}"
    assert "dlt_space_track_general_perturbations" in st_keys, f"Missing gp key; got {st_keys}"
    assert "dlt_space_track_decays" in st_keys, f"Missing decay key; got {st_keys}"


def test_definitions_include_celestrak_asset():
    from auspex_lakehouse.definitions import defs
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.asset_keys_for_group("celestrak")}
    assert "dlt_celestrak_space_weather" in keys, f"got {keys}"
