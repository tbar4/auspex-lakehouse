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
    # DLT generates keys from source/resource names, e.g. "dlt_snapshot_source_gp"
    # and "dlt_incremental_source_decay"; confirm both are wired under the spacetrack group.
    st_keys = {k.to_user_string() for k in graph.asset_keys_for_group("spacetrack")}
    assert any("gp" in k for k in st_keys), f"No GP asset in spacetrack group; got {st_keys}"
    assert any("decay" in k for k in st_keys), f"No decay asset in spacetrack group; got {st_keys}"
