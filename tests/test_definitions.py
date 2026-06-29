"""Smoke test: the Dagster code location imports and builds without error.

Importing the module constructs the ``Definitions`` object, which validates
asset keys, resource wiring, and sensor targets. This is a cheap CI gate that
catches the most common "the code location won't load" failures before deploy.
"""

from dagster import Definitions


def test_definitions_load():
    from auspex_lakehouse.definitions import defs

    assert isinstance(defs, Definitions)
