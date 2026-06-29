import dlt

from auspex_lakehouse.bronze.dlt.sources.spacetrack._common import login_session
from auspex_lakehouse.bronze.dlt.sources.spacetrack.incremental import (
    INCREMENTAL_CLASSES,
    _incremental_resource,
)
from auspex_lakehouse.bronze.dlt.sources.spacetrack.snapshot import (
    SNAPSHOT_CLASSES,
    _snapshot_resource,
)

SNAPSHOT_BY_NAME = {e[0]: e for e in SNAPSHOT_CLASSES}
INCREMENTAL_BY_NAME = {e[0]: e for e in INCREMENTAL_CLASSES}


@dlt.source
def snapshot_source(name, session=None):
    n, cls, pk, segs, wd, floor = SNAPSHOT_BY_NAME[name]
    return [_snapshot_resource(n, cls, pk, segs, wd, floor)(session)]


@dlt.source
def incremental_source(name, start_date, end_date, session=None):
    n, cls, pk, pred = INCREMENTAL_BY_NAME[name]
    return [_incremental_resource(n, cls, pk, pred)(session, start_date, end_date)]


def _pipeline(name):
    return dlt.pipeline(
        pipeline_name=f"spacetrack_{name}",
        destination="filesystem",
        dataset_name="bronze",
    )


spacetrack_pipelines = {
    name: _pipeline(name)
    for name in list(SNAPSHOT_BY_NAME) + list(INCREMENTAL_BY_NAME)
}

__all__ = [
    "snapshot_source",
    "incremental_source",
    "spacetrack_pipelines",
    "login_session",
    "SNAPSHOT_CLASSES",
    "INCREMENTAL_CLASSES",
]
