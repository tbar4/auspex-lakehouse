import dlt

from auspex_lakehouse.bronze.dlt.sources.celestrak.snapshot import (
    CELESTRAK_DATASETS,
    _csv_snapshot_resource,
)

DATASETS_BY_NAME = {e[0]: e for e in CELESTRAK_DATASETS}


@dlt.source
def celestrak_source(name):
    n, url, pk, floor = DATASETS_BY_NAME[name]
    return [_csv_snapshot_resource(n, url, pk, floor)()]


def _pipeline(name):
    return dlt.pipeline(
        pipeline_name=name,        # name already carries the celestrak_ prefix — no doubling
        destination="filesystem",
        dataset_name="bronze",     # tables land at bronze/<name> = bronze/celestrak_space_weather
    )


celestrak_pipelines = {name: _pipeline(name) for name in DATASETS_BY_NAME}

__all__ = ["celestrak_source", "celestrak_pipelines", "CELESTRAK_DATASETS"]
