import os
from datetime import date, datetime, timezone
from pathlib import PurePosixPath

import boto3
import polars as pl
import requests
from dagster import AssetExecutionContext, AssetKey, AutomationCondition, asset
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from auspex_lakehouse.bronze.dlt.sources import (
    nasa_api,
    nasa_neo_lookup_pipeline,
    nasa_pipeline,
    neo_lookup_rows,
)
from auspex_lakehouse.bronze.dlt.sources.nasa._common import nasa_api_key
from auspex_lakehouse.bronze.dlt.sources.nasa.config import (
    NASA_API_POOL,
    NASA_MAX_LOOKUPS_PER_RUN,
    NASA_REFRESH_DAYS,
)
from auspex_lakehouse.bronze.dlt.sources.nasa.neo_lookup import (
    fetch_neo_lookups,
    select_neo_work_ids,
)
from auspex_lakehouse.partitions import daily_partitions
from auspex_lakehouse.resources.delta import bronze_table_exists, read_bronze_table


class NasaDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData):
        return super().get_asset_spec(data).replace_attributes(
            automation_condition=AutomationCondition.on_cron("0 6 * * *"),
        )


@dlt_assets(
    dlt_source=nasa_api(
        start_date=date.today(),
        end_date=date.today(),
    ),
    dlt_pipeline=nasa_pipeline,
    name="nasa_api_bronze",
    group_name="nasa",
    partitions_def=daily_partitions,
    dagster_dlt_translator=NasaDltTranslator(),
)
def nasa_api_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    partition_key_range = context.partition_key_range
    start = date.fromisoformat(partition_key_range.start)
    end = date.fromisoformat(partition_key_range.end)

    source = nasa_api(start_date=start, end_date=end)
    yield from dlt.run(context=context, dlt_source=source)

@asset(
    name="apod_images",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_api_apod"])],
    automation_condition=AutomationCondition.eager(),
)
def apod_images(context: AssetExecutionContext):
    partition_key = context.partition_key

    df = read_bronze_table("apod").filter(pl.col("date") == partition_key)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    bucket = os.environ["BRONZE_BUCKET_NAME"]

    downloaded = 0
    for row in df.iter_rows(named=True):
        hd_url = row.get("hdurl") or row.get("url")
        if not hd_url:
            continue

        filename = PurePosixPath(hd_url).name
        object_key = f"bronze/apod_images/{partition_key}_{filename}"

        img_resp = requests.get(hd_url, timeout=60)
        img_resp.raise_for_status()

        s3.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=img_resp.content,
            ContentType=img_resp.headers.get("Content-Type", "image/jpeg"),
        )
        downloaded += 1
        context.log.info(f"Uploaded {object_key}")

    context.add_output_metadata({"images_downloaded": downloaded})


def _existing_lookup_index() -> dict[str, datetime]:
    """Map neo_reference_id -> last lookup timestamp from the neo_lookup table.
    Empty on the first run, before the table exists.

    dlt infers the ISO-8601 `lookup_fetched_at` we write as a *timestamp* column,
    so Polars hands it back as a ``datetime`` (not the original string); be robust
    to either, and coerce naive timestamps to UTC so the staleness subtraction in
    ``select_neo_work_ids`` doesn't raise on naive/aware mixing. Keys are coerced
    to ``str`` so they compare equal to the str-coerced candidates."""
    if not bronze_table_exists("neo_lookup"):
        return {}
    df = read_bronze_table("neo_lookup").select(["neo_reference_id", "lookup_fetched_at"])
    index: dict[str, datetime] = {}
    for row in df.iter_rows(named=True):
        ts = row["lookup_fetched_at"]
        if ts is None:
            continue
        if not isinstance(ts, datetime):
            ts = datetime.fromisoformat(ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        index[str(row["neo_reference_id"])] = ts
    return index


@asset(
    name="neo_lookup",
    group_name="nasa",
    partitions_def=daily_partitions,
    deps=[AssetKey(["dlt_nasa_api_neows"])],
    automation_condition=AutomationCondition.eager(),
    pool=NASA_API_POOL,
)
def neo_lookup(context: AssetExecutionContext):
    partition_key = context.partition_key
    # neows table is guaranteed to exist by the dlt_nasa_api_neows dep above.
    candidates = {
        str(neo_id)  # coerce so candidate IDs compare equal to str-keyed existing index
        for neo_id in read_bronze_table("neows")
        .filter(pl.col("date") == partition_key)
        .get_column("neo_reference_id")
        .to_list()
    }
    existing = _existing_lookup_index()
    now = datetime.now(timezone.utc)
    plan = select_neo_work_ids(
        candidates, existing, now, NASA_REFRESH_DAYS, NASA_MAX_LOOKUPS_PER_RUN
    )

    if not plan.selected:
        context.add_output_metadata({"candidates": len(candidates), "fetched_ok": 0})
        return

    rows, stats = fetch_neo_lookups(plan.selected, now.isoformat(), nasa_api_key())
    if rows:
        nasa_neo_lookup_pipeline.run(neo_lookup_rows(rows))

    if stats.stopped_on_rate_limit:
        context.log.warning(
            f"NEO lookup hit the NASA rate limit for partition {partition_key}; "
            f"deferred {len(stats.deferred_on_stop)} id(s) to a future run."
        )
    if plan.deferred_over_cap:
        context.log.warning(
            f"NEO lookup cap reached for partition {partition_key}; deferred "
            f"{len(plan.deferred_over_cap)} id(s) over NASA_MAX_LOOKUPS_PER_RUN to a future run."
        )

    context.add_output_metadata(
        {
            "candidates": len(candidates),
            "new": len(plan.new),
            "stale": len(plan.stale),
            "fetched_ok": stats.fetched_ok,
            "tombstoned": stats.tombstoned,
            "deferred_over_cap": len(plan.deferred_over_cap),
            "stopped_on_rate_limit": stats.stopped_on_rate_limit,
            "deferred_on_stop": len(stats.deferred_on_stop),
        }
    )
