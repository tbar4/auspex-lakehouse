import os
from datetime import date
from pathlib import PurePosixPath

import boto3
import polars as pl
import requests
from dagster import AssetExecutionContext, AssetKey, AutomationCondition, asset
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from deltalake import DeltaTable

from auspex_lakehouse.bronze.dlt.sources import nasa_api, nasa_pipeline
from auspex_lakehouse.partitions import daily_partitions


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

    dt = DeltaTable(
        f"{os.environ['BRONZE_BUCKET_URI']}/bronze/apod",
        storage_options = {
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
            "AWS_ENDPOINT_URL":      os.environ["MINIO_ENDPOINT"],
            "AWS_ALLOW_HTTP":        "true",
            "AWS_REGION":            "us-west-1",
        },
    )

    df = pl.from_arrow(dt.to_pyarrow_table()).filter(
        pl.col("date") == partition_key
    )

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
