import os

from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager

# Credentials are read from the environment (see .env / .env.example).
# Do NOT hard-code secrets here — this file is committed to version control.
delta_io_manager = DeltaLakePolarsIOManager(
    root_uri=f"{os.getenv('BRONZE_BUCKET_URI', 's3://auspex-lakehouse')}/bronze",
    storage_options=S3Config(
        access_key_id=os.getenv("MINIO_ACCESS_KEY", ""),
        secret_access_key=os.getenv("MINIO_SECRET_KEY", ""),
        endpoint=os.getenv("MINIO_ENDPOINT", ""),
        region=os.getenv("AWS_REGION", "us-west-1"),
        allow_http=True,
    ),
)
