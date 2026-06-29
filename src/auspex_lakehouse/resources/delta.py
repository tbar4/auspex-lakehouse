import os

try:
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
except ImportError:
    delta_io_manager = None  # type: ignore[assignment]


import polars as pl
from deltalake import DeltaTable


def _bronze_table_uri(name: str) -> str:
    return f"{os.environ['BRONZE_BUCKET_URI']}/bronze/{name}"


def delta_storage_options() -> dict:
    """Raw storage-options dict for deltalake.DeltaTable against the MinIO bronze bucket."""
    return {
        "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
        "AWS_ENDPOINT_URL": os.environ["MINIO_ENDPOINT"],
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": os.environ.get("AWS_REGION", "us-west-1"),
    }


def bronze_table_exists(name: str) -> bool:
    """True if a Delta table exists at bronze/<name> (False on first run, before any write)."""
    return DeltaTable.is_deltatable(
        _bronze_table_uri(name), storage_options=delta_storage_options()
    )


def read_bronze_table(name: str) -> pl.DataFrame:
    """Open the bronze Delta table <name> as a Polars DataFrame.

    Raises if the table does not exist — callers that may run before the table's
    first write must guard with ``bronze_table_exists`` first.
    """
    dt = DeltaTable(_bronze_table_uri(name), storage_options=delta_storage_options())
    return pl.from_arrow(dt.to_pyarrow_table())
