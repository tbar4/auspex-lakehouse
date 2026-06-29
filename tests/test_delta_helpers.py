import importlib


def test_delta_storage_options_from_env(monkeypatch):
    monkeypatch.setenv("MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("AWS_REGION", "us-west-1")

    delta = importlib.import_module("auspex_lakehouse.resources.delta")
    opts = delta.delta_storage_options()

    assert opts["AWS_ACCESS_KEY_ID"] == "ak"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "sk"
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_REGION"] == "us-west-1"


def test_read_bronze_table_exists_callable():
    delta = importlib.import_module("auspex_lakehouse.resources.delta")
    assert callable(delta.read_bronze_table)
    assert callable(delta.bronze_table_exists)
