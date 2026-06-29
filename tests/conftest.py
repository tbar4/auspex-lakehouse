import os
import subprocess
import sys
from pathlib import Path

import pytest

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


@pytest.fixture(scope="session", autouse=True)
def _dbt_manifest():
    # @dbt_assets reads target/manifest.json at import time, so this session-autouse
    # fixture MUST generate the manifest before any test imports `defs`. Tests must
    # therefore import `defs` lazily (inside the test function), not at module top-level —
    # otherwise pytest collection fails before fixtures run.
    # dagster-dbt builds the asset graph from target/manifest.json; generate it
    # once per session. `dbt parse` needs no MinIO (profile env_vars have defaults).
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)}
    base = [sys.executable, "-m", "dbt.cli.main"]
    subprocess.run(base + ["deps"], cwd=DBT_DIR, check=True, env=env)
    subprocess.run(base + ["parse"], cwd=DBT_DIR, check=True, env=env)
