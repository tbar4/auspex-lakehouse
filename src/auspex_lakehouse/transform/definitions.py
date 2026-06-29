import os
from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=os.getenv("DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[3] / "dbt"))
)
dbt_project.prepare_if_dev()

_SOURCE_ASSET_KEYS = {
    "apod": AssetKey(["dlt_nasa_api_apod"]),
    "neows": AssetKey(["dlt_nasa_api_neows"]),
    "neo_lookup": AssetKey(["neo_lookup"]),
    **{
        t: AssetKey([f"dlt_nasa_donki_{t}"])
        for t in [
            "cme",
            "cme_analysis",
            "gst",
            "ips",
            "flr",
            "sep",
            "mpc",
            "rbe",
            "hss",
            "wsa_enlil_simulations",
            "notifications",
        ]
    },
    "gp": AssetKey(["dlt_spacetrack_gp"]),
    "satcat": AssetKey(["dlt_spacetrack_satcat"]),
    "boxscore": AssetKey(["dlt_spacetrack_boxscore"]),
    "decay": AssetKey(["dlt_spacetrack_decay"]),
    "cdm": AssetKey(["dlt_spacetrack_cdm"]),
    "tip": AssetKey(["dlt_spacetrack_tip"]),
}


class BronzeDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            mapped = _SOURCE_ASSET_KEYS.get(dbt_resource_props["name"])
            if mapped is not None:
                return mapped
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return "dbt_bronze"
        return super().get_group_name(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=BronzeDbtTranslator())
def dbt_bronze_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
