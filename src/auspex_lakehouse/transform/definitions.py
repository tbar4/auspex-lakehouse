import os
from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=os.getenv("DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[3] / "dbt"))
)
dbt_project.prepare_if_dev()

_SOURCE_ASSET_KEYS = {
    "nasa_astronomy_picture_of_the_day": AssetKey(["dlt_nasa_astronomy_picture_of_the_day"]),
    "nasa_near_earth_object_feed": AssetKey(["dlt_nasa_near_earth_object_feed"]),
    "nasa_near_earth_object_lookups": AssetKey(["nasa_near_earth_object_lookups"]),
    **{
        t: AssetKey([f"dlt_{t}"])
        for t in [
            "nasa_donki_coronal_mass_ejections",
            "nasa_donki_coronal_mass_ejection_analyses",
            "nasa_donki_geomagnetic_storms",
            "nasa_donki_interplanetary_shocks",
            "nasa_donki_solar_flares",
            "nasa_donki_solar_energetic_particles",
            "nasa_donki_magnetopause_crossings",
            "nasa_donki_radiation_belt_enhancements",
            "nasa_donki_high_speed_streams",
            "nasa_donki_wsa_enlil_simulations",
            "nasa_donki_notifications",
        ]
    },
    "space_track_general_perturbations": AssetKey(["dlt_space_track_general_perturbations"]),
    "space_track_satellite_catalog": AssetKey(["dlt_space_track_satellite_catalog"]),
    "space_track_boxscore": AssetKey(["dlt_space_track_boxscore"]),
    "space_track_decays": AssetKey(["dlt_space_track_decays"]),
    "space_track_conjunction_data_messages": AssetKey(
        ["dlt_space_track_conjunction_data_messages"]
    ),
    "space_track_tracking_and_impact_predictions": AssetKey(
        ["dlt_space_track_tracking_and_impact_predictions"]
    ),
    "celestrak_space_weather": AssetKey(["dlt_celestrak_space_weather"]),
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
