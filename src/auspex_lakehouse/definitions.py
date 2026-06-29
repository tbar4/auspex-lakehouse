from dagster import (
    AutomationConditionSensorDefinition,
    Definitions,
    load_assets_from_package_module,
)
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

import auspex_lakehouse.bronze as bronze
from auspex_lakehouse.transform import dbt_bronze_assets, dbt_project

# import auspex_lakehouse.silver as silver

defs = Definitions(
    assets=[
        *load_assets_from_package_module(bronze),
        dbt_bronze_assets,
        # *load_assets_from_package_module(silver),
    ],
    resources={
        "dlt": DagsterDltResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
    sensors=[
        AutomationConditionSensorDefinition(
            name="automation_condition_sensor",
            target="*",  # evaluates all assets with automation_condition set
        )
    ],
)
