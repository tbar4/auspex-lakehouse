from dagster import (
    AutomationConditionSensorDefinition,
    Definitions,
    load_assets_from_package_module,
)
from dagster_dlt import DagsterDltResource

import auspex_lakehouse.bronze as bronze

#import auspex_lakehouse.silver as silver

defs = Definitions(
    assets=[
        *load_assets_from_package_module(bronze),
 #       *load_assets_from_package_module(silver),
    ],
    resources={
        "dlt": DagsterDltResource(),
    },
    sensors=[
        AutomationConditionSensorDefinition(
            name="automation_condition_sensor",
            target="*",   # evaluates all assets with automation_condition set
        )
    ],
)
