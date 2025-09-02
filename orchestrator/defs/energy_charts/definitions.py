from dagster import Definitions, EnvVar, load_assets_from_package_module
from orchestrator.defs import energy_charts

energy_charts_assets = load_assets_from_package_module(
    package_module = energy_charts.assets,
    group_name="energy_charts"
)

defs = Definitions(
    assets=energy_charts_assets,
    asset_checks=energy_charts.energy_charts_checks,
    jobs=energy_charts.energy_charts_jobs,
    schedules=energy_charts.energy_charts_schedules,
    sensors=energy_charts.energy_charts_sensors
)