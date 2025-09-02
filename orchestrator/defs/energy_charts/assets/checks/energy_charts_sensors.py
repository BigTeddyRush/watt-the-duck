from dagster import DefaultSensorStatus, build_sensor_for_freshness_checks
from .energy_charts_asset_checks import energy_charts_daily_freshness_checks

energy_charts_daily_freshness_check_sensor = build_sensor_for_freshness_checks(
    freshness_checks=energy_charts_daily_freshness_checks,
    minimum_interval_seconds=600,
    name ="energy_charts_daily_freshness_check_sensor",
    default_status=DefaultSensorStatus.STOPPED
)

energy_charts_sensors = [energy_charts_daily_freshness_check_sensor]