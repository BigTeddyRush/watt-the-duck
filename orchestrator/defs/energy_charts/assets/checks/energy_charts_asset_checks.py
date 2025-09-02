import pandas as pd
from datetime import timedelta
from dagster import AssetCheckSeverity, build_last_update_freshness_checks, AssetKey
from ..configs.energy_charts_api_default_params_config import energy_charts_asset_default_configs

energy_charts_daily_resources = list(energy_charts_asset_default_configs["daily_partitioned"].keys())
energy_charts_daily_assets = [AssetKey(['EXTRACTION', f'energy_charts_to_duckdb_{asset}']) for asset in energy_charts_daily_resources]

energy_charts_daily_freshness_checks = build_last_update_freshness_checks(
    assets=energy_charts_daily_assets,
    lower_bound_delta=timedelta(days=1),
    timezone="Europe/Berlin",
    severity=AssetCheckSeverity.WARN
)

energy_charts_checks = [*energy_charts_daily_freshness_checks]
