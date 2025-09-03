from dagster import define_asset_job, ScheduleDefinition, AssetKey, RetryPolicy
from .assets.configs.energy_charts_api_default_params_config import energy_charts_asset_default_configs
from .assets.configs.energy_charty_partitions import energy_charts_daily_partitions_def
from orchestrator.configs.get_latest_partition import latest_partition_execution_fn

daily_partitioned_assets = []
daily_partitioned_ops_config = {}
daily_partitioned_keys = ["price", "public_power_forecast", "cross_border_electricity_trading"]

for key in daily_partitioned_keys:
    asset_key = AssetKey(['EXTRACTION', f'energy_charts_to_duckdb_{key}'])
    daily_partitioned_assets.append(asset_key)
    op_key = f"energy_charts_ingestion_{key}"
    daily_partitioned_ops_config[op_key] = {
        "config": energy_charts_asset_default_configs["daily_partitioned"][key]
    }

energy_charts_daily_partitioned_import_job = define_asset_job(
    name="energy_charts_daily_partitioned_job",
    selection=daily_partitioned_assets,
    config={
        "ops": daily_partitioned_ops_config
    },
    op_retry_policy=RetryPolicy(max_retries=3,delay=60),
    tags={"dagster/max_runtime": 600}
)

energy_charts_daily_partitioned_import_schedule="0 0 * * *"

energy_charts_daily_partitioned_job_schedule = ScheduleDefinition(job=energy_charts_daily_partitioned_import_job,
                                                                  cron_schedule=energy_charts_daily_partitioned_import_schedule,
                                                                  execution_timezone="Europe/Berlin",
                                                                  execution_fn=lambda context: latest_partition_execution_fn(context, energy_charts_daily_partitions_def))

daily_partitioned_assets = []

energy_charts_jobs = [energy_charts_daily_partitioned_import_job]
energy_charts_schedules = [energy_charts_daily_partitioned_job_schedule]