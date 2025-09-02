from collections.abc import Callable
from dagster import AssetExecutionContext, BackfillPolicy, DailyPartitionsDefinition
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from .dlt_sources import energy_charts_importer
from .configs import EnergyChartsApiParamsConfig
from orchestrator.configs.dlt_translator import CustomDagsterDltTranslatorFileSystem
from orchestrator.configs.filesystem_ingestion_pipeline import filesystem_ingestion_pipeline
import pandas as pd
from .configs.energy_charty_partitions import energy_charts_daily_partitions_def
from .configs.energy_charts_api_default_params_config import EnergyChartsResources

def energy_charts_to_duckdb(source_name: str, resource_name: str, resolution: str) -> Callable:

    if "partitioned" in resolution:
        partitions_def=energy_charts_daily_partitions_def
    else:
        partitions_def=None

    @dlt_assets(
        dlt_source=energy_charts_importer(context=None,
                                          resource_name=resource_name,
                                          config_params={}),
        dlt_pipeline=filesystem_ingestion_pipeline(
            source_name=source_name,
            resource_name=resource_name),
        name=f"{source_name}_ingestion_{resource_name}",
        dagster_dlt_translator=CustomDagsterDltTranslatorFileSystem(source_name = source_name,
                                                                    resource_name = resource_name,
                                                                    domain = "energy_data",
                                                                    owner = ["v.flasbart@gp-joule.de","team:DataHub"]),
        partitions_def=partitions_def,
        backfill_policy=BackfillPolicy.single_run() if partitions_def else None
    )
    def energy_charts_to_duckdb(
        context: AssetExecutionContext,
        dlt: DagsterDltResource,
        config: EnergyChartsApiParamsConfig
    ):

        config_params = config.model_dump()

        if partitions_def is not None:

            if context.has_partition_key:

                # Single partition run
                partition_start = pd.Timestamp(context.partition_key,tz="Europe/Berlin")
                partition_end = partition_start

            elif context.has_partition_key_range:

                # Range partition run
                partition_start = pd.Timestamp(context.partition_key_range.start,tz="Europe/Berlin")
                partition_end = pd.Timestamp(context.partition_key_range.end,tz="Europe/Berlin")

            else:
                raise ValueError("No partition key or partition key range found for partitioned run")

            FORECAST_RESOURCES = {
                EnergyChartsResources.cross_border_electricity_trading.value,
                EnergyChartsResources.public_power_forecast.value,
            }

            is_forecast = resource_name in FORECAST_RESOURCES

            # if is_forecast tomorrow 00:00 til tomorrow 23:45
            # if not is_forecast yesterday 00:00 til yesterday 23:45
            start_day_offset = 1 if is_forecast else -1
            end_day_offset   = 1 if is_forecast else -1

            start_dt = partition_start + pd.Timedelta(days=start_day_offset)
            end_dt   = partition_end   + pd.Timedelta(days=end_day_offset)

            config_params.update({
                "start": start_dt.strftime("%Y-%m-%d"),
                "end": end_dt.strftime("%Y-%m-%d")
            })

        context.log.info(f"Config_params: {config_params}")

        yield from dlt.run(
            context=context,
            dlt_source=energy_charts_importer(context=context,
                                              resource_name=resource_name,
                                              config_params=config_params),
            loader_file_format="parquet"
        )

    return energy_charts_to_duckdb