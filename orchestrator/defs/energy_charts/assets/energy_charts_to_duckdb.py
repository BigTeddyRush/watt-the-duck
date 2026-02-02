from collections.abc import Callable
from dagster import AssetExecutionContext, BackfillPolicy
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import pandas as pd

from .dlt_sources import energy_charts_importer
from .configs import EnergyChartsApiParamsConfig
from orchestrator.configs.dlt_translator import CustomDagsterDltTranslatorFileSystem
from orchestrator.configs.filesystem_ingestion_pipeline import filesystem_ingestion_pipeline
from .configs.energy_charty_partitions import energy_charts_daily_partitions_def
from .configs.energy_charts_api_default_params_config import EnergyChartsResources

def energy_charts_to_duckdb(source_name: str, resource_name: str, resolution: str) -> Callable:

    partitions_def = energy_charts_daily_partitions_def if "partitioned" in resolution else None

    @dlt_assets(
        # Provide only static bits here
        dlt_source=energy_charts_importer(context=None, resource_name=resource_name, config_params={}),
        # We will override the pipeline at runtime inside the asset body
        dlt_pipeline=None,
        name=f"{source_name}_ingestion_{resource_name}",
        dagster_dlt_translator=CustomDagsterDltTranslatorFileSystem(
            source_name=source_name,
            resource_name=resource_name,
            domain="energy_data",
            owner=["BigTeddyRush@mail.de", "team:Teddy"],
        ),
        partitions_def=partitions_def,
    )
    def _asset(
        context: AssetExecutionContext,
        dlt: DagsterDltResource,
        config: EnergyChartsApiParamsConfig,
    ):
        config_params = config.model_dump()

        # --- Compute the data-day range from the Dagster partition ---
        if partitions_def is not None:
            if context.has_partition_key:
                partition_start = pd.Timestamp(context.partition_key, tz="Europe/Berlin")
                partition_end = partition_start
            elif context.has_partition_key_range:
                partition_start = pd.Timestamp(context.partition_key_range.start, tz="Europe/Berlin")
                partition_end = pd.Timestamp(context.partition_key_range.end, tz="Europe/Berlin")
            else:
                raise ValueError("No partition key found for partitioned run")

            FORECAST_RESOURCES = {
                EnergyChartsResources.cross_border_electricity_trading.value,
                EnergyChartsResources.public_power_forecast.value,
                EnergyChartsResources.price.value,
            }
            is_forecast = resource_name in FORECAST_RESOURCES

            # if forecast: tomorrow; else: yesterday
            day_offset = 1 if is_forecast else -1
            start_dt = partition_start + pd.Timedelta(days=day_offset)
            end_dt   = partition_end   + pd.Timedelta(days=day_offset)

            config_params.update({
                "start": start_dt.strftime("%Y-%m-%d"),
                "end":   end_dt.strftime("%Y-%m-%d"),
            })

            # --- Build layout using the *data* day, not load day ---
            if context.has_partition_key:
                layout = (
                    f"{{table_name}}/"
                    f"year={start_dt.strftime('%Y')}/"
                    f"month={start_dt.strftime('%m')}/"
                    f"day={start_dt.strftime('%d')}/"
                    f"{{load_id}}.{{file_id}}.{{ext}}"
                )
            else:
                layout = (
                    f"{{table_name}}/"
                    f"refill/"
                    f"start={start_dt.strftime('%Y')}{start_dt.strftime('%m')}{start_dt.strftime('%d')}/"
                    f"end={end_dt.strftime('%Y')}{end_dt.strftime('%m')}{end_dt.strftime('%d')}/"
                    f"{{load_id}}.{{file_id}}.{{ext}}"
                )
        else:
            # Non-partitioned: keep default behavior or choose a default
            layout = None

        context.log.info(f"Config_params: {config_params}")
        context.log.info(f"Filesystem layout for this run: {layout}")

        # --- Create a fresh pipeline *now* with the runtime layout ---
        runtime_pipeline = filesystem_ingestion_pipeline(
            source_name=source_name,
            resource_name=resource_name,
            layout=layout,
        )

        # --- Run dlt with the runtime pipeline and the runtime source ---
        yield from dlt.run(
            context=context,
            dlt_source=energy_charts_importer(
                context=context,
                resource_name=resource_name,
                config_params=config_params
            ),
            dlt_pipeline=runtime_pipeline,     
            loader_file_format="parquet",
        )

    return _asset
