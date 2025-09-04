# energy_charts_importer.py
from __future__ import annotations

import os
import yaml
import dlt
from dagster import Failure
from typing import Dict, Iterable

from ..configs.energy_charts_api_default_params_config import EnergyChartsResources
from orchestrator.utils.energycharts_api import EnergyChartsApi, EnergyChartsApiError


CONFIG_FILE_PIPELINE: str = "energy_charts_ingestion_pipeline"
CONFIG_FILE_NAME: str = "energy_charts_ingestion_config.yaml"

CURRENT_DIR: str = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH: str = os.path.join(CURRENT_DIR, CONFIG_FILE_NAME)

with open(CONFIG_PATH, "r") as f:
    CONFIG_FILE_DATA: dict = yaml.safe_load(f)

schema_contract = CONFIG_FILE_DATA[CONFIG_FILE_PIPELINE]["defaults"]["schema_contract"]


def _logger_from_context(context):
    # adapt EnergyChartsApi logger to Dagster/DLT context
    return lambda msg: context.log.info(str(msg))


@dlt.source()
def energy_charts_importer(context, resource_name: str, config_params: Dict):
    """
    Thin DLT source that delegates all HTTP + normalization to EnergyChartsApi.
    """

    api = EnergyChartsApi(logger=_logger_from_context(context))

    # ---------------- Cross-border (both variants supported) ----------------
    if resource_name == EnergyChartsResources.cross_border_electricity_trading.value:

        @dlt.resource(
            table_name=resource_name,
            schema_contract=schema_contract,
            write_disposition=config_params.get("write_disposition"),
        )
        def cross_border_electricity_trading():
            try:
                rows = api.get_cross_border_electricity_trading(config_params)
            except EnergyChartsApiError as e:
                raise Failure(str(e))
            yield rows

        return cross_border_electricity_trading

    # ---------------- Public power forecast ----------------
    if resource_name == EnergyChartsResources.public_power_forecast.value:

        @dlt.resource(
            table_name=resource_name,
            schema_contract=schema_contract,
            write_disposition=config_params.get("write_disposition"),
        )
        def public_power_forecast_loader():
            try:
                rows = api.get_public_power_forecast(config_params)
            except EnergyChartsApiError as e:
                raise Failure(str(e))
            yield rows

        return public_power_forecast_loader

    # ---------------- Day-ahead price ----------------
    if resource_name == EnergyChartsResources.price.value:

        @dlt.resource(
            table_name="day_ahead_price",
            schema_contract=schema_contract,
            write_disposition=config_params.get("write_disposition"),
        )
        def price_loader():
            try:
                rows = api.get_day_ahead_price(config_params)
            except EnergyChartsApiError as e:
                raise Failure(str(e))
            yield rows

        return price_loader

    # ---------------- Unknown resource ----------------
    raise Failure(f"Unsupported resource_name: {resource_name}")
