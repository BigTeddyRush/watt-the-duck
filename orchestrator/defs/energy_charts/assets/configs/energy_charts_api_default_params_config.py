from .energy_charts_api_params_config import EnergyChartsApiParamsConfig
from datetime import datetime,  timedelta
from enum import Enum

class EnergyChartsResources(Enum):
    public_power = "public_power"
    public_power_forecast = "public_power_forecast"
    total_power = "total_power"
    frequency = "frequency"
    cross_border_electricity_trading = "cross_border_electricity_trading"
    cross_border_physical_flows = "cross_border_physical_flows"
    price = "price"
    installed_power = "installed_power"
    ren_share_forecast = "ren_share_forecast"
    signal = "signal"
    solar_share = "solar_share"
    wind_onshore_share = "wind_onshore_share"
    wind_offshore_share = "wind_offshore_share"
    ren_share_daily_avg = "ren_share_daily_avg"
    solar_share_daily_avg = "solar_share_daily_avg"
    wind_onshore_share_daily_avg = "wind_onshore_share_daily_avg"
    wind_offshore_share_daily_avg = "wind_offshore_share_daily_avg"

# Daily partitioned request

public_power_forecast_default_config = EnergyChartsApiParamsConfig(
    production_types=["solar","wind_onshore","wind_offshore","load"],
    forecast_types=["current","intraday","day-ahead"]
)

cross_border_electricity_trading_default_config = EnergyChartsApiParamsConfig()

price_default_config = EnergyChartsApiParamsConfig()


energy_charts_asset_default_configs = {
    "daily_partitioned": {
        EnergyChartsResources.public_power_forecast.value: public_power_forecast_default_config.model_dump(),
        EnergyChartsResources.cross_border_electricity_trading.value: cross_border_electricity_trading_default_config.model_dump(),
        EnergyChartsResources.price.value: price_default_config.model_dump(),
    }
}