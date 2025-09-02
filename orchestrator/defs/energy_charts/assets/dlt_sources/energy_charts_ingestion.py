import dlt
import os
import yaml
import pandas as pd
from dlt.sources.helpers import requests
from ..configs.energy_charts_api_default_params_config import EnergyChartsResources
from dagster import Failure

CONFIG_FILE_PIPELINE: str = "energy_charts_ingestion_pipeline"
CONFIG_FILE_NAME: str = "energy_charts_ingestion_config.yaml"

CURRENT_DIR: str = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH: str = os.path.join(CURRENT_DIR,CONFIG_FILE_NAME)

with open(CONFIG_PATH, "r") as f:
    CONFIG_FILE_DATA: dict = yaml.safe_load(f)

schema_contract = CONFIG_FILE_DATA[CONFIG_FILE_PIPELINE]["defaults"]['schema_contract']

@dlt.source()
def energy_charts_importer(context, resource_name:str, config_params: dict):

    base_url = "https://api.energy-charts.info/"

    if resource_name in [EnergyChartsResources.public_power.value,
                         EnergyChartsResources.total_power.value]:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def power_loader():
            url = f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url = url,
                                        params = config_params)
                context.log.info(f"Succesfully fetched data from {url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {url}: {e}")
                        
            response_json = response.json()

            temp_data = {ptype['name']: ptype['data'] for ptype in response_json['production_types']}
            temp_data['unix_seconds'] = response_json['unix_seconds']
            temp_data['country'] = config_params['country']
            temp_data_df = pd.DataFrame(temp_data)

            power_json = temp_data_df.to_dict(orient='records')

            yield from power_json

        return power_loader

    elif resource_name in [EnergyChartsResources.cross_border_electricity_trading.value,
                           EnergyChartsResources.cross_border_physical_flows.value]:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def cross_border():
            if resource_name == EnergyChartsResources.cross_border_electricity_trading.value:
                url = f"{base_url}cbet"
            elif resource_name == EnergyChartsResources.cross_border_physical_flows.value:
                url = f"{base_url}cbpf"

            context.log.info(f"Fetching data from URL: {url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url = url,
                                        params = config_params)
                context.log.info(f"Succesfully fetched data from {url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {url}: {e}")

            response_json = response.json()

            temp_data = {ptype['name']: ptype['data'] for ptype in response_json['countries']}
            temp_data['unix_seconds'] = response_json['unix_seconds']
            temp_data['country'] = config_params['country']
            temp_data_df = pd.DataFrame(temp_data)

            power_json = temp_data_df.to_dict(orient='records')

            yield from power_json

        return cross_border

    elif resource_name in [EnergyChartsResources.solar_share.value,
                           EnergyChartsResources.wind_onshore_share.value,
                           EnergyChartsResources.wind_offshore_share.value]:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def share_loader():
            url = f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url = url,
                                        params = config_params)
                context.log.info(f"Succesfully fetched data from {url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {url}: {e}")

            response_json = response.json()

            share_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            share_df["country"] = config_params["country"]
            share_df = share_df.drop(columns=["deprecated"])

            share_json = share_df.to_dict(orient='records')

            yield from share_json

        return share_loader

    elif resource_name in [EnergyChartsResources.ren_share_daily_avg.value,
                           EnergyChartsResources.solar_share_daily_avg.value,
                           EnergyChartsResources.wind_onshore_share_daily_avg.value,
                           EnergyChartsResources.wind_offshore_share_daily_avg.value]:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def daily_avg_loader():
            url = f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url = url,
                                        params = config_params)
                context.log.info(f"Succesfully fetched data from {url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {url}: {e}")
                
            response_json = response.json()

            daily_avg_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            daily_avg_df["country"] = config_params["country"]
            daily_avg_df = daily_avg_df.drop(columns=["deprecated"])

            daily_avg_json = daily_avg_df.to_dict(orient='records')

            yield from daily_avg_json

        return daily_avg_loader

    elif resource_name == EnergyChartsResources.public_power_forecast.value:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def public_power_forecast_loader():
            public_power_forecast_url = f"{base_url}{resource_name}"

            public_power_forecast_df = []

            for production_type in config_params["production_types"]:

                if production_type == "load":
                    forecast_types = ["day-ahead"]
                else:
                    forecast_types = config_params["forecast_types"]

                for forecast_type in forecast_types:

                    config_params_temp = {
                        "country": config_params["country"],
                        "start": config_params["start"],
                        "end": config_params["end"],
                        "production_type": production_type,
                        "forecast_type": forecast_type
                    }

                    context.log.info(f"Fetching data from URL: {public_power_forecast_url} with params: {config_params_temp}")

                    # Dagster will only cause an error if all Sub-Assets do not have data
                    try:
                        response = requests.get(url=public_power_forecast_url,
                                                    params=config_params_temp)
                        context.log.info(f"Succesfully fetched data from {public_power_forecast_url}")
                    except requests.RequestException as e:
                        context.log.warn(f"Error fetching data: {e}")
                        continue
                    
                    public_power_forecast_df_temp = pd.DataFrame({key: pd.Series(value) for key, value in response.json().items()})
                    public_power_forecast_df_temp['production_type'] = config_params_temp['production_type']
                    public_power_forecast_df_temp['forecast_type'] = config_params_temp['forecast_type']
                    public_power_forecast_df_temp['country'] = config_params_temp['country']

                    public_power_forecast_df.append(public_power_forecast_df_temp)

            if not public_power_forecast_df:
                raise Failure(f"No data returned from {public_power_forecast_url} with params: {config_params_temp}")
            public_power_forecast_df = pd.concat(public_power_forecast_df, ignore_index=True)
            public_power_forecast_json = public_power_forecast_df.to_dict(orient='records')

            yield from public_power_forecast_json

        return public_power_forecast_loader

    elif resource_name == EnergyChartsResources.price.value:

        @dlt.resource(table_name="day_ahead_price",
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def price_loader():
            price_url= f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {price_url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url=price_url,
                                        params=config_params)
                context.log.info(f"Succesfully fetched data from {price_url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {price_url}: {e}")

            response_json = response.json()

            price_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            price_df["bzn"] = config_params["bzn"]
            price_df = price_df.drop(columns=["deprecated", "license_info"])

            price_json = price_df.to_dict(orient='records')

            yield from price_json

        return price_loader

    elif resource_name == EnergyChartsResources.installed_power.value:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def installed_power_loader():
            installed_power_url = f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {installed_power_url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url=installed_power_url,
                                        params=config_params)
                context.log.info(f"Succesfully fetched data from {installed_power_url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {installed_power_url}: {e}")

            response_json = response.json()

            power_data = {ptype['name']: ptype['data'] for ptype in response_json['production_types']}
            power_data['time'] = response_json['time']
            power_data['country'] = config_params['country']
            power_data_df = pd.DataFrame(power_data)

            installed_power_json = power_data_df.to_dict(orient='records')

            yield from installed_power_json

        return installed_power_loader

    elif resource_name == EnergyChartsResources.frequency.value:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def frequency_loader():
            frequency_url= f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {frequency_url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url=frequency_url,
                                        params=config_params)
                context.log.info(f"Succesfully fetched data from {frequency_url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {frequency_url}: {e}")

            response_json = response.json()

            frequency_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            frequency_df['region'] = config_params['region']
            frequency_df = frequency_df.drop(columns=["deprecated"])

            frequency_json = frequency_df.to_dict(orient='records')

            yield from frequency_json

        return frequency_loader

    elif resource_name == EnergyChartsResources.signal.value:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def signal_loader():
            signal_url= f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {signal_url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url=signal_url,
                                        params=config_params)
                context.log.info(f"Succesfully fetched data from {signal_url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {signal_url}: {e}")

            response_json = response.json()

            signal_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            signal_df["country"] = config_params["country"]
            signal_df["postal_code"] = config_params["postal_code"]
            signal_df = signal_df.drop(columns=["substitute","deprecated"])

            signal_json = signal_df.to_dict(orient='records')

            yield from signal_json

        return signal_loader

    elif resource_name == EnergyChartsResources.ren_share_forecast.value:

        @dlt.resource(table_name=resource_name,
                      schema_contract=schema_contract,
                      write_disposition=config_params.get('write_disposition'))
        def ren_share_forecast_loader():
            ren_share_forecast_url= f"{base_url}{resource_name}"

            context.log.info(f"Fetching data from URL: {ren_share_forecast_url}")

            # Dagster will only cause an error if all Sub-Assets do not have data
            try:
                response = requests.get(url=ren_share_forecast_url,
                                        params=config_params)
                context.log.info(f"Succesfully fetched data from {ren_share_forecast_url}")
            except requests.RequestException as e:
                raise Failure(f"Failed to fetch data from {ren_share_forecast_url}: {e}")

            response_json = response.json()

            ren_share_forecast_df = pd.DataFrame({key: pd.Series(value) for key, value in response_json.items()})
            ren_share_forecast_df["country"] = config_params["country"]
            ren_share_forecast_df = ren_share_forecast_df.drop(columns=["substitute","deprecated"])

            ren_share_forecast_json = ren_share_forecast_df.to_dict(orient='records')

            yield from ren_share_forecast_json

        return ren_share_forecast_loader