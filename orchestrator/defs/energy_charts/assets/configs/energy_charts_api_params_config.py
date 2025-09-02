from dagster import Config
from pydantic import Field
from typing import Optional, List

class EnergyChartsApiParamsConfig(Config):
    country: Optional[str] = Field(
        default="de",
        description="[M] EIC code of a bidding zone."
    )
    bzn: Optional[str] = Field(
        default="DE-LU",
        description="Bidding zone for retreiving day ahread prices."
    )
    region: Optional[str] = Field(
        default="UCTE",
        description="Region for retreiving frequency data."
    )
    postal_code: Optional[str] = Field(
        default=None,
        description="Postal code for local grid state."
    )
    start: Optional[str] = Field(
        default=None,
        description="Start date of data extraction."
    )
    end: Optional[str] = Field(
        default=None,
        description="End date of data extraction."
    )
    time_step: Optional[str] = Field(
        default="monthly",
        description="Time step can be either yearly or monthly (only for Germany)."
    )
    year: Optional[int] = Field(
        default=-1,
        description="Year to extract data from or relative from today."
    )
    installation_decomission: Optional[bool] = Field(
        default=None,
        description="If true, the net installation / decommission numbers are returned instead of total installed power."
    )
    production_types: Optional[List[str]] = Field(
        default=None,
        description="production_type: Can be solar, wind_onshore, wind_offshore or load."
    )
    forecast_types: Optional[List[str]] = Field(
        default=None,
        description="forecast_type: Can be current, intraday or day-ahead."
    )
    write_disposition: Optional[str] = Field(
        default="append",
        description="Write disposition for the data ['append','replace'] defaults to append."
    )
