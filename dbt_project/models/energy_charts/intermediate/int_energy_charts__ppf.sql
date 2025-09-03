{{ config(materialized='incremental', unique_key='_dlt_id') }}

select
  unix_seconds as unix_seconds_15min,
  {{ dbt_utils.star(
       from=ref('stg_energy_charts__ppf'),
       except=["unix_seconds", "_dlt_load_id", "day", "month", "year"]
  ) }}
from {{ ref('stg_energy_charts__ppf') }}
