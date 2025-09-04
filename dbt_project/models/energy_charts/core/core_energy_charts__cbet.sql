{{ config(materialized='incremental', unique_key='unix_seconds_15min') }}

select
  to_timestamp(unix_seconds_15min) as ts_utc,
  {{ dbt_utils.star(
       from=ref('int_energy_charts__cbet'),
       except=["unix_seconds_15min"]
  ) }}
from {{ ref('int_energy_charts__cbet') }}
