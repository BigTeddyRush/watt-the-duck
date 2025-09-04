{{ config(materialized='incremental', unique_key='unix_seconds_15min') }}

with base as (
  select *
  from {{ ref('stg_energy_charts__dap') }}
),

-- generate offset 0, 1, 2, 3 for 15-minute steps
offsets as (
  select 0 as step union all
  select 1 union all
  select 2 union all
  select 3
),

expanded as (
  select
    -- new 15-min timestamp (in seconds)
    b.unix_seconds + (o.step * 900) as unix_seconds_15min,

    -- bring through all columns from the staging table, except those we replace/drop
    {{ dbt_utils.star(
         from=ref('stg_energy_charts__dap'),
         relation_alias='b',
         except=["unix_seconds", "_dlt_id", "_dlt_load_id", "day", "month", "year"]
    ) }},
  from base b
  cross join offsets o
)

select * from expanded
