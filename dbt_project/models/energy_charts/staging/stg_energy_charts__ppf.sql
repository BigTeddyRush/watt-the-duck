{{ config(materialized='table') }}

{% set base = '../data/energy_charts/public_power_forecast/' %}

{% if var('reload_data') %}
  -- Full reload: read all partitions
  with src as (
  select *
  from read_parquet('{{ base }}**/*.parquet')
  ),
  tagged as (
    select
      *,
      row_number() over (partition by unix_seconds) as rn
    from src
  )
  select * exclude (rn)
  from tagged
  where rn = 1
{% else %}
  -- Incremental daily load: only yesterday's partition
  select *
  from read_parquet(
    '{{ base }}'
    || 'year='  || strftime(current_date + interval 1 day, '%Y')
    || '/month='|| strftime(current_date + interval 1 day, '%m')
    || '/day='  || strftime(current_date + interval 1 day, '%d')
    || '/*.parquet'
)
{% endif %}
