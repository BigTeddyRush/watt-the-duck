{{ config(materialized='table') }}

{% set base = '../data/energy_charts/cross_border_electricity_trading/' %}

{% if var('reload_data') %}
-- Full reload: read all partitions
select * from read_parquet('{{ base }}**/*.parquet')
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
