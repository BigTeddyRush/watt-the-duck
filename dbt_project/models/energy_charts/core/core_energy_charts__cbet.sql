{{ config(
    materialized = 'incremental',
    unique_key   = 'unix_seconds_15min'
) }}

select
    to_timestamp(unix_seconds_15min) as ts_utc,
    country,

    -- multiply every other column by 1000
    {% for col in adapter.get_columns_in_relation(ref('int_energy_charts__cbet')) %}
        {% if col.name not in ['unix_seconds_15min','country'] %}
            {{ col.name }} * 1000 as {{ col.name }}{% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}

from {{ ref('int_energy_charts__cbet') }}
