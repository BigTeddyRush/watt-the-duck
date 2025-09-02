from dagster import DailyPartitionsDefinition

# energy_charts daily partitions definition
energy_charts_daily_partitions_def = DailyPartitionsDefinition(
    start_date="2023-01-01",
    timezone="Europe/Berlin",
    end_offset=1
)