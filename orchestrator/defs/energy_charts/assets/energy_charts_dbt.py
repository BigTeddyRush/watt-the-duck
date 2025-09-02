from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from orchestrator.dbt_projects import dbt_project
from orchestrator.configs.dbt_translator import CustomDagsterDbtTranslator
from orchestrator.configs.dbt_args_config import DbtArgsConfig

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="energy_charts",
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        source_name="energy_charts",
        domain="energy_data",
        sharable_layers=["CORE"],
        deps_asset_name="energy_charts_to_duckdb"
    )
)
def energy_charts_dbt(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtArgsConfig
):
    dbt_cli_args = [config.write_command]

    for key, value in config.args.items():
        if isinstance(value, bool) and value:
            dbt_cli_args.append(f"--{key}")
        elif value is not None:
            dbt_cli_args.append(f"--{key}={value}")

    yield from dbt.cli(dbt_cli_args, context=context).stream()
    if config.test_after_run:
        yield from dbt.cli(["test"], context=context).stream()