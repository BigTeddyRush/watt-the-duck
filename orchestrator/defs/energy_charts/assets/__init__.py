from dagster import SourceAsset, AssetsDefinition
from .energy_charts_dbt import energy_charts_dbt
from .energy_charts_to_duckdb import energy_charts_to_duckdb
from orchestrator.configs.asset_tags import AssetTags
from .configs.energy_charts_api_default_params_config import energy_charts_asset_default_configs

energy_charts_dbt_external_assets = [
    SourceAsset(
        dep_key,
        tags=AssetTags(
            layer=dep_key.path[0],
            source="energy_charts",
            domain="energy_data",
            sharable=False
        ).to_dict()
    )
    for dep_key in energy_charts_dbt.dependency_keys
    if dep_key.path[0] in ["EXTERNAL_ASSETS"]
]

def consolidate_resources(configs):
    all_resources = []
    for resolution, resources in configs.items():
        for resource_name in resources.keys():
            all_resources.append((resolution, resource_name))
    return all_resources

energy_charts_resources = consolidate_resources(energy_charts_asset_default_configs)

energy_charts_to_duckdb_assets = [
    energy_charts_to_duckdb(
        source_name="energy_charts",
        resource_name=resource,
        resolution=resolution
    )
    for resolution, resource in energy_charts_resources
]