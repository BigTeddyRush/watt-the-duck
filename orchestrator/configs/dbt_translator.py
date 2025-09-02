import re
from typing import List, Mapping, Any, Optional, Iterable
from dagster import AssetKey, AssetSelection, AutomationCondition
from orchestrator.configs.asset_tags import AssetTags
from dagster_dbt.asset_decorator import DagsterDbtTranslator

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def __init__(
        self,
        domain: str,
        sharable_layers: List[str] = ["CORE","PUBLIC","PRESENTATION"],
        source_name: Optional[str] = None,
        deps_asset_name: Optional[str] = None,
        owners: Optional[List[str]] = None
    ):
        super().__init__()

        self.source_name = source_name
        self.domain = domain
        self.SHARABLE_LAYERS = sharable_layers
        self.DEPS_ASSET_NAME = deps_asset_name
        self.owners = owners if owners else ["team:DataHub"]

    def get_owners(self, resource) -> Iterable[str]:
        """Return owners for the asset. Could be static or dynamic from resource."""
        return self.owners

    def get_deps_asset_keys(self) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return [AssetKey(self.DEPS_ASSET_NAME)] if self.DEPS_ASSET_NAME else []

    def get_asset_key(self, resource) -> AssetKey:
        asset_key = super().get_asset_key(resource)
        normalized_path = [
            re.sub(r".*_EXT_RAW$", "EXTERNAL_ASSETS", p) for p in asset_key.path
        ]
        return AssetKey(normalized_path)

    def get_tags(self, resource) -> Mapping[str, str]:
        dbt_layer = super().get_asset_key(resource).path[0]

        return AssetTags(
            layer=dbt_layer,
            source=self.source_name,
            domain=self.domain,
            sharable=dbt_layer in self.SHARABLE_LAYERS
        ).to_dict()

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[AutomationCondition]:

        schema_name = dbt_resource_props.get("schema", "")
        materialized_type = dbt_resource_props.get("config", {}).get("materialized", "")

        # Define allowed prefixes for PUBLIC based on sharable layers (PUBLIC and CORE)
        allowed_prefixes = [
            f"{layer}/" for layer in self.SHARABLE_LAYERS
        ]

        # Mapping schema -> prefixes and label
        schema_config = {
            "STAGING": {"prefixes": ["EXTRACTION/"], "label": "staging"},
            "INTERMEDIATE": {"prefixes": ["STAGING/"], "label": "int"},
            "CORE": {"prefixes": ["INTERMEDIATE/"], "label": "core"},
            "PUBLIC": {"prefixes": allowed_prefixes, "label": "public"},
            "PRESENTATION": {"prefixes": allowed_prefixes, "label": "presentation"},
        }

        def base_condition(prefixes: list[str], label: str) -> AutomationCondition:
            return (
                (AutomationCondition.eager()
                .allow(AssetSelection.key_prefixes(*prefixes)))
                .__or__(AutomationCondition.code_version_changed())
                .with_label(f"code_change_or_eager_{label}")
            )

        # Handle tables and incremental models
        if materialized_type in ("table", "incremental"):
            if schema_name in schema_config:
                cfg = schema_config[schema_name]
                return base_condition(cfg["prefixes"], cfg["label"])

        # Handle views
        if materialized_type == "view":
            return (
                AutomationCondition.code_version_changed()
                .with_label("view_code_change")
            )

        return None