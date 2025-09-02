from orchestrator.configs.asset_tags import AssetTags
from dagster import AssetKey, AssetSpec
from typing import Set, List
from dagster_embedded_elt.dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

class CustomDagsterDltTranslatorFileSystem(DagsterDltTranslator):

    def __init__(self,
                 source_name: str,
                 resource_name: str,
                 owner: List[str],
                 domain: str,
                 deps_asset_name: str | None = None,
                 corrected_destination: List[str] = ["azure","parquet"]):

        self.resource_name = resource_name
        self.source_name = source_name
        self.domain = domain
        self.owner = owner
        self.CORRECTED_DESTINATION = corrected_destination
        self.DLT_DEFAULT_DESTINATION = "filesystem"
        self.ASSET_PREFIX = "EXTRACTION"
        self.ASSET_PARENT_NAME = f"{self.source_name}_to_duckdb"
        self.DEPS_ASSET_NAME = deps_asset_name

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        default_spec = super().get_asset_spec(data)

        kinds: Set[str] = default_spec.kinds

        if self.DLT_DEFAULT_DESTINATION in kinds:
            kinds.remove(self.DLT_DEFAULT_DESTINATION)
            kinds.update(self.CORRECTED_DESTINATION)

        """Overrides asset spec to override asset key to be the dlt resource name."""
        return default_spec.replace_attributes(
            key=AssetKey([self.ASSET_PREFIX, f"{self.ASSET_PARENT_NAME}_{self.resource_name}"]),
            owners=self.owner,
            description=f"Asset for ingesting {self.source_name} data for the source: {self.resource_name}",
            tags=AssetTags(
                    domain=self.domain,
                    source=self.source_name,
                    layer=self.ASSET_PREFIX,
                    sharable=False
                ).to_dict(),
            deps=self.DEPS_ASSET_NAME and [AssetKey(self.DEPS_ASSET_NAME)],
            kinds=kinds
        )