from dagster import Definitions, components
from .dbt_projects import dbt_project
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_dbt import DbtCliResource
import orchestrator.defs.energy_charts

resources = {
    "dlt": DagsterDltResource(),
    "dbt": DbtCliResource(project_dir=dbt_project.project_dir),
}

defs = Definitions.merge(
    components.load_defs(orchestrator.defs.energy_charts),
    Definitions(resources=resources)
)