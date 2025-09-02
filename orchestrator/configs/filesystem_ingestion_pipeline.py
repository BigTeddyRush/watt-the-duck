import pendulum
from dlt import pipeline
from dlt.pipeline import Pipeline
from dlt.destinations import filesystem

def filesystem_ingestion_pipeline(
    source_name: str,
    resource_name: str,
    layout: str = None,
) -> Pipeline:

    pipeline_name = f"{source_name}_ingestion_pipeline_{resource_name}"

    if layout is None:
        layout = "{table_name}/year={YYYY}/month={MM}/day={DD}/{load_id}.{file_id}.{ext}"

    return pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(
            layout=layout,
            current_datetime=pendulum.now(),
            kwargs={
                "auto_mkdir": True
            }
        ),
        dataset_name=source_name
    )