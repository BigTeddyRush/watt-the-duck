from dagster import PartitionsDefinition, RunRequest

def latest_partition_execution_fn(context,partitions_def: PartitionsDefinition):
    latest_partition = partitions_def.get_last_partition_key()
    return RunRequest(
        run_key=latest_partition,
        partition_key=latest_partition,
    )