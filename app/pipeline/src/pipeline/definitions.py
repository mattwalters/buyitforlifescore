from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder, multiprocess_executor


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return Definitions.merge(
        loaded,
        Definitions(executor=multiprocess_executor.configured({"max_concurrent": 8})),
    )
