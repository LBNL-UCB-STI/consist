from consist import HydratedRunOutput, HydratedRunOutputsResult, MaterializationResult
from consist.core.materialize import (
    HydratedRunOutput as CoreHydratedRunOutput,
    HydratedRunOutputsResult as CoreHydratedRunOutputsResult,
    MaterializationResult as CoreMaterializationResult,
)


def test_materialization_result_is_exported_from_top_level() -> None:
    assert MaterializationResult is CoreMaterializationResult


def test_hydrated_run_output_types_are_exported_from_top_level() -> None:
    assert HydratedRunOutput is CoreHydratedRunOutput
    assert HydratedRunOutputsResult is CoreHydratedRunOutputsResult
