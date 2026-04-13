from consist import (
    HydratedRunOutput,
    HydratedRunOutputsResult,
    MaterializationResult,
    StagedInput,
    StagedInputsResult,
    stage_artifact,
    stage_inputs,
)
from consist.api import stage_artifact as stage_artifact_api
from consist.api import stage_inputs as stage_inputs_api
from consist.core.materialize import (
    HydratedRunOutput as CoreHydratedRunOutput,
    HydratedRunOutputsResult as CoreHydratedRunOutputsResult,
    MaterializationResult as CoreMaterializationResult,
    StagedInput as CoreStagedInput,
    StagedInputsResult as CoreStagedInputsResult,
)


def test_materialization_result_is_exported_from_top_level() -> None:
    assert MaterializationResult is CoreMaterializationResult


def test_hydrated_run_output_types_are_exported_from_top_level() -> None:
    assert HydratedRunOutput is CoreHydratedRunOutput
    assert HydratedRunOutputsResult is CoreHydratedRunOutputsResult


def test_staged_input_types_and_helpers_are_exported_from_top_level() -> None:
    assert StagedInput is CoreStagedInput
    assert StagedInputsResult is CoreStagedInputsResult
    assert stage_artifact is stage_artifact_api
    assert stage_inputs is stage_inputs_api
