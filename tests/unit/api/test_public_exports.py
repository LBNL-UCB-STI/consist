from consist import (
    ArtifactRecoveryCopyRegistration,
    HydratedRunOutput,
    HydratedRunOutputsResult,
    MaterializationResult,
    RunOutputRecoveryCopiesRegistration,
    StagedInput,
    StagedInputsResult,
    register_artifact_recovery_copy,
    register_run_output_recovery_copies,
    stage_artifact,
    stage_inputs,
)
from consist.api import (
    register_artifact_recovery_copy as register_artifact_recovery_copy_api,
)
from consist.api import (
    register_run_output_recovery_copies as register_run_output_recovery_copies_api,
)
from consist.api import stage_artifact as stage_artifact_api
from consist.api import stage_inputs as stage_inputs_api
from consist.core.materialize import (
    ArtifactRecoveryCopyRegistration as CoreArtifactRecoveryCopyRegistration,
    HydratedRunOutput as CoreHydratedRunOutput,
    HydratedRunOutputsResult as CoreHydratedRunOutputsResult,
    MaterializationResult as CoreMaterializationResult,
    RunOutputRecoveryCopiesRegistration as CoreRunOutputRecoveryCopiesRegistration,
    StagedInput as CoreStagedInput,
    StagedInputsResult as CoreStagedInputsResult,
)


def test_materialization_result_is_exported_from_top_level() -> None:
    assert MaterializationResult is CoreMaterializationResult


def test_hydrated_run_output_types_are_exported_from_top_level() -> None:
    assert HydratedRunOutput is CoreHydratedRunOutput
    assert HydratedRunOutputsResult is CoreHydratedRunOutputsResult


def test_recovery_copy_types_and_helpers_are_exported_from_top_level() -> None:
    assert ArtifactRecoveryCopyRegistration is CoreArtifactRecoveryCopyRegistration
    assert RunOutputRecoveryCopiesRegistration is CoreRunOutputRecoveryCopiesRegistration
    assert register_artifact_recovery_copy is register_artifact_recovery_copy_api
    assert register_run_output_recovery_copies is register_run_output_recovery_copies_api


def test_staged_input_types_and_helpers_are_exported_from_top_level() -> None:
    assert StagedInput is CoreStagedInput
    assert StagedInputsResult is CoreStagedInputsResult
    assert stage_artifact is stage_artifact_api
    assert stage_inputs is stage_inputs_api
