import consist
from types import SimpleNamespace

from consist import (
    ArtifactRecoveryCopyRegistration,
    ArchivedRunOutputFile,
    ArchivedRunOutputFilesReport,
    HydratedRunOutput,
    HydratedRunOutputsResult,
    MaterializedArtifact,
    MaterializationResult,
    RunOutputRecoveryCopiesRegistration,
    StagedInput,
    StagedInputsResult,
    materialize_artifact,
    register_artifact_recovery_copy,
    register_run_output_recovery_copies,
    archive_run_output_files,
    stage_artifact,
    stage_inputs,
    TrackerLike,
)
from consist.protocols import TrackerLike as ProtocolTrackerLike
from consist.api import (
    archive_run_output_files as archive_run_output_files_api,
)
from consist.api import (
    materialize_artifact as materialize_artifact_api,
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
    ArchivedRunOutputFile as CoreArchivedRunOutputFile,
    ArchivedRunOutputFilesReport as CoreArchivedRunOutputFilesReport,
    ArtifactRecoveryCopyRegistration as CoreArtifactRecoveryCopyRegistration,
    HydratedRunOutput as CoreHydratedRunOutput,
    HydratedRunOutputsResult as CoreHydratedRunOutputsResult,
    MaterializedArtifact as CoreMaterializedArtifact,
    MaterializationResult as CoreMaterializationResult,
    RunOutputRecoveryCopiesRegistration as CoreRunOutputRecoveryCopiesRegistration,
    StagedInput as CoreStagedInput,
    StagedInputsResult as CoreStagedInputsResult,
)


def test_materialization_result_is_exported_from_top_level() -> None:
    assert MaterializationResult is CoreMaterializationResult
    assert MaterializedArtifact is CoreMaterializedArtifact
    assert materialize_artifact is materialize_artifact_api


def test_materialize_artifact_top_level_delegates_to_tracker() -> None:
    artifact = object()
    sentinel = object()

    def _materialize_artifact(received_artifact, **kwargs):
        assert received_artifact is artifact
        assert kwargs["target_root"] == "workspace"
        return sentinel

    tracker = SimpleNamespace(materialize_artifact=_materialize_artifact)

    assert (
        materialize_artifact(artifact, target_root="workspace", tracker=tracker)
        is sentinel
    )


def test_hydrated_run_output_types_are_exported_from_top_level() -> None:
    assert HydratedRunOutput is CoreHydratedRunOutput
    assert HydratedRunOutputsResult is CoreHydratedRunOutputsResult


def test_recovery_copy_types_and_helpers_are_exported_from_top_level() -> None:
    assert ArtifactRecoveryCopyRegistration is CoreArtifactRecoveryCopyRegistration
    assert (
        RunOutputRecoveryCopiesRegistration is CoreRunOutputRecoveryCopiesRegistration
    )
    assert register_artifact_recovery_copy is register_artifact_recovery_copy_api
    assert (
        register_run_output_recovery_copies is register_run_output_recovery_copies_api
    )
    assert ArchivedRunOutputFile is CoreArchivedRunOutputFile
    assert ArchivedRunOutputFilesReport is CoreArchivedRunOutputFilesReport
    assert archive_run_output_files is archive_run_output_files_api
    assert not hasattr(consist, "ArchiveRunOutputFileStatus")
    assert not hasattr(consist, "ArchiveRunOutputVerificationStatus")


def test_staged_input_types_and_helpers_are_exported_from_top_level() -> None:
    assert StagedInput is CoreStagedInput
    assert StagedInputsResult is CoreStagedInputsResult
    assert stage_artifact is stage_artifact_api
    assert stage_inputs is stage_inputs_api


def test_public_package_exports_are_all_present() -> None:
    missing = [name for name in consist.__all__ if not hasattr(consist, name)]
    assert missing == []
    assert TrackerLike is ProtocolTrackerLike
