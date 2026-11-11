from __future__ import annotations

from types import SimpleNamespace
import warnings

import pytest

import consist
from consist.artifact import Artifact
from consist.models.run import Run
from consist.protocols import TrackerLike as ProtocolTrackerLike
from consist.api import (
    archive_run_output_files as archive_run_output_files_api,
    materialize_artifact as materialize_artifact_api,
    register_artifact_recovery_copy as register_artifact_recovery_copy_api,
    register_run_output_recovery_copies as register_run_output_recovery_copies_api,
    stage_artifact as stage_artifact_api,
    stage_inputs as stage_inputs_api,
)
from consist.core.materialize import (
    ArchivedRunOutputFile as CoreArchivedRunOutputFile,
    ArchivedRunOutputFilesReport as CoreArchivedRunOutputFilesReport,
    ArtifactRecoveryCopyRegistration as CoreArtifactRecoveryCopyRegistration,
    HydratedRunOutput as CoreHydratedRunOutput,
    HydratedRunOutputsResult as CoreHydratedRunOutputsResult,
    MaterializationResult as CoreMaterializationResult,
    MaterializedArtifact as CoreMaterializedArtifact,
    RunOutputRecoveryCopiesRegistration as CoreRunOutputRecoveryCopiesRegistration,
    StagedInput as CoreStagedInput,
    StagedInputsResult as CoreStagedInputsResult,
)

CANONICAL_ALL_58 = (
    "Tracker",
    "Artifact",
    "Run",
    "RunResult",
    "RunArtifacts",
    "ScenarioContext",
    "Coupler",
    "OutputSet",
    "OutputArtifactSpec",
    "ArchivedOutputs",
    "RunCompletionArchivePolicy",
    "ExecutionOptions",
    "OutputPolicyOptions",
    "CacheOptions",
    "IdentityInputs",
    "Mount",
    "SiteContext",
    "ParameterizedSQL",
    "run",
    "trace",
    "start_run",
    "scenario",
    "define_step",
    "ref",
    "refs",
    "output_path",
    "output_dir",
    "exec",
    "use_tracker",
    "load",
    "load_df",
    "ingest",
    "log_artifact",
    "log_artifacts",
    "log_input",
    "log_output",
    "log_dataframe",
    "log_meta",
    "observe",
    "locate",
    "stage",
    "archive",
    "evict",
    "archive_artifact",
    "archive_run_outputs",
    "archive_current_run_outputs",
    "query_df",
    "query_rows",
    "find_runs",
    "find_run",
    "run_set",
    "RunSet",
    "pivot_facets",
    "config_run_query",
    "config_run_rows",
    "ConsistError",
    "DatabaseMigrationRequired",
    "IncompatibleSchemaError",
)


def _deprecated_top_level(name: str):
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always", DeprecationWarning)
        value = consist.__getattr__(name)
    assert any(issubclass(w.category, DeprecationWarning) for w in recorded)
    return value


def test_canonical_58_public_exports() -> None:
    assert consist.__all__ == CANONICAL_ALL_58
    assert len(consist.__all__) == 58
    assert consist.Artifact is Artifact
    assert consist.Run is Run
    for name in consist.__all__:
        resolved = (
            consist.__dict__[name]
            if name in consist.__dict__
            else consist.__getattr__(name)
        )
        assert resolved is not None


def test_materialization_result_is_forwarded_with_deprecation_from_top_level() -> None:
    assert _deprecated_top_level("MaterializationResult") is CoreMaterializationResult
    assert _deprecated_top_level("MaterializedArtifact") is CoreMaterializedArtifact
    assert _deprecated_top_level("materialize_artifact") is materialize_artifact_api


def test_materialize_artifact_delegates_to_tracker() -> None:
    artifact = object()
    sentinel = object()

    def _materialize_artifact(received_artifact, **kwargs):
        assert received_artifact is artifact
        assert kwargs["target_root"] == "workspace"
        return sentinel

    tracker = SimpleNamespace(materialize_artifact=_materialize_artifact)

    assert (
        materialize_artifact_api(artifact, target_root="workspace", tracker=tracker)
        is sentinel
    )


def test_hydrated_run_output_types_are_forwarded_from_top_level() -> None:
    assert _deprecated_top_level("HydratedRunOutput") is CoreHydratedRunOutput
    assert (
        _deprecated_top_level("HydratedRunOutputsResult")
        is CoreHydratedRunOutputsResult
    )


def test_recovery_copy_types_and_helpers_are_forwarded_from_top_level() -> None:
    assert (
        _deprecated_top_level("ArtifactRecoveryCopyRegistration")
        is CoreArtifactRecoveryCopyRegistration
    )
    assert (
        _deprecated_top_level("RunOutputRecoveryCopiesRegistration")
        is CoreRunOutputRecoveryCopiesRegistration
    )
    assert (
        _deprecated_top_level("register_artifact_recovery_copy")
        is register_artifact_recovery_copy_api
    )
    assert (
        _deprecated_top_level("register_run_output_recovery_copies")
        is register_run_output_recovery_copies_api
    )
    assert _deprecated_top_level("ArchivedRunOutputFile") is CoreArchivedRunOutputFile
    assert (
        _deprecated_top_level("ArchivedRunOutputFilesReport")
        is CoreArchivedRunOutputFilesReport
    )
    assert (
        _deprecated_top_level("archive_run_output_files")
        is archive_run_output_files_api
    )
    for absent in (
        "ArchiveRunOutputFileStatus",
        "ArchiveRunOutputVerificationStatus",
        "db_session",
        "run_query",
        "manifest",
    ):
        with pytest.raises(AttributeError):
            consist.__getattr__(absent)


def test_staged_input_types_and_helpers_are_forwarded_from_top_level() -> None:
    assert _deprecated_top_level("StagedInput") is CoreStagedInput
    assert _deprecated_top_level("StagedInputsResult") is CoreStagedInputsResult
    assert _deprecated_top_level("stage_artifact") is stage_artifact_api
    assert _deprecated_top_level("stage_inputs") is stage_inputs_api
    assert _deprecated_top_level("TrackerLike") is ProtocolTrackerLike
