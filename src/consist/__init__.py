"""
Consist: Automatic provenance tracking and intelligent caching for scientific simulation workflows.

This package provides the main public API for Consist, allowing users to interact with
the Tracker for managing runs, logging artifacts, and ingesting data.
"""

from importlib.metadata import PackageNotFoundError, version as package_version


try:
    __version__ = package_version("consist")
except PackageNotFoundError:
    __version__ = "unknown"

# Models
from consist.models.run import (
    CouplerSnapshotPhase,
    Run,
    RunCouplerSnapshot,
    RunResult,
)
from consist.models.artifact import Artifact, ArchivedOutputs
from consist.models.gtfs import (
    GTFS_SCHEMA_BY_TABLE_NAME,
    GTFS_SCHEMAS,
    GTFS_TABLE_NAMES,
    GtfsAgency,
    GtfsCalendar,
    GtfsCalendarDates,
    GtfsFareAttributes,
    GtfsFareRules,
    GtfsFeedInfo,
    GtfsFrequencies,
    GtfsRoutes,
    GtfsSchema,
    GtfsShapes,
    GtfsStopTimes,
    GtfsStops,
    GtfsTransfers,
    GtfsTrips,
)
from consist.core.gtfs import (
    GtfsCanonicalizationResult,
    GtfsFeedSnapshot,
    canonicalize_gtfs_bundle,
    discover_gtfs_members,
)
from consist.runset import AlignedPair, RunSet

# Core
from consist.core.tracker import Tracker
from consist.core.coupler import Coupler
from consist.core.config_canonicalization import (
    CanonicalizationArtifactMember,
    CanonicalizationReference,
    CanonicalizationSnapshot,
)
from consist.core.indexing import (
    FacetIndex,
    RunFieldIndex,
    index_by_facet,
    index_by_field,
)
from consist.core.resolved_binding import (
    AdmissionEvidence,
    ArtifactIdentity,
    BoundArtifact,
    ResolvedBinding,
    ResolvedBindingBuilder,
    TrackedArtifactLocator,
)
from consist.core.workflow import StepIdentity

# API
from consist.api import (
    ARTIFACT_PROVENANCE_SCHEMA_VERSION,  # noqa: F401
    FLOAT32_MATRICES_V1_POLICY,  # noqa: F401
    PRESERVE_CANONICAL_POLICY,  # noqa: F401
    AdmissionIdentity,  # noqa: F401
    AdmissionReference,  # noqa: F401
    AdmissionReport,  # noqa: F401
    ArrayCanonicalizationPolicy,  # noqa: F401
    ArtifactProvenanceSidecar,  # noqa: F401
    DeclaredDigestExpectation,  # noqa: F401
    FileIdentity,  # noqa: F401
    MatrixContainerSummary,  # noqa: F401
    PriorRunExpectation,  # noqa: F401
    RunContext,
    admission_directory_identity,  # noqa: F401
    admission_file_identity,  # noqa: F401
    artifact_provenance_sidecar_path,  # noqa: F401
    canonicalize_float32_array,  # noqa: F401
    canonicalize_float64_array,  # noqa: F401
    canonicalize_float_array,  # noqa: F401
    check_admission_reference,  # noqa: F401
    check_admission_reference_expected_identity,  # noqa: F401
    check_admission_reference_prior_run_expectation,  # noqa: F401
    check_artifact_identity,  # noqa: F401
    check_expected_identity,  # noqa: F401
    check_prior_run_expectation,  # noqa: F401
    compare_path_to_identity,  # noqa: F401
    compute_array_entry_sha256,  # noqa: F401
    compute_logical_matrices_sha256,  # noqa: F401
    compute_omx_logical_matrices_sha256,  # noqa: F401
    compute_zarr_logical_matrices_sha256,  # noqa: F401
    inspect_matrix_container,  # noqa: F401
    normalize_logical_matrices_identity,  # noqa: F401
    read_artifact_provenance_sidecar,  # noqa: F401
    resolve_canonicalization_policy,  # noqa: F401
    write_artifact_provenance_sidecar,  # noqa: F401
    load,
    load_df,
    load_relation,
    active_relation_count,
    RelationConnectionLeakWarning,
    to_df,
    run,
    ref,
    refs,
    trace,
    start_run,
    log_artifact,
    log_input,
    log_output,
    log_artifacts,
    log_dataframe,
    define_step,
    require_runtime_kwargs,
    ingest,
    canonicalize_gtfs,
    capture_outputs,
    use_tracker,
    set_current_tracker,
    current_tracker,
    current_run,
    current_consist,
    output_dir,
    output_path,
    log_meta,
    view,
    ibis_connection,
    ibis_grouped_view,
    ibis_view,
    cached_output,
    cached_artifacts,
    get_run_result,
    get_coupler_snapshot,
    diff_coupler_snapshots,
    hydrate_run_outputs,
    hydrate_run_outputs_to_destinations,
    materialize_run_outputs,
    materialize_artifact,
    stage_artifact,
    stage_inputs,
    set_artifact_recovery_roots,
    register_artifact_recovery_copy,
    register_run_output_recovery_copies,
    archive_run_output_files,
    archive_artifact,
    archive_run_outputs,
    archive_current_run_outputs,
    get_artifact,
    register_artifact_facet_parser,
    scenario,
    noop_scenario,
    single_step_scenario,
    register_views,
    find_run,
    find_runs,
    find_latest_run,
    find_matching_run,
    find_matching_runs,
    run_set,
    db_session,
    run_query,
    config_run_query,
    config_run_rows,
    pivot_facets,
    # Type guards for artifact narrowing
    is_dataframe_artifact,
    is_tabular_artifact,
    is_json_artifact,
    is_zarr_artifact,
    is_hdf_artifact,
    is_spatial_artifact,
)

# Types
from consist.types import (
    ArtifactSpec,
    BindingResult,
    BuiltinSchemaLiteral,
    CacheOptions,
    EnumCapture,
    DriverType,
    ExecutionOptions,
    FilenamePattern,
    IntCapture,
    H5ChildSpec,
    OutputArtifactSpec,
    OutputPolicyOptions,
    OutputPathRef,
    OutputSet,
)
from consist.core.noop import (
    NoopArtifact,
    NoopCoupler,
    NoopRunResult,
    NoopScenarioContext,
    NoopTracker,
)
from consist.core.materialize import (
    ArchivedRunOutputFile,
    ArchivedRunOutputFilesReport,
    ArtifactRecoveryCopyRegistration,
    HydratedRunOutput,
    HydratedRunOutputsResult,
    MaterializedArtifact,
    MaterializationResult,
    RunOutputRecoveryCopiesRegistration,
    StagedInput,
    StagedInputsResult,
)
from consist.runtime import create_tracker
from consist.protocols import (
    ArtifactLike,
    RunIdentifiedResultLike,
    RunResultLike,
    ScenarioLike,
    TrackerLike,
)

# Orchestration
from consist.core.orchestration import (
    BatchResult,
    BatchRunSpec,
    ExecutionAttempt,
    ExecutionSpec,
    ExecutionTarget,
    PythonCallableTarget,
    RunSpecResult,
    execute_worker_run,
    resolve_callable,
)
from consist.core.step_contracts import (
    StepContract,
    collect_step_contracts,
    resolve_step_contract,
)

__all__ = [
    "__version__",
    # Core objects
    "Tracker",
    "Coupler",
    "RunContext",
    "AdmissionIdentity",
    "AdmissionReference",
    "AdmissionReport",
    "ArrayCanonicalizationPolicy",
    "ArtifactProvenanceSidecar",
    "MatrixContainerSummary",
    "ARTIFACT_PROVENANCE_SCHEMA_VERSION",
    "PRESERVE_CANONICAL_POLICY",
    "FLOAT32_MATRICES_V1_POLICY",
    "DeclaredDigestExpectation",
    "FileIdentity",
    "PriorRunExpectation",
    "AdmissionEvidence",
    "ArtifactIdentity",
    "BoundArtifact",
    "ResolvedBinding",
    "ResolvedBindingBuilder",
    "TrackedArtifactLocator",
    "StepIdentity",
    "CanonicalizationArtifactMember",
    "CanonicalizationReference",
    "CanonicalizationSnapshot",
    "Run",
    "RunResult",
    "CouplerSnapshotPhase",
    "RunCouplerSnapshot",
    "Artifact",
    "ArchivedOutputs",
    "RunSet",
    "AlignedPair",
    "GtfsSchema",
    "GtfsAgency",
    "GtfsStops",
    "GtfsRoutes",
    "GtfsCalendar",
    "GtfsCalendarDates",
    "GtfsFeedInfo",
    "GtfsShapes",
    "GtfsTrips",
    "GtfsStopTimes",
    "GtfsFrequencies",
    "GtfsTransfers",
    "GtfsFareAttributes",
    "GtfsFareRules",
    "GTFS_SCHEMAS",
    "GTFS_SCHEMA_BY_TABLE_NAME",
    "GTFS_TABLE_NAMES",
    "GtfsCanonicalizationResult",
    "GtfsFeedSnapshot",
    "canonicalize_gtfs_bundle",
    "canonicalize_gtfs",
    "discover_gtfs_members",
    # Types
    "BindingResult",
    "ArtifactSpec",
    "OutputArtifactSpec",
    "OutputPathRef",
    "BuiltinSchemaLiteral",
    "IntCapture",
    "EnumCapture",
    "FilenamePattern",
    "DriverType",
    "CacheOptions",
    "OutputPolicyOptions",
    "OutputSet",
    "ExecutionOptions",
    "H5ChildSpec",
    "NoopArtifact",
    "NoopCoupler",
    "NoopRunResult",
    "NoopScenarioContext",
    "NoopTracker",
    "HydratedRunOutput",
    "HydratedRunOutputsResult",
    "MaterializedArtifact",
    "ArtifactRecoveryCopyRegistration",
    "RunOutputRecoveryCopiesRegistration",
    "ArchivedRunOutputFile",
    "ArchivedRunOutputFilesReport",
    "MaterializationResult",
    "StagedInput",
    "StagedInputsResult",
    "create_tracker",
    "ArtifactLike",
    "RunIdentifiedResultLike",
    "RunResultLike",
    "ScenarioLike",
    "TrackerLike",
    # Indexing helpers
    "FacetIndex",
    "RunFieldIndex",
    "index_by_facet",
    "index_by_field",
    # Functional helpers
    "load",
    "load_df",
    "load_relation",
    "active_relation_count",
    "RelationConnectionLeakWarning",
    "to_df",
    "run",
    "admission_directory_identity",
    "admission_file_identity",
    "artifact_provenance_sidecar_path",
    "canonicalize_float32_array",
    "canonicalize_float64_array",
    "canonicalize_float_array",
    "check_admission_reference",
    "check_admission_reference_expected_identity",
    "check_admission_reference_prior_run_expectation",
    "check_artifact_identity",
    "check_expected_identity",
    "check_prior_run_expectation",
    "compare_path_to_identity",
    "compute_array_entry_sha256",
    "compute_logical_matrices_sha256",
    "compute_omx_logical_matrices_sha256",
    "compute_zarr_logical_matrices_sha256",
    "inspect_matrix_container",
    "normalize_logical_matrices_identity",
    "read_artifact_provenance_sidecar",
    "resolve_canonicalization_policy",
    "write_artifact_provenance_sidecar",
    "ref",
    "refs",
    "trace",
    "start_run",
    "log_artifact",
    "log_input",
    "log_output",
    "log_artifacts",
    "log_dataframe",
    "define_step",
    "require_runtime_kwargs",
    "ingest",
    "capture_outputs",
    "use_tracker",
    "set_current_tracker",
    "current_tracker",
    "current_run",
    "current_consist",
    "output_dir",
    "output_path",
    "log_meta",
    "view",
    "ibis_connection",
    "ibis_grouped_view",
    "ibis_view",
    "cached_output",
    "cached_artifacts",
    "get_run_result",
    "get_coupler_snapshot",
    "diff_coupler_snapshots",
    "hydrate_run_outputs",
    "hydrate_run_outputs_to_destinations",
    "materialize_run_outputs",
    "materialize_artifact",
    "stage_artifact",
    "stage_inputs",
    "set_artifact_recovery_roots",
    "register_artifact_recovery_copy",
    "register_run_output_recovery_copies",
    "archive_run_output_files",
    "archive_artifact",
    "archive_run_outputs",
    "archive_current_run_outputs",
    "get_artifact",
    "register_artifact_facet_parser",
    "scenario",
    "noop_scenario",
    "single_step_scenario",
    "register_views",
    "find_run",
    "find_runs",
    "find_latest_run",
    "find_matching_run",
    "find_matching_runs",
    "run_set",
    "db_session",
    "run_query",
    "config_run_query",
    "config_run_rows",
    "pivot_facets",
    # Type guards for artifact narrowing
    "is_dataframe_artifact",
    "is_tabular_artifact",
    "is_json_artifact",
    "is_zarr_artifact",
    "is_hdf_artifact",
    "is_spatial_artifact",
    # Orchestration
    "BatchResult",
    "BatchRunSpec",
    "ExecutionAttempt",
    "ExecutionSpec",
    "ExecutionTarget",
    "PythonCallableTarget",
    "RunSpecResult",
    "execute_worker_run",
    "resolve_callable",
    "StepContract",
    "collect_step_contracts",
    "resolve_step_contract",
]
