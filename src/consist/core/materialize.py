"""Historical output materialization and hydration helpers.

This module exposes two related recovery layers for prior run outputs:

- ``materialize_run_outputs(...)`` performs the physical copy/export work and
  returns the legacy aggregate ``MaterializationResult`` summary.
- ``hydrate_run_outputs(...)`` performs the same recovery work but returns a
  key-indexed ``HydratedRunOutputsResult`` with detached artifacts, resolved
  paths, and per-key statuses that are immediately usable in a new workspace.

The keyed hydration result is the higher-level API. The aggregate
``MaterializationResult`` remains the compatibility wrapper for callers that
only need summary buckets.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Mapping as MappingABC
from contextlib import suppress
from dataclasses import dataclass, field
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import (
    Any,
    Iterable,
    Literal,
    Mapping,
    Sequence,
    TYPE_CHECKING,
    Callable,
    cast,
    get_args,
)
from typing_extensions import Protocol, runtime_checkable

from consist.core.container_policy import validate_recovery_registration_policy
from consist.core.directory_artifacts import (
    build_directory_manifest,
    build_shapefile_bundle_manifest,
    directory_allows_extra_files,
    file_bundle_entry_name,
    is_directory_artifact as _is_directory_artifact,
    is_file_bundle_artifact as _is_file_bundle_artifact,
    is_valid_directory_artifact_tree,
    is_valid_shapefile_bundle_source,
    materialize_directory_tree,
    materialize_shapefile_bundle,
    require_directory_artifact_manifest as _directory_artifact_manifest,
    require_shapefile_bundle_metadata,
    validate_directory_destination,
    validate_shapefile_bundle_destination,
    validate_shapefile_bundle_root,
)
from consist.core.persistence import DatabaseManager
from consist.core.stores import get_hot_data_db
from consist.core.views import _quote_ident, _validate_identifier
from consist.digest import (
    DIGEST_DRIVERS,
    Digest,
    compute_digest,
    file_sha256,
    verify_digest,
)
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

HydrationStatus = Literal[
    "materialized_from_filesystem",
    "materialized_directory_from_filesystem",
    "materialized_file_bundle_from_filesystem",
    "materialized_from_db",
    "preserved_existing",
    "skipped_unmapped",
    "missing_source",
    "failed",
]

MaterializedArtifactStatus = Literal[
    "already_present",
    "preserved_existing",
    "materialized_from_source_root",
    "materialized_from_historical_root",
    "materialized_from_recovery_root",
    "materialized_from_artifact_uri",
    "missing_source",
    "skipped_unmapped",
    "hash_mismatch",
    "blocked_by_container_policy",
    "unsupported_directory",
    "symlink_destination",
    "failed",
]

StagingStatus = Literal[
    "staged",
    "preserved_existing",
    "missing_source",
    "failed",
]

RecoveryCopyStatus = Literal[
    "registered",
    "missing_copy",
    "hash_mismatch",
    "skipped_unmapped",
    "blocked_by_container_policy",
    "symlink_destination",
    "unsupported_directory",
    "unverifiable_hash",
    "failed",
]

ArchiveRunOutputFileStatus = Literal[
    "copied",
    "preserved_existing",
    "destination_exists",
    "missing_source",
    "skipped_unmapped",
    "unsupported_directory",
    "symlink_source",
    "symlink_destination",
    "unverifiable_hash",
    "hash_mismatch",
    "failed",
]

ArchiveRunOutputVerificationStatus = Literal[
    "verified",
    "not_requested",
    "unverifiable_hash",
    "hash_mismatch",
    "failed",
]


@dataclass(slots=True)
class MaterializationResult:
    """Aggregate summary for historical run-output materialization.

    This is the compatibility result returned by
    ``Tracker.materialize_run_outputs(...)`` and the top-level
    ``consist.materialize_run_outputs(...)`` helper. It groups outcomes into
    summary buckets rather than returning one object per requested key.

    For new recovery flows, prefer ``HydratedRunOutputsResult`` because it
    preserves per-key status, detached artifacts, and directly usable paths.

    Attributes
    ----------
    materialized_from_filesystem : dict[str, str]
        Keys restored by copying bytes from historical filesystem locations.
        Values are the destination paths.
    materialized_from_db : dict[str, str]
        Keys reconstructed from DuckDB for ingested CSV/Parquet artifacts.
        Values are the destination paths.
    skipped_existing : list[str]
        Keys whose destination already existed and was preserved.
    skipped_unmapped : list[str]
        Keys that could not be mapped back to a historical relative path.
    skipped_missing_source : list[str]
        Keys whose historical bytes were unavailable and could not be recovered.
    failed : list[tuple[str, str]]
        Keys that failed during recovery, paired with the failure message.
    """

    materialized_from_filesystem: dict[str, str] = field(default_factory=dict)
    materialized_from_db: dict[str, str] = field(default_factory=dict)
    skipped_existing: list[str] = field(default_factory=list)
    skipped_unmapped: list[str] = field(default_factory=list)
    skipped_missing_source: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def materialized(self) -> dict[str, str]:
        """Return all successfully materialized key-to-path entries."""
        return {
            **self.materialized_from_filesystem,
            **self.materialized_from_db,
        }

    @property
    def has_failures(self) -> bool:
        """Return whether any output encountered a materialization failure."""
        return bool(self.failed)

    @property
    def complete(self) -> bool:
        """Return whether every requested output was materialized or retained."""
        return not (self.skipped_unmapped or self.skipped_missing_source or self.failed)

    @property
    def summary(self) -> str:
        """Return deterministic aggregate materialization counts."""
        return (
            f"materialized_fs={len(self.materialized_from_filesystem)} "
            f"materialized_db={len(self.materialized_from_db)} "
            f"skipped_existing={len(self.skipped_existing)} "
            f"skipped_unmapped={len(self.skipped_unmapped)} "
            f"skipped_missing_source={len(self.skipped_missing_source)} "
            f"failed={len(self.failed)}"
        )


@dataclass(frozen=True, slots=True)
class HydratedRunOutput:
    """Per-key outcome for ``hydrate_run_outputs(...)``.

    Attributes
    ----------
    key : str
        Output key requested by the caller.
    artifact : Artifact
        Detached copy of the historical artifact record. When ``resolvable`` is
        ``True``, the detached artifact's runtime ``abs_path`` points at the
        hydrated destination so helpers like ``artifact.as_path()`` can be used
        directly in the new workspace. The detached artifact preserves the
        canonical fingerprint surface on ``artifact.hash``.
    path : Path | None
        Destination path under the requested target root, when one is known.
        This can be populated even for non-resolvable outcomes to show the
        intended restore location.
    status : HydrationStatus
        One of:

        - ``"materialized_from_filesystem"``: copied from historical cold bytes.
        - ``"materialized_from_db"``: exported from DuckDB for an ingested
          CSV/Parquet artifact.
        - ``"preserved_existing"``: destination already existed and was reused.
        - ``"skipped_unmapped"``: no safe historical relative-path mapping was
          available.
        - ``"missing_source"``: historical bytes were unavailable and no DB
          fallback applied.
        - ``"failed"``: recovery attempted but failed due to a collision,
          policy check, or copy/export error.
    message : str | None
        Optional warning or error detail for ``"missing_source"`` and
        ``"failed"`` outcomes.
    resolvable : bool
        Whether the returned detached artifact is immediately usable from the
        hydrated workspace. When ``True``, ``path`` exists or is treated as the
        preserved reusable destination.
    """

    key: str
    artifact: Artifact
    path: Path | None
    status: HydrationStatus
    artifact_kind: Literal["file", "directory", "file_bundle"] = "file"
    entry_path: Path | None = None
    message: str | None = None
    resolvable: bool = False


@dataclass(frozen=True, slots=True)
class MaterializedArtifact:
    """Outcome for materializing one artifact into a target root.

    Attributes
    ----------
    artifact : Artifact
        Detached artifact associated with the result.
    key : str
        Artifact key.
    path : Path | None
        Usable materialized or preserved destination, when available.
    source_path : Path | None
        Filesystem source used for materialization.
    target_path : Path | None
        Intended destination even when materialization did not succeed.
    status : MaterializedArtifactStatus
        Per-artifact materialization outcome.
    message : str | None
        Optional diagnostic for a non-happy-path outcome.
    resolvable : bool
        Whether the detached artifact can be used at ``path`` immediately.
    """

    artifact: Artifact
    key: str
    path: Path | None
    source_path: Path | None
    target_path: Path | None
    status: MaterializedArtifactStatus
    message: str | None = None
    resolvable: bool = False


@dataclass(slots=True)
class HydratedRunOutputsResult(MappingABC[str, HydratedRunOutput]):
    """Key-indexed recovery result for historical run-output hydration.

    This mapping is returned by ``hydrate_run_outputs(...)`` and preserves the
    caller's requested key order. Each value carries the detached artifact,
    destination path, and per-key status.

    New users should generally start with this result because it answers the
    three practical questions in one object: which keys were requested, what
    path each key resolved to, and whether the returned artifact is immediately
    usable in the current workspace.

    Examples
    --------
    >>> hydrated = tracker.hydrate_run_outputs(
    ...     "prior_run",
    ...     keys=["persons", "households"],
    ...     target_root="restored",
    ... )
    >>> hydrated["persons"].status
    'materialized_from_filesystem'
    >>> hydrated.paths["persons"].name
    'persons.parquet'
    """

    outputs: dict[str, HydratedRunOutput] = field(default_factory=dict)
    source_run_id: str | None = None

    def __getitem__(self, key: str) -> HydratedRunOutput:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def items(self):
        return self.outputs.items()

    def keys(self):
        return self.outputs.keys()

    def values(self):
        return self.outputs.values()

    @property
    def paths(self) -> dict[str, Path]:
        """Return resolvable output destinations keyed by output key."""
        return {
            key: output.path
            for key, output in self.outputs.items()
            if output.path is not None and output.resolvable
        }

    @property
    def resolvable(self) -> dict[str, HydratedRunOutput]:
        """Return hydration outcomes with immediately usable artifacts."""
        return {
            key: output for key, output in self.outputs.items() if output.resolvable
        }

    @property
    def failed_keys(self) -> list[str]:
        """Return keys whose hydration status is ``"failed"``."""
        return [
            key for key, output in self.outputs.items() if output.status == "failed"
        ]

    @property
    def complete(self) -> bool:
        """Return whether no output is unmapped, missing, or failed."""
        incomplete_statuses = {"skipped_unmapped", "missing_source", "failed"}
        return all(
            output.status not in incomplete_statuses for output in self.outputs.values()
        )

    @property
    def summary(self) -> str:
        """Return deterministic per-status hydration counts."""
        counts: dict[str, int] = {
            "materialized_from_filesystem": 0,
            "materialized_directory_from_filesystem": 0,
            "materialized_file_bundle_from_filesystem": 0,
            "materialized_from_db": 0,
            "preserved_existing": 0,
            "skipped_unmapped": 0,
            "missing_source": 0,
            "failed": 0,
        }
        for output in self.outputs.values():
            counts[output.status] += 1
        return (
            f"materialized_fs={counts['materialized_from_filesystem']} "
            f"materialized_directory_fs={counts['materialized_directory_from_filesystem']} "
            f"materialized_file_bundle_fs={counts['materialized_file_bundle_from_filesystem']} "
            f"materialized_db={counts['materialized_from_db']} "
            f"preserved_existing={counts['preserved_existing']} "
            f"skipped_unmapped={counts['skipped_unmapped']} "
            f"missing_source={counts['missing_source']} "
            f"failed={counts['failed']}"
        )


@dataclass(frozen=True, slots=True)
class ArtifactRecoveryCopyRegistration:
    """Outcome for adopting one externally copied artifact recovery location.

    Attributes
    ----------
    artifact : Artifact
        Artifact represented by the recovery copy.
    key : str | None
        Artifact key when available.
    artifact_id : str
        Persistent artifact identifier, or an empty string when unavailable.
    recovery_root : Path
        Root supplied for recovery-copy registration.
    expected_path : Path | None
        URI-relative path expected beneath ``recovery_root``.
    status : RecoveryCopyStatus
        Verification or metadata-persistence outcome.
    message : str | None
        Optional diagnostic for a blocked or failed result.
    metadata_updated : bool
        Whether recovery-root metadata was committed.
    verification_succeeded : bool
        Whether the copy satisfied the requested byte-verification policy.

    Notes
    -----
    ``registered`` means Consist verified the existing copy according to the
    requested policy and persisted the recovery root. Other statuses are
    advisory blockers; identity is unchanged and metadata was not updated.
    """

    artifact: Artifact
    key: str | None
    artifact_id: str
    recovery_root: Path
    expected_path: Path | None
    status: RecoveryCopyStatus
    message: str | None = None
    metadata_updated: bool = False
    verification_succeeded: bool = False


@dataclass(slots=True)
class RunOutputRecoveryCopiesRegistration(
    MappingABC[str, ArtifactRecoveryCopyRegistration]
):
    """Key-indexed result for adopting externally copied run-output bytes.

    Parameters
    ----------
    outputs : dict[str, ArtifactRecoveryCopyRegistration]
        Per-output registration outcomes, keyed by run-output key.
    """

    outputs: dict[str, ArtifactRecoveryCopyRegistration] = field(default_factory=dict)

    def __getitem__(self, key: str) -> ArtifactRecoveryCopyRegistration:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def items(self):
        return self.outputs.items()

    def keys(self):
        return self.outputs.keys()

    def values(self):
        return self.outputs.values()

    @property
    def registered(self) -> dict[str, ArtifactRecoveryCopyRegistration]:
        """Return outputs whose verified recovery roots were committed."""
        return {
            key: output
            for key, output in self.outputs.items()
            if output.status == "registered"
        }

    @property
    def blocked(self) -> dict[str, ArtifactRecoveryCopyRegistration]:
        """Return outputs that were not registered successfully."""
        return {
            key: output
            for key, output in self.outputs.items()
            if output.status != "registered"
        }

    @property
    def complete(self) -> bool:
        """Return whether every selected output was registered successfully."""
        return not self.blocked

    @property
    def summary(self) -> str:
        """Return deterministic status counts for the registration results."""
        counts = Counter(output.status for output in self.outputs.values())
        known_statuses = list(get_args(RecoveryCopyStatus))
        extra_statuses = sorted(set(counts) - set(known_statuses))
        return " ".join(
            f"{status}={counts[status]}" for status in known_statuses + extra_statuses
        )


@dataclass(frozen=True, slots=True)
class ArchivedRunOutputFile:
    """Outcome for archiving one regular run-output file.

    Attributes
    ----------
    artifact : Artifact
        Output artifact selected for archival.
    key : str
        Run-output key.
    source_path : Path | None
        Source path discovered for a copy attempt.
    target_path : Path | None
        Canonical path below the requested recovery root.
    copy_status : ArchiveRunOutputFileStatus
        Copy or existing-target outcome.
    verification_status : ArchiveRunOutputVerificationStatus
        Outcome of the requested verification policy.
    metadata_committed : bool
        Whether recovery-root metadata was persisted for this output.
    message : str | None
        Optional diagnostic for a non-happy-path outcome.
    """

    artifact: Artifact
    key: str
    source_path: Path | None
    target_path: Path | None
    copy_status: ArchiveRunOutputFileStatus
    verification_status: ArchiveRunOutputVerificationStatus
    metadata_committed: bool = False
    message: str | None = None


@dataclass(slots=True)
class ArchivedRunOutputFilesReport(MappingABC[str, ArchivedRunOutputFile]):
    """Key-indexed report for ``Tracker.archive_run_output_files(...)``.

    Parameters
    ----------
    outputs : dict[str, ArchivedRunOutputFile]
        Per-output archival outcomes, keyed by requested output key.

    Notes
    -----
    ``complete`` is a result for this invocation only; it does not represent a
    durable archive-workflow state.
    """

    outputs: dict[str, ArchivedRunOutputFile] = field(default_factory=dict)

    def __getitem__(self, key: str) -> ArchivedRunOutputFile:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def items(self):
        return self.outputs.items()

    def keys(self):
        return self.outputs.keys()

    def values(self):
        return self.outputs.values()

    @property
    def complete(self) -> bool:
        """Return whether every output met policy and committed metadata.

        An empty report is complete by convention.
        """
        return all(
            output.metadata_committed
            and output.verification_status in {"verified", "not_requested"}
            for output in self.outputs.values()
        )

    @property
    def summary(self) -> str:
        """Return deterministic copy, verification, and commit counts."""
        copy_counts = Counter(output.copy_status for output in self.outputs.values())
        verification_counts = Counter(
            output.verification_status for output in self.outputs.values()
        )
        committed = sum(output.metadata_committed for output in self.outputs.values())
        copied = copy_counts["copied"]
        verified = verification_counts["verified"]
        return f"copied={copied} verified={verified} metadata_committed={committed}"


@dataclass(frozen=True, slots=True)
class StagedInput:
    """Per-key outcome for canonical input staging.

    Attributes
    ----------
    key : str
        Input key requested by the caller.
    artifact : Artifact
        Detached copy of the input artifact. When ``resolvable`` is ``True``,
        the detached artifact's runtime ``abs_path`` points at the staged
        destination so helpers like ``artifact.as_path()`` can be used
        directly. The detached artifact preserves the canonical fingerprint
        surface on ``artifact.hash``.
    path : Path | None
        Explicit staging destination, when one was known.
    status : StagingStatus
        One of:

        - ``"staged"``: bytes were copied to the requested destination.
        - ``"preserved_existing"``: destination already existed with matching
          content and was reused.
        - ``"missing_source"``: no readable source bytes were found for the
          canonical artifact.
        - ``"failed"``: staging was attempted but failed due to a collision,
          policy check, or copy error.
    message : str | None
        Optional warning or error detail for ``"missing_source"`` and
        ``"failed"`` outcomes.
    resolvable : bool
        Whether the returned detached artifact is immediately usable from the
        staged destination.
    """

    key: str
    artifact: Artifact
    path: Path | None
    status: StagingStatus
    message: str | None = None
    resolvable: bool = False


@dataclass(slots=True)
class StagedInputsResult(MappingABC[str, StagedInput]):
    """Key-indexed result for canonical input staging.

    This mapping preserves the caller's requested key order and answers the
    practical questions for each requested input: what destination was used,
    whether the staged artifact is immediately usable, and whether staging
    copied bytes or reused an existing local path.
    """

    outputs: dict[str, StagedInput] = field(default_factory=dict)

    def __getitem__(self, key: str) -> StagedInput:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def items(self):
        return self.outputs.items()

    def keys(self):
        return self.outputs.keys()

    def values(self):
        return self.outputs.values()

    @property
    def paths(self) -> dict[str, Path]:
        return {
            key: output.path
            for key, output in self.outputs.items()
            if output.path is not None and output.resolvable
        }

    @property
    def resolvable(self) -> dict[str, StagedInput]:
        return {
            key: output for key, output in self.outputs.items() if output.resolvable
        }

    @property
    def failed_keys(self) -> list[str]:
        return [
            key for key, output in self.outputs.items() if output.status == "failed"
        ]

    @property
    def complete(self) -> bool:
        incomplete_statuses = {"missing_source", "failed"}
        return all(
            output.status not in incomplete_statuses for output in self.outputs.values()
        )

    @property
    def summary(self) -> str:
        counts: dict[str, int] = {
            "staged": 0,
            "preserved_existing": 0,
            "missing_source": 0,
            "failed": 0,
        }
        for output in self.outputs.values():
            counts[output.status] += 1
        return (
            f"staged={counts['staged']} "
            f"preserved_existing={counts['preserved_existing']} "
            f"missing_source={counts['missing_source']} "
            f"failed={counts['failed']}"
        )


@dataclass(frozen=True, slots=True)
class PlannedMaterialization:
    artifact: Artifact
    keys: tuple[str, ...]
    source_kind: Literal["filesystem", "db_export"]
    source_path: Path | None
    destination: Path
    relative_path: Path
    artifacts_by_key: Mapping[str, Artifact] | None = None

    def artifact_for_key(self, key: str) -> Artifact:
        if self.artifacts_by_key is not None and key in self.artifacts_by_key:
            return self.artifacts_by_key[key]
        return self.artifact


@dataclass(frozen=True, slots=True)
class PlannedRunOutputHydration:
    key_order: tuple[str, ...]
    plan: list[PlannedMaterialization]
    preflight_results: HydratedRunOutputsResult


def _detach_artifact_for_hydration(
    artifact: Artifact,
    *,
    hydrated_path: Path | None,
) -> Artifact:
    resolved_abs = str(hydrated_path.resolve()) if hydrated_path is not None else None
    return (
        artifact.model_copy(deep=True).with_resolver(None).with_abs_path(resolved_abs)
    )


def _make_hydrated_output(
    artifact: Artifact,
    *,
    status: HydrationStatus,
    path: Path | None,
    entry_path: Path | None = None,
    message: str | None = None,
    resolvable: bool,
) -> HydratedRunOutput:
    normalized_path = path.resolve() if path is not None else None
    normalized_entry_path = entry_path.resolve() if entry_path is not None else None
    detached = _detach_artifact_for_hydration(
        artifact,
        hydrated_path=(normalized_entry_path or normalized_path)
        if resolvable
        else None,
    )
    return HydratedRunOutput(
        key=artifact.key,
        artifact=detached,
        path=normalized_path,
        status=status,
        artifact_kind=(
            "directory"
            if _is_directory_artifact(artifact)
            else "file_bundle"
            if _is_file_bundle_artifact(artifact)
            else "file"
        ),
        entry_path=normalized_entry_path,
        message=message,
        resolvable=resolvable,
    )


def _shapefile_bundle_metadata(artifact: Artifact) -> tuple[str, dict[str, object]]:
    return require_shapefile_bundle_metadata(artifact, fallback_uri_name=True)


def _is_legacy_zarr_artifact(artifact: Artifact) -> bool:
    """Return whether a Zarr artifact predates immutable-tree manifests."""
    return artifact.driver == "zarr" and not _is_directory_artifact(artifact)


def _is_legacy_shapefile_artifact(artifact: Artifact) -> bool:
    """Return whether a Shapefile artifact predates immutable bundle manifests."""
    return artifact.driver == "shapefile" and not _is_file_bundle_artifact(artifact)


def _make_materialized_artifact(
    artifact: Artifact,
    *,
    status: MaterializedArtifactStatus,
    path: Path | None,
    source_path: Path | None,
    target_path: Path | None,
    message: str | None = None,
    resolvable: bool,
) -> MaterializedArtifact:
    normalized_path = path.resolve() if path is not None else None
    normalized_source = source_path.resolve() if source_path is not None else None
    normalized_target = target_path.resolve() if target_path is not None else None
    detached = _detach_artifact_for_hydration(
        artifact,
        hydrated_path=normalized_path if resolvable else None,
    )
    return MaterializedArtifact(
        artifact=detached,
        key=artifact.key,
        path=normalized_path,
        source_path=normalized_source,
        target_path=normalized_target,
        status=status,
        message=message,
        resolvable=resolvable,
    )


def _make_staged_output(
    artifact: Artifact,
    *,
    status: StagingStatus,
    path: Path | None,
    message: str | None = None,
    resolvable: bool,
) -> StagedInput:
    normalized_path = path.resolve() if path is not None else None
    detached = _detach_artifact_for_hydration(
        artifact,
        hydrated_path=normalized_path if resolvable else None,
    )
    return StagedInput(
        key=artifact.key,
        artifact=detached,
        path=normalized_path,
        status=status,
        message=message,
        resolvable=resolvable,
    )


def _ordered_hydrated_results(
    key_order: Sequence[str],
    outputs: Mapping[str, HydratedRunOutput],
    *,
    source_run_id: str | None = None,
) -> HydratedRunOutputsResult:
    ordered: dict[str, HydratedRunOutput] = {}
    for key in key_order:
        output = outputs.get(key)
        if output is not None:
            ordered[key] = output
    for key, output in outputs.items():
        if key not in ordered:
            ordered[key] = output
    return HydratedRunOutputsResult(
        outputs=ordered,
        source_run_id=source_run_id,
    )


def _ordered_staged_results(
    key_order: Sequence[str],
    outputs: Mapping[str, StagedInput],
) -> StagedInputsResult:
    ordered: dict[str, StagedInput] = {}
    for key in key_order:
        output = outputs.get(key)
        if output is not None:
            ordered[key] = output
    for key, output in outputs.items():
        if key not in ordered:
            ordered[key] = output
    return StagedInputsResult(outputs=ordered)


def fold_hydrated_run_outputs_result(
    result: HydratedRunOutputsResult,
) -> MaterializationResult:
    """Fold keyed hydration outcomes into the legacy summary result.

    Parameters
    ----------
    result : HydratedRunOutputsResult
        Key-indexed hydration outcomes produced by the shared recovery core.

    Returns
    -------
    MaterializationResult
        Aggregate summary that preserves the older materialization API shape.
    """
    folded = MaterializationResult()
    for key, output in result.items():
        if (
            output.status
            in {
                "materialized_from_filesystem",
                "materialized_directory_from_filesystem",
                "materialized_file_bundle_from_filesystem",
            }
            and output.path is not None
        ):
            folded.materialized_from_filesystem[key] = str(output.path)
        elif output.status == "materialized_from_db" and output.path is not None:
            folded.materialized_from_db[key] = str(output.path)
        elif output.status == "preserved_existing":
            folded.skipped_existing.append(key)
        elif output.status == "skipped_unmapped":
            folded.skipped_unmapped.append(key)
        elif output.status == "missing_source":
            folded.skipped_missing_source.append(key)
        elif output.status == "failed":
            folded.failed.append((key, output.message or "unknown failure"))
    return folded


def _ensure_destination_not_symlink(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"Symlink detected in destination path: {path}")


def _compute_path_checksum(path: Path) -> str:
    if path.is_dir():
        digest = hashlib.sha256()
        base = path.resolve()
        for root, dirnames, filenames in os.walk(base):
            dirnames.sort()
            filenames.sort()
            rel_root = Path(root).resolve().relative_to(base).as_posix()
            digest.update(f"dir:{rel_root}".encode("utf-8"))
            for dirname in dirnames:
                rel_dir = Path(root, dirname).resolve().relative_to(base).as_posix()
                digest.update(f"subdir:{rel_dir}".encode("utf-8"))
            for filename in filenames:
                file_path = Path(root, filename).resolve()
                rel_file = file_path.relative_to(base).as_posix()
                digest.update(f"file:{rel_file}".encode("utf-8"))
                digest.update(file_sha256(file_path).encode("utf-8"))
        return digest.hexdigest()
    return file_sha256(path)


def _cleanup_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    with suppress(FileNotFoundError):
        path.unlink()


def _materialize_path(
    *,
    source: Path,
    destination: Path,
    preserve_existing: bool,
) -> tuple[bool, bool]:
    """Return ``(materialized, skipped_existing)`` using atomic staging."""
    _ensure_destination_not_symlink(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if source == destination:
        return True, False

    if destination.exists():
        if destination.is_symlink():
            raise ValueError(f"Symlink detected in destination path: {destination}")
        if preserve_existing:
            if source.is_dir() != destination.is_dir():
                raise ValueError(
                    f"Destination type mismatch for {destination}; refusing to overwrite."
                )
            return False, True

    if source.is_dir():
        if preserve_existing:
            try:
                destination.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                return False, True
            try:
                shutil.copytree(source, destination, dirs_exist_ok=True)
            except Exception:
                shutil.rmtree(destination, ignore_errors=True)
                raise
            return True, False
        tmp_root = Path(
            tempfile.mkdtemp(dir=str(destination.parent), prefix=".consist-dir-")
        )
        tmp_dest = tmp_root / "payload"
        backup_root: Path | None = None
        backup_path: Path | None = None
        try:
            shutil.copytree(source, tmp_dest, dirs_exist_ok=False)
            if destination.exists():
                backup_root = Path(
                    tempfile.mkdtemp(
                        dir=str(destination.parent),
                        prefix=f".consist-backup-{destination.name}-",
                    )
                )
                backup_path = backup_root / "payload"
                destination.rename(backup_path)
            os.replace(tmp_dest, destination)
        except Exception:
            if (
                backup_path is not None
                and backup_path.exists()
                and not destination.exists()
            ):
                os.replace(backup_path, destination)
            raise
        finally:
            _cleanup_path(tmp_root)
            if backup_root is not None:
                _cleanup_path(backup_root)
        return True, False

    if preserve_existing and destination.exists():
        return False, True
    fd, tmp_path = tempfile.mkstemp(dir=str(destination.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        shutil.copy2(source, tmp_path_obj)
        if preserve_existing:
            try:
                os.link(tmp_path_obj, destination)
            except FileExistsError:
                return False, True
        else:
            os.replace(tmp_path_obj, destination)
    finally:
        with suppress(FileNotFoundError):
            tmp_path_obj.unlink()
    return True, False


def build_allowed_materialization_roots(
    *,
    run_dir: Path,
    mounts: Mapping[str, str] | None = None,
    allow_external_paths: bool,
) -> tuple[Path, ...] | None:
    if allow_external_paths:
        return None

    roots: list[Path] = []
    for candidate in [Path(run_dir), *(Path(root) for root in (mounts or {}).values())]:
        resolved = candidate.resolve()
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots)


def _normalize_allowed_roots(
    allowed_base: Path | Sequence[Path] | None,
) -> tuple[Path, ...] | None:
    if allowed_base is None:
        return None
    if isinstance(allowed_base, Path):
        return (allowed_base.resolve(),)

    roots: list[Path] = []
    for root in allowed_base:
        resolved = Path(root).resolve()
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots)


def validate_allowed_materialization_destination(
    destination: Path,
    allowed_base: Path | Sequence[Path] | None,
) -> None:
    allowed_roots = _normalize_allowed_roots(allowed_base)
    if not allowed_roots:
        return

    resolved_destination = destination.resolve()
    for root in allowed_roots:
        try:
            resolved_destination.relative_to(root)
            return
        except ValueError:
            continue

    if len(allowed_roots) == 1:
        allowed_root = allowed_roots[0]
        raise ValueError(
            f"Destination path {resolved_destination} is outside allowed base "
            f"{allowed_root}. Choose a target under the tracker run_dir or a "
            "configured mount root, or set allow_external_paths=True or "
            "CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
        )

    allowed_display = ", ".join(str(root) for root in allowed_roots)
    raise ValueError(
        f"Destination path {resolved_destination} is outside allowed roots: "
        f"{allowed_display}. Choose a target under the tracker run_dir or a "
        "configured mount root, or set allow_external_paths=True or "
        "CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
    )


def _validate_allowed_base(
    destination: Path, allowed_base: Path | Sequence[Path] | None
) -> None:
    validate_allowed_materialization_destination(destination, allowed_base)


def _get_output_artifacts_for_run(tracker: "Tracker", run_id: str) -> list[Artifact]:
    db = tracker.db
    if db is None:
        return []

    return list(db.get_output_artifacts_for_run(run_id))


def _get_artifact_owning_run(
    tracker: "Tracker",
    *,
    selected_run,
    artifact: Artifact,
):
    artifact_run_id = artifact.run_id
    if not artifact_run_id or artifact_run_id == selected_run.id:
        return selected_run
    db = tracker.db
    if db is None:
        return selected_run
    producing_run = db.get_run(str(artifact_run_id))
    return producing_run or selected_run


def _output_set_hydration_kind(tracker: "Tracker", artifact: Artifact) -> str | None:
    """Return the persisted OutputSet role when Slice 1 must reject it."""
    artifact_meta = artifact.meta if isinstance(artifact.meta, MappingABC) else {}
    if artifact.driver == "artifact_set" or artifact_meta.get("artifact_set") is True:
        return "parent"
    if artifact_meta.get("output_set_manifest") is True:
        return "manifest"
    if artifact_meta.get("output_set_member") is True:
        return "member"
    if artifact.parent_artifact_id is None:
        return None

    parent = tracker.get_parent_artifact(artifact)
    parent_meta = (
        parent.meta
        if parent is not None and isinstance(parent.meta, MappingABC)
        else {}
    )
    if parent is not None and (
        parent.driver == "artifact_set" or parent_meta.get("artifact_set") is True
    ):
        return "member"
    return None


def _derive_remappable_relative_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    scheme_prefixes: Mapping[str, str | Path] | None = None,
) -> Path | None:
    """Derive the safe URI-relative layout for an artifact."""
    if not artifact.uri:
        return None

    uri = artifact.uri
    scheme: str | None = None
    if "://" in uri:
        scheme = uri.split("://", 1)[0]

    merged_prefixes: dict[str, str | Path] = (
        dict(scheme_prefixes) if scheme_prefixes is not None else {}
    )

    if scheme and scheme not in ("file", "workspace") and scheme not in merged_prefixes:
        inferred: Path | None = None
        if scheme in tracker.fs.mounts and tracker.run_dir:
            try:
                mount_resolved = Path(tracker.fs.mounts[scheme]).resolve()
                run_resolved = Path(tracker.run_dir).resolve()
                rel = mount_resolved.relative_to(run_resolved)
                if rel != Path("."):
                    inferred = rel
            except (ValueError, RuntimeError):
                pass
        if inferred is None:
            owning_run = _get_artifact_run(tracker, artifact=artifact)
            if owning_run is not None and isinstance(owning_run.meta, MappingABC):
                run_mounts = owning_run.meta.get("mounts")
                phys_run_dir = owning_run.meta.get("_physical_run_dir")
                if (
                    isinstance(run_mounts, MappingABC)
                    and scheme in run_mounts
                    and phys_run_dir
                ):
                    try:
                        mount_resolved = Path(run_mounts[scheme]).resolve()
                        run_resolved = Path(phys_run_dir).resolve()
                        rel = mount_resolved.relative_to(run_resolved)
                        if rel != Path("."):
                            inferred = rel
                    except (ValueError, RuntimeError):
                        pass
        if inferred is not None:
            merged_prefixes[scheme] = inferred

    prefixes_arg = (
        merged_prefixes
        if (scheme_prefixes is not None or bool(merged_prefixes))
        else None
    )

    if prefixes_arg is None:
        return tracker.fs.get_remappable_relative_path(uri)
    return tracker.fs.get_remappable_relative_path(uri, scheme_prefixes=prefixes_arg)


def _derive_historical_root(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    run,
    preserve_raw_paths: bool = False,
) -> Path | None:
    """Derive the historical root that can supply artifact bytes."""
    meta = run.meta if (run is not None and isinstance(run.meta, MappingABC)) else {}
    artifact_meta = artifact.meta if isinstance(artifact.meta, MappingABC) else {}

    return tracker.fs.get_historical_root(
        artifact.uri,
        original_run_dir=meta.get("_physical_run_dir"),
        mounts_snapshot=meta.get("mounts"),
        artifact_mount_root=artifact_meta.get("mount_root"),
        resolve=not preserve_raw_paths,
    )


@runtime_checkable
class _HasArtifactRecoveryRoots(Protocol):
    def artifact_recovery_roots(self, artifact: Any) -> Any: ...


def _artifact_recovery_roots(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    preserve_raw_paths: bool = False,
) -> tuple[Path, ...]:
    """Return ordered recovery roots recorded for an artifact."""
    raw_roots: Any = ()
    if isinstance(tracker, _HasArtifactRecoveryRoots):
        fn = tracker.artifact_recovery_roots
        try:
            raw_roots = fn(artifact, resolve=not preserve_raw_paths)  # type: ignore[call-arg]
        except TypeError:
            raw_roots = fn(artifact)
    return tuple(
        Path(root)
        for root in tracker.fs.normalize_recovery_roots(
            raw_roots, resolve=not preserve_raw_paths
        )
    )


def _ordered_candidate_roots(
    *,
    source_root: Path | None,
    historical_root: Path | None,
    recovery_roots: Sequence[Path],
    preserve_raw_paths: bool = False,
) -> tuple[Path, ...]:
    """Order and deduplicate recovery source roots."""
    ordered: list[Path] = []
    seen: set[str] = set()
    for root in (
        ([source_root] if source_root is not None else [])
        + ([historical_root] if historical_root is not None else [])
        + list(recovery_roots)
    ):
        candidate = root.absolute() if preserve_raw_paths else root.resolve()
        identity = str(candidate)
        if identity in seen:
            continue
        seen.add(identity)
        ordered.append(candidate)
    return tuple(ordered)


def find_existing_recovery_source_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    run,
    source_root: Path | None,
    source_validator: Callable[[Path], bool] | None = None,
    fallback_validator: Callable[[Path], bool] | None = None,
    preserve_raw_paths: bool = False,
) -> tuple[Path | None, Path | None, bool]:
    """Find the first valid filesystem source for historical recovery."""
    relative_path = _derive_remappable_relative_path(tracker, artifact=artifact)
    if relative_path is None:
        return None, None, False

    historical_root = _derive_historical_root(
        tracker,
        artifact=artifact,
        run=run,
        preserve_raw_paths=preserve_raw_paths,
    )
    recovery_roots = _artifact_recovery_roots(
        tracker, artifact=artifact, preserve_raw_paths=preserve_raw_paths
    )
    candidate_roots = _ordered_candidate_roots(
        source_root=source_root,
        historical_root=historical_root,
        recovery_roots=recovery_roots,
        preserve_raw_paths=preserve_raw_paths,
    )
    validators = (
        (source_validator, fallback_validator)
        if fallback_validator is not None
        else (source_validator,)
    )
    for validator in validators:
        for root in candidate_roots:
            candidate = root / relative_path
            validator_path = candidate if preserve_raw_paths else candidate.resolve()
            if candidate.exists() and (validator is None or validator(validator_path)):
                return (
                    relative_path,
                    candidate if preserve_raw_paths else candidate.resolve(),
                    True,
                )

    return relative_path, None, bool(candidate_roots)


def _get_artifact_run(tracker: "Tracker", *, artifact: Artifact):
    if not artifact.run_id:
        return None

    return tracker.get_run(str(artifact.run_id))


def _direct_artifact_source_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    relative_path: Path,
    allow_tracker_uri: bool,
) -> Path | None:
    runtime_abs_path = artifact.abs_path
    if runtime_abs_path:
        candidate = Path(str(runtime_abs_path))
        if candidate.exists():
            return candidate

    if not allow_tracker_uri:
        return None

    uri = artifact.uri
    if uri.startswith("workspace://") or uri.startswith("./"):
        candidate = Path(tracker.run_dir) / relative_path
        if candidate.exists():
            return candidate

    if "://" in uri:
        scheme, raw_rel_path = uri.split("://", 1)
        mounts = tracker.fs.mounts
        if scheme in mounts:
            unprefixed_rel = tracker.fs.normalize_remappable_relative_path(raw_rel_path)
            if unprefixed_rel is not None:
                candidate = Path(mounts[scheme]) / unprefixed_rel
                if candidate.exists():
                    return candidate

    try:
        candidate = Path(tracker.resolve_uri(artifact.uri)).resolve()
    except (OSError, ValueError, AttributeError):
        return None
    if candidate.exists():
        return candidate
    return None


def _verify_scalar_artifact_digest(
    candidate_path: Path, artifact: Artifact, expected_digest: Digest
) -> bool:
    if expected_digest.scheme == "caller_supplied_v1:sha256":
        return False
    if expected_digest.scheme not in DIGEST_DRIVERS:
        return False
    meta = artifact.meta if isinstance(artifact.meta, MappingABC) else {}
    options: dict[str, Any] = {}
    member_paths = meta.get("output_set_member_paths")
    if isinstance(member_paths, MappingABC):
        options["member_paths"] = member_paths
    if meta.get("file_bundle_artifact") is True:
        options["shapefile_bundle"] = True
    return verify_digest(
        candidate_path,
        expected_digest,
        driver=artifact.driver,
        member=artifact.member,
        options=options or None,
    )


def _is_valid_scalar_recovery_candidate(
    tracker: "Tracker", candidate_path: Path, artifact: Artifact
) -> bool:
    expected = _portable_artifact_digest(tracker, artifact)
    if not candidate_path.is_file() or expected is None:
        return True
    if expected.scheme == "caller_supplied_v1:sha256":
        return True
    try:
        return _verify_scalar_artifact_digest(candidate_path, artifact, expected)
    except AttributeError:
        return True
    except (FileNotFoundError, OSError, ValueError):
        return False


@runtime_checkable
class _HasDbAttr(Protocol):
    @property
    def db(self) -> Any: ...


def _select_materialize_artifact_source(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    relative_path: Path,
    source_root: Path | None,
    validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
) -> tuple[
    Path | None,
    Literal[
        "source_root",
        "historical_root",
        "recovery_root",
        "artifact_uri",
    ]
    | None,
    bool,
    bool,
]:
    if (
        source_root is None
        and validate_content_hash != "never"
        and isinstance(tracker, _HasDbAttr)
        and isinstance(tracker.db, DatabaseManager)
    ):
        from consist.core.tracker_recovery import resolve_verified_local_path_detailed

        resolved, saw_mismatch, winning_row = resolve_verified_local_path_detailed(
            tracker,
            artifact,
            allow_recovery_roots=True,
            ignore_runtime_abs_path=bool(artifact.run_id),
            verify_digest=(validate_content_hash == "always"),
        )
        if resolved is not None:
            if winning_row is not None and winning_row.site in ("archive", "advisory"):
                resolved_kind: Literal[
                    "source_root", "historical_root", "recovery_root", "artifact_uri"
                ] = "recovery_root"
            elif artifact.run_id:
                resolved_kind = "historical_root"
            else:
                resolved_kind = "artifact_uri"
            return resolved, resolved_kind, True, False
        if saw_mismatch:
            return None, None, True, True

    candidate_roots: list[
        tuple[
            Literal["source_root", "historical_root", "recovery_root"],
            Path,
        ]
    ] = []
    if source_root is not None:
        candidate_roots.append(("source_root", source_root))

    owning_run = _get_artifact_run(tracker, artifact=artifact)
    if owning_run is not None:
        historical_root = _derive_historical_root(
            tracker,
            artifact=artifact,
            run=owning_run,
        )
        if historical_root is not None:
            candidate_roots.append(("historical_root", historical_root))

    candidate_roots.extend(
        ("recovery_root", root)
        for root in _artifact_recovery_roots(tracker, artifact=artifact)
    )

    unprefixed_rel = (
        tracker.fs.normalize_remappable_relative_path(artifact.uri.split("://", 1)[1])
        if "://" in artifact.uri
        else None
    )
    is_dir_artifact = _is_directory_artifact(artifact)
    is_bundle_artifact = _is_file_bundle_artifact(artifact)

    def _is_valid_manifest_candidate(candidate_path: Path) -> bool:
        if validate_content_hash == "never":
            if is_dir_artifact:
                return candidate_path.is_dir()
            return candidate_path.exists()
        if is_dir_artifact:
            return (
                is_valid_directory_artifact_tree(candidate_path, artifact) is not False
            )
        if is_bundle_artifact:
            return (
                is_valid_shapefile_bundle_source(candidate_path, artifact) is not False
            )
        return _is_valid_scalar_recovery_candidate(tracker, candidate_path, artifact)

    saw_mismatch = False
    seen_roots: set[str] = set()
    for source_kind, root in candidate_roots:
        resolved_root = Path(root).resolve()
        root_key = str(resolved_root)
        if root_key in seen_roots:
            continue
        seen_roots.add(root_key)
        candidate = resolved_root / relative_path
        if candidate.exists():
            if _is_valid_manifest_candidate(candidate):
                return candidate, source_kind, True, False
            if validate_content_hash != "never":
                saw_mismatch = True
        if unprefixed_rel is not None and unprefixed_rel != relative_path:
            fallback_candidate = resolved_root / unprefixed_rel
            if fallback_candidate.exists():
                if _is_valid_manifest_candidate(fallback_candidate):
                    return fallback_candidate, source_kind, True, False
                if validate_content_hash != "never":
                    saw_mismatch = True

    direct_source = _direct_artifact_source_path(
        tracker,
        artifact=artifact,
        relative_path=relative_path,
        allow_tracker_uri=not bool(artifact.run_id),
    )
    if direct_source is not None:
        if _is_valid_manifest_candidate(direct_source):
            return direct_source, "artifact_uri", bool(candidate_roots), False
        if validate_content_hash != "never":
            saw_mismatch = True

    return None, None, bool(candidate_roots), saw_mismatch


def _materialized_status_for_source(
    source_kind: Literal[
        "source_root",
        "historical_root",
        "recovery_root",
        "artifact_uri",
    ],
) -> MaterializedArtifactStatus:
    if source_kind == "source_root":
        return "materialized_from_source_root"
    if source_kind == "historical_root":
        return "materialized_from_historical_root"
    if source_kind == "recovery_root":
        return "materialized_from_recovery_root"
    return "materialized_from_artifact_uri"


def _portable_artifact_digest(tracker: "Tracker", artifact: Artifact) -> Digest | None:
    if tracker.identity.hashing_strategy != "full":
        return None
    digest = artifact.digest
    if digest is None:
        return None
    if digest.scheme in {
        "stat_file_v1:sha256",
        "stat_dir_v1:sha256",
        "output_set_v1:sha256",
    }:
        return None
    return digest


def _reject_symlink_from_source_root(source_path: Path, relative_path: Path) -> None:
    root = source_path
    for _ in relative_path.parts:
        parent = root.parent
        if parent == root:
            break
        root = parent
    current = root
    if current.is_symlink():
        raise ValueError(
            f"directory artifact source contains a symlink component: {current}"
        )
    for part in relative_path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(
                f"directory artifact source contains a symlink component: {current}"
            )


def _path_has_symlink_component(path: Path) -> bool:
    candidate = path if path.is_absolute() else path.absolute()
    current = Path(candidate.anchor)
    for part in candidate.parts[1:]:
        current = current / part
        if current.is_symlink():
            return True
    return False


def materialize_artifact(
    tracker: "Tracker",
    artifact: Artifact,
    *,
    target_root: Path,
    source_root: Path | None,
    allowed_base: Path | Sequence[Path] | None,
    preserve_existing: bool,
    on_missing: Literal["warn", "raise"],
    validate_content_hash: Literal["never", "if-present", "always"],
    scheme_prefixes: Mapping[str, str | Path] | None = None,
) -> MaterializedArtifact:
    """Materialize one artifact into a target root using recovery metadata."""
    if validate_content_hash not in {"never", "if-present", "always"}:
        raise ValueError(
            "validate_content_hash must be one of 'never', 'if-present', or 'always'."
        )

    source_path: Path | None = None
    destination: Path | None = None

    def _fail(
        status: MaterializedArtifactStatus,
        msg: str | None = None,
        *,
        tgt: Path | None = None,
    ) -> MaterializedArtifact:
        return _make_materialized_artifact(
            artifact,
            status=status,
            path=None,
            source_path=source_path,
            target_path=destination if tgt is None else tgt,
            message=msg,
            resolvable=False,
        )

    def _ok(status: MaterializedArtifactStatus) -> MaterializedArtifact:
        return _make_materialized_artifact(
            artifact,
            status=status,
            path=destination,
            source_path=source_path,
            target_path=destination,
            resolvable=True,
        )

    if artifact.driver == "h5_table" or (
        artifact.driver in {"h5", "hdf5"}
        and (artifact.parent_artifact_id is not None or artifact.member is not None)
    ):
        policy_validation = validate_recovery_registration_policy(
            artifact, parent=tracker.get_parent_artifact(artifact)
        )
        if not policy_validation.allowed:
            return _fail("blocked_by_container_policy", policy_validation.message)

    relative_path = _derive_remappable_relative_path(
        tracker, artifact=artifact, scheme_prefixes=scheme_prefixes
    )
    if relative_path is None:
        return _fail(
            "skipped_unmapped",
            f"Artifact {artifact.key!r} does not have a rematerializable URI layout.",
        )

    raw_destination = target_root / relative_path
    if raw_destination.is_symlink():
        msg = f"Symlink detected in destination path: {raw_destination.resolve()}"
        if on_missing == "raise":
            raise ValueError(msg)
        return _fail("symlink_destination", msg, tgt=raw_destination)

    destination = raw_destination.resolve()
    try:
        validate_allowed_materialization_destination(destination, allowed_base)
    except ValueError as exc:
        if on_missing == "raise":
            raise
        return _fail("failed", str(exc))

    source_path, source_kind, _has_candidate_roots, saw_source_mismatch = (
        _select_materialize_artifact_source(
            tracker,
            artifact=artifact,
            relative_path=relative_path,
            source_root=source_root,
            validate_content_hash=validate_content_hash,
        )
    )

    expected_digest = _portable_artifact_digest(tracker, artifact)
    expected_hash = expected_digest.hex if expected_digest is not None else None
    if validate_content_hash == "always" and expected_digest is None:
        return _fail(
            "failed",
            f"Cannot validate artifact {artifact.key!r}: no portable content hash is available.",
        )

    should_validate = expected_digest is not None and validate_content_hash != "never"
    meta = artifact.meta if isinstance(artifact.meta, MappingABC) else {}

    def _check_bundle(
        bundle_root: Path, entry_name: str, fallback_file: Path, label: str
    ) -> str | None:
        manifest = meta.get("file_bundle_manifest") or meta.get("directory_manifest")
        try:
            if manifest is not None:
                validate_shapefile_bundle_root(
                    bundle_root, entry_name, manifest, require_clean_root=False
                )
            elif expected_hash:
                entry_cand = (
                    bundle_root / entry_name
                    if (bundle_root / entry_name).is_file()
                    else fallback_file
                )
                if (
                    build_shapefile_bundle_manifest(entry_cand)["tree_hash"]
                    != expected_hash
                ):
                    return f"{label} hash does not match artifact {artifact.key!r}."
        except (ValueError, OSError) as exc:
            return f"{label} hash does not match artifact {artifact.key!r}: {exc}"
        return None

    if destination.exists() and preserve_existing:
        if _is_file_bundle_artifact(artifact):
            if destination.is_dir():
                return _fail(
                    "failed",
                    f"Destination {destination} exists but is a directory; expected Shapefile entry.",
                )
            if should_validate:
                entry = file_bundle_entry_name(artifact, fallback=destination.name)
                assert entry is not None
                err = _check_bundle(
                    destination.parent, entry, destination, "Existing destination"
                )
                if err is not None:
                    return _fail("hash_mismatch", err)
            return _ok("already_present")

        if _is_directory_artifact(artifact) or destination.is_dir():
            if not destination.is_dir():
                return _fail(
                    "failed",
                    f"Destination {destination} exists but is not a directory.",
                )
            manifest = meta.get("directory_manifest")
            needs_partial = False
            if should_validate:
                if manifest is not None:
                    try:
                        already_complete = validate_directory_destination(
                            destination,
                            manifest,
                            preserve_existing=True,
                            allow_extra_files=directory_allows_extra_files(artifact),
                        )
                    except (ValueError, OSError) as exc:
                        return _fail(
                            "hash_mismatch",
                            f"Existing destination hash does not match artifact {artifact.key!r}: {exc}",
                        )
                    if not already_complete:
                        if source_path is None:
                            return _fail(
                                "hash_mismatch",
                                f"Existing destination for artifact {artifact.key!r} is incomplete and no recovery source is available.",
                            )
                        needs_partial = True
                elif expected_hash:
                    try:
                        if (
                            build_directory_manifest(destination).get("tree_hash")
                            != expected_hash
                        ):
                            return _fail(
                                "hash_mismatch",
                                f"Existing destination hash does not match artifact {artifact.key!r}.",
                            )
                    except (ValueError, OSError) as exc:
                        return _fail(
                            "hash_mismatch",
                            f"Existing destination validation failed for {artifact.key!r}: {exc}",
                        )
            elif (
                isinstance(manifest, Mapping)
                and isinstance(manifest.get("entries"), list)
                and source_path is not None
            ):
                needs_partial = any(
                    isinstance(e, Mapping)
                    and isinstance(e.get("path"), str)
                    and not (destination / e["path"]).exists()
                    for e in manifest["entries"]
                )
            if not needs_partial:
                return _ok("already_present")
        else:
            if destination.is_dir():
                return _fail(
                    "failed", f"Destination {destination} exists but is a directory."
                )
            if should_validate and (
                expected_digest is None
                or not _verify_scalar_artifact_digest(
                    destination, artifact, expected_digest
                )
            ):
                return _fail(
                    "hash_mismatch",
                    f"Existing destination hash does not match artifact {artifact.key!r}.",
                )
            return _ok("already_present")

    if source_path is None or source_kind is None:
        if saw_source_mismatch:
            return _fail(
                "hash_mismatch",
                f"Source hash does not match artifact {artifact.key!r}.",
            )
        msg = f"No source bytes found for artifact {artifact.key!r}."
        if on_missing == "raise":
            raise FileNotFoundError(msg)
        return _fail("missing_source", msg)

    if _path_has_symlink_component(source_path):
        msg = f"Symlink detected in source path: {source_path}"
        if on_missing == "raise":
            raise ValueError(msg)
        return _fail("failed", msg)

    if _is_file_bundle_artifact(artifact):
        entry = file_bundle_entry_name(artifact, fallback=destination.name)
        assert entry is not None
        manifest = meta.get("file_bundle_manifest") or meta.get("directory_manifest")
        if source_path.is_dir():
            source_bundle_root = source_path
        elif source_path.is_file():
            source_bundle_root = source_path.parent
        else:
            msg = (
                f"Source path {source_path} is neither a regular file nor a directory."
            )
            if on_missing == "raise":
                raise RuntimeError(msg)
            return _fail("failed", msg)

        if should_validate:
            err = _check_bundle(source_bundle_root, entry, source_path, "Source")
            if err is not None:
                return _fail("hash_mismatch", err)

        destination.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(manifest, Mapping) and isinstance(manifest.get("entries"), list):
            members_to_copy = [
                e["path"]
                for e in manifest["entries"]
                if isinstance(e, Mapping) and "path" in e
            ]
        else:
            stem = Path(entry).stem
            try:
                with os.scandir(source_bundle_root) as scan:
                    members_to_copy = [
                        c.name
                        for c in scan
                        if c.is_file()
                        and (c.name == entry or c.name.startswith(f"{stem}."))
                    ]
            except OSError as exc:
                if on_missing == "raise":
                    raise RuntimeError(
                        f"Failed scanning bundle source {source_bundle_root}: {exc}"
                    ) from exc
                return _fail("failed", str(exc))

        try:
            for member_name in members_to_copy:
                _materialize_path(
                    source=source_bundle_root / member_name,
                    destination=destination.parent / member_name,
                    preserve_existing=preserve_existing,
                )
        except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
            if on_missing == "raise":
                raise RuntimeError(
                    f"Failed to materialize bundle {artifact.key!r}: {exc}"
                ) from exc
            return _fail("failed", str(exc))

        if should_validate and manifest is not None:
            err = _check_bundle(destination.parent, entry, destination, "Destination")
            if err is not None:
                return _fail("hash_mismatch", err)
        return _ok(_materialized_status_for_source(source_kind))

    if _is_directory_artifact(artifact) or source_path.is_dir():
        if not source_path.is_dir():
            msg = f"Source path is not a directory: {source_path}"
            if on_missing == "raise":
                raise RuntimeError(msg)
            return _fail("failed", msg)
        manifest = meta.get("directory_manifest")
        if should_validate:
            if manifest:
                try:
                    materialized = materialize_directory_tree(
                        source_path,
                        destination,
                        manifest,
                        preserve_existing=preserve_existing,
                        allow_extra_files=directory_allows_extra_files(artifact),
                    )
                except (ValueError, OSError) as exc:
                    return _fail(
                        "hash_mismatch",
                        f"Directory validation failed for artifact {artifact.key!r}: {exc}",
                    )
                return _ok(
                    _materialized_status_for_source(source_kind)
                    if materialized
                    else "already_present"
                )
            if expected_hash:
                try:
                    if (
                        build_directory_manifest(source_path).get("tree_hash")
                        != expected_hash
                    ):
                        return _fail(
                            "hash_mismatch",
                            f"Source hash does not match artifact {artifact.key!r}.",
                        )
                except (ValueError, OSError) as exc:
                    return _fail(
                        "hash_mismatch",
                        f"Validation failed for {artifact.key!r}: {exc}",
                    )

        try:
            if (
                preserve_existing
                and destination.exists()
                and isinstance(manifest, Mapping)
                and isinstance(manifest.get("entries"), list)
            ):
                materialized = any(
                    _materialize_path(
                        source=source_path / entry["path"],
                        destination=destination / entry["path"],
                        preserve_existing=True,
                    )[0]
                    for entry in manifest["entries"]
                    if isinstance(entry, Mapping) and isinstance(entry.get("path"), str)
                )
                skipped_existing = not materialized
            else:
                materialized, skipped_existing = _materialize_path(
                    source=source_path,
                    destination=destination,
                    preserve_existing=preserve_existing,
                )
        except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
            if on_missing == "raise":
                raise RuntimeError(
                    f"Failed to materialize directory artifact {artifact.key!r}: {exc}"
                ) from exc
            return _fail("failed", str(exc))
        if skipped_existing:
            return _ok("already_present")
        if materialized:
            return _ok(_materialized_status_for_source(source_kind))

    if not source_path.is_file():
        msg = f"Source path is not a regular file: {source_path}"
        if on_missing == "raise":
            raise RuntimeError(msg)
        return _fail("failed", msg)

    if should_validate and (
        expected_digest is None
        or not _verify_scalar_artifact_digest(source_path, artifact, expected_digest)
    ):
        return _fail(
            "hash_mismatch", f"Source hash does not match artifact {artifact.key!r}."
        )

    try:
        materialized, skipped_existing = _materialize_path(
            source=source_path,
            destination=destination,
            preserve_existing=preserve_existing,
        )
    except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
        if on_missing == "raise":
            raise RuntimeError(
                f"Failed to materialize artifact {artifact.key!r}: {exc}"
            ) from exc
        return _fail("failed", str(exc))

    if skipped_existing:
        return _ok("already_present")
    if materialized:
        return _ok(_materialized_status_for_source(source_kind))

    msg = f"Failed to materialize artifact {artifact.key!r} to {destination}."
    if on_missing == "raise":
        raise RuntimeError(msg)
    return _fail("failed", msg)


def _source_identity(item: PlannedMaterialization) -> tuple[str, str]:
    if item.source_kind == "filesystem":
        assert item.source_path is not None
        return ("filesystem", str(item.source_path.resolve()))
    return ("db_export", str(item.artifact.id))


def _resolve_staging_source_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
) -> tuple[Path | None, bool]:
    from consist.core.tracker_recovery import (
        resolve_verified_local_path_with_diagnostics,
        verify_candidate_against_artifact,
    )

    if validate_content_hash != "never":
        verified, saw_mismatch = resolve_verified_local_path_with_diagnostics(
            tracker,
            artifact,
            allow_recovery_roots=True,
            verify_digest=(validate_content_hash == "always"),
        )
        if verified is not None:
            return verified.resolve(), False
        if isinstance(tracker, _HasDbAttr) and isinstance(tracker.db, DatabaseManager):
            return None, saw_mismatch

    saw_any_mismatch = False
    for raw_cand in (
        (tracker.resolve_uri(artifact.uri) if callable(tracker.resolve_uri) else None),
        artifact.abs_path,
    ):
        if not raw_cand:
            continue
        try:
            cand = Path(str(raw_cand)).resolve()
            if cand.exists():
                if (
                    validate_content_hash == "never"
                    or verify_candidate_against_artifact(cand, artifact)
                ):
                    return cand, False
                saw_any_mismatch = True
        except (OSError, ValueError, TypeError):
            pass

    relative_path = tracker.fs.get_remappable_relative_path(artifact.uri)
    if relative_path is not None:
        if artifact.run_id:
            owning_run = tracker.get_run(str(artifact.run_id))
            if owning_run is not None:
                _, source, _ = find_existing_recovery_source_path(
                    tracker,
                    artifact=artifact,
                    run=owning_run,
                    source_root=None,
                )
                if source is not None:
                    if (
                        validate_content_hash == "never"
                        or verify_candidate_against_artifact(source, artifact)
                    ):
                        return source, False
                    saw_any_mismatch = True
        for root in _artifact_recovery_roots(tracker, artifact=artifact):
            cand = (root / relative_path).resolve()
            if cand.exists():
                if (
                    validate_content_hash == "never"
                    or verify_candidate_against_artifact(cand, artifact)
                ):
                    return cand, False
                saw_any_mismatch = True
    return None, saw_any_mismatch


def _stage_artifact_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    destination: Path,
    overwrite: bool,
    validate_content_hash: Literal["never", "if-present", "always"],
    allow_external_paths: bool | None,
) -> StagedInput:
    allow_external = (
        tracker.allow_external_paths
        if allow_external_paths is None
        else allow_external_paths
    )
    allowed_base = build_allowed_materialization_roots(
        run_dir=Path(tracker.run_dir),
        mounts=tracker.fs.mounts,
        allow_external_paths=allow_external,
    )

    destination_path = Path(destination)

    def _fail_stage(
        status: StagingStatus, msg: str, *, warn_prefix: bool = False
    ) -> StagedInput:
        logging.warning("[Consist] %s", msg) if warn_prefix else logging.warning(msg)
        return _make_staged_output(
            artifact,
            status=status,
            path=destination_path,
            message=msg,
            resolvable=False,
        )

    def _ok_stage(status: StagingStatus) -> StagedInput:
        return _make_staged_output(
            artifact,
            status=status,
            path=destination_path,
            resolvable=True,
        )

    try:
        if validate_content_hash not in {"never", "if-present", "always"}:
            raise ValueError(
                "validate_content_hash must be one of 'never', 'if-present', or 'always'."
            )
        _ensure_destination_not_symlink(destination_path)
        destination_path = destination_path.resolve()
        validate_allowed_materialization_destination(destination_path, allowed_base)

        if validate_content_hash == "always" and artifact.digest is None:
            source_probe, _ = _resolve_staging_source_path(
                tracker, artifact=artifact, validate_content_hash="never"
            )
            if source_probe is None or not source_probe.exists():
                return _fail_stage(
                    "missing_source",
                    f"[Consist] Cannot stage artifact {artifact.key!r}: source path missing.",
                )
            return _fail_stage(
                "failed",
                f"[Consist] Cannot validate artifact {artifact.key!r}: artifact.digest is missing.",
            )

        source_path, saw_mismatch = _resolve_staging_source_path(
            tracker, artifact=artifact, validate_content_hash=validate_content_hash
        )
        if source_path is None or not source_path.exists():
            if saw_mismatch:
                return _fail_stage(
                    "failed",
                    f"[Consist] Content hash mismatch while staging {artifact.key!r}.",
                )
            return _fail_stage(
                "missing_source",
                f"[Consist] Cannot stage artifact {artifact.key!r}: source path missing.",
            )

        if artifact.digest is not None and validate_content_hash != "never":
            digest_matches = (
                verify_digest(
                    source_path,
                    artifact.digest,
                    driver=artifact.driver,
                    member=artifact.member,
                )
                if artifact.digest.scheme in DIGEST_DRIVERS
                else False
            )
            if not digest_matches:
                actual_digest = (
                    compute_digest(
                        source_path,
                        driver="directory" if source_path.is_dir() else artifact.driver,
                        member=artifact.member,
                    )
                    if source_path.exists()
                    else None
                )
                return _fail_stage(
                    "failed",
                    f"[Consist] Content hash mismatch while staging {artifact.key!r}: "
                    f"expected {artifact.digest}, found {actual_digest}.",
                )

        if source_path == destination_path:
            return _ok_stage("preserved_existing")

        if destination_path.exists():
            if destination_path.is_symlink():
                return _fail_stage(
                    "failed",
                    f"Symlink detected in destination path: {destination_path}",
                    warn_prefix=True,
                )
            if source_path.is_dir() != destination_path.is_dir():
                return _fail_stage(
                    "failed",
                    f"Destination type mismatch for {destination_path}; refusing to overwrite.",
                    warn_prefix=True,
                )
            if not overwrite:
                if _compute_path_checksum(source_path) == _compute_path_checksum(
                    destination_path
                ):
                    return _ok_stage("preserved_existing")
                return _fail_stage(
                    "failed",
                    f"Destination already exists and differs for {destination_path}; refusing to overwrite.",
                    warn_prefix=True,
                )

        materialized, skipped_existing = _materialize_path(
            source=source_path, destination=destination_path, preserve_existing=False
        )
        if skipped_existing:
            return _ok_stage("preserved_existing")
        if materialized:
            return _ok_stage("staged")
        return _fail_stage(
            "failed",
            f"Failed to stage artifact {artifact.key!r} to {destination_path}.",
            warn_prefix=True,
        )
    except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
        logging.warning(
            "[Consist] Failed to stage artifact %r to %s: %s",
            artifact.key,
            destination_path,
            exc,
        )
        return _make_staged_output(
            artifact,
            status="failed",
            path=destination_path,
            message=str(exc),
            resolvable=False,
        )


def stage_artifacts_to_destinations(
    tracker: "Tracker",
    items: Sequence[tuple[str, Artifact, Path]],
    *,
    mode: Literal["copy", "hardlink", "symlink"] = "copy",
    overwrite: bool = False,
    validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
    allow_external_paths: bool | None = None,
) -> StagedInputsResult:
    """Stage explicit artifact/destination pairs into a keyed result."""
    if mode != "copy":
        raise ValueError(
            f"Unsupported staging mode {mode!r}; only 'copy' is supported."
        )

    outputs: dict[str, StagedInput] = {}
    key_order: list[str] = []
    for key, artifact, destination in items:
        key_order.append(key)
        if artifact.key != key:
            msg = (
                f"Resolved artifact key {artifact.key!r} does not match requested "
                f"staging key {key!r}."
            )
            outputs[key] = _make_staged_output(
                artifact,
                status="failed",
                path=Path(destination),
                message=msg,
                resolvable=False,
            )
            continue
        outputs[key] = _stage_artifact_path(
            tracker,
            artifact=artifact,
            destination=Path(destination),
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )
    return _ordered_staged_results(tuple(key_order), outputs)


def plan_run_output_hydration(
    tracker: "Tracker",
    run,
    *,
    target_root: Path | None,
    destinations_by_key: Mapping[str, Path] | None = None,
    source_root: Path | None,
    keys: Sequence[str] | None,
    preserve_existing: bool,
    db_fallback: Literal["never", "if_ingested"],
) -> PlannedRunOutputHydration:
    """Plan historical output hydration for a run without mutating the target."""
    if (target_root is None) == (destinations_by_key is None):
        raise ValueError(
            "Provide exactly one of target_root or destinations_by_key for run output hydration."
        )

    outputs: dict[str, HydratedRunOutput] = {}
    raw_outputs = _get_output_artifacts_for_run(tracker, run.id)

    key_counts: dict[str, int] = {}
    for artifact in raw_outputs:
        key_counts[artifact.key] = key_counts.get(artifact.key, 0) + 1
    duplicate_keys = sorted(key for key, count in key_counts.items() if count > 1)
    if duplicate_keys:
        raise ValueError(
            "Run has duplicate output keys; run-scoped materialization is ambiguous: "
            + ", ".join(repr(key) for key in duplicate_keys)
        )

    outputs_by_key = {artifact.key: artifact for artifact in raw_outputs}
    key_order: tuple[str, ...]
    requested_destinations: dict[str, Path] | None = None
    normalized_destinations: dict[str, Path] | None = None
    if destinations_by_key is not None:
        if keys is not None:
            raise ValueError(
                "keys cannot be combined with destinations_by_key; destination keys select outputs."
            )
        requested_destinations = {
            key: Path(destination).expanduser().absolute()
            for key, destination in destinations_by_key.items()
        }
        requested_keys = list(requested_destinations)
        missing_keys = [key for key in requested_keys if key not in outputs_by_key]
        if missing_keys:
            raise KeyError(
                "Requested output keys were not found for run "
                f"{run.id!r}: {', '.join(repr(key) for key in missing_keys)}"
            )
        selected_outputs = [outputs_by_key[key] for key in requested_keys]
        key_order = tuple(requested_keys)
    elif keys is not None:
        requested_keys = list(keys)
        missing_keys = [key for key in requested_keys if key not in outputs_by_key]
        if missing_keys:
            raise KeyError(
                "Requested output keys were not found for run "
                f"{run.id!r}: {', '.join(repr(key) for key in missing_keys)}"
            )
        selected_outputs = [outputs_by_key[key] for key in requested_keys]
        key_order = tuple(requested_keys)
    else:
        selected_outputs = list(raw_outputs)
        key_order = tuple(artifact.key for artifact in selected_outputs)

    if requested_destinations is not None:
        normalized_destinations = {
            artifact.key: (
                requested_destinations[artifact.key]
                if _is_directory_artifact(artifact)
                or _is_file_bundle_artifact(artifact)
                else requested_destinations[artifact.key].resolve()
            )
            for artifact in selected_outputs
        }

    if (
        db_fallback == "never"
        and source_root is None
        and any(
            _is_directory_artifact(artifact) or _is_file_bundle_artifact(artifact)
            for artifact in selected_outputs
        )
    ):
        raise ValueError(
            "immutable directory and file-bundle hydration with db_fallback='never' "
            "requires source_root"
        )

    if normalized_destinations is not None:
        unsupported_output_sets = [
            (artifact.key, kind)
            for artifact in selected_outputs
            if (kind := _output_set_hydration_kind(tracker, artifact)) is not None
        ]
        if unsupported_output_sets:
            details = ", ".join(
                f"{key!r} ({kind})" for key, kind in unsupported_output_sets
            )
            raise ValueError(
                "OutputSet hydration is deferred for Slice 1; "
                f"requested OutputSet artifacts: {details}."
            )

    destination_collisions: set[Path] = set()
    if normalized_destinations is not None:
        destination_counts = Counter(normalized_destinations.values())
        destination_collisions = {
            destination
            for destination, count in destination_counts.items()
            if count > 1
        }

    planned_by_destination: dict[Path, PlannedMaterialization] = {}
    conflicted_destinations: set[Path] = set()

    if normalized_destinations is not None:
        for artifact in selected_outputs:
            destination = normalized_destinations[artifact.key]
            if destination in destination_collisions:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="failed",
                    path=destination,
                    message=f"destination collision at {destination}",
                    resolvable=False,
                )

    for artifact in selected_outputs:
        if artifact.key in outputs:
            continue
        if _is_legacy_zarr_artifact(artifact):
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="failed",
                path=(
                    normalized_destinations[artifact.key]
                    if normalized_destinations is not None
                    else None
                ),
                message=(
                    "legacy Zarr artifacts without an immutable directory manifest "
                    "cannot be hydrated; re-log the output under the current contract."
                ),
                resolvable=False,
            )
            continue
        if _is_legacy_shapefile_artifact(artifact):
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="failed",
                path=(
                    normalized_destinations[artifact.key]
                    if normalized_destinations is not None
                    else None
                ),
                message=(
                    "legacy Shapefile artifacts without an immutable bundle manifest "
                    "cannot be hydrated; re-log the output under the current contract."
                ),
                resolvable=False,
            )
            continue
        if _is_directory_artifact(artifact) and normalized_destinations is not None:
            destination = normalized_destinations[artifact.key]
            try:
                preserved = validate_directory_destination(
                    destination,
                    _directory_artifact_manifest(artifact),
                    preserve_existing=preserve_existing,
                    allow_extra_files=directory_allows_extra_files(artifact),
                )
            except ValueError as exc:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="failed",
                    path=destination,
                    message=str(exc),
                    resolvable=False,
                )
                continue
            if preserved:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="preserved_existing",
                    path=destination,
                    resolvable=True,
                )
                continue
        if _is_file_bundle_artifact(artifact) and normalized_destinations is not None:
            destination = normalized_destinations[artifact.key]
            try:
                entry, manifest = _shapefile_bundle_metadata(artifact)
                preserved = validate_shapefile_bundle_destination(
                    destination,
                    entry,
                    manifest,
                    preserve_existing=preserve_existing,
                )
            except ValueError as exc:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="failed",
                    path=destination,
                    message=str(exc),
                    resolvable=False,
                )
                continue
            if preserved:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="preserved_existing",
                    path=destination,
                    entry_path=destination / entry,
                    resolvable=True,
                )
                continue
        saw_hydration_mismatch = False
        if _is_directory_artifact(artifact) and db_fallback == "never":
            _directory_artifact_manifest(artifact)
            relative_path = tracker.fs.get_remappable_relative_path(artifact.uri)
            source_path = (
                Path(source_root) / relative_path
                if source_root is not None and relative_path is not None
                else None
            )
            has_candidate_roots = relative_path is not None
        elif _is_file_bundle_artifact(artifact) and db_fallback == "never":
            _shapefile_bundle_metadata(artifact)
            relative_path = tracker.fs.get_remappable_relative_path(artifact.uri)
            source_path = (
                Path(source_root) / relative_path
                if source_root is not None and relative_path is not None
                else None
            )
            has_candidate_roots = relative_path is not None
        elif (
            source_root is None
            and isinstance(tracker, _HasDbAttr)
            and isinstance(tracker.db, DatabaseManager)
        ):
            from consist.core.tracker_recovery import (
                resolve_verified_local_path_detailed,
            )

            is_bundle_artifact = _is_file_bundle_artifact(artifact)
            relative_path = _derive_remappable_relative_path(tracker, artifact=artifact)
            if relative_path is None:
                source_path = None
                has_candidate_roots = False
            else:
                resolved_loc, saw_hydration_mismatch, _ = (
                    resolve_verified_local_path_detailed(
                        tracker,
                        artifact,
                        allow_recovery_roots=True,
                        ignore_runtime_abs_path=True,
                    )
                )
                source_path = (
                    (resolved_loc if resolved_loc.is_dir() else resolved_loc.parent)
                    if (is_bundle_artifact and resolved_loc is not None)
                    else resolved_loc
                )
                has_candidate_roots = True
        else:
            owning_run = _get_artifact_owning_run(
                tracker, selected_run=run, artifact=artifact
            )
            is_dir_artifact = _is_directory_artifact(artifact)
            is_bundle_artifact = _is_file_bundle_artifact(artifact)
            has_manifest_validator = is_dir_artifact or is_bundle_artifact
            has_scalar_hash_validator = not has_manifest_validator and (
                artifact.digest is not None
            )
            relative_path, source_path, has_candidate_roots = (
                find_existing_recovery_source_path(
                    tracker,
                    artifact=artifact,
                    run=owning_run,
                    source_root=source_root,
                    source_validator=(
                        (
                            lambda candidate, artifact=artifact: (
                                is_valid_directory_artifact_tree(candidate, artifact)
                                is not False
                            )
                        )
                        if is_dir_artifact
                        else (
                            lambda candidate, artifact=artifact: (
                                is_valid_shapefile_bundle_source(candidate, artifact)
                                is not False
                            )
                        )
                        if is_bundle_artifact
                        else (
                            lambda candidate, artifact=artifact: (
                                _is_valid_scalar_recovery_candidate(
                                    tracker, candidate, artifact
                                )
                            )
                        )
                        if has_scalar_hash_validator
                        else None
                    ),
                    fallback_validator=(
                        (lambda _: True) if has_manifest_validator else None
                    ),
                    preserve_raw_paths=has_manifest_validator,
                )
            )
            if is_bundle_artifact and source_path is not None:
                source_path = (
                    source_path if source_path.is_dir() else source_path.parent
                )
        if relative_path is None:
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="skipped_unmapped",
                path=None,
                resolvable=False,
            )
            continue

        destination = (
            normalized_destinations[artifact.key]
            if normalized_destinations is not None
            else cast(Path, target_root) / relative_path
        )

        if destination in destination_collisions:
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="failed",
                path=destination,
                message=f"destination collision at {destination}",
                resolvable=False,
            )
            continue

        source_kind: Literal["filesystem", "db_export"]
        planned_source_path: Path | None
        if source_path is not None:
            source_kind = "filesystem"
            planned_source_path = source_path
        else:
            if not has_candidate_roots:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="skipped_unmapped",
                    path=None,
                    resolvable=False,
                )
                continue
            artifact_meta = (
                artifact.meta if isinstance(artifact.meta, MappingABC) else {}
            )
            if db_fallback == "if_ingested" and artifact_meta.get("is_ingested", False):
                driver = str(artifact.driver or "").lower()
                if driver not in {"csv", "parquet"}:
                    outputs[artifact.key] = _make_hydrated_output(
                        artifact,
                        status="failed",
                        path=destination,
                        message=(
                            "unsupported DB export for ingested driver "
                            f"{artifact.driver!r}"
                        ),
                        resolvable=False,
                    )
                    continue
                source_kind = "db_export"
                planned_source_path = None
            else:
                outputs[artifact.key] = _make_hydrated_output(
                    artifact,
                    status="failed" if saw_hydration_mismatch else "missing_source",
                    path=destination,
                    message=(
                        f"Source hash mismatch for artifact {artifact.key!r}."
                        if saw_hydration_mismatch
                        else None
                    ),
                    resolvable=False,
                )
                continue

        planned = PlannedMaterialization(
            artifact=artifact,
            keys=(artifact.key,),
            source_kind=source_kind,
            source_path=planned_source_path,
            destination=destination,
            relative_path=relative_path,
            artifacts_by_key={artifact.key: artifact},
        )

        if destination in conflicted_destinations:
            outputs[artifact.key] = _make_hydrated_output(
                artifact,
                status="failed",
                path=destination,
                message=f"destination collision at {destination}",
                resolvable=False,
            )
            continue

        existing = planned_by_destination.get(destination)
        if existing is None:
            planned_by_destination[destination] = planned
            continue

        if _source_identity(existing) == _source_identity(planned):
            combined_artifacts = dict(existing.artifacts_by_key or {})
            combined_artifacts[artifact.key] = artifact
            planned_by_destination[destination] = PlannedMaterialization(
                artifact=existing.artifact,
                keys=existing.keys + (artifact.key,),
                source_kind=existing.source_kind,
                source_path=existing.source_path,
                destination=existing.destination,
                relative_path=existing.relative_path,
                artifacts_by_key=combined_artifacts,
            )
            continue

        for key in existing.keys + (artifact.key,):
            collision_artifact = (
                (
                    existing.artifacts_by_key
                    or {existing.artifact.key: existing.artifact}
                )[key]
                if key in (existing.artifacts_by_key or {})
                else artifact
            )
            outputs[key] = _make_hydrated_output(
                collision_artifact,
                status="failed",
                path=destination,
                message=f"destination collision at {destination}",
                resolvable=False,
            )
        conflicted_destinations.add(destination)
        del planned_by_destination[destination]

    plan: list[PlannedMaterialization] = []
    for item in sorted(
        planned_by_destination.values(),
        key=lambda item: (str(item.destination), item.keys),
    ):
        if (
            preserve_existing
            and item.destination.exists()
            and not _is_directory_artifact(item.artifact)
            and not _is_file_bundle_artifact(item.artifact)
        ):
            if item.destination.is_symlink():
                for key in item.keys:
                    artifact = item.artifact_for_key(key)
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status="failed",
                        path=item.destination,
                        message=f"Symlink detected in destination path: {item.destination}",
                        resolvable=False,
                    )
            else:
                for key in item.keys:
                    artifact = item.artifact_for_key(key)
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status="preserved_existing",
                        path=item.destination,
                        resolvable=True,
                    )
            continue
        plan.append(item)
    return PlannedRunOutputHydration(
        key_order=key_order,
        plan=plan,
        preflight_results=_ordered_hydrated_results(key_order, outputs),
    )


def build_run_output_materialize_plan(
    tracker: "Tracker",
    run,
    *,
    target_root: Path,
    source_root: Path | None,
    keys: Sequence[str] | None,
    preserve_existing: bool,
    db_fallback: Literal["never", "if_ingested"],
) -> tuple[list[PlannedMaterialization], MaterializationResult]:
    """Build the legacy materialization plan and aggregate preflight result."""
    planned = plan_run_output_hydration(
        tracker,
        run,
        target_root=target_root,
        destinations_by_key=None,
        source_root=source_root,
        keys=keys,
        preserve_existing=preserve_existing,
        db_fallback=db_fallback,
    )
    return planned.plan, fold_hydrated_run_outputs_result(planned.preflight_results)


def execute_planned_output_hydration(
    plan: Sequence[PlannedMaterialization],
    *,
    tracker: "Tracker",
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
    preserve_existing: bool = True,
) -> HydratedRunOutputsResult:
    """Execute a planned historical output hydration."""
    outputs: dict[str, HydratedRunOutput] = {}

    def _record_item(
        item: PlannedMaterialization,
        status: HydrationStatus,
        path: Path,
        *,
        entry_path: Path | None = None,
        message: str | None = None,
        resolvable: bool = True,
    ) -> None:
        for k in item.keys:
            outputs[k] = _make_hydrated_output(
                item.artifact_for_key(k),
                status=status,
                path=path,
                entry_path=entry_path,
                message=message,
                resolvable=resolvable,
            )

    for item in plan:
        try:
            _validate_allowed_base(item.destination, allowed_base)

            if _is_directory_artifact(item.artifact):
                if item.source_path is None:
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                _reject_symlink_from_source_root(item.source_path, item.relative_path)
                if not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                materialized = materialize_directory_tree(
                    item.source_path,
                    item.destination,
                    _directory_artifact_manifest(item.artifact),
                    preserve_existing=preserve_existing,
                    allow_extra_files=directory_allows_extra_files(item.artifact),
                )
                _record_item(
                    item,
                    "materialized_directory_from_filesystem"
                    if materialized
                    else "preserved_existing",
                    item.destination,
                )
                continue

            if _is_file_bundle_artifact(item.artifact):
                if item.source_path is None:
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                bundle_rel = (
                    item.relative_path
                    if item.source_path.name == item.relative_path.name
                    else item.relative_path.parent
                )
                _reject_symlink_from_source_root(item.source_path, bundle_rel)
                if not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                entry, manifest = _shapefile_bundle_metadata(item.artifact)
                materialized = materialize_shapefile_bundle(
                    item.source_path,
                    item.destination,
                    entry,
                    manifest,
                    preserve_existing=preserve_existing,
                )
                _record_item(
                    item,
                    "materialized_file_bundle_from_filesystem"
                    if materialized
                    else "preserved_existing",
                    item.destination,
                    entry_path=item.destination / entry,
                )
                continue

            if item.source_kind == "filesystem":
                if item.source_path is None or not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                materialized, skipped_existing = _materialize_path(
                    source=item.source_path,
                    destination=item.destination,
                    preserve_existing=preserve_existing,
                )
                if skipped_existing:
                    _record_item(item, "preserved_existing", item.destination)
                elif materialized:
                    _record_item(item, "materialized_from_filesystem", item.destination)
                continue

            destination_str = materialize_ingested_artifact_from_db(
                artifact=item.artifact,
                tracker=tracker,
                destination=item.destination,
                overwrite=not preserve_existing,
            )
            _record_item(item, "materialized_from_db", Path(destination_str))

        except FileNotFoundError as exc:
            if on_missing == "raise":
                raise
            logging.warning(
                "[Consist] Cannot materialize run output %s: %s",
                ",".join(repr(key) for key in item.keys),
                exc,
            )
            _record_item(
                item,
                "missing_source",
                item.destination,
                message=str(exc),
                resolvable=False,
            )
        except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
            if on_missing == "raise":
                raise RuntimeError(
                    f"Failed to materialize run outputs for keys {item.keys!r}: {exc}"
                ) from exc
            logging.warning(
                "[Consist] Failed to materialize run output %s -> %s: %s",
                ",".join(repr(key) for key in item.keys),
                item.destination,
                exc,
            )
            _record_item(
                item, "failed", item.destination, message=str(exc), resolvable=False
            )

    key_order = tuple(key for item in plan for key in item.keys)
    return _ordered_hydrated_results(key_order, outputs)


def materialize_planned_outputs(
    plan: Sequence[PlannedMaterialization],
    *,
    tracker: "Tracker",
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
    preserve_existing: bool = True,
) -> MaterializationResult:
    """Execute a plan and fold outcomes into ``MaterializationResult``."""
    return fold_hydrated_run_outputs_result(
        execute_planned_output_hydration(
            plan,
            tracker=tracker,
            allowed_base=allowed_base,
            on_missing=on_missing,
            preserve_existing=preserve_existing,
        )
    )


def _raise_if_preflight_failed(
    preflight_results: Mapping[str, HydratedRunOutput],
) -> None:
    for key, item in preflight_results.items():
        if item.status == "missing_source":
            raise FileNotFoundError(
                f"Cannot materialize run output {key!r}: "
                f"{item.message or f'source path missing ({item.path})'}"
            )
        if item.status == "failed":
            raise RuntimeError(
                f"Failed to materialize run outputs for keys ({key!r},): "
                f"{item.message or 'materialization failed'}"
            )


def hydrate_run_outputs(
    tracker: "Tracker",
    run,
    *,
    target_root: Path,
    source_root: Path | None,
    keys: Sequence[str] | None,
    allowed_base: Path | Sequence[Path] | None,
    preserve_existing: bool,
    on_missing: Literal["warn", "raise"],
    db_fallback: Literal["never", "if_ingested"],
) -> HydratedRunOutputsResult:
    """Hydrate selected historical outputs into a target workspace root."""
    planned = plan_run_output_hydration(
        tracker,
        run,
        target_root=target_root,
        destinations_by_key=None,
        source_root=source_root,
        keys=keys,
        preserve_existing=preserve_existing,
        db_fallback=db_fallback,
    )
    if on_missing == "raise":
        _raise_if_preflight_failed(planned.preflight_results)
    executed = execute_planned_output_hydration(
        planned.plan,
        tracker=tracker,
        allowed_base=allowed_base,
        on_missing=on_missing,
        preserve_existing=preserve_existing,
    )
    merged = dict(planned.preflight_results.items())
    merged.update(executed.items())
    return _ordered_hydrated_results(
        planned.key_order,
        merged,
        source_run_id=run.id,
    )


def hydrate_run_outputs_to_destinations(
    tracker: "Tracker",
    run,
    *,
    destinations_by_key: Mapping[str, Path],
    source_root: Path | None,
    allowed_base: Path | Sequence[Path] | None,
    preserve_existing: bool,
    on_missing: Literal["warn", "raise"],
    db_fallback: Literal["never", "if_ingested"],
) -> HydratedRunOutputsResult:
    """Hydrate selected outputs to exact caller-provided destinations."""
    normalized_destinations = {
        key: Path(destination).expanduser().absolute()
        for key, destination in destinations_by_key.items()
    }
    for destination in normalized_destinations.values():
        validate_allowed_materialization_destination(destination, allowed_base)

    planned = plan_run_output_hydration(
        tracker,
        run,
        target_root=None,
        destinations_by_key=normalized_destinations,
        source_root=source_root,
        keys=None,
        preserve_existing=preserve_existing,
        db_fallback=db_fallback,
    )
    if on_missing == "raise":
        _raise_if_preflight_failed(planned.preflight_results)
    executed = execute_planned_output_hydration(
        planned.plan,
        tracker=tracker,
        allowed_base=allowed_base,
        on_missing=on_missing,
        preserve_existing=preserve_existing,
    )
    merged = dict(planned.preflight_results.items())
    merged.update(executed.items())
    return _ordered_hydrated_results(
        planned.key_order,
        merged,
        source_run_id=run.id,
    )


def materialize_artifacts(
    tracker: "Tracker",
    items: Sequence[tuple[Artifact, Path]],
    *,
    on_missing: Literal["warn", "raise"] = "warn",
) -> dict[str, str]:
    """Synchronize cached artifact bytes to specific filesystem destinations."""
    from consist.core.tracker_recovery import (
        resolve_verified_local_path,
        stage_verified_source,
    )

    materialized: dict[str, str] = {}

    for artifact, destination in items:
        _ensure_destination_not_symlink(Path(destination))
        destination_path = Path(destination).resolve()
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        source_path = resolve_verified_local_path(
            tracker, artifact, allow_recovery_roots=True
        )
        if source_path is None or not source_path.exists():
            try:
                probe = Path(tracker.resolve_uri(artifact.uri)).resolve()
            except (OSError, ValueError) as e:
                msg = f"[Consist] Failed to resolve URI for artifact {artifact.key!r}: {e}"
                if on_missing == "raise":
                    raise RuntimeError(msg) from e
                logging.warning(msg)
                continue
            msg = (
                f"[Consist] Cannot materialize cached output {artifact.key!r}: "
                f"resolved source path missing ({probe})."
            )
            if on_missing == "raise":
                raise FileNotFoundError(msg)
            logging.warning(msg)
            continue

        source_resolved = source_path.resolve()
        try:
            if destination_path.exists():
                if destination_path.is_symlink():
                    raise ValueError(
                        f"Symlink detected in destination path: {destination_path}"
                    )
                if source_resolved.is_dir() != destination_path.is_dir():
                    raise ValueError(
                        f"Destination type mismatch for {destination_path}; "
                        "refusing to overwrite."
                    )
                materialized[artifact.key] = str(destination_path)
                continue
            if source_resolved == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            try:
                stage_verified_source(
                    artifact,
                    source_resolved,
                    destination_path,
                    mutable=False,
                    overwrite=False,
                )
            except FileExistsError:
                pass
            if destination_path.exists():
                materialized[artifact.key] = str(destination_path)
        except (OSError, shutil.Error, ValueError) as e:
            msg = (
                f"[Consist] Failed to materialize cached output {artifact.key!r} "
                f"from {source_resolved} -> {destination_path}: {e}"
            )
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)

    return materialized


def materialize_artifacts_from_sources(
    items: Sequence[tuple[Artifact, Path, Path]],
    *,
    allowed_base: Path | Sequence[Path] | None,
    on_missing: Literal["warn", "raise"] = "warn",
    overwrite_existing: bool = False,
) -> dict[str, str]:
    """Rehydrate artifacts from explicit source paths to specified destinations."""
    materialized: dict[str, str] = {}
    for artifact, source, destination in items:
        _ensure_destination_not_symlink(Path(destination))
        source_path = Path(source).resolve()
        destination_path = Path(destination).resolve()
        validate_allowed_materialization_destination(destination_path, allowed_base)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        if not source_path.exists():
            msg = (
                f"[Consist] Cannot materialize cached input {artifact.key!r}: "
                f"source path missing ({source_path})."
            )
            if on_missing == "raise":
                raise FileNotFoundError(msg)
            logging.warning(msg)
            continue

        try:
            if destination_path.exists():
                if destination_path.is_symlink():
                    raise ValueError(
                        f"Symlink detected in destination path: {destination_path}"
                    )
                if source_path.is_dir() != destination_path.is_dir():
                    raise ValueError(
                        f"Destination type mismatch for {destination_path}; "
                        "refusing to overwrite."
                    )
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            materialized_flag, skipped = _materialize_path(
                source=source_path,
                destination=destination_path,
                preserve_existing=not overwrite_existing,
            )
            if materialized_flag or skipped or destination_path.exists():
                materialized[artifact.key] = str(destination_path)
        except (OSError, shutil.Error, ValueError) as e:
            msg = (
                f"[Consist] Failed to materialize cached input {artifact.key!r} "
                f"from {source_path} -> {destination_path}: {e}"
            )
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)

    return materialized


def build_materialize_items_for_keys(
    outputs: Iterable[Artifact],
    *,
    destinations_by_key: dict[str, Path],
) -> list[tuple[Artifact, Path]]:
    """Construct materialization mappings by correlating artifacts with target keys."""
    outputs_by_key = {a.key: a for a in outputs}
    items: list[tuple[Artifact, Path]] = []
    for key, dest in destinations_by_key.items():
        artifact = outputs_by_key.get(key)
        if artifact is not None:
            items.append((artifact, Path(dest)))
    return items


def stage_artifact(
    tracker: "Tracker",
    artifact: Artifact,
    destination: Path,
    *,
    mode: Literal["copy", "hardlink", "symlink"] = "copy",
    overwrite: bool = False,
    validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
    allow_external_paths: bool | None = None,
) -> StagedInput:
    """Stage one artifact to a requested filesystem destination."""
    return stage_artifacts_to_destinations(
        tracker,
        [(artifact.key, artifact, Path(destination))],
        mode=mode,
        overwrite=overwrite,
        validate_content_hash=validate_content_hash,
        allow_external_paths=allow_external_paths,
    )[artifact.key]


def stage_inputs(
    tracker: "Tracker",
    inputs_by_key: Mapping[str, Artifact],
    destinations_by_key: Mapping[str, Path],
    *,
    mode: Literal["copy", "hardlink", "symlink"] = "copy",
    overwrite: bool = False,
    validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
    allow_external_paths: bool | None = None,
) -> StagedInputsResult:
    """Stage multiple keyed artifacts to their requested destinations."""
    if mode != "copy":
        raise ValueError(
            f"Unsupported staging mode {mode!r}; only 'copy' is supported."
        )
    items: list[tuple[str, Artifact, Path]] = []
    for key, destination in destinations_by_key.items():
        artifact = inputs_by_key.get(key)
        if artifact is None:
            raise KeyError(
                f"Input key {key!r} was not found in the resolved artifact set."
            )
        items.append((key, artifact, Path(destination)))
    return stage_artifacts_to_destinations(
        tracker,
        items,
        mode=mode,
        overwrite=overwrite,
        validate_content_hash=validate_content_hash,
        allow_external_paths=allow_external_paths,
    )


def _load_global_table_columns(table_name: str, hot_db: Any) -> list[str]:
    """Inspect ``global_tables.<table_name>`` column names from DuckDB."""
    try:
        rows = hot_db.fetchall(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_catalog = current_database() "
            "AND table_schema = 'global_tables' "
            "AND table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to reflect table 'global_tables.{table_name}': {e}"
        ) from e

    if not rows:
        raise RuntimeError(
            f"Failed to reflect table 'global_tables.{table_name}': "
            "table does not exist or has no columns."
        )

    return list(dict.fromkeys(str(row[0]) for row in rows))


def materialize_ingested_artifact_from_db(
    *,
    artifact: Artifact,
    tracker: "Tracker",
    destination: Path,
    overwrite: bool = False,
) -> str:
    """Reconstruct an ingested artifact from the analytical database."""
    hot_db = get_hot_data_db(tracker) if tracker else None
    if not tracker or hot_db is None:
        raise RuntimeError(
            "Cannot materialize ingested artifact: tracker has no DB engine."
        )
    if not artifact.meta.get("is_ingested", False):
        raise ValueError(
            f"Artifact {artifact.key!r} is not marked as ingested; "
            "cannot reconstruct from DB."
        )

    driver = str(artifact.driver or "").lower()
    if driver not in {"csv", "parquet"}:
        raise ValueError(
            "Only csv/parquet artifacts can be reconstructed from DuckDB "
            f"(got driver={artifact.driver!r})."
        )

    table_name = artifact.meta.get("dlt_table_name") or artifact.key
    if not isinstance(table_name, str) or not table_name:
        raise ValueError("Artifact table name is missing; cannot reconstruct from DB.")
    _validate_identifier(table_name, label="table name")

    column_names = _load_global_table_columns(table_name, hot_db)
    quoted_cols = ", ".join(_quote_ident(col) for col in column_names)
    has_artifact_id = "consist_artifact_id" in column_names
    has_run_id = "consist_run_id" in column_names

    if has_artifact_id and has_run_id and artifact.run_id is not None:
        where_sql = (
            'WHERE (CAST("consist_artifact_id" AS VARCHAR) = ? '
            'OR ("consist_artifact_id" IS NULL AND CAST("consist_run_id" AS VARCHAR) = ?))'
        )
        query_params: list[Any] = [str(artifact.id), str(artifact.run_id)]
    elif has_artifact_id:
        where_sql = 'WHERE CAST("consist_artifact_id" AS VARCHAR) = ?'
        query_params = [str(artifact.id)]
    elif has_run_id and artifact.run_id is not None:
        where_sql = 'WHERE CAST("consist_run_id" AS VARCHAR) = ?'
        query_params = [str(artifact.run_id)]
    else:
        raise RuntimeError(
            f"Table 'global_tables.{table_name}' is missing consist_artifact_id."
        )

    sql = (
        f'SELECT {quoted_cols} FROM "global_tables".{_quote_ident(table_name)} '
        f"{where_sql}"
    )
    df = hot_db.fetchdf(sql, query_params)
    if df.empty:
        raise FileNotFoundError(
            f"No ingested rows found for artifact {artifact.key!r} ({artifact.id})."
        )

    _ensure_destination_not_symlink(Path(destination))
    destination_path = Path(destination).resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if destination_path.exists() and destination_path.is_dir():
        if not overwrite:
            raise ValueError(
                f"Destination type mismatch for {destination_path}; refusing to overwrite."
            )

    fd, tmp_path = tempfile.mkstemp(dir=str(destination_path.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        if driver == "csv":
            df.to_csv(tmp_path_obj, index=False)
        else:
            df.to_parquet(tmp_path_obj, index=False)

        if destination_path.exists() and not overwrite:
            tmp_path_obj.unlink(missing_ok=True)
            return str(destination_path)

        if overwrite:
            if destination_path.exists() and destination_path.is_dir():
                shutil.rmtree(destination_path)
            os.replace(tmp_path_obj, destination_path)
        else:
            try:
                os.link(tmp_path_obj, destination_path)
            except FileExistsError:
                return str(destination_path)
    finally:
        with suppress(FileNotFoundError):
            tmp_path_obj.unlink()

    return str(destination_path)
