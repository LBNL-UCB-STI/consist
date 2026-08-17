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
from dataclasses import dataclass, field
from contextlib import suppress
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import (
    Iterable,
    Literal,
    Mapping,
    Sequence,
    TYPE_CHECKING,
    Callable,
    cast,
    get_args,
)

import pandas as pd
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import SQLAlchemyError

from consist.core.container_policy import validate_recovery_registration_policy
from consist.core.directory_artifacts import (
    materialize_directory_tree,
    materialize_shapefile_bundle,
    validate_directory_destination,
    validate_directory_manifest,
    validate_shapefile_bundle_destination,
)
from consist.core.stores import get_hot_data_engine
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

_FILE_HASH_CHUNK_SIZE = 8 * 1024 * 1024


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

    def __post_init__(self) -> None:
        if self.artifacts_by_key is None:
            object.__setattr__(
                self,
                "artifacts_by_key",
                {key: self.artifact for key in self.keys},
            )


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
    clone = artifact.model_copy(deep=True)
    private_state = getattr(clone, "__pydantic_private__", None)
    if private_state is None:
        with suppress(Exception):
            object.__setattr__(clone, "__pydantic_private__", {})
        private_state = getattr(clone, "__pydantic_private__", None)
    if isinstance(private_state, dict):
        private_state["_tracker"] = None
        private_state["_abs_path"] = (
            str(hydrated_path.resolve()) if hydrated_path is not None else None
        )
    with suppress(Exception):
        object.__setattr__(clone, "_tracker", None)
    with suppress(Exception):
        object.__setattr__(
            clone,
            "_abs_path",
            str(hydrated_path.resolve()) if hydrated_path is not None else None,
        )
    return clone


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


def _is_directory_artifact(artifact: Artifact) -> bool:
    """Return whether an artifact has the strict directory-artifact contract."""
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    return meta.get("directory_artifact") is True


def _is_file_bundle_artifact(artifact: Artifact) -> bool:
    """Return whether an artifact has the strict Shapefile bundle contract."""
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    return meta.get("file_bundle_artifact") is True


def _shapefile_bundle_metadata(artifact: Artifact) -> tuple[str, dict[str, object]]:
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    entry = meta.get("file_bundle_entry")
    manifest = meta.get("file_bundle_manifest")
    if not isinstance(entry, str) or Path(entry).name != entry:
        raise ValueError(
            f"shapefile bundle artifact {artifact.key!r} has no valid entry"
        )
    if not isinstance(manifest, Mapping):
        raise ValueError(f"shapefile bundle artifact {artifact.key!r} has no manifest")
    normalized = validate_directory_manifest(manifest)
    if artifact.hash != normalized["tree_hash"]:
        raise ValueError(
            f"shapefile bundle artifact {artifact.key!r} manifest does not match artifact identity"
        )
    return entry, normalized


def _is_legacy_zarr_artifact(artifact: Artifact) -> bool:
    """Return whether a Zarr artifact predates immutable-tree manifests."""
    return artifact.driver == "zarr" and not _is_directory_artifact(artifact)


def _is_legacy_shapefile_artifact(artifact: Artifact) -> bool:
    """Return whether a Shapefile artifact predates immutable bundle manifests."""
    return artifact.driver == "shapefile" and not _is_file_bundle_artifact(artifact)


def _directory_artifact_manifest(artifact: Artifact) -> dict[str, object]:
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    manifest = meta.get("directory_manifest")
    if not isinstance(manifest, Mapping):
        raise ValueError(
            f"directory artifact {artifact.key!r} has no persisted manifest"
        )
    normalized = validate_directory_manifest(manifest)
    if artifact.hash != normalized["tree_hash"]:
        raise ValueError(
            f"directory artifact {artifact.key!r} manifest does not match artifact identity"
        )
    return normalized


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


def _compute_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_FILE_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
                digest.update(_compute_file_sha256(file_path).encode("utf-8"))
        return digest.hexdigest()
    return _compute_file_sha256(path)


def _copy_file_atomic(source: Path, destination: Path) -> bool:
    fd, tmp_path = tempfile.mkstemp(dir=str(destination.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        shutil.copy2(source, tmp_path_obj)
        return _link_file_atomic(tmp_path_obj, destination)
    finally:
        with suppress(FileNotFoundError):
            tmp_path_obj.unlink()


def _link_file_atomic(source: Path, destination: Path) -> bool:
    try:
        os.link(source, destination)
    except FileExistsError:
        return False
    return True


def _copy_dir_safe(source: Path, destination: Path) -> bool:
    try:
        destination.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        return False

    try:
        shutil.copytree(source, destination, dirs_exist_ok=True)
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    return True


def _copy_dir_to_temp(source: Path, parent: Path) -> Path:
    temporary_root = Path(tempfile.mkdtemp(dir=str(parent), prefix=".consist-dir-"))
    tmp_destination = temporary_root / "payload"
    shutil.copytree(source, tmp_destination, dirs_exist_ok=False)
    return tmp_destination


def _cleanup_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    with suppress(FileNotFoundError):
        path.unlink()


def _replace_path(source: Path, destination: Path) -> None:
    backup_path: Path | None = None
    backup_root: Path | None = None
    if destination.exists():
        backup_root = Path(
            tempfile.mkdtemp(
                dir=str(destination.parent),
                prefix=f".consist-backup-{destination.name}-",
            )
        )
        backup_path = backup_root / "payload"
        destination.rename(backup_path)

    try:
        os.replace(source, destination)
    except Exception:
        if backup_path is not None and backup_path.exists():
            os.replace(backup_path, destination)
        raise
    else:
        if backup_root is not None:
            _cleanup_path(backup_root)


def _materialize_path(
    *,
    source: Path,
    destination: Path,
    preserve_existing: bool,
) -> tuple[bool, bool]:
    """
    Return ``(materialized, skipped_existing)`` for one filesystem operation.
    """
    _ensure_destination_not_symlink(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if source == destination:
        return True, False

    if destination.exists():
        if preserve_existing:
            if source.is_dir() != destination.is_dir():
                raise ValueError(
                    f"Destination type mismatch for {destination}; refusing to overwrite."
                )
            return False, True

        if destination.is_symlink():
            raise ValueError(f"Symlink detected in destination path: {destination}")

    if preserve_existing:
        if source.is_dir():
            copied = _copy_dir_safe(source, destination)
        else:
            copied = _copy_file_atomic(source, destination)
        if copied or destination.exists():
            return True, False
        return False, True

    if source.is_dir():
        tmp_copy = _copy_dir_to_temp(source, destination.parent)
        _replace_path(tmp_copy, destination)
        _cleanup_path(tmp_copy.parent)
        return True, False

    fd, tmp_path = tempfile.mkstemp(dir=str(destination.parent))
    os.close(fd)
    tmp_path_obj = Path(tmp_path)
    try:
        shutil.copy2(source, tmp_path_obj)
        _replace_path(tmp_path_obj, destination)
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
    artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}
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
        parent.meta if parent is not None and isinstance(parent.meta, dict) else {}
    )
    if parent is not None and (
        parent.driver == "artifact_set" or parent_meta.get("artifact_set") is True
    ):
        return "member"
    return None


def _derive_historical_remap(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    run,
) -> tuple[Path, Path] | None:
    """Derive a recoverable historical root and URI-relative artifact path.

    Parameters
    ----------
    tracker : Tracker
        Tracker that owns filesystem URI policy.
    artifact : Artifact
        Artifact whose canonical URI is being remapped.
    run : Run
        Historical producing run containing mount metadata.

    Returns
    -------
    tuple[Path, Path] | None
        Historical root and safe relative path, or ``None`` when unmappable.
    """
    meta = run.meta if isinstance(run.meta, dict) else {}
    artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}

    return tracker.fs.get_historical_remap(
        artifact.container_uri,
        original_run_dir=meta.get("_physical_run_dir"),
        mounts_snapshot=meta.get("mounts"),
        artifact_mount_root=artifact_meta.get("mount_root"),
    )


def _derive_remappable_relative_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
) -> Path | None:
    """Derive the safe URI-relative layout for an artifact.

    Parameters
    ----------
    tracker : Tracker
        Tracker that owns filesystem URI policy.
    artifact : Artifact
        Artifact whose canonical URI is inspected.

    Returns
    -------
    Path | None
        Portable relative path, or ``None`` for an unmappable URI.
    """
    return tracker.fs.get_remappable_relative_path(artifact.container_uri)


def _derive_historical_root(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    run,
    preserve_raw_paths: bool = False,
) -> Path | None:
    """Derive the historical root that can supply artifact bytes.

    Parameters
    ----------
    tracker : Tracker
        Tracker that owns filesystem URI policy.
    artifact : Artifact
        Artifact whose canonical URI is inspected.
    run : Run
        Historical producing run containing mount metadata.
    preserve_raw_paths : bool, default False
        Whether to retain path components for later symlink validation.

    Returns
    -------
    Path | None
        Historical root, or ``None`` when no remappable root is recorded.
    """
    meta = run.meta if isinstance(run.meta, dict) else {}
    artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}

    return tracker.fs.get_historical_root(
        artifact.container_uri,
        original_run_dir=meta.get("_physical_run_dir"),
        mounts_snapshot=meta.get("mounts"),
        artifact_mount_root=artifact_meta.get("mount_root"),
        resolve=not preserve_raw_paths,
    )


def _artifact_recovery_roots(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    preserve_raw_paths: bool = False,
) -> tuple[Path, ...]:
    """Return ordered recovery roots recorded for an artifact.

    Parameters
    ----------
    tracker : Tracker
        Tracker that normalizes recovery-root metadata.
    artifact : Artifact
        Artifact whose advisory roots are requested.
    preserve_raw_paths : bool, default False
        Whether to retain path components for later symlink validation.

    Returns
    -------
    tuple[Path, ...]
        Normalized recovery roots in metadata order.
    """
    artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    return tuple(
        Path(root)
        for root in tracker.fs.normalize_recovery_roots(
            artifact_meta.get("recovery_roots"), resolve=not preserve_raw_paths
        )
    )


def _ordered_candidate_roots(
    *,
    source_root: Path | None,
    historical_root: Path | None,
    recovery_roots: Sequence[Path],
    preserve_raw_paths: bool = False,
) -> tuple[Path, ...]:
    """Order and deduplicate recovery source roots.

    Parameters
    ----------
    source_root : Path | None
        Caller-supplied root, with highest precedence.
    historical_root : Path | None
        Root recorded with the producing run.
    recovery_roots : sequence of Path
        Advisory per-artifact fallback roots.
    preserve_raw_paths : bool, default False
        Whether to avoid resolving symlinks during normalization.

    Returns
    -------
    tuple[Path, ...]
        Distinct roots in lookup order.
    """
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
    preserve_raw_paths: bool = False,
) -> tuple[Path | None, Path | None, bool]:
    """Find the first valid filesystem source for historical recovery.

    Parameters
    ----------
    tracker : Tracker
        Tracker that owns filesystem URI and recovery-root policy.
    artifact : Artifact
        Historical artifact whose bytes are being located.
    run : Run
        Producing run used to derive historical roots.
    source_root : Path | None
        Optional caller-supplied root tried before recorded locations.
    source_validator : callable, optional
        Predicate applied to existing candidates. False candidates are skipped;
        exceptions are propagated to the caller.
    preserve_raw_paths : bool, default False
        Whether the validator should receive unresolved candidate paths for
        caller-owned symlink validation.

    Returns
    -------
    tuple[Path | None, Path | None, bool]
        URI-relative path, existing source path, and whether any source roots
        were available to probe. The first two values are ``None`` for an
        unmappable artifact.

    Notes
    -----
    Candidates are probed in order: ``source_root``, historical run/mount root,
    then the artifact's ordered ``recovery_roots``.
    """
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
    for root in candidate_roots:
        candidate = root / relative_path
        validator_path = candidate if preserve_raw_paths else candidate.resolve()
        if candidate.exists() and (
            source_validator is None or source_validator(validator_path)
        ):
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

    uri = artifact.container_uri
    if uri.startswith("workspace://") or uri.startswith("./"):
        candidate = Path(tracker.run_dir) / relative_path
        if candidate.exists():
            return candidate

    if "://" in uri:
        scheme, _ = uri.split("://", 1)
        mounts = tracker.fs.mounts
        if scheme in mounts:
            candidate = Path(mounts[scheme]) / relative_path
            if candidate.exists():
                return candidate

    try:
        candidate = Path(tracker.resolve_uri(artifact.container_uri)).resolve()
    except (OSError, ValueError, AttributeError):
        return None
    if candidate.exists():
        return candidate
    return None


def _select_materialize_artifact_source(
    tracker: "Tracker",
    *,
    artifact: Artifact,
    relative_path: Path,
    source_root: Path | None,
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
]:
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

    seen_roots: set[str] = set()
    for source_kind, root in candidate_roots:
        resolved_root = Path(root).resolve()
        root_key = str(resolved_root)
        if root_key in seen_roots:
            continue
        seen_roots.add(root_key)
        candidate = resolved_root / relative_path
        if candidate.exists():
            return candidate, source_kind, True

    direct_source = _direct_artifact_source_path(
        tracker,
        artifact=artifact,
        relative_path=relative_path,
        allow_tracker_uri=not bool(artifact.run_id),
    )
    if direct_source is not None:
        return direct_source, "artifact_uri", bool(candidate_roots)

    return None, None, bool(candidate_roots)


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


def _portable_artifact_hash(tracker: "Tracker", artifact: Artifact) -> str | None:
    if tracker.identity.hashing_strategy != "full":
        return None
    return artifact.hash


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
) -> MaterializedArtifact:
    """Materialize one artifact into a target root using recovery metadata.

    Parameters
    ----------
    tracker : Tracker
        Tracker used for source discovery and policy checks.
    artifact : Artifact
        Artifact to materialize.
    target_root : Path
        Root below which its URI-relative path is recreated.
    source_root : Path | None
        Optional source root tried before recorded recovery locations.
    allowed_base : Path, sequence of Path, or None
        Allowed destination root or roots; ``None`` disables that check.
    preserve_existing : bool
        Whether an existing compatible destination can be retained.
    on_missing : {"warn", "raise"}
        Policy for unavailable bytes and materialization failures.
    validate_content_hash : {"never", "if-present", "always"}
        Content-validation policy for source and destination bytes.

    Returns
    -------
    MaterializedArtifact
        Detached artifact, source/target paths, and a recovery outcome.
    """
    if validate_content_hash not in {"never", "if-present", "always"}:
        raise ValueError(
            "validate_content_hash must be one of 'never', 'if-present', or 'always'."
        )

    if artifact.driver == "h5_table":
        parent = tracker.get_parent_artifact(artifact)
        policy_validation = validate_recovery_registration_policy(
            artifact,
            parent=parent,
        )
        if not policy_validation.allowed:
            return _make_materialized_artifact(
                artifact,
                status="blocked_by_container_policy",
                path=None,
                source_path=None,
                target_path=None,
                message=policy_validation.message,
                resolvable=False,
            )

    relative_path = _derive_remappable_relative_path(tracker, artifact=artifact)
    if relative_path is None:
        return _make_materialized_artifact(
            artifact,
            status="skipped_unmapped",
            path=None,
            source_path=None,
            target_path=None,
            message=(
                f"Artifact {artifact.key!r} does not have a rematerializable URI layout."
            ),
            resolvable=False,
        )

    raw_destination = target_root / relative_path
    if raw_destination.is_symlink():
        msg = f"Symlink detected in destination path: {raw_destination.resolve()}"
        if on_missing == "raise":
            raise ValueError(msg)
        return _make_materialized_artifact(
            artifact,
            status="symlink_destination",
            path=None,
            source_path=None,
            target_path=raw_destination,
            message=msg,
            resolvable=False,
        )

    destination = raw_destination.resolve()
    try:
        validate_allowed_materialization_destination(destination, allowed_base)
    except ValueError as exc:
        if on_missing == "raise":
            raise
        return _make_materialized_artifact(
            artifact,
            status="failed",
            path=None,
            source_path=None,
            target_path=destination,
            message=str(exc),
            resolvable=False,
        )

    source_path, source_kind, _has_candidate_roots = (
        _select_materialize_artifact_source(
            tracker,
            artifact=artifact,
            relative_path=relative_path,
            source_root=source_root,
        )
    )

    expected_hash = _portable_artifact_hash(tracker, artifact)
    if validate_content_hash == "always" and not expected_hash:
        return _make_materialized_artifact(
            artifact,
            status="failed",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=(
                f"Cannot validate artifact {artifact.key!r}: no portable content hash "
                "is available."
            ),
            resolvable=False,
        )

    should_validate = bool(expected_hash) and validate_content_hash != "never"

    if destination.exists() and preserve_existing:
        if destination.is_dir():
            return _make_materialized_artifact(
                artifact,
                status="unsupported_directory",
                path=None,
                source_path=source_path,
                target_path=destination,
                message=(
                    "Directory artifact materialization is not supported yet; "
                    "directory manifest support is required for safe recovery."
                ),
                resolvable=False,
            )
        if should_validate and _compute_file_sha256(destination) != expected_hash:
            return _make_materialized_artifact(
                artifact,
                status="hash_mismatch",
                path=None,
                source_path=source_path,
                target_path=destination,
                message=(
                    f"Existing destination hash does not match artifact {artifact.key!r}."
                ),
                resolvable=False,
            )
        return _make_materialized_artifact(
            artifact,
            status="already_present",
            path=destination,
            source_path=source_path,
            target_path=destination,
            resolvable=True,
        )

    if source_path is None or source_kind is None:
        msg = f"No source bytes found for artifact {artifact.key!r}."
        if on_missing == "raise":
            raise FileNotFoundError(msg)
        return _make_materialized_artifact(
            artifact,
            status="missing_source",
            path=None,
            source_path=None,
            target_path=destination,
            message=msg,
            resolvable=False,
        )

    if _path_has_symlink_component(source_path):
        msg = f"Symlink detected in source path: {source_path}"
        if on_missing == "raise":
            raise ValueError(msg)
        return _make_materialized_artifact(
            artifact,
            status="failed",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=msg,
            resolvable=False,
        )

    if source_path.is_dir():
        return _make_materialized_artifact(
            artifact,
            status="unsupported_directory",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=(
                "Directory artifact materialization is not supported yet; "
                "directory manifest support is required for safe recovery."
            ),
            resolvable=False,
        )

    if not source_path.is_file():
        msg = f"Source path is not a regular file: {source_path}"
        if on_missing == "raise":
            raise RuntimeError(msg)
        return _make_materialized_artifact(
            artifact,
            status="failed",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=msg,
            resolvable=False,
        )

    if should_validate and _compute_file_sha256(source_path) != expected_hash:
        return _make_materialized_artifact(
            artifact,
            status="hash_mismatch",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=f"Source hash does not match artifact {artifact.key!r}.",
            resolvable=False,
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
        return _make_materialized_artifact(
            artifact,
            status="failed",
            path=None,
            source_path=source_path,
            target_path=destination,
            message=str(exc),
            resolvable=False,
        )

    if skipped_existing:
        return _make_materialized_artifact(
            artifact,
            status="already_present",
            path=destination,
            source_path=source_path,
            target_path=destination,
            resolvable=True,
        )
    if materialized:
        return _make_materialized_artifact(
            artifact,
            status=_materialized_status_for_source(source_kind),
            path=destination,
            source_path=source_path,
            target_path=destination,
            resolvable=True,
        )

    msg = f"Failed to materialize artifact {artifact.key!r} to {destination}."
    if on_missing == "raise":
        raise RuntimeError(msg)
    return _make_materialized_artifact(
        artifact,
        status="failed",
        path=None,
        source_path=source_path,
        target_path=destination,
        message=msg,
        resolvable=False,
    )


def _source_identity(item: PlannedMaterialization) -> tuple[str, str]:
    if item.source_kind == "filesystem":
        assert item.source_path is not None
        return ("filesystem", str(item.source_path.resolve()))
    return ("db_export", str(item.artifact.id))


def _resolve_staging_source_path(
    tracker: "Tracker",
    *,
    artifact: Artifact,
) -> Path | None:
    canonical_path: Path | None = None
    try:
        candidate = Path(tracker.resolve_uri(artifact.container_uri)).resolve()
        if candidate.exists():
            canonical_path = candidate
    except (OSError, ValueError, AttributeError):
        canonical_path = None

    if canonical_path is not None:
        return canonical_path

    runtime_abs_path = artifact.abs_path
    if runtime_abs_path:
        candidate = Path(runtime_abs_path).resolve()
        if candidate.exists():
            return candidate

    relative_path = tracker.fs.get_remappable_relative_path(artifact.container_uri)
    if relative_path is None:
        return None

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
                return source

    for root in _artifact_recovery_roots(tracker, artifact=artifact):
        candidate = (root / relative_path).resolve()
        if candidate.exists():
            return candidate
    return None


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

    try:
        _ensure_destination_not_symlink(destination_path)
        destination_path = destination_path.resolve()
        validate_allowed_materialization_destination(destination_path, allowed_base)

        source_path = _resolve_staging_source_path(tracker, artifact=artifact)
        if source_path is None or not source_path.exists():
            msg = f"[Consist] Cannot stage artifact {artifact.key!r}: source path missing."
            logging.warning(msg)
            return _make_staged_output(
                artifact,
                status="missing_source",
                path=destination_path,
                message=msg,
                resolvable=False,
            )

        if validate_content_hash not in {"never", "if-present", "always"}:
            raise ValueError(
                "validate_content_hash must be one of 'never', 'if-present', or 'always'."
            )

        if validate_content_hash == "always" and not artifact.hash:
            msg = f"[Consist] Cannot validate artifact {artifact.key!r}: artifact.hash is missing."
            logging.warning(msg)
            return _make_staged_output(
                artifact,
                status="failed",
                path=destination_path,
                message=msg,
                resolvable=False,
            )

        source_hash: str | None = None
        if artifact.hash and validate_content_hash != "never":
            source_hash = _compute_path_checksum(source_path)
            if source_hash != artifact.hash:
                msg = (
                    f"[Consist] Content hash mismatch while staging {artifact.key!r}: "
                    f"expected {artifact.hash}, found {source_hash}."
                )
                logging.warning(msg)
                return _make_staged_output(
                    artifact,
                    status="failed",
                    path=destination_path,
                    message=msg,
                    resolvable=False,
                )

        if source_path == destination_path:
            return _make_staged_output(
                artifact,
                status="preserved_existing",
                path=destination_path,
                resolvable=True,
            )

        if destination_path.exists():
            if destination_path.is_symlink():
                msg = f"Symlink detected in destination path: {destination_path}"
                logging.warning("[Consist] %s", msg)
                return _make_staged_output(
                    artifact,
                    status="failed",
                    path=destination_path,
                    message=msg,
                    resolvable=False,
                )
            if source_path.is_dir() != destination_path.is_dir():
                msg = f"Destination type mismatch for {destination_path}; refusing to overwrite."
                logging.warning("[Consist] %s", msg)
                return _make_staged_output(
                    artifact,
                    status="failed",
                    path=destination_path,
                    message=msg,
                    resolvable=False,
                )

            if not overwrite:
                if source_hash is None:
                    source_hash = _compute_path_checksum(source_path)
                destination_hash = _compute_path_checksum(destination_path)
                if source_hash == destination_hash:
                    return _make_staged_output(
                        artifact,
                        status="preserved_existing",
                        path=destination_path,
                        resolvable=True,
                    )

                msg = f"Destination already exists and differs for {destination_path}; "
                msg += "refusing to overwrite."
                logging.warning("[Consist] %s", msg)
                return _make_staged_output(
                    artifact,
                    status="failed",
                    path=destination_path,
                    message=msg,
                    resolvable=False,
                )

        materialized, skipped_existing = _materialize_path(
            source=source_path,
            destination=destination_path,
            preserve_existing=False,
        )
        if skipped_existing:
            return _make_staged_output(
                artifact,
                status="preserved_existing",
                path=destination_path,
                resolvable=True,
            )
        if materialized:
            return _make_staged_output(
                artifact,
                status="staged",
                path=destination_path,
                resolvable=True,
            )

        msg = f"Failed to stage artifact {artifact.key!r} to {destination_path}."
        logging.warning("[Consist] %s", msg)
        return _make_staged_output(
            artifact,
            status="failed",
            path=destination_path,
            message=msg,
            resolvable=False,
        )
    except (OSError, shutil.Error, RuntimeError, ValueError) as exc:
        msg = f"[Consist] Failed to stage artifact {artifact.key!r} to {destination_path}: {exc}"
        logging.warning(msg)
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
    """Stage explicit artifact/destination pairs into a keyed result.

    Parameters
    ----------
    tracker : Tracker
        Tracker used for source discovery and destination policy.
    items : sequence of (str, Artifact, Path)
        Requested key, resolved artifact, and exact staging destination.
    mode : {"copy", "hardlink", "symlink"}, default "copy"
        Requested operation; only ``"copy"`` is currently supported.
    overwrite : bool, default False
        Whether existing destinations may be replaced.
    validate_content_hash : {"never", "if-present", "always"}, default "if-present"
        Content-validation policy for staged bytes.
    allow_external_paths : bool | None, optional
        Override for the tracker's external-destination policy.

    Returns
    -------
    StagedInputsResult
        Per-key staging outcomes in item order.
    """
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
    """Plan historical output hydration for a run without mutating the target.

    The planner resolves which artifacts belong to the requested keys, derives
    their historical relative paths, chooses a recovery source for each output,
    and emits preflight results for outcomes that do not need execution.

    Preflight outcomes include:

    - unknown keys (raised immediately)
    - ambiguous duplicate output keys on the run (raised immediately)
    - unmapped historical outputs
    - destination collisions
    - ``preserve_existing=True`` short-circuits

    Parameters
    ----------
    tracker : Tracker
        Tracker providing DB access, filesystem remapping, and mount policy.
    run
        Historical run record whose outputs are being restored.
    target_root : Path | None
        Root directory under which historical relative layout is recreated.
        Mutually exclusive with ``destinations_by_key``.
    destinations_by_key : Mapping[str, Path] | None
        Exact destination for each requested output key. Its keys define the
        complete requested-output selection.
    source_root : Path | None
        Optional override root to probe before the original historical source.
    keys : Sequence[str] | None
        Specific output keys to restore. ``None`` means all run outputs.
    preserve_existing : bool
        Whether existing destinations should be treated as reusable.
    db_fallback : {"never", "if_ingested"}
        Whether ingested CSV/Parquet artifacts may be exported from DuckDB when
        cold bytes are unavailable.

    Returns
    -------
    PlannedRunOutputHydration
        Executable hydration work and preflight per-key outcomes.

    Raises
    ------
    ValueError
        If destination selection is invalid or output keys are ambiguous.
    KeyError
        If a requested output key is unknown.
    """
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
        if _is_directory_artifact(artifact) and db_fallback == "never":
            _directory_artifact_manifest(artifact)
            relative_path = tracker.fs.get_remappable_relative_path(
                artifact.container_uri
            )
            source_path = (
                Path(source_root) / relative_path
                if source_root is not None and relative_path is not None
                else None
            )
            has_candidate_roots = relative_path is not None
        elif _is_file_bundle_artifact(artifact) and db_fallback == "never":
            _shapefile_bundle_metadata(artifact)
            relative_path = tracker.fs.get_remappable_relative_path(
                artifact.container_uri
            )
            source_path = (
                Path(source_root) / relative_path
                if source_root is not None and relative_path is not None
                else None
            )
            has_candidate_roots = relative_path is not None
        else:
            owning_run = _get_artifact_owning_run(
                tracker, selected_run=run, artifact=artifact
            )
            relative_path, source_path, has_candidate_roots = (
                find_existing_recovery_source_path(
                    tracker,
                    artifact=artifact,
                    run=owning_run,
                    source_root=source_root,
                )
            )
            if _is_file_bundle_artifact(artifact) and source_path is not None:
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
            artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}
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
                    status="missing_source",
                    path=destination,
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
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status="failed",
                        path=item.destination,
                        message=f"Symlink detected in destination path: {item.destination}",
                        resolvable=False,
                    )
            else:
                for key in item.keys:
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
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
    """Build the legacy materialization plan and aggregate preflight result.

    Parameters
    ----------
    tracker : Tracker
        Tracker used for output discovery and source planning.
    run : Run
        Historical run whose outputs should be materialized.
    target_root : Path
        Root below which canonical output paths are recreated.
    source_root : Path | None
        Optional source root tried before recorded sources.
    keys : sequence of str | None
        Requested output keys; ``None`` selects all outputs.
    preserve_existing : bool
        Whether existing targets are retained during planning.
    db_fallback : {"never", "if_ingested"}
        Whether eligible ingested data may be exported from the DB.

    Returns
    -------
    tuple[list[PlannedMaterialization], MaterializationResult]
        Executable plan and compatibility aggregate for preflight results.
    """
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
    """Execute a planned historical output hydration.

    Parameters
    ----------
    plan : Sequence[PlannedMaterialization]
        Planned filesystem copies or DuckDB exports to execute.
    tracker : Tracker
        Tracker used for DB exports and path policy.
    allowed_base : Path | Sequence[Path] | None
        Allowed destination roots. ``None`` disables destination-root checks.
    on_missing : {"warn", "raise"}, default "warn"
        Error policy for missing source bytes and execution failures.
    preserve_existing : bool, default True
        Whether existing destinations should be kept in place.

    Returns
    -------
    HydratedRunOutputsResult
        Per-key execution outcomes for the plan. Preflight-only results are not
        included; callers merge them separately.
    """
    outputs: dict[str, HydratedRunOutput] = {}

    for item in plan:
        try:
            _validate_allowed_base(item.destination, allowed_base)

            if _is_directory_artifact(item.artifact):
                if item.source_path is None or not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                manifest = _directory_artifact_manifest(item.artifact)
                materialized = materialize_directory_tree(
                    item.source_path,
                    item.destination,
                    manifest,
                    preserve_existing=preserve_existing,
                )
                status: HydrationStatus = (
                    "materialized_directory_from_filesystem"
                    if materialized
                    else "preserved_existing"
                )
                for key in item.keys:
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status=status,
                        path=item.destination,
                        resolvable=True,
                    )
                continue

            if _is_file_bundle_artifact(item.artifact):
                if item.source_path is None or not item.source_path.exists():
                    raise FileNotFoundError(f"source path missing ({item.source_path})")
                entry, manifest = _shapefile_bundle_metadata(item.artifact)
                materialized = materialize_shapefile_bundle(
                    item.source_path,
                    item.destination,
                    entry,
                    manifest,
                    preserve_existing=preserve_existing,
                )
                status: HydrationStatus = (
                    "materialized_file_bundle_from_filesystem"
                    if materialized
                    else "preserved_existing"
                )
                for key in item.keys:
                    artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                    outputs[key] = _make_hydrated_output(
                        artifact,
                        status=status,
                        path=item.destination,
                        entry_path=item.destination / entry,
                        resolvable=True,
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
                    for key in item.keys:
                        artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[
                            key
                        ]
                        outputs[key] = _make_hydrated_output(
                            artifact,
                            status="preserved_existing",
                            path=item.destination,
                            resolvable=True,
                        )
                    continue
                if materialized:
                    for key in item.keys:
                        artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[
                            key
                        ]
                        outputs[key] = _make_hydrated_output(
                            artifact,
                            status="materialized_from_filesystem",
                            path=item.destination,
                            resolvable=True,
                        )
                continue

            destination_str = materialize_ingested_artifact_from_db(
                artifact=item.artifact,
                tracker=tracker,
                destination=item.destination,
                overwrite=not preserve_existing,
            )
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="materialized_from_db",
                    path=Path(destination_str),
                    resolvable=True,
                )

        except FileNotFoundError as exc:
            if on_missing == "raise":
                raise
            logging.warning(
                "[Consist] Cannot materialize run output %s: %s",
                ",".join(repr(key) for key in item.keys),
                exc,
            )
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="missing_source",
                    path=item.destination,
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
            for key in item.keys:
                artifact = cast(Mapping[str, Artifact], item.artifacts_by_key)[key]
                outputs[key] = _make_hydrated_output(
                    artifact,
                    status="failed",
                    path=item.destination,
                    message=str(exc),
                    resolvable=False,
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
    """Execute a plan and fold outcomes into ``MaterializationResult``.

    Parameters
    ----------
    plan : sequence of PlannedMaterialization
        Filesystem copies or DB exports to execute.
    tracker : Tracker
        Tracker used for DB exports and path policy.
    allowed_base : Path, sequence of Path, or None
        Allowed destination roots; ``None`` disables that check.
    on_missing : {"warn", "raise"}, default "warn"
        Policy for unavailable source bytes and execution errors.
    preserve_existing : bool, default True
        Whether existing destinations should be retained.

    Returns
    -------
    MaterializationResult
        Compatibility aggregate grouped by materialization status.
    """
    return fold_hydrated_run_outputs_result(
        execute_planned_output_hydration(
            plan,
            tracker=tracker,
            allowed_base=allowed_base,
            on_missing=on_missing,
            preserve_existing=preserve_existing,
        )
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
    """Hydrate selected historical outputs into a target workspace root.

    Parameters
    ----------
    tracker : Tracker
        Tracker used for planning, source discovery, and execution.
    run : Run
        Historical run whose outputs are being hydrated.
    target_root : Path
        Root below which canonical output paths are recreated.
    source_root : Path | None
        Optional source root tried before recorded recovery sources.
    keys : sequence of str | None
        Requested output keys; ``None`` selects all outputs.
    allowed_base : Path, sequence of Path, or None
        Allowed destination roots; ``None`` disables that check.
    preserve_existing : bool
        Whether existing destinations should be retained.
    on_missing : {"warn", "raise"}
        Policy for unavailable bytes and execution failures.
    db_fallback : {"never", "if_ingested"}
        Whether eligible ingested outputs may be rebuilt from the DB.

    Returns
    -------
    HydratedRunOutputsResult
        Per-key results with detached artifacts whose ``abs_path`` points to a
        resolvable hydrated destination.
    """
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
    """Hydrate selected outputs to exact caller-provided destinations.

    Parameters
    ----------
    tracker : Tracker
        Tracker used for planning, source discovery, and execution.
    run : Run
        Historical run whose outputs are being hydrated.
    destinations_by_key : mapping of str to Path
        Exact destination for each requested output key.
    source_root : Path | None
        Optional source root tried before recorded recovery sources.
    allowed_base : Path, sequence of Path, or None
        Allowed destination roots; ``None`` disables that check.
    preserve_existing : bool
        Whether existing destinations should be retained.
    on_missing : {"warn", "raise"}
        Policy for unavailable bytes and execution failures.
    db_fallback : {"never", "if_ingested"}
        Whether eligible ingested outputs may be rebuilt from the DB.

    Returns
    -------
    HydratedRunOutputsResult
        Per-key results using the supplied exact destinations.
    """
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
    """
    Synchronize cached artifact bytes to specific filesystem destinations.

    This function performs physical materialization by copying the resolved source
    data of cached artifacts to the requested locations. It ensures atomic
    file writes and safe directory recursion. If the destination already contains
    matching data, the operation is bypassed to prevent redundant I/O.

    Parameters
    ----------
    tracker : Tracker
        The active Tracker instance used to resolve virtualized artifact URIs
        to physical host filesystem paths.
    items : Sequence[tuple[Artifact, Path]]
        A collection of (Artifact, Path) pairs defining the source artifacts
        and their respective target destinations.
    on_missing : {"warn", "raise"}, default "warn"
        The error handling policy for cases where the resolved source path
        is absent from the filesystem.

    Returns
    -------
    dict[str, str]
        A mapping of artifact keys to their successfully materialized
        absolute filesystem paths.
    """
    materialized: dict[str, str] = {}

    for artifact, destination in items:
        _ensure_destination_not_symlink(Path(destination))
        destination_path = Path(destination).resolve()
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            source_path = Path(tracker.resolve_uri(artifact.container_uri)).resolve()
        except (OSError, ValueError) as e:
            msg = f"[Consist] Failed to resolve URI for artifact {artifact.key!r}: {e}"
            if on_missing == "raise":
                raise RuntimeError(msg) from e
            logging.warning(msg)
            continue

        if not source_path.exists():
            msg = (
                f"[Consist] Cannot materialize cached output {artifact.key!r}: "
                f"resolved source path missing ({source_path})."
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
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path == destination_path:
                materialized[artifact.key] = str(destination_path)
                continue
            if source_path.is_dir():
                copied = _copy_dir_safe(source_path, destination_path)
            else:
                copied = _copy_file_atomic(source_path, destination_path)
            if copied:
                materialized[artifact.key] = str(destination_path)
            elif destination_path.exists():
                materialized[artifact.key] = str(destination_path)
        except (OSError, shutil.Error, ValueError) as e:
            msg = (
                f"[Consist] Failed to materialize cached output {artifact.key!r} "
                f"from {source_path} -> {destination_path}: {e}"
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
    """
    Rehydrate artifacts from explicit source paths to specified destinations.

    This specialized materialization utility is typically employed during
    cross-run hydration (e.g., 'inputs-missing' mode) where the original
    on-disk source is located in a historical run directory. It enforces
    directory containment security via the `allowed_base` parameter.

    Parameters
    ----------
    items : Sequence[tuple[Artifact, Path, Path]]
        A collection of (Artifact, SourcePath, DestinationPath) triples
        defining the explicit rehydration mapping.
    allowed_base : Path | None
        A security boundary for the materialization. If provided, all
        destination paths must reside within this directory tree.
    on_missing : {"warn", "raise"}, default "warn"
        The error handling policy for cases where the explicit source path
        is absent from the filesystem.
    overwrite_existing : bool, default False
        If True, replace existing destinations after safety checks pass.

    Returns
    -------
    dict[str, str]
        A mapping of artifact keys to their successfully materialized
        absolute filesystem paths.
    """
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
            if destination_path.exists():
                if not overwrite_existing:
                    materialized[artifact.key] = str(destination_path)
                    continue
                if destination_path.is_dir():
                    shutil.rmtree(destination_path)
                else:
                    destination_path.unlink()
            if source_path.is_dir():
                copied = _copy_dir_safe(source_path, destination_path)
            else:
                copied = _copy_file_atomic(source_path, destination_path)
            if copied:
                materialized[artifact.key] = str(destination_path)
            elif destination_path.exists():
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
    """
    Construct materialization mappings by correlating artifacts with target keys.

    This helper facilitates the preparation of materialization payloads by
    matching a collection of candidate artifacts against a set of desired
    destination keys.

    Parameters
    ----------
    outputs : Iterable[Artifact]
        The collection of candidate artifacts available for materialization.
    destinations_by_key : dict[str, Path]
        A mapping of artifact keys to their intended target filesystem paths.

    Returns
    -------
    list[tuple[Artifact, Path]]
        A list of (Artifact, Path) pairs ready for the `materialize_artifacts`
        utility.
    """
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
    """Stage one artifact to a requested filesystem destination.

    Parameters
    ----------
    tracker : Tracker
        Tracker used to resolve canonical paths, allowed roots, and recovery
        fallbacks.
    artifact : Artifact
        Canonical input artifact to stage.
    destination : Path
        Explicit local destination where the staged copy should exist.
    mode : {"copy", "hardlink", "symlink"}, default "copy"
        Staging mode. In v1, only ``"copy"`` is supported.
    overwrite : bool, default False
        Whether to replace an existing non-identical destination.
    validate_content_hash : {"never", "if-present", "always"}, default "if-present"
        Whether to validate staged file bytes against ``artifact.hash`` when a
        file hash is available.
    allow_external_paths : bool | None, default None
        Override for the tracker's external-path policy.

    Returns
    -------
    StagedInput
        Detached artifact view plus the per-key staging outcome.
    """
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
    """Stage multiple keyed artifacts to their requested destinations.

    Parameters
    ----------
    tracker : Tracker
        Tracker used to resolve canonical paths, allowed roots, and recovery
        fallbacks.
    inputs_by_key : Mapping[str, Artifact]
        Resolved input artifact mapping, typically the same key space later
        used for ``inputs={...}`` on ``run(...)`` or ``ScenarioContext.run(...)``.
    destinations_by_key : Mapping[str, Path]
        Explicit staging destinations keyed by input name.
    mode : {"copy", "hardlink", "symlink"}, default "copy"
        Staging mode. In v1, only ``"copy"`` is supported.
    overwrite : bool, default False
        Whether to replace an existing non-identical destination.
    validate_content_hash : {"never", "if-present", "always"}, default "if-present"
        Whether to validate staged file bytes against ``artifact.hash`` when a
        file hash is available.
    allow_external_paths : bool | None, default None
        Override for the tracker's external-path policy.

    Returns
    -------
    StagedInputsResult
        Key-indexed staging outcomes in the caller's requested key order.
    """
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


def materialize_ingested_artifact_from_db(
    *,
    artifact: Artifact,
    tracker: "Tracker",
    destination: Path,
    overwrite: bool = False,
) -> str:
    """
    Reconstruct an ingested artifact from the analytical database.

    This recovery mechanism is utilized when a cached artifact is required but
    its physical materialization has been purged from the filesystem. If the
    artifact was previously ingested, its data is exported from DuckDB to
    the specified destination.

    Parameters
    ----------
    artifact : Artifact
        The metadata record of the artifact to be reconstructed.
    tracker : Tracker
        The active Tracker instance containing the analytical engine and
        database connection.
    destination : Path
        The target filesystem path where the reconstructed data will be
        materialized.

    Returns
    -------
    str
        The absolute filesystem path of the reconstructed artifact.

    Raises
    ------
    RuntimeError
        If the database engine is unavailable or the source table cannot be resolved.
    ValueError
        If the artifact is not marked as ingested or uses an unsupported
        materialization driver.
    """
    hot_data_engine = get_hot_data_engine(tracker)
    if not tracker or hot_data_engine is None:
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

    metadata = MetaData()
    try:
        table = Table(
            table_name,
            metadata,
            schema="global_tables",
            autoload_with=hot_data_engine,
        )
    except SQLAlchemyError as e:
        raise RuntimeError(
            f"Failed to reflect table 'global_tables.{table_name}': {e}"
        ) from e

    if "consist_artifact_id" not in table.c:
        raise RuntimeError(
            f"Table 'global_tables.{table_name}' is missing consist_artifact_id."
        )

    stmt = select(table).where(table.c.consist_artifact_id == str(artifact.id))
    df = pd.read_sql(stmt, hot_data_engine)
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
            _replace_path(tmp_path_obj, destination_path)
        else:
            if not _link_file_atomic(tmp_path_obj, destination_path):
                return str(destination_path)
    finally:
        with suppress(FileNotFoundError):
            tmp_path_obj.unlink()

    return str(destination_path)
