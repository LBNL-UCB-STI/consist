"""
Verify external files, directories, and containers against prior-run or declared digests.

Admission always uses full content-addressed scheme drivers from ``consist.digest``
and returns a policy-neutral ``AdmissionReport``.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from consist.core.logical_array_identity import LogicalProfileName
from consist.core.persistence import DatabaseManager
from consist.digest import (
    Digest,
    compute_digest,
    logical_matrices_scheme_for_policy,
    normalize_logical_matrices_identity,
)
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ADMISSION_REPORT_SCHEMA_VERSION = 3
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_GIT_LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1\n"

_LOGICAL_MATRIX_SCHEMES = frozenset(
    {
        "logical_matrices_canonical_v1:sha256",
        "logical_matrices_f32_v1:sha256",
    }
)
_SUPPORTED_ADMISSION_SCHEMES = frozenset(
    {
        "sha256:file",
        "dir_manifest_v1:sha256",
        "logical_matrices_canonical_v1:sha256",
        "logical_matrices_f32_v1:sha256",
        "h5_fixed_v1:sha256",
        "h5_dataset_v1:sha256",
        "ctd1:sha256",
        "output_set_v1:sha256",
    }
)

AdmissionOutcome = Literal["verified", "mismatched", "unverified", "unreadable"]
ExpectedIdentitySource = Literal["prior_run", "declared_digest"]
IdentityScheme = Literal["auto", "container", "logical"]


def _parse_admission_digest(
    raw: Digest | str,
    *,
    logical_profile: LogicalProfileName = "preserve_canonical",
) -> Digest:
    """Parse and validate a content-addressed ``Digest`` supported for admission."""
    if isinstance(raw, str) and raw.strip().startswith("sha256:logical_matrices:"):
        normalized = normalize_logical_matrices_identity(raw, policy=logical_profile)
        parsed = Digest.parse(normalized)
    else:
        parsed = Digest.parse(raw)
    if parsed.scheme not in _SUPPORTED_ADMISSION_SCHEMES:
        raise ValueError(
            f"Unsupported admission digest scheme {parsed.scheme!r}; "
            f"expected one of {sorted(_SUPPORTED_ADMISSION_SCHEMES)}"
        )
    return parsed


class DeclaredDigestExpectation(BaseModel):
    """Locally declared content-addressed ``Digest`` expectation and optional source context."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: Digest
    source_label: str | None = None
    source_uri: str | None = None
    logical_profile: LogicalProfileName = "preserve_canonical"

    @model_validator(mode="before")
    @classmethod
    def _coerce_identity_and_profile(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        raw_identity = data.get("identity")
        if raw_identity is None:
            return data
        explicit_profile = data.get("logical_profile")
        profile: LogicalProfileName = (
            explicit_profile
            if explicit_profile in ("preserve_canonical", "float32_matrices_v1")
            else "preserve_canonical"
        )
        parsed = _parse_admission_digest(raw_identity, logical_profile=profile)
        updated = dict(data)
        updated["identity"] = parsed
        if (
            explicit_profile is None
            and parsed.scheme == "logical_matrices_f32_v1:sha256"
        ):
            updated["logical_profile"] = "float32_matrices_v1"
        return updated

    @field_validator("source_label", "source_uri")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("declared digest source metadata must not be empty")
        return value


class PriorRunExpectation(BaseModel):
    """Caller expectation for an artifact produced or consumed by a completed prior run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_run_id: str
    artifact_key: str
    expected_direction: Literal["input", "output", "any"] = "any"
    source_tracker_db: str | Path | None = None
    expected_bytes_path: str | Path | None = None
    input_role: str | None = None
    identity_scheme: IdentityScheme = "auto"
    logical_profile: LogicalProfileName | None = None
    expected_logical_hash: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_logical_fields(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        raw_hash = data.get("expected_logical_hash")
        if raw_hash is None:
            return data
        raw_profile = data.get("logical_profile")
        profile: LogicalProfileName = (
            raw_profile
            if raw_profile in ("preserve_canonical", "float32_matrices_v1")
            else "preserve_canonical"
        )
        normalized_hash = normalize_logical_matrices_identity(
            str(raw_hash), policy=profile
        )
        updated = dict(data)
        updated["expected_logical_hash"] = normalized_hash
        if raw_profile is None and normalized_hash.startswith(
            "logical_matrices_f32_v1:sha256:"
        ):
            updated["logical_profile"] = "float32_matrices_v1"
        return updated

    @field_validator("expected_run_id", "artifact_key")
    @classmethod
    def _validate_non_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("expected_run_id and artifact_key must not be empty")
        return value

    @field_validator("input_role")
    @classmethod
    def _validate_input_role(cls, value: str | None) -> str | None:
        if value is not None and (not value or not value.strip()):
            raise ValueError("input_role must not be empty or blank")
        return value

    @classmethod
    def from_artifact(
        cls,
        artifact: Artifact,
        *,
        expected_run_id: str | None = None,
        expected_direction: Literal["input", "output", "any"] = "any",
        source_tracker_db: str | Path | None = None,
        expected_bytes_path: str | Path | None = None,
        input_role: str | None = None,
        identity_scheme: IdentityScheme = "auto",
        logical_profile: LogicalProfileName | None = None,
        expected_logical_hash: str | None = None,
    ) -> "PriorRunExpectation":
        """Construct a ``PriorRunExpectation`` from a resolved Consist ``Artifact``."""
        run_id = expected_run_id or (
            str(artifact.run_id) if artifact.run_id is not None else None
        )
        if not run_id:
            raise ValueError(
                "PriorRunExpectation.from_artifact requires artifact.run_id or explicit expected_run_id"
            )
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        resolved_logical_hash = expected_logical_hash
        resolved_profile = logical_profile
        if resolved_logical_hash is None:
            if (
                artifact.digest is not None
                and artifact.digest.scheme in _LOGICAL_MATRIX_SCHEMES
            ):
                resolved_logical_hash = str(artifact.digest)
                if resolved_profile is None:
                    resolved_profile = (
                        "float32_matrices_v1"
                        if artifact.digest.scheme == "logical_matrices_f32_v1:sha256"
                        else "preserve_canonical"
                    )
            elif isinstance(meta.get("logical_identity"), str):
                if resolved_profile is None:
                    profile_meta = meta.get("logical_profile")
                    resolved_profile = (
                        profile_meta
                        if profile_meta in ("preserve_canonical", "float32_matrices_v1")
                        else "preserve_canonical"
                    )
                resolved_logical_hash = meta["logical_identity"]
        return cls(
            expected_run_id=run_id,
            artifact_key=artifact.key,
            expected_direction=expected_direction,
            source_tracker_db=source_tracker_db,
            expected_bytes_path=expected_bytes_path,
            input_role=input_role,
            identity_scheme=identity_scheme,
            logical_profile=resolved_profile,
            expected_logical_hash=resolved_logical_hash,
        )

    @property
    def run_id(self) -> str:
        """Alias for ``expected_run_id``."""
        return self.expected_run_id

    @property
    def direction(self) -> Literal["input", "output", "any"]:
        """Alias for ``expected_direction``."""
        return self.expected_direction


class AdmissionReference(BaseModel):
    """Caller-proven runtime context for one admitted input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_key: str | None = None
    execution_path: str | Path
    input_role: str | None = None
    config_key: str | None = None
    config_reference_key: str | None = None
    feed_key: str | None = None
    raw_config_value: str | None = None
    canonical_value: str | None = None
    configured_path: str | Path | None = None
    consumer_path: str | None = None

    @model_validator(mode="after")
    def _validate_lookup_or_role(self) -> "AdmissionReference":
        if self.artifact_key is None and not self.input_role:
            raise ValueError(
                "input_role is required when artifact_key is absent for declared-digest admission"
            )
        return self


class AdmissionReport(BaseModel):
    """Policy-neutral result of checking one external file, directory, or container."""

    model_config = ConfigDict(frozen=True)

    report_schema_version: Literal[1, 2, 3] = ADMISSION_REPORT_SCHEMA_VERSION
    outcome: AdmissionOutcome
    input_role: str
    artifact_key: str | None
    config_key: str | None = None
    config_reference_key: str | None = None
    feed_key: str | None = None
    raw_config_value: str | None = None
    canonical_value: str | None = None
    configured_path: str | None = None
    execution_path: str
    physical_target_path: str | None = None
    consumer_path: str | None = None
    expected_source: ExpectedIdentitySource = "prior_run"
    expected_source_label: str | None = None
    expected_source_uri: str | None = None
    expected_run_id: str | None
    expected_artifact_id: str | None = None
    observed_artifact_id: str | None = None
    digest_algorithm: Literal["sha256"] = "sha256"
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None
    expected_bytes_path: str | None = None
    observations: tuple[str, ...] = ()
    recommended_action: str | None = None
    reason: str | None = None
    expected_direction: Literal["input", "output", "any"] | None = None
    source_tracker_db: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _default_prior_run_direction(cls, data: Any) -> Any:
        if isinstance(data, Mapping):
            if data.get("expected_source", "prior_run") == "prior_run":
                if data.get("expected_direction") is None:
                    updated = dict(data)
                    updated["expected_direction"] = "any"
                    return updated
        return data

    @model_validator(mode="after")
    def _validate_expected_source_fields(self) -> "AdmissionReport":
        if self.expected_source == "prior_run":
            if self.artifact_key is None or self.expected_run_id is None:
                raise ValueError(
                    "prior-run admission reports require artifact_key and expected_run_id"
                )
            if self.expected_direction not in ("input", "output", "any"):
                raise ValueError(
                    "prior-run admission reports require expected_direction to be 'input', 'output', or 'any'"
                )
        elif self.expected_source == "declared_digest":
            if self.artifact_key is not None or self.expected_run_id is not None:
                raise ValueError(
                    "declared-digest admission reports must not contain a prior-run artifact key or run id"
                )
            if (
                self.expected_direction is not None
                or self.source_tracker_db is not None
            ):
                raise ValueError(
                    "declared-digest admission reports must not contain expected_direction or source_tracker_db"
                )
        return self

    def canonical_json(self) -> str:
        """Serialize the report deterministically for sidecars and automation."""
        payload = self.model_dump(mode="json")
        if self.expected_source == "declared_digest":
            payload.pop("expected_direction", None)
            payload.pop("source_tracker_db", None)
        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


def hash_semantics_for_new_artifact(
    *,
    path: Path | None,
    hashing_strategy: str,
    source: Literal["computed", "caller_supplied"],
    digest: Digest | None = None,
) -> dict[str, object]:
    """Describe provenance semantics of a newly logged artifact digest."""
    if source == "caller_supplied":
        return {
            "version": 1,
            "algorithm": "unknown",
            "kind": "unknown",
            "digest_contract": "unknown",
            "source": "caller_supplied",
        }

    if digest is not None and digest.scheme == "dir_manifest_v1:sha256":
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "directory",
            "digest_contract": "manifest_v1",
            "source": "manifest_v1",
        }
    if digest is not None and digest.scheme == "output_set_v1:sha256":
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "output_set",
            "digest_contract": "output_set_v1",
            "source": "output_set_v1",
        }

    assert path is not None
    if path.is_file() and hashing_strategy == "full":
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "file",
            "digest_contract": "raw_file_bytes",
            "source": "computed_full",
        }
    if path.is_file():
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "file",
            "digest_contract": "file_metadata",
            "source": "computed_fast",
        }
    if path.is_dir() and hashing_strategy == "full":
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "directory",
            "digest_contract": "legacy_directory_content",
            "source": "computed_full_directory",
        }
    if path.is_dir():
        return {
            "version": 1,
            "algorithm": "sha256",
            "kind": "directory",
            "digest_contract": "legacy_directory_metadata",
            "source": "computed_fast_directory",
        }
    return {
        "version": 1,
        "algorithm": "sha256",
        "kind": "unknown",
        "digest_contract": "unknown",
        "source": "computed_unknown",
    }


def _is_git_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as source:
        return source.read(len(_GIT_LFS_POINTER_PREFIX)) == _GIT_LFS_POINTER_PREFIX


def _resolved_path(path: Path) -> str | None:
    try:
        return str(path.resolve(strict=False))
    except OSError:
        return None


def _known_admission_identity(artifact: Artifact) -> Digest | None:
    """Return a trusted content-addressed ``Digest`` when artifact provenance proves it."""
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    digest = artifact.digest

    semantics = meta.get("hash_semantics")
    if isinstance(semantics, Mapping):
        sem_source = semantics.get("source")
        if sem_source in {
            "caller_supplied",
            "computed_fast",
            "computed_fast_directory",
            "computed_unknown",
        }:
            return None
        if (
            sem_source == "computed_full_directory"
            and (
                digest is None
                or digest.scheme
                not in {"dir_manifest_v1:sha256", "output_set_v1:sha256"}
            )
            and meta.get("directory_artifact") is not True
        ):
            return None

    if meta.get("directory_artifact") is True:
        manifest = meta.get("directory_manifest")
        if isinstance(manifest, Mapping):
            tree_hash = manifest.get("tree_hash")
            if isinstance(tree_hash, str) and _SHA256_HEX.fullmatch(tree_hash):
                if digest is None or digest.hex == tree_hash:
                    return Digest("dir_manifest_v1:sha256", tree_hash)
        if digest is not None and digest.scheme == "dir_manifest_v1:sha256":
            return digest
        return None

    if digest is None or digest.scheme not in _SUPPORTED_ADMISSION_SCHEMES:
        return None
    return digest


def _known_logical_admission_identity(
    artifact: Artifact,
) -> tuple[Digest | None, LogicalProfileName | None]:
    """Extract an optional logical array ``Digest`` and profile from ``artifact``."""
    if (
        artifact.digest is not None
        and artifact.digest.scheme in _LOGICAL_MATRIX_SCHEMES
    ):
        prof: LogicalProfileName = (
            "float32_matrices_v1"
            if artifact.digest.scheme == "logical_matrices_f32_v1:sha256"
            else "preserve_canonical"
        )
        return artifact.digest, prof

    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    raw_profile = meta.get("logical_profile")
    explicit_profile: LogicalProfileName | None = (
        raw_profile
        if raw_profile in ("preserve_canonical", "float32_matrices_v1")
        else None
    )
    if isinstance(meta.get("logical_identity"), str):
        profile = explicit_profile or "preserve_canonical"
        try:
            normalized = normalize_logical_matrices_identity(
                meta["logical_identity"], policy=profile
            )
            parsed = Digest.parse(normalized)
            inferred_profile: LogicalProfileName = (
                "float32_matrices_v1"
                if parsed.scheme == "logical_matrices_f32_v1:sha256"
                else profile
            )
            return parsed, inferred_profile
        except ValueError:
            pass
    return None, None


def _report(
    *,
    outcome: AdmissionOutcome,
    execution_path: Path,
    physical_target_path: str | None,
    expected_run_id: str | None,
    artifact_key: str | None,
    input_role: str,
    observations: tuple[str, ...],
    expected_artifact_id: str | None = None,
    observed_artifact_id: str | None = None,
    expected_source_label: str | None = None,
    expected_source_uri: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    recommended_action: str | None = None,
    reason: str | None = None,
    expected_source: ExpectedIdentitySource = "prior_run",
    expected_direction: Literal["input", "output", "any"] | None = None,
    source_tracker_db: str | None = None,
) -> AdmissionReport:
    if expected_source == "prior_run" and expected_direction is None:
        expected_direction = "any"
    return AdmissionReport(
        outcome=outcome,
        input_role=input_role,
        artifact_key=artifact_key,
        execution_path=str(execution_path),
        physical_target_path=physical_target_path,
        expected_source=expected_source,
        expected_run_id=expected_run_id,
        expected_artifact_id=expected_artifact_id,
        observed_artifact_id=observed_artifact_id,
        expected_source_label=expected_source_label,
        expected_source_uri=expected_source_uri,
        expected_bytes_source=expected_bytes_source,
        expected_bytes_path=expected_bytes_path,
        observations=observations,
        recommended_action=recommended_action,
        reason=reason,
        expected_direction=expected_direction,
        source_tracker_db=source_tracker_db,
    )


@dataclass(frozen=True, slots=True)
class _CandidateObservation:
    """One observed candidate path and its computed ``Digest``."""

    execution_path: Path
    physical_target_path: str | None
    observed_identity: Digest | None
    outcome: AdmissionOutcome | None = None
    observations: tuple[str, ...] = ()
    recommended_action: str | None = None
    reason: str | None = None


def _observe_candidate(path: str | Path) -> _CandidateObservation:
    """Read one regular-file candidate once via ``compute_digest``."""
    candidate = Path(path).expanduser()
    physical_target_path = _resolved_path(candidate)
    if not candidate.is_file():
        return _CandidateObservation(
            execution_path=candidate,
            physical_target_path=physical_target_path,
            observed_identity=None,
            outcome="unreadable",
            observations=("file_unreadable",),
            reason="Admission requires a readable regular file.",
        )
    try:
        if _is_git_lfs_pointer(candidate):
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=None,
                outcome="unverified",
                observations=("looks_like_git_lfs_pointer",),
                recommended_action="Run git lfs pull to materialize the file bytes.",
            )
        return _CandidateObservation(
            execution_path=candidate,
            physical_target_path=physical_target_path,
            observed_identity=compute_digest(
                candidate, options={"scheme": "sha256:file"}
            ),
        )
    except OSError as exc:
        return _CandidateObservation(
            execution_path=candidate,
            physical_target_path=physical_target_path,
            observed_identity=None,
            outcome="unreadable",
            observations=("file_unreadable",),
            reason=str(exc),
        )


def _observe_candidate_for_scheme(
    path: str | Path,
    *,
    scheme: str,
    member: str | None = None,
    logical_profile: LogicalProfileName = "preserve_canonical",
    member_paths: Mapping[str, Any] | None = None,
) -> _CandidateObservation:
    """Observe a candidate path through ``compute_digest`` for ``scheme``."""
    if scheme == "sha256:file":
        return _observe_candidate(path)

    candidate = Path(path).expanduser()
    physical_target_path = _resolved_path(candidate)

    def _fail(obs: str, reason: str) -> _CandidateObservation:
        return _CandidateObservation(
            execution_path=candidate,
            physical_target_path=physical_target_path,
            observed_identity=None,
            outcome="unreadable",
            observations=(obs,),
            reason=reason,
        )

    if scheme == "dir_manifest_v1:sha256":
        is_shp = candidate.suffix.lower() == ".shp"
        if is_shp and not candidate.is_file():
            return _fail(
                "file_unreadable",
                "Shapefile bundle admission requires a readable .shp file.",
            )
        if not is_shp and not candidate.is_dir():
            return _fail(
                "directory_unreadable",
                "Directory manifest admission requires a readable directory.",
            )
        try:
            opts: dict[str, Any] = {"scheme": "dir_manifest_v1:sha256"}
            if is_shp:
                opts["shapefile_bundle"] = True
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=compute_digest(
                    candidate,
                    driver="shapefile" if is_shp else None,
                    options=opts,
                ),
            )
        except (OSError, ValueError) as exc:
            return _fail("directory_unreadable", str(exc))

    if scheme in _LOGICAL_MATRIX_SCHEMES:
        if not candidate.exists() or (
            not candidate.is_file() and not candidate.is_dir()
        ):
            return _fail(
                "file_unreadable",
                "Logical matrix admission requires an existing OMX/HDF5/NetCDF file or Zarr directory.",
            )
        try:
            if candidate.is_file() and _is_git_lfs_pointer(candidate):
                return _CandidateObservation(
                    execution_path=candidate,
                    physical_target_path=physical_target_path,
                    observed_identity=None,
                    outcome="unverified",
                    observations=("looks_like_git_lfs_pointer",),
                    recommended_action="Run git lfs pull to materialize the file bytes.",
                )
            effective_profile: LogicalProfileName = (
                "float32_matrices_v1"
                if scheme == "logical_matrices_f32_v1:sha256"
                else logical_profile
            )
            effective_scheme = logical_matrices_scheme_for_policy(effective_profile)
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=compute_digest(
                    candidate,
                    member=member,
                    options={"scheme": effective_scheme, "policy": effective_profile},
                ),
            )
        except (OSError, KeyError, ValueError, RuntimeError) as exc:
            return _fail("logical_container_unreadable", str(exc))

    if (
        scheme == "output_set_v1:sha256"
        and candidate.is_dir()
        and member_paths is not None
    ):
        try:
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=compute_digest(
                    candidate,
                    driver="output_set",
                    options={
                        "scheme": "output_set_v1:sha256",
                        "member_paths": member_paths,
                    },
                ),
            )
        except (OSError, KeyError, ValueError, RuntimeError) as exc:
            return _fail("directory_unreadable", str(exc))

    if scheme in {
        "h5_fixed_v1:sha256",
        "h5_dataset_v1:sha256",
        "ctd1:sha256",
        "output_set_v1:sha256",
    }:
        if not candidate.is_file():
            return _fail(
                "file_unreadable",
                f"Admission for {scheme} requires a readable regular file.",
            )
        try:
            if _is_git_lfs_pointer(candidate):
                return _CandidateObservation(
                    execution_path=candidate,
                    physical_target_path=physical_target_path,
                    observed_identity=None,
                    outcome="unverified",
                    observations=("looks_like_git_lfs_pointer",),
                    recommended_action="Run git lfs pull to materialize the file bytes.",
                )
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=compute_digest(
                    candidate,
                    member=member,
                    options={"scheme": scheme},
                ),
            )
        except (OSError, KeyError, ValueError, RuntimeError) as exc:
            return _fail("file_unreadable", str(exc))

    raise ValueError(f"Unsupported admission digest scheme: {scheme!r}")


def _report_candidate_failure(
    observation: _CandidateObservation,
    *,
    expected_source: ExpectedIdentitySource,
    expected_run_id: str | None,
    artifact_key: str | None,
    input_role: str,
    expected_identity: Digest | None = None,
    expected_source_label: str | None = None,
    expected_source_uri: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    expected_direction: Literal["input", "output", "any"] | None = None,
    source_tracker_db: str | None = None,
) -> AdmissionReport:
    assert observation.outcome is not None
    return _report(
        outcome=observation.outcome,
        execution_path=observation.execution_path,
        physical_target_path=observation.physical_target_path,
        expected_source=expected_source,
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=input_role,
        expected_artifact_id=(
            str(expected_identity) if expected_identity is not None else None
        ),
        expected_source_label=expected_source_label,
        expected_source_uri=expected_source_uri,
        expected_bytes_source=expected_bytes_source,
        expected_bytes_path=expected_bytes_path,
        observations=observation.observations,
        recommended_action=observation.recommended_action,
        reason=observation.reason,
        expected_direction=expected_direction,
        source_tracker_db=source_tracker_db,
    )


def _compare_observed_identity(
    observation: _CandidateObservation,
    *,
    expected_identity: Digest,
    expected_source: ExpectedIdentitySource,
    expected_run_id: str | None,
    artifact_key: str | None,
    input_role: str,
    expected_source_label: str | None = None,
    expected_source_uri: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    expected_direction: Literal["input", "output", "any"] | None = None,
    source_tracker_db: str | None = None,
    extra_observations: tuple[str, ...] = (),
) -> AdmissionReport:
    observed_identity = observation.observed_identity
    assert observed_identity is not None
    outcome: AdmissionOutcome = (
        "verified" if observed_identity == expected_identity else "mismatched"
    )
    base_obs = ("matched",) if outcome == "verified" else ("mismatched",)
    observations = base_obs + extra_observations
    return _report(
        outcome=outcome,
        execution_path=observation.execution_path,
        physical_target_path=observation.physical_target_path,
        expected_source=expected_source,
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=input_role,
        expected_artifact_id=str(expected_identity),
        observed_artifact_id=str(observed_identity),
        expected_source_label=expected_source_label,
        expected_source_uri=expected_source_uri,
        expected_bytes_source=expected_bytes_source,
        expected_bytes_path=expected_bytes_path,
        observations=observations,
        reason=(
            None
            if outcome == "verified"
            else (
                "Resolved file does not match the expected prior-run artifact."
                if expected_source == "prior_run"
                else "Resolved file does not match the declared digest."
            )
        ),
        expected_direction=expected_direction,
        source_tracker_db=source_tracker_db,
    )


def compare_path_to_identity(
    *,
    execution_path: str | Path,
    expected_identity: Digest | str,
    input_role: str,
    expected_source: ExpectedIdentitySource,
    artifact_key: str | None = None,
    expected_run_id: str | None = None,
    expected_source_label: str | None = None,
    expected_source_uri: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    expected_direction: Literal["input", "output", "any"] | None = None,
    source_tracker_db: str | None = None,
    logical_profile: LogicalProfileName = "preserve_canonical",
    member: str | None = None,
) -> AdmissionReport:
    """Compare one candidate path with a typed expected ``Digest`` via ``compute_digest``."""
    parsed_identity = _parse_admission_digest(
        expected_identity,
        logical_profile=logical_profile,
    )
    observation = _observe_candidate_for_scheme(
        execution_path,
        scheme=parsed_identity.scheme,
        member=member,
        logical_profile=logical_profile,
    )
    if observation.observed_identity is None:
        return _report_candidate_failure(
            observation,
            expected_source=expected_source,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=input_role,
            expected_identity=parsed_identity,
            expected_source_label=expected_source_label,
            expected_source_uri=expected_source_uri,
            expected_bytes_source=expected_bytes_source,
            expected_bytes_path=expected_bytes_path,
            expected_direction=expected_direction,
            source_tracker_db=source_tracker_db,
        )
    extra_obs: tuple[str, ...] = ()
    if parsed_identity.scheme in _LOGICAL_MATRIX_SCHEMES:
        extra_obs = (
            ("logical_identity_verified",)
            if observation.observed_identity == parsed_identity
            else ("logical_identity_mismatched",)
        )
    return _compare_observed_identity(
        observation,
        expected_identity=parsed_identity,
        expected_source=expected_source,
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=input_role,
        expected_source_label=expected_source_label,
        expected_source_uri=expected_source_uri,
        expected_bytes_source=expected_bytes_source,
        expected_bytes_path=expected_bytes_path,
        expected_direction=expected_direction,
        source_tracker_db=source_tracker_db,
        extra_observations=extra_obs,
    )


def _expected_artifact(
    database: DatabaseManager,
    *,
    expected_run_id: str,
    artifact_key: str,
    expected_direction: Literal["input", "output", "any"] = "any",
) -> tuple[Artifact | None, str | None]:
    run = database.get_run(expected_run_id)
    if run is None:
        return None, "expected_prior_run_absent"
    if run.status != "completed":
        return None, "expected_prior_run_not_completed"
    matches = database.find_linked_artifacts_exact(
        run_id=expected_run_id,
        key=artifact_key,
        direction=(
            expected_direction if expected_direction in ("input", "output") else None
        ),
    )

    if not matches:
        if expected_direction == "output":
            return None, "expected_output_role_absent"
        if expected_direction == "input":
            return None, "expected_input_role_absent"
        return None, "expected_role_absent"

    if len(matches) != 1:
        if expected_direction == "output":
            return None, "expected_output_role_ambiguous"
        if expected_direction == "input":
            return None, "expected_input_role_ambiguous"
        return None, "expected_role_ambiguous"

    return matches[0], None


def _paths_are_same(candidate: Path, expected: Path) -> bool:
    if _resolved_path(candidate) == _resolved_path(expected):
        return True
    try:
        return os.path.samefile(candidate, expected)
    except OSError:
        return False


def _historical_stored_hex(artifact: Artifact) -> str | None:
    if artifact.digest is not None:
        return artifact.digest.hex
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    legacy_hash = meta.get("legacy_v04_hash")
    if isinstance(legacy_hash, str) and _SHA256_HEX.fullmatch(legacy_hash):
        return legacy_hash
    return None


def check_artifact_identity(
    tracker: "Tracker" | None = None,
    *,
    execution_path: str | Path,
    expected_run_id: str,
    artifact_key: str,
    expected_bytes_path: str | Path | None = None,
    input_role: str | None = None,
    expected_direction: Literal["input", "output", "any"] = "any",
    source_tracker_db: str | Path | None = None,
    identity_scheme: IdentityScheme = "auto",
    logical_profile: LogicalProfileName | None = None,
    expected_logical_hash: str | None = None,
) -> AdmissionReport:
    """Compare a resolved file or directory with one artifact from an explicit completed run."""
    if source_tracker_db is None and (tracker is None or tracker.db is None):
        raise RuntimeError("Artifact admission requires a provenance database.")

    role = input_role or artifact_key
    source_db_str: str | None = None
    source_db_file: Path | None = None
    if source_tracker_db is not None:
        source_db_file = Path(source_tracker_db).expanduser()
        source_db_str = _resolved_path(source_db_file) or str(source_db_file)
        if not source_db_file.is_file():
            candidate = Path(execution_path).expanduser()
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=_resolved_path(candidate),
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observations=("source_tracker_db_unreadable",),
                reason=(
                    f"Source tracker database does not exist or is not a readable regular file: {source_tracker_db}"
                ),
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )

    candidate = Path(execution_path).expanduser()
    physical_target_path = _resolved_path(candidate)

    file_observation: _CandidateObservation | None = None
    if not candidate.exists():
        file_observation = _observe_candidate(execution_path)
        return _report_candidate_failure(
            file_observation,
            expected_source="prior_run",
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )
    if candidate.is_file() and identity_scheme != "logical":
        file_observation = _observe_candidate(execution_path)
        if file_observation.observed_identity is None:
            return _report_candidate_failure(
                file_observation,
                expected_source="prior_run",
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
    observed_file_id_str = (
        str(file_observation.observed_identity)
        if file_observation is not None
        and file_observation.observed_identity is not None
        else None
    )

    owns_db = False
    db: DatabaseManager | None = None
    if source_db_file is not None:
        try:
            db = DatabaseManager(
                str(source_db_file),
                skip_schema_init=True,
                read_only=True,
            )
            owns_db = True
        except Exception as exc:
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=observed_file_id_str,
                observations=("source_tracker_db_unreadable",),
                reason=f"Failed to open source tracker database '{source_tracker_db}': {exc}",
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
    else:
        assert tracker is not None and tracker.db is not None
        db = tracker.db

    try:
        try:
            artifact, expected_error = _expected_artifact(
                db,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                expected_direction=expected_direction,
            )
        except Exception as exc:
            if owns_db:
                return _report(
                    outcome="unverified",
                    execution_path=candidate,
                    physical_target_path=physical_target_path,
                    expected_run_id=expected_run_id,
                    artifact_key=artifact_key,
                    input_role=role,
                    observed_artifact_id=observed_file_id_str,
                    observations=("source_tracker_db_unreadable",),
                    reason=f"Failed to read source tracker database '{source_tracker_db}': {exc}",
                    expected_direction=expected_direction,
                    source_tracker_db=source_db_str,
                )
            raise
    finally:
        if owns_db and db is not None:
            db.close()

    if expected_error is not None:
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=observed_file_id_str,
            observations=(expected_error,),
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )
    assert artifact is not None

    expected_identity = _known_admission_identity(artifact)
    artifact_logical_identity, artifact_logical_profile = (
        _known_logical_admission_identity(artifact)
    )
    resolved_logical_profile: LogicalProfileName = (
        logical_profile or artifact_logical_profile or "preserve_canonical"
    )
    resolved_logical_identity: Digest | None = (
        Digest.parse(
            normalize_logical_matrices_identity(
                expected_logical_hash, policy=resolved_logical_profile
            )
        )
        if expected_logical_hash is not None
        else (
            Digest.parse(
                normalize_logical_matrices_identity(
                    artifact_logical_identity.hex, policy=resolved_logical_profile
                )
            )
            if artifact_logical_identity is not None and logical_profile is not None
            else artifact_logical_identity
        )
    )

    if identity_scheme == "logical":
        if resolved_logical_identity is None:
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=observed_file_id_str,
                observations=(
                    "expected_artifact_unverifiable",
                    "expected_logical_identity_absent",
                ),
                reason="Expected prior-run artifact does not carry a logical array identity.",
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        return compare_path_to_identity(
            execution_path=candidate,
            expected_identity=resolved_logical_identity,
            input_role=role,
            expected_source="prior_run",
            artifact_key=artifact_key,
            expected_run_id=expected_run_id,
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
            logical_profile=resolved_logical_profile,
        )

    expected_source: Literal["explicit_immutable_path"] | None = None
    expected_audit_path: str | None = None
    if expected_identity is None and expected_bytes_path is not None:
        expected_source = "explicit_immutable_path"
        expected_path = Path(expected_bytes_path).expanduser()
        expected_audit_path = _resolved_path(expected_path)
        if not expected_path.is_file() or _paths_are_same(candidate, expected_path):
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=observed_file_id_str,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=(
                    "expected_artifact_unverifiable",
                    "expected_bytes_not_distinct",
                ),
                reason="Expected bytes must be a distinct readable regular file.",
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        try:
            if _is_git_lfs_pointer(expected_path):
                return _report(
                    outcome="unverified",
                    execution_path=candidate,
                    physical_target_path=physical_target_path,
                    expected_run_id=expected_run_id,
                    artifact_key=artifact_key,
                    input_role=role,
                    observed_artifact_id=observed_file_id_str,
                    expected_bytes_source=expected_source,
                    expected_bytes_path=expected_audit_path,
                    observations=("looks_like_git_lfs_pointer",),
                    recommended_action="Run git lfs pull to materialize the expected bytes.",
                    expected_direction=expected_direction,
                    source_tracker_db=source_db_str,
                )
            corroborated = compute_digest(
                expected_path, options={"scheme": "sha256:file"}
            )
        except OSError as exc:
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=observed_file_id_str,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=("expected_artifact_unverifiable",),
                reason=str(exc),
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        stored_hex = _historical_stored_hex(artifact)
        if stored_hex is None or corroborated.hex != stored_hex:
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=observed_file_id_str,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=(
                    "expected_artifact_unverifiable",
                    "expected_bytes_not_correlated",
                ),
                reason=(
                    "Expected bytes did not corroborate the historical artifact hash."
                ),
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        expected_identity = corroborated

    if expected_identity is None:
        if identity_scheme == "auto" and resolved_logical_identity is not None:
            return compare_path_to_identity(
                execution_path=candidate,
                expected_identity=resolved_logical_identity,
                input_role=role,
                expected_source="prior_run",
                artifact_key=artifact_key,
                expected_run_id=expected_run_id,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
                logical_profile=resolved_logical_profile,
            )
        if file_observation is None and not candidate.is_dir():
            file_observation = _observe_candidate(execution_path)
            if file_observation.observed_identity is None:
                return _report_candidate_failure(
                    file_observation,
                    expected_source="prior_run",
                    expected_run_id=expected_run_id,
                    artifact_key=artifact_key,
                    input_role=role,
                    expected_direction=expected_direction,
                    source_tracker_db=source_db_str,
                )
            observed_file_id_str = str(file_observation.observed_identity)
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=observed_file_id_str,
            observations=("expected_artifact_unverifiable",),
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )

    artifact_meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    raw_member_paths = artifact_meta.get("output_set_member_paths")
    member_paths_opt = (
        raw_member_paths if isinstance(raw_member_paths, Mapping) else None
    )

    if expected_identity.scheme == "sha256:file" and file_observation is not None:
        container_observation = file_observation
    else:
        container_observation = _observe_candidate_for_scheme(
            candidate,
            scheme=expected_identity.scheme,
            member=artifact.member,
            logical_profile=resolved_logical_profile,
            member_paths=member_paths_opt,
        )

    if container_observation.observed_identity is not None:
        container_report = _compare_observed_identity(
            container_observation,
            expected_identity=expected_identity,
            expected_source="prior_run",
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            expected_bytes_source=expected_source,
            expected_bytes_path=expected_audit_path,
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )
        if container_report.outcome == "verified" or identity_scheme == "container":
            return container_report
    else:
        if identity_scheme == "container" or resolved_logical_identity is None:
            return _report_candidate_failure(
                container_observation,
                expected_source="prior_run",
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                expected_identity=expected_identity,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        container_report = None

    if identity_scheme == "auto" and resolved_logical_identity is not None:
        logical_obs = _observe_candidate_for_scheme(
            candidate,
            scheme=resolved_logical_identity.scheme,
            logical_profile=resolved_logical_profile,
        )
        if logical_obs.observed_identity is not None:
            extra_obs = (
                ("container_bytes_mismatched", "logical_identity_verified")
                if logical_obs.observed_identity == resolved_logical_identity
                else ("container_bytes_mismatched", "logical_identity_mismatched")
            )
            return _compare_observed_identity(
                logical_obs,
                expected_identity=resolved_logical_identity,
                expected_source="prior_run",
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
                extra_observations=extra_obs,
            )

    if container_report is not None:
        return container_report
    return _report_candidate_failure(
        container_observation,
        expected_source="prior_run",
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=role,
        expected_identity=expected_identity,
        expected_bytes_source=expected_source,
        expected_bytes_path=expected_audit_path,
        expected_direction=expected_direction,
        source_tracker_db=source_db_str,
    )


def _attach_reference_context(
    report: AdmissionReport, reference: AdmissionReference
) -> AdmissionReport:
    return AdmissionReport.model_validate(
        {
            **report.model_dump(),
            "config_key": reference.config_key,
            "config_reference_key": reference.config_reference_key,
            "feed_key": reference.feed_key,
            "raw_config_value": reference.raw_config_value,
            "canonical_value": reference.canonical_value,
            "configured_path": (
                str(reference.configured_path)
                if reference.configured_path is not None
                else None
            ),
            "consumer_path": reference.consumer_path,
        }
    )


def check_admission_reference(
    tracker: "Tracker" | None = None,
    *,
    reference: AdmissionReference,
    expected_run_id: str,
    expected_bytes_path: str | Path | None = None,
    expected_direction: Literal["input", "output", "any"] = "any",
    source_tracker_db: str | Path | None = None,
    identity_scheme: IdentityScheme = "auto",
    logical_profile: LogicalProfileName | None = None,
    expected_logical_hash: str | None = None,
) -> AdmissionReport:
    """Check a runtime-resolved input without recreating consumer path logic."""
    if reference.artifact_key is None:
        raise ValueError("prior-run admission references require artifact_key")
    report = check_artifact_identity(
        tracker,
        execution_path=reference.execution_path,
        expected_run_id=expected_run_id,
        artifact_key=reference.artifact_key,
        expected_bytes_path=expected_bytes_path,
        input_role=reference.input_role,
        expected_direction=expected_direction,
        source_tracker_db=source_tracker_db,
        identity_scheme=identity_scheme,
        logical_profile=logical_profile,
        expected_logical_hash=expected_logical_hash,
    )
    return _attach_reference_context(report, reference)


def check_expected_identity(
    *,
    execution_path: str | Path,
    input_role: str,
    expectation: DeclaredDigestExpectation,
) -> AdmissionReport:
    """Compare one file or directory to a caller-declared ``Digest``."""
    if not input_role:
        raise ValueError("declared-digest admission requires input_role")
    return compare_path_to_identity(
        execution_path=execution_path,
        expected_identity=expectation.identity,
        expected_source="declared_digest",
        input_role=input_role,
        expected_source_label=expectation.source_label,
        expected_source_uri=expectation.source_uri,
        logical_profile=expectation.logical_profile,
    )


def check_admission_reference_expected_identity(
    *,
    reference: AdmissionReference,
    expectation: DeclaredDigestExpectation,
) -> AdmissionReport:
    """Compare a runtime-resolved file or directory to a declared ``Digest`` without a Tracker."""
    if reference.artifact_key is not None:
        raise ValueError(
            "declared-digest admission references must not contain artifact_key"
        )
    assert reference.input_role is not None
    report = check_expected_identity(
        execution_path=reference.execution_path,
        input_role=reference.input_role,
        expectation=expectation,
    )
    return _attach_reference_context(report, reference)


def check_prior_run_expectation(
    tracker: "Tracker" | None = None,
    *,
    execution_path: str | Path,
    expectation: PriorRunExpectation,
    input_role: str | None = None,
) -> AdmissionReport:
    """Compare a resolved file or directory with a ``PriorRunExpectation``."""
    role = input_role if input_role is not None else expectation.input_role
    return check_artifact_identity(
        tracker,
        execution_path=execution_path,
        expected_run_id=expectation.expected_run_id,
        artifact_key=expectation.artifact_key,
        expected_bytes_path=expectation.expected_bytes_path,
        input_role=role,
        expected_direction=expectation.expected_direction,
        source_tracker_db=expectation.source_tracker_db,
        identity_scheme=expectation.identity_scheme,
        logical_profile=expectation.logical_profile,
        expected_logical_hash=expectation.expected_logical_hash,
    )


def check_admission_reference_prior_run_expectation(
    tracker: "Tracker" | None = None,
    *,
    reference: AdmissionReference,
    expectation: PriorRunExpectation,
) -> AdmissionReport:
    """Check a runtime-resolved input reference against a ``PriorRunExpectation``."""
    if (
        reference.artifact_key is not None
        and reference.artifact_key != expectation.artifact_key
    ):
        raise ValueError(
            f"Admission reference artifact_key '{reference.artifact_key}' "
            f"does not match expectation artifact_key '{expectation.artifact_key}'"
        )
    artifact_key = (
        reference.artifact_key
        if reference.artifact_key is not None
        else expectation.artifact_key
    )
    input_role = (
        reference.input_role
        if reference.input_role is not None
        else expectation.input_role
    )

    report = check_artifact_identity(
        tracker,
        execution_path=reference.execution_path,
        expected_run_id=expectation.expected_run_id,
        artifact_key=artifact_key,
        expected_bytes_path=expectation.expected_bytes_path,
        input_role=input_role,
        expected_direction=expectation.expected_direction,
        source_tracker_db=expectation.source_tracker_db,
        identity_scheme=expectation.identity_scheme,
        logical_profile=expectation.logical_profile,
        expected_logical_hash=expectation.expected_logical_hash,
    )
    return _attach_reference_context(report, reference)
