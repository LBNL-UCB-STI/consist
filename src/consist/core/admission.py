"""
Verify external files against prior-run or declared raw-file identities.

Admission identity is deliberately separate from Consist's cache fingerprinting.
It always uses a full SHA-256 digest of a regular file's raw bytes and returns a
policy-neutral report; callers such as PILATES decide whether an observation is
fatal. Expected identities may come from an explicit prior run or a typed local
declaration, but neither source changes cache identity or artifacts on its own.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from sqlmodel import col, select

from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ADMISSION_REPORT_SCHEMA_VERSION = 3
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_FILE_IDENTITY = re.compile(r"sha256:file:[0-9a-f]{64}")
_GIT_LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1\n"

AdmissionOutcome = Literal["verified", "mismatched", "unverified", "unreadable"]
ExpectedIdentitySource = Literal["prior_run", "declared_digest"]


class FileIdentity(BaseModel):
    """One canonical SHA-256 identity for the raw bytes of a regular file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: str

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: str) -> str:
        if not _FILE_IDENTITY.fullmatch(value):
            raise ValueError(
                "file identity must be sha256:file:<64 lowercase hexadecimal characters>"
            )
        return value

    @classmethod
    def parse(cls, value: str) -> "FileIdentity":
        """Parse one canonical raw-file SHA-256 identity."""
        return cls(value=value)

    def __str__(self) -> str:
        """Return the canonical self-describing identity."""
        return self.value


class DeclaredDigestExpectation(BaseModel):
    """A locally declared raw-file identity and optional opaque source context."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: FileIdentity
    source_label: str | None = None
    source_uri: str | None = None

    @field_validator("source_label", "source_uri")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("declared digest source metadata must not be empty")
        return value


class PriorRunExpectation(BaseModel):
    """
    Caller expectation for an artifact produced or consumed by a prior run.

    Supports validating candidates against inputs or outputs of prior runs,
    stored in the current tracker's provenance database or an external
    provenance database (``source_tracker_db``).

    Attributes
    ----------
    expected_run_id : str
        Explicit run ID that must identify a completed run.
    artifact_key : str
        Exact artifact key that must identify the artifact link.
    expected_direction : {"input", "output", "any"}, default "any"
        Direction of the artifact link on the prior run.
    source_tracker_db : str or pathlib.Path or None, optional
        Path to an external DuckDB provenance database. If None, the current
        tracker's provenance database is queried.
    expected_bytes_path : str or pathlib.Path or None, optional
        Distinct immutable expected-byte fallback for a historical artifact
        whose stored hash lacks explicit full-file semantics.
    input_role : str or None, optional
        Logical role to record in the report. Defaults to ``artifact_key``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_run_id: str
    artifact_key: str
    expected_direction: Literal["input", "output", "any"] = "any"
    source_tracker_db: str | Path | None = None
    expected_bytes_path: str | Path | None = None
    input_role: str | None = None

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

    @property
    def run_id(self) -> str:
        """Alias for expected_run_id."""
        return self.expected_run_id

    @property
    def direction(self) -> Literal["input", "output", "any"]:
        """Alias for expected_direction."""
        return self.expected_direction


class AdmissionReference(BaseModel):
    """
    Represent caller-proven runtime context for one admitted input.

    ``execution_path`` is the host-readable file Consist hashes. ``consumer_path``
    is the final path supplied to the consumer and may be in another namespace,
    such as a container. Consist derives ``physical_target_path`` from
    ``execution_path`` and does not accept caller control over that audit value.
    The calling workflow is responsible for proving that execution and consumer
    paths describe the same staged bytes before it creates this value.

    Attributes
    ----------
    artifact_key : str, optional
        Exact persisted input-artifact key to resolve on the expected run. It
        may be absent only when a declared-digest entry point supplies the
        expected identity directly; then ``input_role`` is required.
    execution_path : str or pathlib.Path
        Host-readable regular file whose bytes Consist checks.
    input_role : str, optional
        Stronger logical role to record without changing artifact lookup.
    config_key : str, optional
        Source configuration key that selected the input.
    config_reference_key : str, optional
        Canonical configuration-reference key when it differs from
        ``config_key``.
    feed_key : str, optional
        Adapter-defined feed selector associated with the input.
    raw_config_value : str, optional
        Unmodified configuration value before canonicalization.
    canonical_value : str, optional
        Portable configuration value produced during canonicalization.
    configured_path : str or pathlib.Path, optional
        Path derived from configuration before launch-time resolution.
    consumer_path : str, optional
        Final path provided to the consumer, such as a container path.

    Notes
    -----
    The model is frozen and forbids unknown fields. It records runtime context
    but does not validate a mount, staging, or container mapping; that proof is
    owned by the calling workflow.
    """

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
    """
    Represent the policy-neutral result of checking one external file.

    Attributes
    ----------
    report_schema_version : int
        Version of the serialized admission-report schema.
    outcome : {"verified", "mismatched", "unverified", "unreadable"}
        High-level result. ``verified`` means the observed and expected byte
        identities match; ``mismatched`` means both identities are known and
        differ; ``unverified`` means Consist could not establish a trusted
        expected identity; and ``unreadable`` means the observed file could not
        be read as a regular file.
    input_role : str
        Logical role supplied by the caller, defaulting to ``artifact_key``.
    artifact_key : str or None
        Exact persisted input-artifact key selected on the expected run, or
        ``None`` for a declared-digest check.
    config_key : str or None
        Source configuration key when supplied by a future runtime adapter.
    config_reference_key : str or None
        Canonical configuration-reference key when distinct from ``config_key``.
    feed_key : str or None
        Feed selector associated with the input, when available.
    raw_config_value : str or None
        Unmodified value read from workflow configuration.
    canonical_value : str or None
        Portable canonical value produced by configuration canonicalization.
    configured_path : str or None
        Path derived from configuration before execution-path resolution.
    execution_path : str
        File path supplied for admission and intended for workflow execution.
    physical_target_path : str or None
        Resolved physical target used for path-audit and alias detection.
    consumer_path : str or None
        Final path supplied to the consumer, such as a container path.
    expected_source : {"prior_run", "declared_digest"}
        Source used to resolve the expected identity.
    expected_source_label, expected_source_uri : str or None
        Opaque optional metadata retained for a declared identity source.
    expected_run_id : str or None
        Explicit completed run supplying the expected input artifact, when the
        expectation comes from a prior run.
    expected_artifact_id : str or None
        Trusted self-describing expected identity, such as
        ``sha256:file:<hex>``.
    observed_artifact_id : str or None
        Full-file identity computed from ``execution_path`` when readable.
    digest_algorithm : {"sha256"}
        Digest algorithm used by the admission identity contract.
    expected_bytes_source : {"explicit_immutable_path"} or None
        Evidence source used to reverify a historical hash with unknown
        semantics.
    expected_bytes_path : str or None
        Resolved audit path for the expected-byte evidence.
    observations : tuple[str, ...]
        Stable machine-readable facts supporting ``outcome``.
    recommended_action : str or None
        Workflow-independent remediation, used only when Consist can identify
        a generally valid action.
    reason : str or None
        Human-readable explanation for the outcome.
    expected_direction : {"input", "output", "any"} or None, optional
        Expected artifact direction when resolved against a prior run.
    source_tracker_db : str or None, optional
        External DuckDB provenance database path supplying the expected run.

    Notes
    -----
    Reports are frozen Pydantic models. They describe evidence but do not assign
    workflow severity or mutate run state.
    """

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
        if isinstance(data, dict):
            if data.get("expected_source", "prior_run") == "prior_run":
                if data.get("expected_direction") is None:
                    data = dict(data)
                    data["expected_direction"] = "any"
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
        """
        Serialize the report deterministically for sidecars and automation.

        Returns
        -------
        str
            Compact JSON with sorted keys and all schema fields represented.

        Notes
        -----
        The returned string has no trailing newline or ephemeral timestamp.
        Identical report values therefore produce identical serialized text.
        """
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
) -> dict[str, object]:
    """
    Describe the semantics of a newly logged artifact fingerprint.

    Parameters
    ----------
    path : pathlib.Path or None
        Resolved artifact path. Required when ``source="computed"`` and ignored
        when the hash was supplied by the caller.
    hashing_strategy : {"full", "fast"}
        Tracker hashing strategy active when the fingerprint was computed.
    source : {"computed", "caller_supplied"}
        Whether Consist computed the fingerprint or accepted it from a caller.

    Returns
    -------
    dict[str, object]
        Versioned metadata containing ``algorithm``, ``kind``,
        ``digest_contract``, and ``source``. Caller-supplied hashes remain
        explicitly unknown unless another operation validates their semantics.

    Raises
    ------
    AssertionError
        If ``source="computed"`` is used without a path.

    Notes
    -----
    This helper describes the existing ``Artifact.hash`` value; it does not
    compute or replace that value. Admission trusts only the exact full-file
    SHA-256 semantics emitted for a regular file under the ``full`` strategy.
    """
    if source == "caller_supplied":
        return {
            "version": 1,
            "algorithm": "unknown",
            "kind": "unknown",
            "digest_contract": "unknown",
            "source": "caller_supplied",
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


def admission_file_identity(path: str | Path) -> str:
    """
    Compute the canonical admission identity for one regular file.

    Parameters
    ----------
    path : str or pathlib.Path
        File whose raw bytes should be hashed.

    Returns
    -------
    str
        Self-describing identity in the form ``sha256:file:<hex>``.

    Raises
    ------
    ValueError
        If ``path`` is missing or is not a regular file.
    OSError
        If the file cannot be opened or read.

    Notes
    -----
    This function always hashes full file contents and does not consult or
    modify a tracker's cache hashing strategy.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise ValueError(f"Admission only supports regular files: {file_path}")

    digest = hashlib.sha256()
    with file_path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:file:{digest.hexdigest()}"


def _is_git_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as source:
        return source.read(len(_GIT_LFS_POINTER_PREFIX)) == _GIT_LFS_POINTER_PREFIX


def _resolved_path(path: Path) -> str | None:
    try:
        return str(path.resolve(strict=False))
    except OSError:
        return None


def _known_admission_identity(artifact: Artifact) -> str | None:
    """
    Return a trusted full-file identity only when artifact semantics prove it.

    Parameters
    ----------
    artifact : Artifact
        Persisted artifact from the explicitly selected expected run.

    Returns
    -------
    str or None
        ``sha256:file:<hex>`` when the artifact records Consist-computed raw
        regular-file SHA-256 semantics or explicit admission identity,
        otherwise ``None``.

    Notes
    -----
    Admission intentionally rejects cache fingerprints, directory hashes, and
    caller-supplied hashes even when they happen to be hexadecimal SHA-256 text.
    """
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else None
    if meta and isinstance(meta.get("admission_file_identity"), str):
        explicit_identity = meta["admission_file_identity"]
        if _FILE_IDENTITY.fullmatch(explicit_identity):
            return explicit_identity

    semantics = meta.get("hash_semantics") if meta else None
    if not isinstance(semantics, Mapping):
        return None
    if dict(semantics) != {
        "version": 1,
        "algorithm": "sha256",
        "kind": "file",
        "digest_contract": "raw_file_bytes",
        "source": "computed_full",
    }:
        return None
    if not artifact.hash or not _SHA256_HEX.fullmatch(artifact.hash):
        return None
    return f"sha256:file:{artifact.hash}"


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
    """
    Construct one policy-neutral admission report from normalized evidence.

    Parameters
    ----------
    outcome : AdmissionOutcome
        Classified evidence result.
    execution_path : pathlib.Path
        Candidate path supplied for admission.
    physical_target_path : str, optional
        Resolved audit path derived from the candidate.
    expected_source, expected_run_id, artifact_key, input_role
        Expected-identity provenance and report role fields.
    observations : tuple[str, ...]
        Stable machine-readable facts supporting the outcome.
    expected_artifact_id, observed_artifact_id, expected_bytes_source,
    expected_bytes_path, recommended_action, reason
        Optional expected, observed, fallback, and human-facing evidence fields.
    expected_direction, source_tracker_db
        Direction filter and external provenance database path when applicable.

    Returns
    -------
    AdmissionReport
        Frozen schema-v3 report ready for caller policy handling or serialization.
    """
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
    """One observed candidate path and its full-file admission identity."""

    execution_path: Path
    physical_target_path: str | None
    observed_identity: FileIdentity | None
    outcome: AdmissionOutcome | None = None
    observations: tuple[str, ...] = ()
    recommended_action: str | None = None
    reason: str | None = None


def _observe_candidate(path: str | Path) -> _CandidateObservation:
    """Read one candidate once with the admission-v1 file classifications."""
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
            observed_identity=FileIdentity.parse(admission_file_identity(candidate)),
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


def _report_candidate_failure(
    observation: _CandidateObservation,
    *,
    expected_source: ExpectedIdentitySource,
    expected_run_id: str | None,
    artifact_key: str | None,
    input_role: str,
    expected_identity: FileIdentity | None = None,
    expected_source_label: str | None = None,
    expected_source_uri: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    expected_direction: Literal["input", "output", "any"] | None = None,
    source_tracker_db: str | None = None,
) -> AdmissionReport:
    """Render a candidate read failure without claiming an observed identity."""
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
    expected_identity: FileIdentity,
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
) -> AdmissionReport:
    """Compare one already-read candidate identity with an expected identity."""
    observed_identity = observation.observed_identity
    assert observed_identity is not None
    outcome: AdmissionOutcome = (
        "verified" if observed_identity == expected_identity else "mismatched"
    )
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
        observations=("matched",) if outcome == "verified" else ("mismatched",),
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
    expected_identity: FileIdentity,
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
) -> AdmissionReport:
    """Compare one readable regular file with a typed expected raw-file identity.

    This source-neutral primitive computes a full SHA-256 over the candidate's
    raw bytes and does not read a Tracker, consult a registry, or use a
    cache-hashing strategy. The resolver that supplied ``expected_identity``
    remains responsible for any source-specific lookup or trust decision.
    """
    observation = _observe_candidate(execution_path)
    if observation.observed_identity is None:
        return _report_candidate_failure(
            observation,
            expected_source=expected_source,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=input_role,
            expected_identity=expected_identity,
            expected_source_label=expected_source_label,
            expected_source_uri=expected_source_uri,
            expected_bytes_source=expected_bytes_source,
            expected_bytes_path=expected_bytes_path,
            expected_direction=expected_direction,
            source_tracker_db=source_tracker_db,
        )
    return _compare_observed_identity(
        observation,
        expected_identity=expected_identity,
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
    )


def _expected_artifact(
    database: DatabaseManager,
    *,
    expected_run_id: str,
    artifact_key: str,
    expected_direction: Literal["input", "output", "any"] = "any",
) -> tuple[Artifact | None, str | None]:
    """
    Resolve one exact expected artifact without key-collapsing history helpers.

    Parameters
    ----------
    database : DatabaseManager
        Provenance database storing the expected run.
    expected_run_id : str
        Explicit run ID that must identify a completed run.
    artifact_key : str
        Exact key that must select one artifact link.
    expected_direction : {"input", "output", "any"}, default "any"
        Direction filter for the artifact link on the prior run.

    Returns
    -------
    tuple[Artifact or None, str or None]
        The uniquely selected artifact and no error code, or ``None`` and
        a stable observation code describing an absent, non-completed, or
        ambiguous expectation.

    Notes
    -----
    This helper deliberately does not use convenience lookups that collapse
    duplicate keys across history. Admission must retain ambiguity rather than
    selecting an arbitrary prior artifact.
    """
    with database.session_scope() as session:
        run = session.get(Run, expected_run_id)
        if run is None:
            return None, "expected_prior_run_absent"
        if run.status != "completed":
            return None, "expected_prior_run_not_completed"
        statement = (
            select(Artifact)
            .join(
                RunArtifactLink,
                Artifact.id == RunArtifactLink.artifact_id,  # ty: ignore[invalid-argument-type]
            )
            .where(RunArtifactLink.run_id == expected_run_id)
            .where(Artifact.key == artifact_key)
        )
        if expected_direction in ("input", "output"):
            statement = statement.where(RunArtifactLink.direction == expected_direction)
        statement = statement.order_by(col(Artifact.id))
        matches = list(session.exec(statement).all())

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
    """
    Determine whether two candidate paths name the same filesystem object.

    Parameters
    ----------
    candidate : pathlib.Path
        Observed file proposed for admission.
    expected : pathlib.Path
        Caller-supplied immutable expected-byte file.

    Returns
    -------
    bool
        ``True`` when resolved paths match or the operating system reports the
        paths as the same file, including hardlink aliases.

    Notes
    -----
    The expected-byte fallback must be distinct from observed bytes, so this
    check treats either lexical resolution or ``os.path.samefile`` equality as
    non-distinct.
    """
    if _resolved_path(candidate) == _resolved_path(expected):
        return True
    try:
        return os.path.samefile(candidate, expected)
    except OSError:
        return False


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
) -> AdmissionReport:
    """
    Compare a resolved file with one artifact from an explicit completed run.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Tracker whose provenance database contains the expected run and artifact,
        unless ``source_tracker_db`` is specified.
    execution_path : str or pathlib.Path
        Resolved regular file that the receiving workflow intends to consume.
    expected_run_id : str
        Exact completed run ID supplying the expected identity. Consist
        does not select or infer a baseline run automatically.
    artifact_key : str
        Exact artifact key that must identify one link on the expected run.
    expected_bytes_path : str or pathlib.Path or None, optional
        Distinct immutable copy of the historical bytes used only when the
        stored artifact hash lacks explicit full-file semantics. Its full-file
        digest must corroborate the historical 64-character hash, and it must
        not resolve to the observed file through a path, symlink, or hardlink.
    input_role : str or None, optional
        Stronger logical role to record in the report. Defaults to
        ``artifact_key`` and does not alter expected-artifact lookup.
    expected_direction : {"input", "output", "any"}, default "any"
        Direction filter for the artifact link on the prior run. Defaults to
        "any" to admit both inputs and outputs of prior runs.
    source_tracker_db : str or pathlib.Path or None, optional
        External DuckDB provenance database path supplying the expected run.

    Returns
    -------
    AdmissionReport
        Versioned report containing the outcome, expected and observed
        identities, audit paths, and supporting observations.

    Raises
    ------
    RuntimeError
        If neither ``source_tracker_db`` nor ``tracker`` has a configured
        provenance database.

    Notes
    -----
    Missing files, placeholder files, unresolved expectations, and byte
    mismatches are represented in the returned report rather than raised as
    policy exceptions. This function does not copy files, change run status, or
    decide whether an observation should stop a workflow.

    Examples
    --------
    Check a resolved feed against one exact input from a pinned baseline run::

        report = check_artifact_identity(
            tracker,
            execution_path="inputs/gtfs.zip",
            expected_run_id="baseline-beam-run",
            artifact_key="config:seattle/r5/seattle_gtfs.zip",
        )

        if report.outcome != "verified":
            raise RuntimeError(report.canonical_json())
    """
    if source_tracker_db is None and (tracker is None or tracker.db is None):
        raise RuntimeError("Artifact admission requires a provenance database.")

    role = input_role or artifact_key
    source_db_str: str | None = None
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

    observation = _observe_candidate(execution_path)
    candidate = observation.execution_path
    physical_target_path = observation.physical_target_path
    if observation.observed_identity is None:
        return _report_candidate_failure(
            observation,
            expected_source="prior_run",
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )
    observed_identity = observation.observed_identity

    owns_db = False
    db: DatabaseManager | None = None
    if source_tracker_db is not None:
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
                observed_artifact_id=str(observed_identity),
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
                    observed_artifact_id=str(observed_identity),
                    observations=("source_tracker_db_unreadable",),
                    reason=f"Failed to read source tracker database '{source_tracker_db}': {exc}",
                    expected_direction=expected_direction,
                    source_tracker_db=source_db_str,
                )
            raise
    finally:
        if owns_db and db is not None:
            db.engine.dispose()

    if expected_error is not None:
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=str(observed_identity),
            observations=(expected_error,),
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )
    assert artifact is not None

    expected_identity = _known_admission_identity(artifact)
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
                observed_artifact_id=str(observed_identity),
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
                    observed_artifact_id=str(observed_identity),
                    expected_bytes_source=expected_source,
                    expected_bytes_path=expected_audit_path,
                    observations=("looks_like_git_lfs_pointer",),
                    recommended_action="Run git lfs pull to materialize the expected bytes.",
                    expected_direction=expected_direction,
                    source_tracker_db=source_db_str,
                )
            expected_identity = admission_file_identity(expected_path)
        except OSError as exc:
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=str(observed_identity),
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=("expected_artifact_unverifiable",),
                reason=str(exc),
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        stored_hash = artifact.hash
        if (
            not isinstance(stored_hash, str)
            or not _SHA256_HEX.fullmatch(stored_hash)
            or expected_identity != f"sha256:file:{stored_hash}"
        ):
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observed_artifact_id=str(observed_identity),
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

    if expected_identity is None:
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=str(observed_identity),
            observations=("expected_artifact_unverifiable",),
            expected_direction=expected_direction,
            source_tracker_db=source_db_str,
        )

    return _compare_observed_identity(
        observation,
        expected_identity=FileIdentity.parse(expected_identity),
        expected_source="prior_run",
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=role,
        expected_bytes_source=expected_source,
        expected_bytes_path=expected_audit_path,
        expected_direction=expected_direction,
        source_tracker_db=source_db_str,
    )


def _attach_reference_context(
    report: AdmissionReport, reference: AdmissionReference
) -> AdmissionReport:
    """Attach configuration and consumer path runtime context from an AdmissionReference."""
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
) -> AdmissionReport:
    """
    Check a runtime-resolved input without recreating consumer path logic.

    The byte comparison delegates to :func:`check_artifact_identity`; this
    wrapper only attaches caller-proven runtime path evidence to its report.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Tracker whose provenance database contains the expected prior run.
    reference : AdmissionReference
        Frozen runtime context containing the host execution path and optional
        configuration and consumer-path evidence.
    expected_run_id : str
        Explicit completed run that supplies the expected input identity.
    expected_bytes_path : str or pathlib.Path, optional
        Distinct immutable expected-byte fallback for a historical artifact
        whose stored hash lacks explicit full-file semantics.
    expected_direction : {"input", "output", "any"}, default "any"
        Direction filter for the artifact link on the prior run. Defaults to
        "any" to admit both inputs and outputs of prior runs.
    source_tracker_db : str or pathlib.Path or None, optional
        External DuckDB provenance database path supplying the expected run.

    Returns
    -------
    AdmissionReport
        The base identity report enriched with validated reference context.

    Raises
    ------
    ValueError
        If ``reference.artifact_key`` is None.
    RuntimeError
        If neither ``source_tracker_db`` nor ``tracker`` has a provenance database.

    Notes
    -----
    ``physical_target_path`` remains derived by the underlying checker from
    ``reference.execution_path``. This API records a final consumer path but
    intentionally does not infer or validate host/container mapping itself.
    """
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
    )
    return _attach_reference_context(report, reference)


def check_expected_identity(
    *,
    execution_path: str | Path,
    input_role: str,
    expectation: DeclaredDigestExpectation,
) -> AdmissionReport:
    """Compare one file to a caller-declared, typed raw-file digest.

    The declaration is an expected identity, not evidence that its publisher or
    source URI is trusted. Consist verifies only equality with the raw bytes of
    ``execution_path`` and never creates an ``Artifact`` or consults a Tracker.
    """
    if not input_role:
        raise ValueError("declared-digest admission requires input_role")
    return compare_path_to_identity(
        execution_path=execution_path,
        expected_identity=expectation.identity,
        expected_source="declared_digest",
        input_role=input_role,
        expected_source_label=expectation.source_label,
        expected_source_uri=expectation.source_uri,
    )


def check_admission_reference_expected_identity(
    *,
    reference: AdmissionReference,
    expectation: DeclaredDigestExpectation,
) -> AdmissionReport:
    """Compare a runtime-resolved file to a declared digest without a Tracker.

    ``AdmissionReference`` retains caller-proven execution, configuration, and
    consumer-path context. A declared-digest reference must not carry an
    artifact key because no prior-run artifact was resolved for this check.
    """
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
    """
    Compare a resolved file with a prior run expectation.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Tracker whose provenance database contains the expected run, unless
        ``expectation.source_tracker_db`` is specified.
    execution_path : str or pathlib.Path
        Resolved regular file that the receiving workflow intends to consume.
    expectation : PriorRunExpectation
        Expected prior run parameters (run_id, artifact_key, direction, db, bytes).
    input_role : str or None, optional
        Logical role to record in the report. Overrides ``expectation.input_role``.

    Returns
    -------
    AdmissionReport
        Versioned report containing the outcome, expected and observed
        identities, audit paths, and supporting observations.
    """
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
    )


def check_admission_reference_prior_run_expectation(
    tracker: "Tracker" | None = None,
    *,
    reference: AdmissionReference,
    expectation: PriorRunExpectation,
) -> AdmissionReport:
    """
    Check a runtime-resolved input reference against a prior run expectation.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Tracker whose provenance database contains the expected run, unless
        ``expectation.source_tracker_db`` is specified.
    reference : AdmissionReference
        Caller runtime context containing execution path and optional config details.
    expectation : PriorRunExpectation
        Expected prior run parameters.

    Returns
    -------
    AdmissionReport
        Enriched admission report with caller runtime context.

    Raises
    ------
    ValueError
        If ``reference.artifact_key`` is present and does not match
        ``expectation.artifact_key``.
    """
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
    )
    return _attach_reference_context(report, reference)
