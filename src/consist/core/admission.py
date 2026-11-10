"""
Verify external files against prior-run or declared raw-file identities.

Admission identity is deliberately separate from Consist's cache fingerprinting.
It always uses a full SHA-256 digest of a regular file's raw bytes and returns a
policy-neutral report; callers such as PILATES decide whether an observation is
fatal. Expected identities may come from an explicit prior run or a typed local
declaration, but neither source changes cache identity or artifacts on its own.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from sqlmodel import col, select

from consist.artifact import Artifact as V2Artifact
from consist.core.logical_array_identity import (
    LogicalProfileName,
    admission_directory_identity,
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
    normalize_logical_matrices_identity,
)
from consist.core.persistence import DatabaseManager
from consist.digest import (
    Agreement,
    Contract,
    Digest,
    Expectation,
    Identity,
    Verdict,
    digest_bytes,
    digest_file,
)
from consist.engine import (
    ensure_digest,
    locate,
    observe,
    stage,
    verify as engine_verify,
)
from consist.location import (
    Location,
    Mount,
    Observation,
    lookup_live_ledger_by_path,
)
from consist.meta import strip_system_meta
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink
from consist.store.detect import SchemaState, detect_schema
from consist.store.ledger import StoreLedger

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ADMISSION_REPORT_SCHEMA_VERSION = 3
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_FILE_IDENTITY = re.compile(r"sha256:file:[0-9a-f]{64}")
_ADMISSION_IDENTITY = re.compile(
    r"sha256:(file|manifest-v1|logical_matrices):[0-9a-f]{64}"
)
_GIT_LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1\n"

AdmissionOutcome = Literal["verified", "mismatched", "unverified", "unreadable"]
ExpectedIdentitySource = Literal["prior_run", "declared_digest"]
AdmissionIdentityKind = Literal["file", "manifest-v1", "logical_matrices"]
IdentityScheme = Literal["auto", "container", "logical"]


class AdmissionIdentity(BaseModel):
    """
    One canonical self-describing SHA-256 identity for file, directory, or logical array admission.

    Supported schemes:
    - ``sha256:file:<hex>``
    - ``sha256:manifest-v1:<hex>``
    - ``sha256:logical_matrices:<hex>``

    Attributes
    ----------
    value : str
        Canonical self-describing identity string in the form
        ``sha256:<file|manifest-v1|logical_matrices>:<64 lowercase hex>``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: str

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: str) -> str:
        if not _ADMISSION_IDENTITY.fullmatch(value):
            raise ValueError(
                "admission identity must be sha256:<file|manifest-v1|logical_matrices>:<64 lowercase hexadecimal characters>"
            )
        return value

    @property
    def kind(self) -> AdmissionIdentityKind:
        """
        Return the identity kind segment.

        Returns
        -------
        {"file", "manifest-v1", "logical_matrices"}
            Kind segment extracted from ``value``.
        """
        kind_part = self.value.split(":", 2)[1]
        return cast(AdmissionIdentityKind, kind_part)

    @property
    def hex_digest(self) -> str:
        """
        Return the 64-character lowercase hexadecimal SHA-256 digest.

        Returns
        -------
        str
            Lowercase 64-character SHA-256 hex digest.
        """
        return self.value.split(":", 2)[2]

    @classmethod
    def parse(cls, value: str) -> "AdmissionIdentity":
        """
        Parse one canonical admission identity.

        Parameters
        ----------
        value : str
            Self-describing SHA-256 identity string.

        Returns
        -------
        AdmissionIdentity
            A ``FileIdentity`` instance when ``value`` uses the ``sha256:file:``
            scheme, or an ``AdmissionIdentity`` instance for ``manifest-v1``
            and ``logical_matrices`` schemes.

        Raises
        ------
        ValueError
            If ``value`` does not match a supported canonical admission scheme.
        """
        if isinstance(value, str) and _FILE_IDENTITY.fullmatch(value):
            return FileIdentity(value=value)
        return cls(value=value)

    def __str__(self) -> str:
        """Return the canonical self-describing identity."""
        return self.value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AdmissionIdentity):
            return self.value == other.value
        return False

    def __hash__(self) -> int:
        return hash(self.value)


class FileIdentity(AdmissionIdentity):
    """
    One canonical SHA-256 identity for the raw bytes of a regular file.

    Attributes
    ----------
    value : str
        Canonical raw-file SHA-256 identity in the form
        ``sha256:file:<64 lowercase hexadecimal characters>``.
    """

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
        """
        Parse one canonical raw-file SHA-256 identity.

        Parameters
        ----------
        value : str
            Canonical file identity string in the form
            ``sha256:file:<64 lowercase hex>``.

        Returns
        -------
        FileIdentity
            Validated ``FileIdentity`` instance.

        Raises
        ------
        ValueError
            If ``value`` is not a valid ``sha256:file:<64 lowercase hex>`` string.
        """
        return cls(value=value)


class DeclaredDigestExpectation(BaseModel):
    """
    A locally declared file, directory, or logical-array identity and optional source context.

    Attributes
    ----------
    identity : AdmissionIdentity
        Expected canonical admission identity (``sha256:file:<hex>``,
        ``sha256:manifest-v1:<hex>``, or ``sha256:logical_matrices:<hex>``).
        Strings matching those schemes are coerced automatically.
    source_label : str or None, optional
        Human-readable label describing the declared digest source.
    source_uri : str or None, optional
        Opaque URI or location describing the declared digest source.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization profile used when ``identity.kind == "logical_matrices"``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: AdmissionIdentity
    source_label: str | None = None
    source_uri: str | None = None
    logical_profile: LogicalProfileName = "preserve_canonical"

    @field_validator("identity", mode="before")
    @classmethod
    def _coerce_identity(cls, value: Any) -> Any:
        if isinstance(value, AdmissionIdentity):
            return value
        if isinstance(value, str):
            return AdmissionIdentity.parse(value)
        return value

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
    identity_scheme : {"auto", "container", "logical"}, default "auto"
        Whether to require exact container bytes/manifest (``"container"``),
        require logical array equivalence (``"logical"``), or try container
        first and fall back to logical array identity when present (``"auto"``).
    logical_profile : {"preserve_canonical", "float32_matrices_v1"} or None, optional
        Canonicalization profile to use when computing logical array identity.
        When ``None``, inferred from the prior-run artifact metadata or defaults
        to ``"preserve_canonical"``.
    expected_logical_hash : str or None, optional
        Optional caller-supplied ``sha256:logical_matrices:<hex>`` (or 64-hex)
        digest overriding or supplementing the prior artifact's metadata.
    """

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

    @field_validator("expected_logical_hash")
    @classmethod
    def _validate_expected_logical_hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_logical_matrices_identity(value)

    @classmethod
    def from_artifact(
        cls,
        artifact: V2Artifact | Artifact,
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
        """
        Construct a ``PriorRunExpectation`` from a resolved Consist ``Artifact``.

        Parameters
        ----------
        artifact : V2Artifact or Artifact
            Consist artifact whose ``key``, ``run_id``, and optional
            ``meta["logical_identity"]`` / ``meta["logical_profile"]`` (or v2
            ``sha256:logical_matrices:v1`` claim) seed the expectation.
        expected_run_id : str or None, optional
            Explicit run ID override. Required when ``artifact.run_id`` is ``None``.
        expected_direction : {"input", "output", "any"}, default "any"
            Expected link direction on the prior run.
        source_tracker_db : str or pathlib.Path or None, optional
            External DuckDB provenance database path.
        expected_bytes_path : str or pathlib.Path or None, optional
            Immutable fallback file path for historical hash corroboration.
        input_role : str or None, optional
            Logical role to record in the admission report.
        identity_scheme : {"auto", "container", "logical"}, default "auto"
            Identity comparison mode.
        logical_profile : {"preserve_canonical", "float32_matrices_v1"} or None, optional
            Canonicalization profile override.
        expected_logical_hash : str or None, optional
            Explicit logical array digest override.

        Returns
        -------
        PriorRunExpectation
            Populated expectation model.

        Raises
        ------
        ValueError
            If neither ``expected_run_id`` nor ``artifact.run_id`` is available,
            or if ``expected_logical_hash`` is malformed.
        """
        run_id = expected_run_id or (
            str(artifact.run_id) if artifact.run_id is not None else None
        )
        if not run_id:
            raise ValueError(
                "PriorRunExpectation.from_artifact requires artifact.run_id or explicit expected_run_id"
            )
        known_logical_hash, known_profile = _known_logical_admission_identity(artifact)
        resolved_logical_hash = (
            expected_logical_hash
            if expected_logical_hash is not None
            else known_logical_hash
        )
        resolved_profile = (
            logical_profile
            if logical_profile is not None
            else (known_profile if resolved_logical_hash is not None else None)
        )
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
        """
        Return the expected run identifier.

        Returns
        -------
        str
            Alias for ``expected_run_id``.
        """
        return self.expected_run_id

    @property
    def direction(self) -> Literal["input", "output", "any"]:
        """
        Return the expected artifact link direction.

        Returns
        -------
        {"input", "output", "any"}
            Alias for ``expected_direction``.
        """
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
    return digest_file(path)


def _is_git_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as source:
        return source.read(len(_GIT_LFS_POINTER_PREFIX)) == _GIT_LFS_POINTER_PREFIX


def _resolved_path(path: Path) -> str | None:
    try:
        return str(path.resolve(strict=False))
    except OSError:
        return None


def _known_admission_identity(artifact: V2Artifact | Artifact) -> str | None:
    """
    Return a trusted container identity when artifact semantics or manifest prove it.

    Parameters
    ----------
    artifact : V2Artifact or Artifact
        Persisted artifact from the explicitly selected expected run.

    Returns
    -------
    str or None
        ``sha256:file:<hex>`` when the artifact records Consist-computed raw
        regular-file SHA-256 semantics or explicit admission identity, or
        ``sha256:manifest-v1:<hex>`` when the artifact is a directory artifact
        with a verified ``directory_manifest``, otherwise ``None``.

    Notes
    -----
    Admission intentionally rejects fast cache fingerprints, legacy directory
    hashes without a directory manifest, and caller-supplied hashes even when
    they happen to be hexadecimal SHA-256 text.
    """
    if isinstance(artifact, V2Artifact):
        if artifact.identity.verifiable:
            file_claim = artifact.identity.get("sha256:file")
            if file_claim is not None:
                return f"sha256:file:{file_claim.hex}"
            manifest_claim = artifact.identity.get("sha256:manifest-v1")
            if manifest_claim is not None:
                return f"sha256:manifest-v1:{manifest_claim.hex}"
        return None

    meta = artifact.meta if isinstance(artifact.meta, Mapping) else None
    if meta and isinstance(meta.get("admission_file_identity"), str):
        explicit_identity = meta["admission_file_identity"]
        if _FILE_IDENTITY.fullmatch(explicit_identity):
            return explicit_identity
    if meta and isinstance(meta.get("admission_identity"), str):
        explicit_identity = meta["admission_identity"]
        if _ADMISSION_IDENTITY.fullmatch(explicit_identity):
            return explicit_identity

    if meta and meta.get("directory_artifact") is True:
        manifest = meta.get("directory_manifest")
        if isinstance(manifest, Mapping):
            tree_hash = manifest.get("tree_hash")
            if (
                isinstance(tree_hash, str)
                and _SHA256_HEX.fullmatch(tree_hash)
                and (not artifact.hash or artifact.hash == tree_hash)
            ):
                return f"sha256:manifest-v1:{tree_hash}"

    semantics = meta.get("hash_semantics") if meta else None
    if not isinstance(semantics, Mapping):
        return None
    semantics_dict = dict(semantics)
    if semantics_dict == {
        "version": 1,
        "algorithm": "sha256",
        "kind": "file",
        "digest_contract": "raw_file_bytes",
        "source": "computed_full",
    }:
        if not artifact.hash or not _SHA256_HEX.fullmatch(artifact.hash):
            return None
        return f"sha256:file:{artifact.hash}"
    if semantics_dict == {
        "version": 1,
        "algorithm": "sha256",
        "kind": "directory",
        "digest_contract": "directory_manifest_v1",
        "source": "computed_manifest",
    }:
        if not artifact.hash or not _SHA256_HEX.fullmatch(artifact.hash):
            return None
        return f"sha256:manifest-v1:{artifact.hash}"
    return None


def _known_logical_admission_identity(
    artifact: V2Artifact | Artifact,
) -> tuple[str | None, LogicalProfileName | None]:
    """
    Extract an optional logical array identity and profile from artifact claims or metadata.
    """
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else None
    raw_profile = meta.get("logical_profile") if meta else None
    explicit_profile: LogicalProfileName | None = (
        raw_profile
        if raw_profile in ("preserve_canonical", "float32_matrices_v1")
        else None
    )
    if isinstance(artifact, V2Artifact):
        logical_claim = artifact.identity.get("sha256:logical_matrices:v1")
        if logical_claim is not None:
            return (
                f"sha256:logical_matrices:{logical_claim.hex}",
                explicit_profile or "preserve_canonical",
            )
    if not meta:
        return None, None
    if isinstance(meta.get("logical_identity"), str):
        try:
            normalized = normalize_logical_matrices_identity(meta["logical_identity"])
            return normalized, explicit_profile or "preserve_canonical"
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
    """
    Construct one policy-neutral admission report from normalized evidence.
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
    """One observed candidate path and its admission identity."""

    execution_path: Path
    physical_target_path: str | None
    observed_identity: AdmissionIdentity | None
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


def _observe_candidate_for_kind(
    path: str | Path,
    *,
    kind: AdmissionIdentityKind,
    logical_profile: LogicalProfileName = "preserve_canonical",
) -> _CandidateObservation:
    """Observe a candidate path according to the target admission identity kind."""
    if kind == "file":
        return _observe_candidate(path)

    candidate = Path(path).expanduser()
    physical_target_path = _resolved_path(candidate)

    if kind == "manifest-v1":
        if not candidate.is_dir():
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=None,
                outcome="unreadable",
                observations=("directory_unreadable",),
                reason="Directory manifest admission requires a readable directory.",
            )
        try:
            identity_str = admission_directory_identity(candidate)
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=AdmissionIdentity.parse(identity_str),
            )
        except OSError as exc:
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=None,
                outcome="unreadable",
                observations=("directory_unreadable",),
                reason=str(exc),
            )

    if kind == "logical_matrices":
        if not candidate.exists():
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=None,
                outcome="unreadable",
                observations=("file_unreadable",),
                reason="Logical matrix admission requires an existing OMX/HDF5 file or Zarr directory.",
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
            if candidate.is_dir():
                digest = compute_zarr_logical_matrices_sha256(
                    candidate, policy=logical_profile
                )
            elif candidate.is_file():
                digest = compute_omx_logical_matrices_sha256(
                    candidate, policy=logical_profile
                )
            else:
                return _CandidateObservation(
                    execution_path=candidate,
                    physical_target_path=physical_target_path,
                    observed_identity=None,
                    outcome="unreadable",
                    observations=("file_unreadable",),
                    reason="Logical matrix admission requires a regular file or directory.",
                )
            identity_str = normalize_logical_matrices_identity(digest)
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=AdmissionIdentity.parse(identity_str),
            )
        except (OSError, KeyError, ValueError, RuntimeError) as exc:
            return _CandidateObservation(
                execution_path=candidate,
                physical_target_path=physical_target_path,
                observed_identity=None,
                outcome="unreadable",
                observations=("logical_container_unreadable",),
                reason=str(exc),
            )

    raise ValueError(f"Unsupported admission identity kind: {kind!r}")


def _report_candidate_failure(
    observation: _CandidateObservation,
    *,
    expected_source: ExpectedIdentitySource,
    expected_run_id: str | None,
    artifact_key: str | None,
    input_role: str,
    expected_identity: AdmissionIdentity | None = None,
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
    expected_identity: AdmissionIdentity,
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
    """Compare one already-read candidate identity with an expected identity."""
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
    expected_identity: AdmissionIdentity | str,
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
) -> AdmissionReport:
    """
    Compare one candidate path with a typed expected admission identity.

    Parameters
    ----------
    execution_path : str or pathlib.Path
        Candidate file or directory path to hash and compare.
    expected_identity : AdmissionIdentity or str
        Canonical expected identity (``sha256:file:<hex>``,
        ``sha256:manifest-v1:<hex>``, or ``sha256:logical_matrices:<hex>``).
    input_role : str
        Logical input role recorded in the returned ``AdmissionReport``.
    expected_source : {"prior_run", "declared_digest"}
        Source of the expected identity.
    artifact_key : str or None, optional
        Artifact key when ``expected_source="prior_run"``.
    expected_run_id : str or None, optional
        Run ID when ``expected_source="prior_run"``.
    expected_source_label : str or None, optional
        Optional human-readable source label for declared digests.
    expected_source_uri : str or None, optional
        Optional source URI for declared digests.
    expected_bytes_source : {"explicit_immutable_path"} or None, optional
        Evidence source used to corroborate a historical artifact hash.
    expected_bytes_path : str or None, optional
        Resolved path of the corroborating immutable file, if any.
    expected_direction : {"input", "output", "any"} or None, optional
        Expected artifact link direction on the prior run.
    source_tracker_db : str or None, optional
        Resolved path of an external provenance database, if used.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"}, default "preserve_canonical"
        Canonicalization profile used when ``expected_identity`` has kind
        ``"logical_matrices"``.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report describing the verification outcome.

    Raises
    ------
    ValueError
        If ``expected_identity`` cannot be parsed as a valid
        ``AdmissionIdentity``.

    Notes
    -----
    This source-neutral primitive computes a full SHA-256 over the candidate's
    raw file bytes, directory manifest, or canonical logical arrays according to
    ``expected_identity.kind`` and does not read a Tracker, consult a registry,
    or use a cache-hashing strategy.
    """
    parsed_identity = (
        expected_identity
        if isinstance(expected_identity, AdmissionIdentity)
        else AdmissionIdentity.parse(expected_identity)
    )
    observation = _observe_candidate_for_kind(
        execution_path,
        kind=parsed_identity.kind,
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
    if parsed_identity.kind == "logical_matrices":
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


def _query_expected_artifact_v2_ledger(
    ledger: StoreLedger,
    *,
    expected_run_id: str,
    artifact_key: str,
    expected_direction: Literal["input", "output", "any"] = "any",
) -> tuple[V2Artifact | None, str | None, str | None]:
    con = ledger.connection
    run_row = con.execute(
        "SELECT status FROM run WHERE id = ?",
        [expected_run_id],
    ).fetchone()
    if run_row is None:
        return (None, "expected_prior_run_absent", None)
    if str(run_row[0]) != "completed":
        return (None, "expected_prior_run_not_completed", None)

    role_prefix = (
        f"expected_{expected_direction}_role"
        if expected_direction in ("input", "output")
        else "expected_role"
    )
    dir_clause = (
        "AND rb.direction = 'output' "
        if expected_direction == "output"
        else (
            "AND rb.direction IN ('input', 'depends_on') "
            if expected_direction == "input"
            else ""
        )
    )
    rows = con.execute(
        f"SELECT DISTINCT a.id, a.key, a.driver, a.anchor, a.run_id, a.schema_id, a.meta "
        f"FROM run_binding rb "
        f"JOIN artifact a ON a.id = rb.artifact_id "
        f"WHERE rb.run_id = ? {dir_clause}"
        f"AND (a.key = ? OR rb.slot = ?) "
        f"ORDER BY a.id",
        [expected_run_id, artifact_key, artifact_key],
    ).fetchall()

    if not rows:
        return (None, f"{role_prefix}_absent", None)

    if len(rows) != 1:
        return (None, f"{role_prefix}_ambiguous", None)

    art_id, art_key, art_driver, art_anchor, art_run_id, art_schema_id, art_raw_meta = (
        rows[0]
    )
    effective_identity, recorded_anchor, _verified_anchor = (
        ledger.resolve_effective_identity(str(art_anchor))
    )
    raw_meta_dict: dict[str, Any] = {}
    if isinstance(art_raw_meta, str) and art_raw_meta:
        try:
            loaded_meta = json.loads(art_raw_meta)
            if isinstance(loaded_meta, Mapping):
                raw_meta_dict = dict(loaded_meta)
        except ValueError:
            raw_meta_dict = {}
    elif isinstance(art_raw_meta, Mapping):
        raw_meta_dict = dict(art_raw_meta)

    v2_art = V2Artifact(
        id=art_id if isinstance(art_id, uuid.UUID) else uuid.UUID(str(art_id)),
        key=str(art_key),
        driver=str(art_driver),
        identity=effective_identity,
        hints=(),
        run_id=str(art_run_id) if art_run_id is not None else None,
        origin=ledger.ref,
        meta=strip_system_meta(raw_meta_dict),
        recorded_anchor=recorded_anchor,
        schema_id=str(art_schema_id) if art_schema_id is not None else None,
    )

    historical_v1_hash: str | None = None
    if not v2_art.identity.verifiable:
        historical_v1_hash = v2_art.anchor.hex
        try:
            audit_row = con.execute(
                "SELECT v1_row_json FROM artifact_migration_audit WHERE artifact_id = ?",
                [v2_art.id],
            ).fetchone()
            if audit_row is not None and isinstance(audit_row[0], str):
                parsed_v1 = json.loads(audit_row[0])
                if isinstance(parsed_v1, Mapping) and isinstance(
                    parsed_v1.get("hash"), str
                ):
                    historical_v1_hash = parsed_v1["hash"]
        except Exception:
            pass
    return (v2_art, None, historical_v1_hash)


def _expected_artifact_v2(
    db_file: Path,
    *,
    expected_run_id: str,
    artifact_key: str,
    expected_direction: Literal["input", "output", "any"] = "any",
    live_ledger: StoreLedger | None = None,
) -> tuple[V2Artifact | None, str | None, str | None]:
    """Resolve one expected v2 `Artifact` from a v2 `StoreLedger` database."""
    live = (
        live_ledger
        if (isinstance(live_ledger, StoreLedger) and not live_ledger._closed)
        else lookup_live_ledger_by_path(db_file)
    )
    if isinstance(live, StoreLedger) and not live._closed:
        return _query_expected_artifact_v2_ledger(
            live,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            expected_direction=expected_direction,
        )
    with StoreLedger(db_file, read_only=True) as ledger:
        return _query_expected_artifact_v2_ledger(
            ledger,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            expected_direction=expected_direction,
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
    identity_scheme: IdentityScheme = "auto",
    logical_profile: LogicalProfileName | None = None,
    expected_logical_hash: str | None = None,
) -> AdmissionReport:
    """
    Compare a resolved file or directory with one artifact from an explicit completed run.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Consist tracker owning the provenance database. May be ``None`` when
        ``source_tracker_db`` is provided.
    execution_path : str or pathlib.Path
        Candidate file or directory path to verify before workflow execution.
    expected_run_id : str
        Explicit completed run ID that produced or consumed the expected artifact.
    artifact_key : str
        Exact artifact key linked to ``expected_run_id``.
    expected_bytes_path : str or pathlib.Path or None, optional
        Distinct immutable file path used to corroborate a historical artifact
        hash that lacks explicit full-file hash semantics.
    input_role : str or None, optional
        Logical role recorded in the report. Defaults to ``artifact_key``.
    expected_direction : {"input", "output", "any"}, default "any"
        Constraint on the artifact link direction on ``expected_run_id``.
    source_tracker_db : str or pathlib.Path or None, optional
        Optional path to an external DuckDB provenance database opened strictly
        read-only without schema initialization.
    identity_scheme : {"auto", "container", "logical"}, default "auto"
        Comparison mode:
        - ``"container"``: require exact raw-file bytes (``sha256:file:<hex>``)
          or directory manifest (``sha256:manifest-v1:<hex>``).
        - ``"logical"``: require canonical logical array equivalence
          (``sha256:logical_matrices:<hex>``).
        - ``"auto"``: check container identity first and, if container bytes or
          formats differ (e.g., ``.omx`` vs. ``.zarr``), fall back to logical
          array identity when available.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"} or None, optional
        Canonicalization profile used when computing logical array identity.
        When ``None``, inferred from the prior-run artifact metadata or defaults
        to ``"preserve_canonical"``.
    expected_logical_hash : str or None, optional
        Optional caller-supplied ``sha256:logical_matrices:<hex>`` (or 64-hex)
        digest overriding or supplementing the prior artifact's metadata.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report describing the verification result and
        supporting observations.

    Raises
    ------
    RuntimeError
        If neither ``source_tracker_db`` nor a tracker with an active provenance
        database is supplied.
    ValueError
        If ``expected_logical_hash`` is provided and is not a valid 64-character
        hexadecimal SHA-256 digest or ``sha256:logical_matrices:<hex>`` URI.
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

    candidate = Path(execution_path).expanduser()
    physical_target_path = _resolved_path(candidate)

    # Fast-path observation for missing paths or regular files when not forcing logical-only.
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

    target_db_file: Path | None = None
    if source_tracker_db is not None:
        target_db_file = Path(source_tracker_db).expanduser()
    elif tracker is not None and tracker.db_path is not None:
        target_db_file = Path(tracker.db_path).expanduser()

    is_v2_db = False
    live_ledger: StoreLedger | None = None
    if target_db_file is not None and target_db_file.is_file():
        found_live = lookup_live_ledger_by_path(target_db_file)
        if isinstance(found_live, StoreLedger) and not found_live._closed:
            live_ledger = found_live
            is_v2_db = True
        else:
            try:
                is_v2_db = detect_schema(target_db_file) is SchemaState.V2
            except Exception:
                is_v2_db = False

    artifact: V2Artifact | Artifact | None = None
    expected_error: str | None = None
    v2_historical_hash: str | None = None

    if is_v2_db and target_db_file is not None:
        try:
            artifact, expected_error, v2_historical_hash = _expected_artifact_v2(
                target_db_file,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                expected_direction=expected_direction,
                live_ledger=live_ledger,
            )
        except Exception as exc:
            if source_tracker_db is not None:
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
    else:
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
                db.engine.dispose()

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
    resolved_logical_identity = (
        normalize_logical_matrices_identity(expected_logical_hash)
        if expected_logical_hash is not None
        else artifact_logical_identity
    )
    resolved_logical_profile: LogicalProfileName = (
        logical_profile or artifact_logical_profile or "preserve_canonical"
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
            expected_identity=AdmissionIdentity.parse(resolved_logical_identity),
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
            expected_identity = admission_file_identity(expected_path)
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
        stored_hash = (
            (v2_historical_hash or artifact.anchor.hex)
            if isinstance(artifact, V2Artifact)
            else artifact.hash
        )
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

    if expected_identity is None:
        if identity_scheme == "auto" and resolved_logical_identity is not None:
            return compare_path_to_identity(
                execution_path=candidate,
                expected_identity=AdmissionIdentity.parse(resolved_logical_identity),
                input_role=role,
                expected_source="prior_run",
                artifact_key=artifact_key,
                expected_run_id=expected_run_id,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
                logical_profile=resolved_logical_profile,
            )
        if file_observation is None:
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

    parsed_expected = AdmissionIdentity.parse(expected_identity)
    if parsed_expected.kind == "file" and file_observation is not None:
        container_observation = file_observation
    else:
        container_observation = _observe_candidate_for_kind(
            candidate,
            kind=parsed_expected.kind,
            logical_profile=resolved_logical_profile,
        )

    if container_observation.observed_identity is not None:
        container_report = _compare_observed_identity(
            container_observation,
            expected_identity=parsed_expected,
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
                expected_identity=parsed_expected,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                expected_direction=expected_direction,
                source_tracker_db=source_db_str,
            )
        container_report = None

    # Auto fallback to logical array identity when container bytes/format differed.
    if identity_scheme == "auto" and resolved_logical_identity is not None:
        logical_expected = AdmissionIdentity.parse(resolved_logical_identity)
        logical_obs = _observe_candidate_for_kind(
            candidate,
            kind="logical_matrices",
            logical_profile=resolved_logical_profile,
        )
        if logical_obs.observed_identity is not None:
            extra_obs = (
                ("container_bytes_mismatched", "logical_identity_verified")
                if logical_obs.observed_identity == logical_expected
                else ("container_bytes_mismatched", "logical_identity_mismatched")
            )
            return _compare_observed_identity(
                logical_obs,
                expected_identity=logical_expected,
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
        expected_identity=parsed_expected,
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
    identity_scheme: IdentityScheme = "auto",
    logical_profile: LogicalProfileName | None = None,
    expected_logical_hash: str | None = None,
) -> AdmissionReport:
    """
    Check a runtime-resolved input without recreating consumer path logic.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Consist tracker owning the provenance database. May be ``None`` when
        ``source_tracker_db`` is provided.
    reference : AdmissionReference
        Caller-proven runtime path and configuration evidence. Must include a
        non-null ``artifact_key``.
    expected_run_id : str
        Explicit completed run ID supplying the expected artifact.
    expected_bytes_path : str or pathlib.Path or None, optional
        Distinct immutable file path used to corroborate a historical artifact
        hash that lacks explicit full-file hash semantics.
    expected_direction : {"input", "output", "any"}, default "any"
        Constraint on the artifact link direction on ``expected_run_id``.
    source_tracker_db : str or pathlib.Path or None, optional
        Optional path to an external DuckDB provenance database opened strictly
        read-only.
    identity_scheme : {"auto", "container", "logical"}, default "auto"
        Whether to verify container bytes, logical array contents, or try
        container first with logical fallback.
    logical_profile : {"preserve_canonical", "float32_matrices_v1"} or None, optional
        Canonicalization profile used when computing logical array identity.
    expected_logical_hash : str or None, optional
        Optional caller-supplied ``sha256:logical_matrices:<hex>`` (or 64-hex)
        digest overriding or supplementing the prior artifact's metadata.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report enriched with the configuration and
        consumer path fields from ``reference``.

    Raises
    ------
    ValueError
        If ``reference.artifact_key`` is ``None`` or ``expected_logical_hash``
        is malformed.
    RuntimeError
        If neither ``source_tracker_db`` nor a tracker with an active provenance
        database is supplied.
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
    """
    Compare one file or directory to a caller-declared, typed digest.

    Parameters
    ----------
    execution_path : str or pathlib.Path
        Candidate file or directory path to verify.
    input_role : str
        Non-empty logical role recorded in the report.
    expectation : DeclaredDigestExpectation
        Declared digest expectation containing an ``AdmissionIdentity``
        (``file``, ``manifest-v1``, or ``logical_matrices``), optional source
        metadata, and ``logical_profile``.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report with ``expected_source="declared_digest"``.

    Raises
    ------
    ValueError
        If ``input_role`` is empty.
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
        logical_profile=expectation.logical_profile,
    )


def check_admission_reference_expected_identity(
    *,
    reference: AdmissionReference,
    expectation: DeclaredDigestExpectation,
) -> AdmissionReport:
    """
    Compare a runtime-resolved file or directory to a declared digest without a Tracker.

    Parameters
    ----------
    reference : AdmissionReference
        Caller-proven runtime path and configuration evidence. Must have
        ``artifact_key=None`` and a non-empty ``input_role``.
    expectation : DeclaredDigestExpectation
        Declared digest expectation to verify against ``reference.execution_path``.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report enriched with the configuration and
        consumer path fields from ``reference``.

    Raises
    ------
    ValueError
        If ``reference.artifact_key`` is not ``None``.
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
    Compare a resolved file or directory with a ``PriorRunExpectation``.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Consist tracker owning the provenance database. May be ``None`` when
        ``expectation.source_tracker_db`` is provided.
    execution_path : str or pathlib.Path
        Candidate file or directory path to verify.
    expectation : PriorRunExpectation
        Typed prior-run expectation specifying ``expected_run_id``,
        ``artifact_key``, ``expected_direction``, ``source_tracker_db``,
        ``identity_scheme``, ``logical_profile``, and optional
        ``expected_logical_hash``.
    input_role : str or None, optional
        Optional logical role override. When ``None``, falls back to
        ``expectation.input_role`` and then ``expectation.artifact_key``.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report describing the verification outcome.

    Raises
    ------
    RuntimeError
        If neither ``expectation.source_tracker_db`` nor a tracker with an
        active provenance database is supplied.
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
    """
    Check a runtime-resolved input reference against a ``PriorRunExpectation``.

    Parameters
    ----------
    tracker : Tracker or None, optional
        Consist tracker owning the provenance database. May be ``None`` when
        ``expectation.source_tracker_db`` is provided.
    reference : AdmissionReference
        Caller-proven runtime path and configuration evidence. If
        ``reference.artifact_key`` is set, it must match
        ``expectation.artifact_key``.
    expectation : PriorRunExpectation
        Typed prior-run expectation to verify against ``reference.execution_path``.

    Returns
    -------
    AdmissionReport
        Policy-neutral admission report enriched with the configuration and
        consumer path fields from ``reference``.

    Raises
    ------
    ValueError
        If ``reference.artifact_key`` is set and differs from
        ``expectation.artifact_key``.
    RuntimeError
        If neither ``expectation.source_tracker_db`` nor a tracker with an
        active provenance database is supplied.
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
        identity_scheme=expectation.identity_scheme,
        logical_profile=expectation.logical_profile,
        expected_logical_hash=expectation.expected_logical_hash,
    )
    return _attach_reference_context(report, reference)


@dataclass(frozen=True, slots=True)
class HydratedAdmittedInput:
    """Result of unified v2 input admission and local path staging."""

    artifact: V2Artifact
    path: Path
    report: AdmissionReport
    verdict: Verdict


def _admission_identity_to_digest(adm_id: AdmissionIdentity | str) -> Digest:
    parsed = (
        adm_id
        if isinstance(adm_id, AdmissionIdentity)
        else AdmissionIdentity.parse(str(adm_id))
    )
    if parsed.kind == "logical_matrices":
        return Digest("sha256:logical_matrices:v1", parsed.hex_digest)
    return Digest(f"sha256:{parsed.kind}", parsed.hex_digest)


def _digest_to_admission_identity_str(digest: Digest) -> str:
    if digest.scheme in ("sha256:logical_matrices:v1", "sha256:logical_matrices"):
        return f"sha256:logical_matrices:{digest.hex}"
    if digest.scheme in ("sha256:file", "sha256:manifest-v1"):
        return str(digest)
    return str(digest)


def _verify_and_attach_digest(
    v2_art: V2Artifact,
    v2_exp: Expectation,
    *,
    effective_contract: Contract,
    local_path: Path,
    ledger: Any,
    effective_mounts: Mapping[str, str | Path | Mount] | None,
    policy: Any = None,
) -> tuple[V2Artifact, Verdict]:
    raw_verdict = engine_verify(
        v2_art,
        v2_exp,
        contract=effective_contract,
        ledger=ledger,
        mounts=effective_mounts,
        policy=policy,
    )
    verdict = (
        raw_verdict
        if isinstance(raw_verdict, Verdict)
        else Verdict(
            Agreement.AGREE if raw_verdict else Agreement.DISAGREE,
            effective_contract.equivalence,
            (),
            True,
        )
    )
    if (
        verdict.matched_scheme is not None
        and v2_art.identity.get(verdict.matched_scheme) is None
        and v2_art.identity.verifiable
    ):
        try:
            v2_art, _ = ensure_digest(
                v2_art,
                verdict.matched_scheme,
                path=local_path,
                ledger=ledger,
                mounts=effective_mounts,
                policy=policy,
            )
        except Exception:
            pass
    return (v2_art, verdict)


def _unreadable_hydrated_input(
    *,
    v2_art: V2Artifact,
    candidate_path: Path,
    role: str,
    expectation: Any,
    tracker: "Tracker" | None,
) -> HydratedAdmittedInput:
    if isinstance(expectation, PriorRunExpectation):
        report = check_prior_run_expectation(
            tracker,
            execution_path=candidate_path,
            expectation=expectation,
            input_role=role,
        )
    elif isinstance(expectation, DeclaredDigestExpectation):
        report = check_expected_identity(
            execution_path=candidate_path,
            input_role=role,
            expectation=expectation,
        )
    elif isinstance(expectation, AdmissionIdentity):
        report = check_expected_identity(
            execution_path=candidate_path,
            input_role=role,
            expectation=DeclaredDigestExpectation(identity=expectation),
        )
    else:
        obs = _observe_candidate(candidate_path)
        report = _report_candidate_failure(
            obs,
            expected_source="declared_digest",
            expected_run_id=None,
            artifact_key=None,
            input_role=role,
        )
    return HydratedAdmittedInput(
        artifact=v2_art,
        path=candidate_path,
        report=report,
        verdict=Verdict(Agreement.UNKNOWN, None, (), False),
    )


def hydrate_admitted_input(
    artifact_or_path: V2Artifact | Artifact | str | Path,
    *,
    input_role: str | None = None,
    destination: str | Path | None = None,
    expectation: (
        Expectation
        | DeclaredDigestExpectation
        | PriorRunExpectation
        | AdmissionIdentity
        | Digest
        | str
        | None
    ) = None,
    contract: Contract | None = None,
    tracker: "Tracker" | None = None,
    ledger: Any = None,
    mounts: Mapping[str, str | Path | Mount] | None = None,
    stage_mode: Literal["auto", "reflink", "hardlink", "symlink", "copy"] = "auto",
) -> HydratedAdmittedInput:
    """
    Observe/locate/stage an input artifact or path and verify it against an expectation.

    Built directly on top of the v2 engine primitives (`observe()`, `locate()`,
    `stage()`, `ensure_digest()`, `verify()`, and `compare()`).
    """
    effective_mounts = (
        mounts
        if mounts is not None
        else (tracker.mounts if tracker is not None else None)
    )

    if isinstance(artifact_or_path, V2Artifact):
        v2_art = artifact_or_path
        if input_role is not None and input_role.strip() and v2_art.key != input_role:
            v2_art = replace(v2_art, key=input_role)
    elif isinstance(artifact_or_path, Artifact):
        legacy_path: Path | None = None
        if artifact_or_path.abs_path:
            legacy_path = Path(artifact_or_path.abs_path)
        elif tracker is not None and artifact_or_path.container_uri:
            resolved_str = tracker.resolve_uri(artifact_or_path.container_uri)
            if resolved_str:
                legacy_path = Path(resolved_str)
        elif artifact_or_path.container_uri:
            legacy_path = Path(artifact_or_path.container_uri)
        role_for_legacy = input_role or artifact_or_path.key
        raw_meta = (
            dict(artifact_or_path.meta)
            if isinstance(artifact_or_path.meta, Mapping)
            else {}
        )
        cleaned_meta = strip_system_meta(raw_meta)
        if legacy_path is None or not legacy_path.exists():
            fallback_path = (
                legacy_path
                if legacy_path is not None
                else Path(artifact_or_path.container_uri or artifact_or_path.key)
            )
            fallback_hex = digest_bytes(
                f"{artifact_or_path.id}:{role_for_legacy}:{artifact_or_path.hash}".encode(
                    "utf-8"
                )
            )
            placeholder_art = V2Artifact(
                id=artifact_or_path.id,
                key=role_for_legacy,
                driver=artifact_or_path.driver,
                identity=Identity(anchor=Digest("legacy-v1:unverified", fallback_hex)),
                hints=(
                    Observation(
                        location=Location(uri=str(fallback_path)),
                        identity=Identity(
                            anchor=Digest("legacy-v1:unverified", fallback_hex)
                        ),
                    ),
                ),
                run_id=(
                    str(artifact_or_path.run_id)
                    if artifact_or_path.run_id is not None
                    else None
                ),
                meta=cleaned_meta,
            )
            return _unreadable_hydrated_input(
                v2_art=placeholder_art,
                candidate_path=fallback_path,
                role=role_for_legacy,
                expectation=expectation,
                tracker=tracker,
            )
        observed = observe(
            legacy_path,
            key=role_for_legacy,
            driver=artifact_or_path.driver,
            ledger=ledger,
            mounts=effective_mounts,
        )
        v2_art = replace(
            observed,
            id=artifact_or_path.id,
            key=role_for_legacy,
            run_id=(
                str(artifact_or_path.run_id)
                if artifact_or_path.run_id is not None
                else None
            ),
            meta=cleaned_meta,
        )
    elif isinstance(artifact_or_path, (str, Path)):
        cand_path = Path(artifact_or_path).expanduser()
        role_for_path = input_role or cand_path.name or "input"
        if not cand_path.exists():
            fallback_hex = digest_bytes(str(cand_path).encode("utf-8"))
            placeholder_art = V2Artifact(
                id=uuid.uuid4(),
                key=role_for_path,
                driver="file",
                identity=Identity(anchor=Digest("legacy-v1:unverified", fallback_hex)),
                hints=(
                    Observation(
                        location=Location(uri=str(cand_path)),
                        identity=Identity(
                            anchor=Digest("legacy-v1:unverified", fallback_hex)
                        ),
                    ),
                ),
            )
            return _unreadable_hydrated_input(
                v2_art=placeholder_art,
                candidate_path=cand_path,
                role=role_for_path,
                expectation=expectation,
                tracker=tracker,
            )
        v2_art = observe(
            cand_path,
            key=input_role,
            ledger=ledger,
            mounts=effective_mounts,
        )
    else:
        raise TypeError(
            f"Unsupported artifact_or_path type: {type(artifact_or_path).__name__}"
        )

    role = input_role or v2_art.key

    # Resolve or stage the local path via v2 engine primitives
    stage_contract = contract if contract is not None else Contract()
    try:
        if destination is not None:
            local_path = stage(
                v2_art,
                Path(destination),
                mode=stage_mode,
                contract=stage_contract,
                ledger=ledger,
                mounts=effective_mounts,
            )
        else:
            located = locate(
                v2_art,
                ledger=ledger,
                mounts=effective_mounts,
            )
            local_path = located.path
    except FileNotFoundError:
        fallback_uri = v2_art.hints[0].location.uri if v2_art.hints else v2_art.key
        fallback_p = Path(fallback_uri.split("://", 1)[-1])
        return _unreadable_hydrated_input(
            v2_art=v2_art,
            candidate_path=fallback_p,
            role=role,
            expectation=expectation,
            tracker=tracker,
        )

    # Determine effective Contract
    if contract is not None:
        effective_contract = contract
    elif (
        (
            isinstance(expectation, DeclaredDigestExpectation)
            and expectation.identity.kind == "logical_matrices"
        )
        or (
            isinstance(expectation, AdmissionIdentity)
            and expectation.kind == "logical_matrices"
        )
        or (
            isinstance(expectation, Digest)
            and expectation.scheme
            in ("sha256:logical_matrices:v1", "sha256:logical_matrices")
        )
        or (
            isinstance(expectation, str)
            and expectation.strip().startswith("sha256:logical_matrices")
        )
        or (
            isinstance(expectation, PriorRunExpectation)
            and expectation.identity_scheme == "logical"
        )
    ):
        effective_contract = Contract(equivalence="sha256:logical_matrices:v1")
    elif local_path.is_dir():
        effective_contract = Contract(equivalence="sha256:manifest-v1")
    else:
        effective_contract = Contract(equivalence="sha256:file")

    # Case 1: PriorRunExpectation
    if isinstance(expectation, PriorRunExpectation):
        report = check_prior_run_expectation(
            tracker,
            execution_path=local_path,
            expectation=expectation,
            input_role=role,
        )
        if report.expected_artifact_id is not None:
            exp_digest = _admission_identity_to_digest(report.expected_artifact_id)
            if contract is None:
                effective_contract = Contract(equivalence=exp_digest.scheme)
            v2_exp = Expectation(
                frozenset({exp_digest}),
                basis=((exp_digest.scheme, "computed"),),
            )
            prior_policy = (
                expectation.logical_profile
                if expectation.logical_profile not in (None, "preserve_canonical")
                else None
            )
            v2_art, verdict = _verify_and_attach_digest(
                v2_art,
                v2_exp,
                effective_contract=effective_contract,
                local_path=local_path,
                ledger=ledger,
                effective_mounts=effective_mounts,
                policy=prior_policy,
            )
        else:
            verdict = Verdict(Agreement.UNKNOWN, None, (), False)
        return HydratedAdmittedInput(
            artifact=v2_art,
            path=local_path,
            report=report,
            verdict=verdict,
        )

    # Case 2: DeclaredDigestExpectation, AdmissionIdentity, Digest, or str
    if isinstance(
        expectation,
        (DeclaredDigestExpectation, AdmissionIdentity, Digest, str),
    ):
        if isinstance(expectation, DeclaredDigestExpectation):
            decl = expectation
            target_digest = _admission_identity_to_digest(decl.identity)
        elif isinstance(expectation, AdmissionIdentity):
            decl = DeclaredDigestExpectation(identity=expectation)
            target_digest = _admission_identity_to_digest(expectation)
        elif isinstance(expectation, Digest):
            target_digest = expectation
            adm_str = _digest_to_admission_identity_str(expectation)
            decl = (
                DeclaredDigestExpectation(identity=AdmissionIdentity.parse(adm_str))
                if _ADMISSION_IDENTITY.fullmatch(adm_str)
                else None
            )
        else:
            raw_str = expectation.strip()
            if _ADMISSION_IDENTITY.fullmatch(raw_str):
                adm_obj = AdmissionIdentity.parse(raw_str)
                decl = DeclaredDigestExpectation(identity=adm_obj)
                target_digest = _admission_identity_to_digest(adm_obj)
            else:
                target_digest = Digest.parse(raw_str)
                adm_str = _digest_to_admission_identity_str(target_digest)
                decl = (
                    DeclaredDigestExpectation(identity=AdmissionIdentity.parse(adm_str))
                    if _ADMISSION_IDENTITY.fullmatch(adm_str)
                    else None
                )

        if contract is None:
            effective_contract = Contract(equivalence=target_digest.scheme)

        decl_policy = (
            decl.logical_profile
            if (
                decl is not None
                and decl.logical_profile not in (None, "preserve_canonical")
            )
            else None
        )
        v2_exp = Expectation(
            frozenset({target_digest}),
            basis=((target_digest.scheme, "declared"),),
        )
        v2_art, verdict = _verify_and_attach_digest(
            v2_art,
            v2_exp,
            effective_contract=effective_contract,
            local_path=local_path,
            ledger=ledger,
            effective_mounts=effective_mounts,
            policy=decl_policy,
        )

        if decl is not None:
            report = check_expected_identity(
                execution_path=local_path,
                input_role=role,
                expectation=decl,
            )
        else:
            outcome: AdmissionOutcome = (
                "verified"
                if verdict.agreement == Agreement.AGREE
                else (
                    "mismatched"
                    if verdict.agreement == Agreement.DISAGREE
                    else "unverified"
                )
            )
            obs_digest = (
                v2_art.identity.get(verdict.matched_scheme)
                if verdict.matched_scheme is not None
                else v2_art.identity.anchor
            )
            report = _report(
                outcome=outcome,
                execution_path=local_path,
                physical_target_path=_resolved_path(local_path),
                expected_source="declared_digest",
                expected_run_id=None,
                artifact_key=None,
                input_role=role,
                expected_artifact_id=_digest_to_admission_identity_str(target_digest),
                observed_artifact_id=(
                    _digest_to_admission_identity_str(obs_digest)
                    if obs_digest is not None
                    else None
                ),
                observations=(
                    ("matched",)
                    if outcome == "verified"
                    else (
                        ("mismatched",) if outcome == "mismatched" else ("unverified",)
                    )
                ),
            )
        return HydratedAdmittedInput(
            artifact=v2_art,
            path=local_path,
            report=report,
            verdict=verdict,
        )

    # Case 3: v2 Expectation or None (default self-consistency / contract verification)
    v2_exp = (
        expectation
        if isinstance(expectation, Expectation)
        else Expectation(
            frozenset(v2_art.identity.all_digests),
            basis=tuple((d.scheme, "computed") for d in v2_art.identity.all_digests),
        )
    )
    if contract is None and v2_exp.pinned_schemes:
        if "sha256:logical_matrices:v1" in v2_exp.pinned_schemes:
            effective_contract = Contract(equivalence="sha256:logical_matrices:v1")
        else:
            effective_contract = Contract(
                equivalence=next(iter(sorted(v2_exp.pinned_schemes)))
            )

    v2_art, verdict = _verify_and_attach_digest(
        v2_art,
        v2_exp,
        effective_contract=effective_contract,
        local_path=local_path,
        ledger=ledger,
        effective_mounts=effective_mounts,
    )

    outcome_v2: AdmissionOutcome = (
        "verified"
        if verdict.agreement == Agreement.AGREE
        else ("mismatched" if verdict.agreement == Agreement.DISAGREE else "unverified")
    )
    matched_scheme = verdict.matched_scheme or effective_contract.equivalence
    obs_d = v2_art.identity.get(matched_scheme) or v2_art.identity.anchor
    exp_d = (
        next(
            (d for d in v2_exp.digests if d.scheme == matched_scheme),
            next(iter(v2_exp.digests), obs_d),
        )
        if v2_exp.digests
        else obs_d
    )
    report = _report(
        outcome=outcome_v2,
        execution_path=local_path,
        physical_target_path=_resolved_path(local_path),
        expected_source="declared_digest",
        expected_run_id=None,
        artifact_key=None,
        input_role=role,
        expected_artifact_id=_digest_to_admission_identity_str(exp_d),
        observed_artifact_id=_digest_to_admission_identity_str(obs_d),
        observations=(
            ("matched",)
            if outcome_v2 == "verified"
            else (("mismatched",) if outcome_v2 == "mismatched" else ("unverified",))
        ),
        reason=(
            None
            if outcome_v2 == "verified"
            else "Resolved file does not match the expected digest."
        ),
    )
    return HydratedAdmittedInput(
        artifact=v2_art,
        path=local_path,
        report=report,
        verdict=verdict,
    )
