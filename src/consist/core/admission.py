"""
Verify external files against identities recorded by completed prior runs.

Admission identity is deliberately separate from Consist's cache fingerprinting.
It always uses a full SHA-256 digest of a regular file's raw bytes and returns a
policy-neutral report; callers such as PILATES decide whether an observation is
fatal. Version 1 resolves expected identities from one explicit prior run.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict
from sqlmodel import col, select

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


ADMISSION_REPORT_SCHEMA_VERSION = 1
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_GIT_LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1\n"

AdmissionOutcome = Literal["verified", "mismatched", "unverified", "unreadable"]


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
    artifact_key : str
        Exact persisted input-artifact key selected on the expected run.
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
    expected_source : {"prior_run"}
        Source used to resolve the expected identity in the V1 contract.
    expected_run_id : str
        Explicit completed run supplying the expected input artifact.
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

    Notes
    -----
    Reports are frozen Pydantic models. They describe evidence but do not assign
    workflow severity or mutate run state.
    """

    model_config = ConfigDict(frozen=True)

    report_schema_version: int = ADMISSION_REPORT_SCHEMA_VERSION
    outcome: AdmissionOutcome
    input_role: str
    artifact_key: str
    config_key: str | None = None
    config_reference_key: str | None = None
    feed_key: str | None = None
    raw_config_value: str | None = None
    canonical_value: str | None = None
    configured_path: str | None = None
    execution_path: str
    physical_target_path: str | None = None
    expected_source: Literal["prior_run"] = "prior_run"
    expected_run_id: str
    expected_artifact_id: str | None = None
    observed_artifact_id: str | None = None
    digest_algorithm: Literal["sha256"] = "sha256"
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None
    expected_bytes_path: str | None = None
    observations: tuple[str, ...] = ()
    recommended_action: str | None = None
    reason: str | None = None

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
        return json.dumps(
            self.model_dump(mode="json"),
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
    semantics = artifact.meta.get("hash_semantics") if artifact.meta else None
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
    expected_run_id: str,
    artifact_key: str,
    input_role: str,
    observations: tuple[str, ...],
    expected_artifact_id: str | None = None,
    observed_artifact_id: str | None = None,
    expected_bytes_source: Literal["explicit_immutable_path"] | None = None,
    expected_bytes_path: str | None = None,
    recommended_action: str | None = None,
    reason: str | None = None,
) -> AdmissionReport:
    return AdmissionReport(
        outcome=outcome,
        input_role=input_role,
        artifact_key=artifact_key,
        execution_path=str(execution_path),
        physical_target_path=physical_target_path,
        expected_run_id=expected_run_id,
        expected_artifact_id=expected_artifact_id,
        observed_artifact_id=observed_artifact_id,
        expected_bytes_source=expected_bytes_source,
        expected_bytes_path=expected_bytes_path,
        observations=observations,
        recommended_action=recommended_action,
        reason=reason,
    )


def _expected_input_artifact(
    tracker: "Tracker", *, expected_run_id: str, artifact_key: str
) -> tuple[Artifact | None, str | None]:
    """Resolve one exact expected input without key-collapsing history helpers."""
    database = tracker.db
    if database is None:
        raise RuntimeError("Artifact admission requires a provenance database.")
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
            .where(RunArtifactLink.direction == "input")
            .where(Artifact.key == artifact_key)
            .order_by(col(Artifact.id))
        )
        matches = list(session.exec(statement).all())
    if not matches:
        return None, "expected_input_role_absent"
    if len(matches) != 1:
        return None, "expected_input_role_ambiguous"
    return matches[0], None


def _paths_are_same(candidate: Path, expected: Path) -> bool:
    if _resolved_path(candidate) == _resolved_path(expected):
        return True
    try:
        return os.path.samefile(candidate, expected)
    except OSError:
        return False


def check_artifact_identity(
    tracker: "Tracker",
    *,
    execution_path: str | Path,
    expected_run_id: str,
    artifact_key: str,
    expected_bytes_path: str | Path | None = None,
    input_role: str | None = None,
) -> AdmissionReport:
    """
    Compare a resolved file with one input from an explicit completed run.

    Parameters
    ----------
    tracker : Tracker
        Tracker whose provenance database contains the expected run and input
        artifact.
    execution_path : str or pathlib.Path
        Resolved regular file that the receiving workflow intends to consume.
    expected_run_id : str
        Exact completed run ID supplying the expected input identity. Consist
        does not select or infer a baseline run automatically.
    artifact_key : str
        Exact artifact key that must identify one ``direction="input"`` link on
        the expected run.
    expected_bytes_path : str or pathlib.Path or None, optional
        Distinct immutable copy of the historical bytes used only when the
        stored artifact hash lacks explicit full-file semantics. Its full-file
        digest must corroborate the historical 64-character hash, and it must
        not resolve to the observed file through a path, symlink, or hardlink.
    input_role : str or None, optional
        Stronger logical role to record in the report. Defaults to
        ``artifact_key`` and does not alter expected-artifact lookup.

    Returns
    -------
    AdmissionReport
        Versioned report containing the outcome, expected and observed
        identities, audit paths, and supporting observations.

    Raises
    ------
    RuntimeError
        If ``tracker`` has no configured provenance database.

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
    candidate = Path(execution_path).expanduser()
    physical_target_path = _resolved_path(candidate)
    role = input_role or artifact_key

    if not candidate.is_file():
        return _report(
            outcome="unreadable",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observations=("file_unreadable",),
            reason="Admission requires a readable regular file.",
        )
    try:
        if _is_git_lfs_pointer(candidate):
            return _report(
                outcome="unverified",
                execution_path=candidate,
                physical_target_path=physical_target_path,
                expected_run_id=expected_run_id,
                artifact_key=artifact_key,
                input_role=role,
                observations=("looks_like_git_lfs_pointer",),
                recommended_action="Run git lfs pull to materialize the file bytes.",
            )
        observed_identity = admission_file_identity(candidate)
    except OSError as exc:
        return _report(
            outcome="unreadable",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observations=("file_unreadable",),
            reason=str(exc),
        )

    artifact, expected_error = _expected_input_artifact(
        tracker, expected_run_id=expected_run_id, artifact_key=artifact_key
    )
    if expected_error is not None:
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=observed_identity,
            observations=(expected_error,),
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
                observed_artifact_id=observed_identity,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=(
                    "expected_artifact_unverifiable",
                    "expected_bytes_not_distinct",
                ),
                reason="Expected bytes must be a distinct readable regular file.",
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
                    observed_artifact_id=observed_identity,
                    expected_bytes_source=expected_source,
                    expected_bytes_path=expected_audit_path,
                    observations=("looks_like_git_lfs_pointer",),
                    recommended_action="Run git lfs pull to materialize the expected bytes.",
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
                observed_artifact_id=observed_identity,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=("expected_artifact_unverifiable",),
                reason=str(exc),
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
                observed_artifact_id=observed_identity,
                expected_bytes_source=expected_source,
                expected_bytes_path=expected_audit_path,
                observations=(
                    "expected_artifact_unverifiable",
                    "expected_bytes_not_correlated",
                ),
                reason=(
                    "Expected bytes did not corroborate the historical artifact hash."
                ),
            )

    if expected_identity is None:
        return _report(
            outcome="unverified",
            execution_path=candidate,
            physical_target_path=physical_target_path,
            expected_run_id=expected_run_id,
            artifact_key=artifact_key,
            input_role=role,
            observed_artifact_id=observed_identity,
            observations=("expected_artifact_unverifiable",),
        )

    outcome: AdmissionOutcome = (
        "verified" if observed_identity == expected_identity else "mismatched"
    )
    return _report(
        outcome=outcome,
        execution_path=candidate,
        physical_target_path=physical_target_path,
        expected_run_id=expected_run_id,
        artifact_key=artifact_key,
        input_role=role,
        expected_artifact_id=expected_identity,
        observed_artifact_id=observed_identity,
        expected_bytes_source=expected_source,
        expected_bytes_path=expected_audit_path,
        observations=("matched",) if outcome == "verified" else ("mismatched",),
        reason=(
            None
            if outcome == "verified"
            else "Resolved file does not match the expected prior-run artifact."
        ),
    )
