"""Outbound archive and recovery-root registration support for ``Tracker``."""

from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Sequence

from sqlmodel import col, select

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.container_policy import validate_recovery_registration_policy
from consist.core.materialize import (
    ArchivedRunOutputFile,
    ArchivedRunOutputFilesReport,
    ArchiveRunOutputFileStatus,
    ArchiveRunOutputVerificationStatus,
    ArtifactRecoveryCopyRegistration,
    RecoveryCopyStatus,
    RunOutputRecoveryCopiesRegistration,
    _output_set_hydration_kind,
)
from consist.core.materialize_options import normalize_materialize_output_keys
from consist.models.artifact import Artifact, ArchivedOutputs

_FILE_HASH_CHUNK_SIZE = 8 * 1024 * 1024


def _compute_file_sha256(path: Path) -> str:
    """Compute a full-content SHA-256 digest.

    Parameters
    ----------
    path : Path
        Regular file whose bytes are hashed.

    Returns
    -------
    str
        Lowercase hexadecimal SHA-256 digest.
    """
    sha256 = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(_FILE_HASH_CHUNK_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


@dataclass(frozen=True, slots=True)
class _ArchiveFileCandidate:
    """One requested file output and its canonical archive destination.

    Attributes
    ----------
    key : str
        Run-output key selected for archival.
    artifact : Artifact
        Output artifact whose URI-relative layout determines the target.
    target_path : Path
        Canonical destination below the requested recovery root.
    source_path : Path | None
        Resolved source file, once source discovery has succeeded.
    """

    key: str
    artifact: Artifact
    target_path: Path
    source_path: Path | None = None


class TrackerArchiveService(_TrackerServiceBase):
    """Archive bytes and persist advisory recovery-root metadata.

    Notes
    -----
    This internal service is the implementation source for the corresponding
    :class:`Tracker` façade methods. Recovery roots are alternate byte sources;
    they do not change an artifact's canonical URI or identity.
    """

    def set_artifact_recovery_roots(
        self,
        artifact: Artifact,
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        *,
        append: bool = False,
    ) -> Artifact:
        """Persist advisory filesystem recovery roots for an artifact.

        Parameters
        ----------
        artifact : Artifact
            Artifact whose recovery metadata should be updated.
        roots : path-like or sequence of path-like
            Ordered filesystem roots to store after normalization.
        append : bool, default False
            If ``True``, append roots after existing distinct roots; otherwise
            replace them. An empty normalized list clears the metadata field.

        Returns
        -------
        Artifact
            The supplied artifact with its in-memory metadata refreshed.

        Raises
        ------
        TypeError
            If ``artifact`` is not an :class:`Artifact`.
        RuntimeError
            If the tracker has no metadata database.

        Notes
        -----
        Recovery roots are ordered fallback locations during historical
        rematerialization. ``container_uri`` remains canonical.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if self.db is None:
            raise RuntimeError(
                "Cannot update artifact recovery roots: tracker has no database configured."
            )

        incoming = self.fs.normalize_recovery_roots(roots)
        existing = self.fs.normalize_recovery_roots(
            (artifact.meta or {}).get("recovery_roots")
        )
        normalized = incoming
        if append:
            normalized = self.fs.normalize_recovery_roots([*existing, *incoming])

        if not normalized:
            current_meta = dict(artifact.meta or {})
            current_meta.pop("recovery_roots", None)
            self.db.update_artifact_meta(
                artifact, {"recovery_roots": None}, raise_on_error=True
            )
            artifact.meta = current_meta
            self._run_artifacts_cache.clear()
            return artifact

        self.db.update_artifact_meta(
            artifact, {"recovery_roots": normalized}, raise_on_error=True
        )
        artifact.meta = dict(artifact.meta or {})
        artifact.meta["recovery_roots"] = normalized
        self._run_artifacts_cache.clear()
        return artifact

    def archive_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> Path:
        """Archive a rematerializable artifact into a stable recovery root.

        Parameters
        ----------
        artifact : Artifact
            Artifact whose source bytes will be copied or moved.
        archive_root : path-like
            Root below which the artifact's URI-relative layout is recreated.
        mode : {"copy", "move"}, default "copy"
            Filesystem operation used when the destination is absent.
        append : bool, default True
            Whether to append ``archive_root`` to existing recovery roots.

        Returns
        -------
        Path
            Archive destination containing the artifact bytes.

        Raises
        ------
        ValueError
            If the artifact is not rematerializable or ``mode`` is invalid.
        FileNotFoundError
            If source bytes cannot be found.
        FileExistsError
            If a distinct archive destination already exists.

        Notes
        -----
        The archive preserves canonical identity and ``container_uri``.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if mode not in {"copy", "move"}:
            raise ValueError("mode must be 'copy' or 'move'.")
        if self.db is None:
            raise RuntimeError(
                "Cannot archive artifact: tracker has no database configured."
            )

        relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI "
                "layout. Use managed output paths or preserve a stable relative "
                "layout before archiving. Absolute-path and file:// artifacts "
                "cannot be recovered from root-only recovery metadata."
            )

        archive_root_path = Path(archive_root).resolve()
        destination = (archive_root_path / relative_path).resolve()
        source_path: Path | None = None
        if artifact.run_id:
            from consist.core.materialize import find_existing_recovery_source_path

            producing_run = self.get_run(str(artifact.run_id))
            if producing_run is not None:
                _, source_path, _ = find_existing_recovery_source_path(
                    self.tracker,
                    artifact=artifact,
                    run=producing_run,
                    source_root=None,
                )
        if source_path is None and artifact.run_id is None and artifact.abs_path:
            candidate = Path(artifact.abs_path).resolve()
            if candidate.exists():
                source_path = candidate
        if source_path is None and artifact.run_id is None:
            candidate = Path(self.resolve_uri(artifact.container_uri)).resolve()
            if candidate.exists():
                source_path = candidate
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        destination_preexisted = destination.exists()
        moved_from: Path | None = None
        if destination.exists():
            if destination.is_symlink():
                raise ValueError(
                    f"Symlink detected in archive destination: {destination}"
                )
            if destination.resolve() != source_path.resolve():
                if source_path.is_file() and destination.is_file():
                    same_size = source_path.stat().st_size == destination.stat().st_size
                    same_hash = same_size and self.identity.compute_file_checksum(
                        str(source_path)
                    ) == self.identity.compute_file_checksum(str(destination))
                    if not same_hash:
                        raise FileExistsError(
                            f"Archive destination already exists: {destination}"
                        )
                else:
                    raise FileExistsError(
                        f"Archive destination already exists: {destination}"
                    )
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source_path.resolve() != destination.resolve():
                if mode == "copy":
                    if source_path.is_dir():
                        shutil.copytree(source_path, destination)
                    else:
                        shutil.copy2(source_path, destination)
                else:
                    moved_from = source_path
                    shutil.move(str(source_path), str(destination))

        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, [archive_root_path], append=append
            )
        except Exception:
            if moved_from is not None and destination.exists():
                moved_from.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination), str(moved_from))
            elif not destination_preexisted and destination.exists():
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
            raise

        if mode == "move":
            artifact.abs_path = str(destination.resolve())
        return destination

    def register_artifact_recovery_copy(
        self,
        artifact: Artifact,
        recovery_root: str | os.PathLike[str],
        *,
        verify: bool = True,
        content_hash: str | None = None,
        append: bool = True,
    ) -> ArtifactRecoveryCopyRegistration:
        """Verify and record an externally copied artifact recovery location.

        Parameters
        ----------
        artifact : Artifact
            Artifact represented by the existing recovery copy.
        recovery_root : path-like
            Root containing the artifact at its URI-relative path.
        verify : bool, default True
            Whether to verify the copy's full-file SHA-256 digest.
        content_hash : str | None, optional
            Expected SHA-256 digest, taking precedence over ``artifact.hash``.
        append : bool, default True
            Whether to append instead of replace recovery-root metadata.

        Returns
        -------
        ArtifactRecoveryCopyRegistration
            Per-artifact verification and metadata-persistence outcome.

        Notes
        -----
        This method never copies bytes. It blocks directory artifacts and HDF5
        child tables until they have an independent recovery contract.
        """
        return self._register_artifact_recovery_copy(
            artifact,
            recovery_root,
            verify=verify,
            content_hash=content_hash,
            append=append,
            persist=True,
        )

    def _register_artifact_recovery_copy(
        self,
        artifact: Artifact,
        recovery_root: str | os.PathLike[str],
        *,
        verify: bool,
        content_hash: str | None,
        append: bool,
        persist: bool,
    ) -> ArtifactRecoveryCopyRegistration:
        """Validate one recovery copy and optionally persist its metadata.

        Parameters
        ----------
        artifact : Artifact
            Artifact represented by the existing copy.
        recovery_root : path-like
            Root containing the URI-relative copy.
        verify : bool
            Whether to require a matching full-file hash.
        content_hash : str | None
            Optional caller-supplied expected hash.
        append : bool
            Whether a successful update appends the recovery root.
        persist : bool
            Whether to write recovery-root metadata after validation.

        Returns
        -------
        ArtifactRecoveryCopyRegistration
            Validation result, including whether bytes were verified and
            metadata was committed.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if self.db is None:
            raise RuntimeError(
                "Cannot register artifact recovery copy: tracker has no database configured."
            )

        recovery_root_path = Path(recovery_root).resolve()
        artifact_id = str(artifact.id) if artifact.id is not None else ""

        def result(
            status: RecoveryCopyStatus,
            *,
            expected_path: Path | None = None,
            message: str | None = None,
            metadata_updated: bool = False,
            verification_succeeded: bool = False,
        ) -> ArtifactRecoveryCopyRegistration:
            return ArtifactRecoveryCopyRegistration(
                artifact=artifact,
                key=artifact.key,
                artifact_id=artifact_id,
                recovery_root=recovery_root_path,
                expected_path=expected_path,
                status=status,
                message=message,
                metadata_updated=metadata_updated,
                verification_succeeded=verification_succeeded,
            )

        parent = (
            self.get_parent_artifact(artifact)
            if artifact.driver == "h5_table"
            else None
        )
        policy_validation = validate_recovery_registration_policy(
            artifact, parent=parent
        )
        if not policy_validation.allowed:
            return result(
                "blocked_by_container_policy", message=policy_validation.message
            )

        relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
        if relative_path is None:
            return result(
                "skipped_unmapped",
                message=(
                    f"Artifact {artifact.key!r} does not have a rematerializable URI "
                    "layout. Absolute-path and file:// artifacts cannot be adopted "
                    "from root-only recovery metadata."
                ),
            )

        expected_path = recovery_root_path / relative_path
        expected_path_resolved = expected_path.resolve()
        if expected_path.is_symlink():
            return result(
                "symlink_destination",
                expected_path=expected_path_resolved,
                message=(
                    "Symlink detected in recovery destination: "
                    f"{expected_path_resolved}"
                ),
            )
        if not expected_path.exists():
            return result(
                "missing_copy",
                expected_path=expected_path_resolved,
                message=f"Expected recovery copy does not exist: {expected_path_resolved}",
            )
        if expected_path.is_dir():
            return result(
                "unsupported_directory",
                expected_path=expected_path_resolved,
                message=(
                    "Directory recovery-copy adoption is not supported yet; use "
                    "archive_artifact(...) or wait for directory manifest support."
                ),
            )
        if not expected_path.is_file():
            return result(
                "failed",
                expected_path=expected_path_resolved,
                message=(
                    "Expected recovery copy is not a regular file: "
                    f"{expected_path_resolved}"
                ),
            )

        expected_hashes: list[tuple[str, str]] = []
        if content_hash is not None:
            expected_hashes.append(("content_hash", content_hash))
        elif artifact.hash and self.identity.hashing_strategy == "full":
            expected_hashes.append(("artifact.hash", artifact.hash))
        if verify and not expected_hashes:
            return result(
                "unverifiable_hash",
                expected_path=expected_path_resolved,
                message=(
                    "Verification requested, but no full file hash is available. "
                    "Pass content_hash=<sha256> or use verify=False to register "
                    "the existing copy without byte verification."
                ),
            )
        if verify:
            try:
                actual_hash = _compute_file_sha256(expected_path_resolved)
            except Exception as exc:
                return result(
                    "failed",
                    expected_path=expected_path_resolved,
                    message=f"Could not hash recovery copy {expected_path_resolved}: {exc}",
                )
            mismatches = [
                label
                for label, expected_hash in expected_hashes
                if actual_hash != expected_hash
            ]
            if mismatches:
                return result(
                    "hash_mismatch",
                    expected_path=expected_path_resolved,
                    message=(
                        "Recovery copy hash did not match "
                        f"{', '.join(mismatches)} for artifact {artifact.key!r}."
                    ),
                )

        if not persist:
            return result(
                "registered",
                expected_path=expected_path_resolved,
                message="Recovery copy verified; metadata update deferred.",
                verification_succeeded=True,
            )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, [recovery_root_path], append=append
            )
        except Exception as exc:
            return result(
                "failed",
                expected_path=expected_path_resolved,
                message=f"Could not update recovery_roots metadata: {exc}",
                verification_succeeded=True,
            )
        return result(
            "registered",
            expected_path=expected_path_resolved,
            message="Recovery copy verified and registered.",
            metadata_updated=True,
            verification_succeeded=True,
        )

    def _set_artifact_recovery_roots_bulk(
        self,
        artifacts: Sequence[Artifact],
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        *,
        append: bool,
    ) -> None:
        """Persist recovery roots for artifacts in one database transaction.

        Parameters
        ----------
        artifacts : sequence of Artifact
            Persisted artifacts receiving the same recovery roots.
        roots : path-like or sequence of path-like
            Roots to normalize and store.
        append : bool
            Whether to append roots to each artifact's current metadata.

        Raises
        ------
        RuntimeError
            If no metadata database is configured.
        TypeError
            If an item is not an artifact.
        ValueError
            If an artifact has no persistent identifier.
        KeyError
            If an artifact is no longer present in the database.
        """
        if self.db is None:
            raise RuntimeError(
                "Cannot update artifact recovery roots: tracker has no database configured."
            )

        incoming = self.fs.normalize_recovery_roots(roots)
        updates: dict[str, tuple[Artifact, dict[str, Any]]] = {}
        for artifact in artifacts:
            if not isinstance(artifact, Artifact):
                raise TypeError("artifact must be an Artifact instance.")
            if artifact.id is None:
                raise ValueError("artifact must have an id.")
            existing = self.fs.normalize_recovery_roots(
                (artifact.meta or {}).get("recovery_roots")
            )
            normalized = incoming
            if append:
                normalized = self.fs.normalize_recovery_roots([*existing, *incoming])
            next_meta = dict(artifact.meta or {})
            if normalized:
                next_meta["recovery_roots"] = normalized
            else:
                next_meta.pop("recovery_roots", None)
            updates[str(artifact.id)] = (artifact, next_meta)

        if not updates:
            return
        artifact_ids = [artifact.id for artifact, _ in updates.values()]
        with self.db.session_scope() as session:
            db_artifacts = session.exec(
                select(Artifact).where(col(Artifact.id).in_(artifact_ids))
            ).all()
            db_artifacts_by_id = {
                str(db_artifact.id): db_artifact for db_artifact in db_artifacts
            }
            missing_ids = sorted(set(updates) - set(db_artifacts_by_id))
            if missing_ids:
                raise KeyError(
                    "Artifacts were not found for recovery root update: "
                    + ", ".join(missing_ids)
                )
            for artifact_id, (_, next_meta) in updates.items():
                db_artifact = db_artifacts_by_id[artifact_id]
                db_artifact.meta = dict(next_meta)
                session.add(db_artifact)
            session.commit()

        for artifact, next_meta in updates.values():
            artifact.meta = dict(next_meta)
        self._run_artifacts_cache.clear()

    def register_run_output_recovery_copies(
        self,
        run_id: str,
        recovery_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        verify: bool = True,
        append: bool = True,
        content_hashes: Mapping[str, str] | None = None,
    ) -> RunOutputRecoveryCopiesRegistration:
        """Verify and record externally copied recovery locations for run outputs.

        Parameters
        ----------
        run_id : str
            Completed run whose outputs are being registered.
        recovery_root : path-like
            Root containing URI-relative recovery copies.
        keys : sequence of str | None, optional
            Outputs to register; ``None`` selects all outputs.
        verify : bool, default True
            Whether every selected copy must have a verified full-file hash.
        append : bool, default True
            Whether to append recovery-root metadata for successful outputs.
        content_hashes : mapping of str to str | None, optional
            Optional per-key SHA-256 proofs for artifacts without full hashes.

        Returns
        -------
        RunOutputRecoveryCopiesRegistration
            Mapping-style result with a real outcome for every selected key.

        Raises
        ------
        KeyError
            If requested output or content-hash keys are unknown.

        Notes
        -----
        Unknown keys fail before filesystem or metadata work. Per-key blockers
        do not prevent other outputs from being registered.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys, caller="register_run_output_recovery_copies"
        )
        outputs = self.get_run_outputs(run_id)
        selected = self._select_required_output_keys(
            outputs, normalized_keys, run_id=run_id
        )
        if content_hashes is not None:
            unknown_hash_keys = [key for key in content_hashes if key not in selected]
            if unknown_hash_keys:
                raise KeyError(
                    "content_hashes contained keys that were not selected for run "
                    f"{run_id!r}: {', '.join(repr(key) for key in unknown_hash_keys)}"
                )

        registered: dict[str, ArtifactRecoveryCopyRegistration] = {}
        pending_metadata_updates: list[
            tuple[str, Artifact, ArtifactRecoveryCopyRegistration]
        ] = []
        for key, artifact in selected.items():
            registration = self.tracker._register_artifact_recovery_copy(
                artifact,
                recovery_root,
                verify=verify,
                content_hash=content_hashes.get(key) if content_hashes else None,
                append=append,
                persist=False,
            )
            registered[key] = registration
            if registration.status == "registered":
                pending_metadata_updates.append((key, artifact, registration))

        recovery_root_path = Path(recovery_root).resolve()
        if pending_metadata_updates:
            try:
                self.tracker._set_artifact_recovery_roots_bulk(
                    [artifact for _, artifact, _ in pending_metadata_updates],
                    [recovery_root_path],
                    append=append,
                )
            except Exception:
                for key, artifact, registration in pending_metadata_updates:
                    try:
                        self.tracker.set_artifact_recovery_roots(
                            artifact, [recovery_root_path], append=append
                        )
                    except Exception as exc:
                        registered[key] = replace(
                            registration,
                            status="failed",
                            message=f"Could not update recovery_roots metadata: {exc}",
                            metadata_updated=False,
                        )
                    else:
                        registered[key] = replace(
                            registration,
                            message="Recovery copy verified and registered.",
                            metadata_updated=True,
                        )
            else:
                for key, _, registration in pending_metadata_updates:
                    registered[key] = replace(
                        registration,
                        message="Recovery copy verified and registered.",
                        metadata_updated=True,
                    )
        return RunOutputRecoveryCopiesRegistration(outputs=registered)

    @staticmethod
    def _archive_result(
        candidate: _ArchiveFileCandidate,
        copy_status: ArchiveRunOutputFileStatus,
        verification_status: ArchiveRunOutputVerificationStatus,
        *,
        message: str | None = None,
    ) -> ArchivedRunOutputFile:
        """Build one immutable entry in an archive-output report.

        Parameters
        ----------
        candidate : _ArchiveFileCandidate
            Output and paths represented by the result.
        copy_status : ArchiveRunOutputFileStatus
            Outcome of destination inspection or byte copying.
        verification_status : ArchiveRunOutputVerificationStatus
            Outcome of the requested byte-verification policy.
        message : str | None, optional
            Human-readable diagnostic for a non-happy-path result.

        Returns
        -------
        ArchivedRunOutputFile
            Report entry with metadata initially uncommitted.
        """
        return ArchivedRunOutputFile(
            artifact=candidate.artifact,
            key=candidate.key,
            source_path=candidate.source_path,
            target_path=candidate.target_path,
            copy_status=copy_status,
            verification_status=verification_status,
            message=message,
        )

    @staticmethod
    def _has_symlink_component(path: Path) -> bool:
        """Return whether a path or any of its ancestors is a symlink.

        Parameters
        ----------
        path : Path
            Path to inspect without resolving it.

        Returns
        -------
        bool
            ``True`` when a symlink component is present.
        """
        return any(component.is_symlink() for component in (path, *path.parents))

    def _inspect_existing_archive_target(
        self,
        candidate: _ArchiveFileCandidate,
        *,
        preserve_existing: bool,
        verify: bool,
    ) -> ArchivedRunOutputFile | None:
        """Evaluate a pre-existing archive destination without changing bytes.

        Parameters
        ----------
        candidate : _ArchiveFileCandidate
            Output and target path being archived.
        preserve_existing : bool
            Whether a matching existing regular file may be retained.
        verify : bool
            Whether a retained file must match the artifact's full hash.

        Returns
        -------
        ArchivedRunOutputFile | None
            Terminal result for an existing target, or ``None`` when copying
            should proceed.
        """
        if not candidate.target_path.exists():
            return None
        if not preserve_existing:
            return self._archive_result(
                candidate,
                "destination_exists",
                "failed",
                message="Archive destination already exists.",
            )
        if not candidate.target_path.is_file():
            return self._archive_result(
                candidate,
                "destination_exists",
                "failed",
                message="Archive destination is not a regular file.",
            )
        if not verify:
            return self._archive_result(
                candidate, "preserved_existing", "not_requested"
            )
        try:
            matches = (
                _compute_file_sha256(candidate.target_path) == candidate.artifact.hash
            )
        except OSError as exc:
            return self._archive_result(
                candidate,
                "destination_exists",
                "failed",
                message=f"Could not hash archive destination: {exc}",
            )
        if not matches:
            return self._archive_result(
                candidate,
                "destination_exists",
                "hash_mismatch",
                message="Archive destination hash did not match artifact hash.",
            )
        return self._archive_result(candidate, "preserved_existing", "verified")

    def _resolve_archive_source(
        self,
        candidate: _ArchiveFileCandidate,
        *,
        producing_run: Any,
    ) -> tuple[_ArchiveFileCandidate | None, ArchivedRunOutputFile | None]:
        """Locate and validate a regular source file for archival.

        Parameters
        ----------
        candidate : _ArchiveFileCandidate
            Output awaiting source-path discovery.
        producing_run : Run | None
            Historical run used to discover canonical and recovery sources.

        Returns
        -------
        tuple[_ArchiveFileCandidate | None, ArchivedRunOutputFile | None]
            Resolved candidate and ``None`` on success; otherwise ``None`` and
            a terminal report entry.

        Notes
        -----
        Raw source paths are retained long enough to reject symlink components
        instead of resolving through them.
        """
        source_path: Path | None = None

        def reject_symlink(path: Path) -> bool:
            if self._has_symlink_component(path):
                raise ValueError(f"Symlink source is not supported: {path}")
            return True

        if producing_run is not None:
            try:
                from consist.core.materialize import find_existing_recovery_source_path

                _, source_path, _ = find_existing_recovery_source_path(
                    self.tracker,
                    artifact=candidate.artifact,
                    run=producing_run,
                    source_root=None,
                    source_validator=reject_symlink,
                    preserve_raw_paths=True,
                )
            except ValueError as exc:
                return None, self._archive_result(
                    candidate, "symlink_source", "failed", message=str(exc)
                )
        if source_path is None:
            return None, self._archive_result(
                candidate,
                "missing_source",
                "failed",
                message="Source bytes are unavailable.",
            )

        resolved = replace(candidate, source_path=source_path)
        if source_path.is_dir():
            return None, self._archive_result(
                resolved,
                "unsupported_directory",
                "failed",
                message="Directory output archival is not supported.",
            )
        if not source_path.is_file():
            return None, self._archive_result(
                resolved,
                "failed",
                "failed",
                message="Source is not a regular file.",
            )
        return resolved, None

    def _copy_and_verify_archive_target(
        self,
        candidate: _ArchiveFileCandidate,
        *,
        verify: bool,
    ) -> ArchivedRunOutputFile:
        """Copy a resolved source atomically and apply the hash policy.

        Parameters
        ----------
        candidate : _ArchiveFileCandidate
            Candidate with a non-null regular ``source_path``.
        verify : bool
            Whether the destination must match the artifact's full hash.

        Returns
        -------
        ArchivedRunOutputFile
            Copy and verification result. Existing bytes are never replaced.
        """
        source_path = candidate.source_path
        if source_path is None:
            raise ValueError("Archive copy requires a resolved source path.")
        try:
            candidate.target_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return self._archive_result(
                candidate,
                "failed",
                "failed",
                message=f"Could not create destination: {exc}",
            )
        if self._has_symlink_component(candidate.target_path):
            return self._archive_result(
                candidate,
                "symlink_destination",
                "failed",
                message="Symlink detected in recovery destination.",
            )

        try:
            from consist.core.materialize import _copy_file_atomic

            copied = _copy_file_atomic(source_path, candidate.target_path)
            if not copied:
                return self._archive_result(
                    candidate,
                    "destination_exists",
                    "failed",
                    message="Archive destination already exists.",
                )
            if (
                verify
                and _compute_file_sha256(candidate.target_path)
                != candidate.artifact.hash
            ):
                return self._archive_result(
                    candidate,
                    "hash_mismatch",
                    "hash_mismatch",
                    message="Copied archive bytes did not match artifact hash.",
                )
        except FileExistsError:
            return self._archive_result(
                candidate,
                "destination_exists",
                "failed",
                message="Archive destination already exists.",
            )
        except OSError as exc:
            return self._archive_result(
                candidate,
                "failed",
                "failed",
                message=f"Could not copy archive bytes: {exc}",
            )
        return self._archive_result(
            candidate, "copied", "verified" if verify else "not_requested"
        )

    @staticmethod
    def _registration_eligible(output: ArchivedRunOutputFile) -> bool:
        """Return whether an archive result is safe to register as a recovery root.

        Parameters
        ----------
        output : ArchivedRunOutputFile
            Copy-stage outcome to evaluate.

        Returns
        -------
        bool
            ``True`` for copied or policy-satisfying retained file outputs.
        """
        return output.copy_status in {
            "copied",
            "preserved_existing",
        } and output.verification_status in {"verified", "not_requested"}

    def _register_archived_outputs(
        self,
        *,
        run_id: str,
        recovery_root: Path,
        verify: bool,
        append: bool,
        report: dict[str, ArchivedRunOutputFile],
    ) -> None:
        """Bulk-register archive results that passed the copy-stage policy.

        Parameters
        ----------
        run_id : str
            Run owning the outputs.
        recovery_root : Path
            Root to register for eligible outputs.
        verify : bool
            Verification policy forwarded to registration.
        append : bool
            Metadata merge policy forwarded to registration.
        report : dict[str, ArchivedRunOutputFile]
            Mutable report updated with actual registration outcomes.

        Notes
        -----
        A copied-and-verified file remains visible even when metadata persistence
        fails so callers can retry registration without recopying bytes.
        """
        eligible = [
            key for key, output in report.items() if self._registration_eligible(output)
        ]
        if not eligible:
            return
        registrations = self.tracker.register_run_output_recovery_copies(
            run_id, recovery_root, keys=eligible, verify=verify, append=append
        )
        for key in eligible:
            registration = registrations[key]
            prior = report[key]
            verification_status = prior.verification_status
            if registration.status == "hash_mismatch":
                verification_status = "hash_mismatch"
            elif registration.status == "unverifiable_hash":
                verification_status = "unverifiable_hash"
            elif not registration.verification_succeeded:
                verification_status = "failed"
            report[key] = replace(
                prior,
                verification_status=verification_status,
                metadata_committed=registration.metadata_updated,
                message=(
                    registration.message
                    if registration.status != "registered"
                    else prior.message
                ),
            )

    def archive_run_output_files(
        self,
        run_id: str,
        recovery_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        verify: bool = True,
        append: bool = True,
    ) -> ArchivedRunOutputFilesReport:
        """Copy regular output files to a recovery root and report each outcome.

        Parameters
        ----------
        run_id : str
            Completed run whose outputs should be archived.
        recovery_root : path-like
            Root below which canonical URI-relative output paths are created.
        keys : sequence of str | None, optional
            Output keys to archive; ``None`` selects all outputs.
        preserve_existing : bool, default True
            Whether a matching target file may be retained without replacement.
        verify : bool, default True
            Whether copied or retained files must match full artifact hashes.
        append : bool, default True
            Whether successful registrations append recovery-root metadata.

        Returns
        -------
        ArchivedRunOutputFilesReport
            Mapping-style per-key copy, verification, and metadata outcome.

        Raises
        ------
        KeyError
            If a requested output key is not present. This happens before
            target directories are created or bytes are copied.

        Notes
        -----
        This additive API is file-only and never overwrites a target. It copies
        or retains bytes before bulk registration. ``report.complete`` describes
        this invocation, not durable archive-workflow state.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys, caller="archive_run_output_files"
        )
        outputs = self.get_run_outputs(run_id)
        selected = self._select_required_output_keys(
            outputs, normalized_keys, run_id=run_id
        )
        recovery_root_path = Path(recovery_root).absolute()
        report: dict[str, ArchivedRunOutputFile] = {}
        producing_run = self.get_run(run_id)

        for key, artifact in selected.items():
            output_set_kind = _output_set_hydration_kind(self.tracker, artifact)
            if output_set_kind is not None:
                report[key] = ArchivedRunOutputFile(
                    artifact=artifact,
                    key=key,
                    source_path=None,
                    target_path=None,
                    copy_status="unsupported_directory",
                    verification_status="failed",
                    message=(
                        "OutputSet "
                        f"{output_set_kind} archival is not supported by the "
                        "file-output archive API."
                    ),
                )
                continue
            relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
            if relative_path is None:
                report[key] = ArchivedRunOutputFile(
                    artifact=artifact,
                    key=key,
                    source_path=None,
                    target_path=None,
                    copy_status="skipped_unmapped",
                    verification_status="failed",
                    message="Artifact does not have a rematerializable URI layout.",
                )
                continue
            candidate = _ArchiveFileCandidate(
                key, artifact, recovery_root_path / relative_path
            )
            if verify and not (
                artifact.hash and self.identity.hashing_strategy == "full"
            ):
                report[key] = self._archive_result(
                    candidate,
                    "unverifiable_hash",
                    "unverifiable_hash",
                    message="Verification requested, but no full file hash is available.",
                )
                continue

            if self._has_symlink_component(candidate.target_path):
                report[key] = self._archive_result(
                    candidate,
                    "symlink_destination",
                    "failed",
                    message="Symlink detected in recovery destination.",
                )
                continue

            existing = self._inspect_existing_archive_target(
                candidate, preserve_existing=preserve_existing, verify=verify
            )
            if existing is not None:
                report[key] = existing
                continue

            resolved, source_failure = self._resolve_archive_source(
                candidate, producing_run=producing_run
            )
            if source_failure is not None:
                report[key] = source_failure
                continue
            if resolved is None:
                raise RuntimeError("Archive source resolution returned no outcome.")
            report[key] = self._copy_and_verify_archive_target(resolved, verify=verify)

        self._register_archived_outputs(
            run_id=run_id,
            recovery_root=recovery_root_path,
            verify=verify,
            append=append,
            report=report,
        )
        return ArchivedRunOutputFilesReport(outputs=report)

    def archive_run_outputs(
        self,
        run_id: str,
        archive_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> ArchivedOutputs:
        """Archive one or more historical run outputs into a stable recovery root.

        Parameters
        ----------
        run_id : str
            Completed run whose outputs should be archived.
        archive_root : path-like
            Root below which URI-relative output paths are recreated.
        keys : sequence of str | None, optional
            Output keys to archive; ``None`` selects all outputs.
        mode : {"copy", "move"}, default "copy"
            Filesystem operation used by the legacy archive API.
        append : bool, default True
            Whether to append the archive root to existing recovery roots.

        Returns
        -------
        ArchivedOutputs
            Read-only key-to-path mapping and refreshed output artifacts.

        Notes
        -----
        This compatibility API retains its sequential-copy semantics. Use
        :meth:`archive_run_output_files` for report-oriented, no-overwrite
        regular-file archival.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys, caller="archive_run_outputs"
        )
        outputs = self.get_run_outputs(run_id)
        selected = self._select_required_output_keys(
            outputs, normalized_keys, run_id=run_id
        )
        archived_paths = {
            key: self.tracker.archive_artifact(
                artifact, archive_root, mode=mode, append=append
            )
            for key, artifact in selected.items()
        }
        refreshed_selected = self._select_required_output_keys(
            self.get_run_outputs(run_id), normalized_keys, run_id=run_id
        )
        return ArchivedOutputs(paths=archived_paths, outputs=refreshed_selected)

    @staticmethod
    def _select_required_output_keys(
        outputs: Dict[str, Artifact],
        normalized_keys: tuple[str, ...] | None,
        *,
        run_id: str,
    ) -> Dict[str, Artifact]:
        """Select output artifacts and reject unknown keys before side effects.

        Parameters
        ----------
        outputs : dict[str, Artifact]
            All outputs available for the run.
        normalized_keys : tuple[str, ...] | None
            Normalized requested keys, or ``None`` to select all outputs.
        run_id : str
            Run identifier used in error messages.

        Returns
        -------
        dict[str, Artifact]
            Selected artifacts in requested-key order.

        Raises
        ------
        KeyError
            If any requested key is not present.
        """
        if normalized_keys is None:
            return outputs
        missing = [key for key in normalized_keys if key not in outputs]
        if missing:
            raise KeyError(
                "Requested output keys were not found for run "
                f"{run_id!r}: {', '.join(repr(key) for key in missing)}"
            )
        return {key: outputs[key] for key in normalized_keys}

    def archive_current_run_outputs(
        self,
        archive_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> ArchivedOutputs:
        """Archive outputs for the currently active run into a recovery root.

        Parameters
        ----------
        archive_root : path-like
            Root below which URI-relative output paths are recreated.
        keys : sequence of str | None, optional
            Output keys to archive; ``None`` selects all active-run outputs.
        mode : {"copy", "move"}, default "copy"
            Filesystem operation used by the legacy archive API.
        append : bool, default True
            Whether to append the archive root to existing recovery roots.

        Returns
        -------
        ArchivedOutputs
            Archived paths and refreshed artifacts for the active run.

        Raises
        ------
        RuntimeError
            If no run is active in the current tracker context.
        """
        if not self.current_consist or self.current_consist.run is None:
            raise RuntimeError(
                "archive_current_run_outputs(...) requires an active run context."
            )
        return self.tracker.archive_run_outputs(
            self.current_consist.run.id,
            archive_root,
            keys=keys,
            mode=mode,
            append=append,
        )
