"""Outbound archive and recovery-root registration support for ``Tracker``."""

from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import replace
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
    """Compute a full content SHA-256 for one regular file."""
    sha256 = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(_FILE_HASH_CHUNK_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


class TrackerArchiveService(_TrackerServiceBase):
    """Archive bytes and persist their advisory recovery-root metadata."""

    def set_artifact_recovery_roots(
        self,
        artifact: Artifact,
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        *,
        append: bool = False,
    ) -> Artifact:
        """Persist advisory filesystem recovery roots for an artifact.

        Recovery roots are ordered fallback locations used during historical
        rematerialization and cache-hit output hydration when canonical cold
        bytes are no longer available at their original location. The
        artifact's ``container_uri`` remains canonical; recovery roots are
        alternate byte sources only.
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

        The archived copy preserves the artifact's URI-relative layout under
        ``archive_root`` and records that root in
        ``artifact.meta["recovery_roots"]`` without changing artifact identity
        or ``container_uri``.
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

        This helper never copies bytes. It verifies the file already located
        at ``recovery_root / <uri-relative-path>`` and then appends or replaces
        ``artifact.meta["recovery_roots"]``. Directory artifacts and HDF5 child
        table artifacts remain blocked until they have an independent recovery
        equivalence contract. A supplied ``content_hash`` takes precedence;
        otherwise ``artifact.hash`` is only a byte proof with full content
        hashing. ``append`` defaults to True to match ``archive_artifact``.
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
        """Validate a recovery copy and optionally persist recovery metadata."""
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

        Unknown requested keys raise before work begins. Per-artifact blockers,
        including missing files and hash mismatches, remain in the keyed report
        without aborting the other selected outputs. Verified roots are appended
        by default; pass ``append=False`` to replace prior recovery roots.
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

        This additive helper is file-only and never overwrites a destination.
        It copies or validates bytes first, then uses
        ``register_run_output_recovery_copies(...)`` for metadata persistence.
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
        eligible: list[str] = []
        producing_run = self.get_run(run_id)

        def result(
            artifact: Artifact,
            key: str,
            copy_status: ArchiveRunOutputFileStatus,
            verification_status: ArchiveRunOutputVerificationStatus,
            *,
            source_path: Path | None = None,
            target_path: Path | None = None,
            message: str | None = None,
        ) -> None:
            report[key] = ArchivedRunOutputFile(
                artifact=artifact,
                key=key,
                source_path=source_path,
                target_path=target_path,
                copy_status=copy_status,
                verification_status=verification_status,
                message=message,
            )

        def has_symlink_component(path: Path) -> bool:
            return any(component.is_symlink() for component in (path, *path.parents))

        def reject_symlink(path: Path) -> bool:
            if has_symlink_component(path):
                raise ValueError(f"Symlink source is not supported: {path}")
            return True

        for key, artifact in selected.items():
            output_set_kind = _output_set_hydration_kind(self.tracker, artifact)
            if output_set_kind is not None:
                result(
                    artifact,
                    key,
                    "unsupported_directory",
                    "failed",
                    message=(
                        "OutputSet "
                        f"{output_set_kind} archival is not supported by the "
                        "file-output archive API."
                    ),
                )
                continue
            relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
            if relative_path is None:
                result(
                    artifact,
                    key,
                    "skipped_unmapped",
                    "failed",
                    message="Artifact does not have a rematerializable URI layout.",
                )
                continue
            target_path = recovery_root_path / relative_path
            if verify and not (
                artifact.hash and self.identity.hashing_strategy == "full"
            ):
                result(
                    artifact,
                    key,
                    "unverifiable_hash",
                    "unverifiable_hash",
                    target_path=target_path,
                    message="Verification requested, but no full file hash is available.",
                )
                continue

            if has_symlink_component(target_path):
                result(
                    artifact,
                    key,
                    "symlink_destination",
                    "failed",
                    target_path=target_path,
                    message="Symlink detected in recovery destination.",
                )
                continue

            if target_path.exists():
                if not preserve_existing:
                    result(
                        artifact,
                        key,
                        "destination_exists",
                        "failed",
                        target_path=target_path,
                        message="Archive destination already exists.",
                    )
                    continue
                if not target_path.is_file():
                    result(
                        artifact,
                        key,
                        "destination_exists",
                        "failed",
                        target_path=target_path,
                        message="Archive destination is not a regular file.",
                    )
                    continue
                if verify:
                    try:
                        matches = _compute_file_sha256(target_path) == artifact.hash
                    except OSError as exc:
                        result(
                            artifact,
                            key,
                            "destination_exists",
                            "failed",
                            target_path=target_path,
                            message=f"Could not hash archive destination: {exc}",
                        )
                        continue
                    if not matches:
                        result(
                            artifact,
                            key,
                            "destination_exists",
                            "hash_mismatch",
                            target_path=target_path,
                            message="Archive destination hash did not match artifact hash.",
                        )
                        continue
                    result(
                        artifact,
                        key,
                        "preserved_existing",
                        "verified",
                        target_path=target_path,
                    )
                else:
                    result(
                        artifact,
                        key,
                        "preserved_existing",
                        "not_requested",
                        target_path=target_path,
                    )
                eligible.append(key)
                continue

            source_path: Path | None = None
            if producing_run is not None:
                try:
                    from consist.core.materialize import (
                        find_existing_recovery_source_path,
                    )

                    _, source_path, _ = find_existing_recovery_source_path(
                        self.tracker,
                        artifact=artifact,
                        run=producing_run,
                        source_root=None,
                        source_validator=reject_symlink,
                        preserve_raw_paths=True,
                    )
                except ValueError as exc:
                    result(
                        artifact,
                        key,
                        "symlink_source",
                        "failed",
                        target_path=target_path,
                        message=str(exc),
                    )
                    continue
            if source_path is None:
                result(
                    artifact,
                    key,
                    "missing_source",
                    "failed",
                    target_path=target_path,
                    message="Source bytes are unavailable.",
                )
                continue
            if source_path.is_dir():
                result(
                    artifact,
                    key,
                    "unsupported_directory",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message="Directory output archival is not supported.",
                )
                continue
            if not source_path.is_file():
                result(
                    artifact,
                    key,
                    "failed",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message="Source is not a regular file.",
                )
                continue

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                result(
                    artifact,
                    key,
                    "failed",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message=f"Could not create destination: {exc}",
                )
                continue
            if has_symlink_component(target_path):
                result(
                    artifact,
                    key,
                    "symlink_destination",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message="Symlink detected in recovery destination.",
                )
                continue

            try:
                from consist.core.materialize import _copy_file_atomic

                copied = _copy_file_atomic(source_path, target_path)
                if not copied:
                    result(
                        artifact,
                        key,
                        "destination_exists",
                        "failed",
                        source_path=source_path,
                        target_path=target_path,
                        message="Archive destination already exists.",
                    )
                    continue
                if verify and _compute_file_sha256(target_path) != artifact.hash:
                    result(
                        artifact,
                        key,
                        "hash_mismatch",
                        "hash_mismatch",
                        source_path=source_path,
                        target_path=target_path,
                        message="Copied archive bytes did not match artifact hash.",
                    )
                    continue
            except FileExistsError:
                result(
                    artifact,
                    key,
                    "destination_exists",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message="Archive destination already exists.",
                )
                continue
            except OSError as exc:
                result(
                    artifact,
                    key,
                    "failed",
                    "failed",
                    source_path=source_path,
                    target_path=target_path,
                    message=f"Could not copy archive bytes: {exc}",
                )
                continue
            result(
                artifact,
                key,
                "copied",
                "verified" if verify else "not_requested",
                source_path=source_path,
                target_path=target_path,
            )
            eligible.append(key)

        if eligible:
            registrations = self.tracker.register_run_output_recovery_copies(
                run_id, recovery_root_path, keys=eligible, verify=verify, append=append
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
                    message=registration.message
                    if registration.status != "registered"
                    else prior.message,
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

        Each output retains its canonical identity and gains ``archive_root``
        as an advisory recovery root. The returned ``ArchivedOutputs`` remains
        a read-only mapping of keys to paths, while ``.outputs`` contains
        refreshed artifacts whose recovery metadata is ready for downstream
        ``sc.run(inputs=...)`` calls.
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
        """Archive outputs for the currently active run into a stable recovery root.

        This convenience wrapper calls ``archive_run_outputs(...)`` for the
        active run without requiring callers to extract its ID first.
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
