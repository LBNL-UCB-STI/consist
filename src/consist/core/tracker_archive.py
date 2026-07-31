"""Outbound archive and recovery-root registration support for ``Tracker``."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Sequence

from sqlmodel import col, select

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.container_policy import validate_recovery_registration_policy
from consist.core.directory_artifacts import (
    materialize_directory_tree,
    materialize_shapefile_bundle,
    validate_directory_manifest,
)
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
from consist.core.output_sets import (
    _manifest_identity_hash,
    _normalize_output_set_relative_path,
)
from consist.models.artifact import Artifact, ArchivedOutputs

_FILE_HASH_CHUNK_SIZE = 8 * 1024 * 1024


def _is_directory_artifact(artifact: Artifact) -> bool:
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    return meta.get("directory_artifact") is True


def _is_file_bundle_artifact(artifact: Artifact) -> bool:
    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
    return meta.get("file_bundle_artifact") is True


def _directory_artifact_manifest(artifact: Artifact) -> dict[str, Any]:
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


def _shapefile_bundle_metadata(artifact: Artifact) -> tuple[str, dict[str, Any]]:
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


@dataclass(frozen=True, slots=True)
class _OutputSetArchiveMember:
    """One manifest-validated OutputSet member awaiting archival."""

    artifact: Artifact
    relative_path: str
    content_hash: str
    source_path: Path
    destination: Path


@dataclass(frozen=True, slots=True)
class _OutputSetArchivePlan:
    """All immutable inputs required to archive one logical OutputSet."""

    parent: Artifact
    parent_source: Path
    destination: Path
    members: tuple[_OutputSetArchiveMember, ...]
    manifest: Artifact
    manifest_source: Path
    manifest_destination: Path
    manifest_content_hash: str


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
        if self._is_output_set_parent(artifact):
            return self._archive_output_set(
                artifact,
                archive_root,
                mode=mode,
                append=append,
            )
        if _is_directory_artifact(artifact):
            return self._archive_directory_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
            )
        if _is_file_bundle_artifact(artifact):
            return self._archive_shapefile_bundle_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
            )
        if artifact.driver == "shapefile":
            raise ValueError(
                "legacy Shapefile artifacts without an immutable bundle manifest "
                "cannot be archived; re-log the output under the current contract."
            )
        if artifact.driver == "zarr":
            raise ValueError(
                "legacy Zarr artifacts without an immutable directory manifest "
                "cannot be archived; re-log the output under the current contract."
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
        if source_path.is_dir():
            raise ValueError(
                "Directory archival requires an explicitly declared directory artifact."
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

    @staticmethod
    def _is_output_set_parent(artifact: Artifact) -> bool:
        """Return whether an artifact is the logical parent of an OutputSet."""
        metadata = artifact.meta if isinstance(artifact.meta, dict) else {}
        return artifact.driver == "artifact_set" or metadata.get("artifact_set") is True

    def _find_output_set_archive_source(
        self,
        artifact: Artifact,
        *,
        producing_run: Any | None,
    ) -> Path:
        """Locate an OutputSet source without resolving through symlinks."""

        def reject_symlink(path: Path) -> bool:
            if self._has_symlink_component(path):
                raise ValueError(f"OutputSet source contains a symlink: {path}")
            return True

        source_path: Path | None = None
        if producing_run is not None:
            from consist.core.materialize import find_existing_recovery_source_path

            _, source_path, _ = find_existing_recovery_source_path(
                self.tracker,
                artifact=artifact,
                run=producing_run,
                source_root=None,
                source_validator=reject_symlink,
                preserve_raw_paths=True,
            )
        if source_path is None and artifact.abs_path:
            candidate = Path(artifact.abs_path)
            if candidate.exists():
                reject_symlink(candidate)
                source_path = candidate
        if source_path is None:
            candidate = Path(self.resolve_uri(artifact.container_uri))
            if candidate.exists():
                reject_symlink(candidate)
                source_path = candidate
        if source_path is None:
            raise FileNotFoundError(
                f"Cannot archive OutputSet artifact {artifact.key!r}: "
                "source bytes are unavailable."
            )
        return source_path

    def _build_output_set_archive_plan(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
    ) -> _OutputSetArchivePlan:
        """Validate a persisted OutputSet graph before changing archive bytes."""
        metadata = artifact.meta if isinstance(artifact.meta, dict) else {}
        output_set_key = metadata.get("output_set_key")
        if not isinstance(output_set_key, str) or output_set_key != artifact.key:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has no matching output_set_key."
            )
        member_count = metadata.get("member_count")
        if type(member_count) is not int or member_count < 0:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has no valid member_count."
            )
        manifest_id = metadata.get("manifest_artifact_id")
        if not isinstance(manifest_id, str) or not manifest_id:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has no manifest artifact id."
            )
        manifest = self.tracker.get_artifact(manifest_id)
        if manifest is None:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} references a missing manifest artifact."
            )
        manifest_metadata = manifest.meta if isinstance(manifest.meta, dict) else {}
        if (
            manifest_metadata.get("output_set_manifest") is not True
            or manifest_metadata.get("output_set_key") != artifact.key
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an invalid manifest artifact."
            )

        parent_relative = self.fs.get_remappable_relative_path(artifact.container_uri)
        manifest_relative = self.fs.get_remappable_relative_path(manifest.container_uri)
        if parent_relative is None or manifest_relative is None:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} does not have a rematerializable "
                "URI layout."
            )
        archive_root_path = Path(archive_root).expanduser().absolute()
        destination = archive_root_path / parent_relative
        manifest_destination = archive_root_path / manifest_relative
        if self._has_symlink_component(destination) or self._has_symlink_component(
            manifest_destination
        ):
            raise ValueError("Symlink detected in OutputSet archive destination.")
        try:
            manifest_destination.relative_to(destination)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"OutputSet manifest {manifest.key!r} must not be stored inside "
                "the member root."
            )

        producing_run = (
            self.get_run(str(artifact.run_id)) if artifact.run_id is not None else None
        )
        parent_source = self._find_output_set_archive_source(
            artifact, producing_run=producing_run
        )
        if not parent_source.is_dir():
            raise ValueError(
                f"OutputSet parent {artifact.key!r} source is not a directory."
            )
        manifest_source = self._find_output_set_archive_source(
            manifest, producing_run=producing_run
        )
        if not manifest_source.is_file():
            raise ValueError(
                f"OutputSet manifest {manifest.key!r} source is not a regular file."
            )
        try:
            manifest_contents = json.loads(manifest_source.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has a malformed manifest."
            ) from exc
        if not isinstance(manifest_contents, Mapping):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has a malformed manifest."
            )
        if (
            not manifest.hash
            or self.identity.compute_file_checksum(manifest_source) != manifest.hash
        ):
            raise ValueError(
                f"OutputSet manifest {manifest.key!r} bytes do not match its "
                "persisted identity."
            )
        manifest_content_hash = _compute_file_sha256(manifest_source)
        if artifact.hash != _manifest_identity_hash(manifest_contents):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} manifest does not match its identity."
            )
        if (
            manifest_contents.get("manifest_version") != 1
            or manifest_contents.get("output_set_key") != artifact.key
            or not isinstance(manifest_contents.get("root_uri"), str)
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an incomplete manifest."
            )

        children = self.tracker.get_child_artifacts(artifact)
        if any(
            (child.meta if isinstance(child.meta, dict) else {}).get(
                "output_set_member"
            )
            is not True
            for child in children
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has a non-member child artifact."
            )
        if len(children) != member_count:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} member count does not match "
                "its persisted children."
            )

        manifest_members = manifest_contents.get("members")
        if not isinstance(manifest_members, list) or len(manifest_members) != len(
            children
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an incomplete manifest."
            )
        manifest_totals = manifest_contents.get("totals")
        if not isinstance(manifest_totals, Mapping):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an incomplete manifest."
            )

        manifest_members_by_id: dict[str, Mapping[str, Any]] = {}
        for manifest_member in manifest_members:
            if not isinstance(manifest_member, Mapping):
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has a malformed manifest member."
                )
            member_id = manifest_member.get("artifact_id")
            if not isinstance(member_id, str) or not member_id:
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has an incomplete manifest."
                )
            if member_id in manifest_members_by_id:
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has duplicate manifest members."
                )
            manifest_members_by_id[member_id] = manifest_member

        members: list[_OutputSetArchiveMember] = []
        total_size_bytes = 0
        destination_paths: set[Path] = set()
        for child in children:
            child_metadata = child.meta if isinstance(child.meta, dict) else {}
            if (
                child.id is None
                or child.parent_artifact_id != artifact.id
                or child_metadata.get("output_set_key") != artifact.key
            ):
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has an invalid member artifact."
                )
            relative_path = _normalize_output_set_relative_path(
                child_metadata.get("output_set_relative_path", "")
            )
            manifest_member = manifest_members_by_id.pop(str(child.id), None)
            if manifest_member is None:
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} manifest is missing "
                    f"member {child.key!r}."
                )
            content_hash = manifest_member.get("content_hash")
            size_bytes = manifest_member.get("size_bytes")
            if (
                manifest_member.get("key") != child.key
                or manifest_member.get("uri") != child.container_uri
                or manifest_member.get("driver") != child.driver
                or manifest_member.get("relative_path") != relative_path
                or not isinstance(content_hash, str)
                or not content_hash
                or type(size_bytes) is not int
                or size_bytes < 0
            ):
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has an incomplete manifest."
                )
            child_relative = self.fs.get_remappable_relative_path(child.container_uri)
            if child_relative is None:
                raise ValueError(
                    f"OutputSet member {child.key!r} does not have a rematerializable "
                    "URI layout."
                )
            member_destination = archive_root_path / child_relative
            if member_destination != destination / Path(relative_path):
                raise ValueError(
                    f"OutputSet member {child.key!r} is outside the set root."
                )
            if member_destination in destination_paths:
                raise ValueError(
                    f"OutputSet parent {artifact.key!r} has duplicate member paths."
                )
            destination_paths.add(member_destination)
            member_source = self._find_output_set_archive_source(
                child, producing_run=producing_run
            )
            if not member_source.is_file():
                raise ValueError(
                    f"OutputSet member {child.key!r} source is not a regular file."
                )
            try:
                source_relative = _normalize_output_set_relative_path(
                    member_source.relative_to(parent_source)
                )
            except ValueError as exc:
                raise ValueError(
                    f"OutputSet member {child.key!r} is outside the set root."
                ) from exc
            if source_relative != relative_path:
                raise ValueError(
                    f"OutputSet member {child.key!r} is outside the set root."
                )
            if member_source.stat().st_size != size_bytes or (
                _compute_file_sha256(member_source) != content_hash
            ):
                raise ValueError(
                    f"OutputSet member {child.key!r} bytes do not match the manifest."
                )
            total_size_bytes += size_bytes
            members.append(
                _OutputSetArchiveMember(
                    artifact=child,
                    relative_path=relative_path,
                    content_hash=content_hash,
                    source_path=member_source,
                    destination=member_destination,
                )
            )
        if manifest_members_by_id:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} manifest has unknown members."
            )
        if (
            manifest_totals.get("file_count") != len(members)
            or manifest_totals.get("byte_size") != total_size_bytes
            or metadata.get("total_size_bytes") != total_size_bytes
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} manifest totals are incomplete."
            )
        if manifest_destination in destination_paths:
            raise ValueError(
                f"OutputSet manifest {manifest.key!r} overlaps a member destination."
            )

        return _OutputSetArchivePlan(
            parent=artifact,
            parent_source=parent_source,
            destination=destination,
            members=tuple(sorted(members, key=lambda member: member.relative_path)),
            manifest=manifest,
            manifest_source=manifest_source,
            manifest_destination=manifest_destination,
            manifest_content_hash=manifest_content_hash,
        )

    def _build_selected_output_set_archive_plans(
        self,
        selected: Mapping[str, Artifact],
        archive_root: str | os.PathLike[str],
    ) -> tuple[
        dict[str, _OutputSetArchivePlan],
        dict[str, _OutputSetArchiveMember],
    ]:
        """Validate selected OutputSets and their nested scalar overlap contract."""
        output_set_plans = {
            key: self._build_output_set_archive_plan(artifact, archive_root)
            for key, artifact in selected.items()
            if self._is_output_set_parent(artifact)
        }
        archive_root_path = Path(archive_root).expanduser().absolute()
        nested_scalar_members: dict[str, _OutputSetArchiveMember] = {}

        for key, artifact in selected.items():
            if key in output_set_plans:
                continue
            relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
            if relative_path is None:
                continue
            destination = archive_root_path / relative_path
            for output_set_key, plan in output_set_plans.items():
                try:
                    destination.relative_to(plan.destination)
                except ValueError:
                    continue
                matching_members = [
                    member
                    for member in plan.members
                    if member.destination == destination
                ]
                if not matching_members:
                    raise ValueError(
                        f"Nested output {key!r} is not a manifest member of "
                        f"OutputSet {output_set_key!r}."
                    )
                member = matching_members[0]
                if (
                    artifact.container_uri != member.artifact.container_uri
                    or artifact.driver != member.artifact.driver
                ):
                    raise ValueError(
                        f"Nested output {key!r} does not match immutable manifest "
                        f"member {member.artifact.key!r} of OutputSet "
                        f"{output_set_key!r}."
                    )
                producing_run = (
                    self.get_run(str(artifact.run_id))
                    if artifact.run_id is not None
                    else None
                )
                source = self._find_output_set_archive_source(
                    artifact, producing_run=producing_run
                )
                if (
                    source != member.source_path
                    or not source.is_file()
                    or _compute_file_sha256(source) != member.content_hash
                ):
                    raise ValueError(
                        f"Nested output {key!r} does not match immutable manifest "
                        f"member {member.artifact.key!r} of OutputSet "
                        f"{output_set_key!r}."
                    )
                nested_scalar_members[key] = member

        return output_set_plans, nested_scalar_members

    def _validate_output_set_archive_destination(
        self,
        destination: Path,
        members: Sequence[_OutputSetArchiveMember],
    ) -> None:
        """Require an existing OutputSet archive root to match exactly."""
        if self._has_symlink_component(destination) or destination.is_symlink():
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {destination}"
            )
        if not destination.exists():
            return
        if not destination.is_dir():
            raise ValueError(
                f"OutputSet archive destination is not a directory: {destination}"
            )

        expected_files = {member.relative_path: member for member in members}
        expected_directories: set[str] = set()
        for relative_path in expected_files:
            parent = Path(relative_path).parent
            while parent != Path("."):
                expected_directories.add(parent.as_posix())
                parent = parent.parent

        actual_files: set[str] = set()
        actual_directories: set[str] = set()
        for path in destination.rglob("*"):
            if path.is_symlink():
                raise ValueError(
                    f"Symlink detected in OutputSet archive destination: {path}"
                )
            relative_path = _normalize_output_set_relative_path(
                path.relative_to(destination)
            )
            if path.is_file():
                actual_files.add(relative_path)
            elif path.is_dir():
                actual_directories.add(relative_path)
            else:
                raise ValueError(f"Unsupported OutputSet archive entry: {path}")
        if (
            actual_files != set(expected_files)
            or actual_directories != expected_directories
        ):
            raise ValueError(
                f"OutputSet archive destination has unexpected or missing members: {destination}"
            )
        for relative_path, member in expected_files.items():
            path = destination / relative_path
            if _compute_file_sha256(path) != member.content_hash:
                raise ValueError(
                    f"OutputSet archive destination member hash mismatch: {path}"
                )

    def _publish_output_set_directory(self, plan: _OutputSetArchivePlan) -> bool:
        """Stage and publish one OutputSet member root without overwriting bytes."""
        if plan.destination.exists():
            self._validate_output_set_archive_destination(
                plan.destination, plan.members
            )
            return False
        if self._has_symlink_component(plan.destination):
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {plan.destination}"
            )
        plan.destination.parent.mkdir(parents=True, exist_ok=True)
        if self._has_symlink_component(plan.destination.parent):
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {plan.destination}"
            )
        staging_root = Path(
            tempfile.mkdtemp(
                dir=str(plan.destination.parent),
                prefix=f".consist-output-set-{plan.destination.name}-",
            )
        )
        staging = staging_root / "payload"
        try:
            staging.mkdir()
            for member in plan.members:
                target = staging / member.relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(member.source_path, target)
                if _compute_file_sha256(target) != member.content_hash:
                    raise ValueError(
                        f"OutputSet member {member.artifact.key!r} changed while archiving."
                    )
            for member in plan.members:
                if _compute_file_sha256(member.source_path) != member.content_hash:
                    raise ValueError(
                        f"OutputSet member {member.artifact.key!r} changed while archiving."
                    )
            self._validate_output_set_archive_destination(staging, plan.members)
            if plan.destination.exists() or plan.destination.is_symlink():
                raise FileExistsError(
                    f"OutputSet archive destination already exists: {plan.destination}"
                )
            os.rename(staging, plan.destination)
            return True
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

    def _publish_output_set_manifest(self, plan: _OutputSetArchivePlan) -> bool:
        """Stage and publish the persisted OutputSet manifest without overwriting."""
        destination = plan.manifest_destination
        if self._has_symlink_component(destination) or destination.is_symlink():
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {destination}"
            )
        if destination.exists():
            if not destination.is_file() or (
                _compute_file_sha256(destination) != plan.manifest_content_hash
            ):
                raise FileExistsError(
                    f"OutputSet manifest archive destination already exists: {destination}"
                )
            return False
        destination.parent.mkdir(parents=True, exist_ok=True)
        if self._has_symlink_component(destination.parent):
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {destination}"
            )
        descriptor, temporary_name = tempfile.mkstemp(
            dir=str(destination.parent),
            prefix=f".consist-output-set-{destination.name}-",
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                with plan.manifest_source.open("rb") as source_file:
                    shutil.copyfileobj(source_file, temporary_file)
            if _compute_file_sha256(temporary_path) != plan.manifest_content_hash:
                raise ValueError(
                    f"OutputSet manifest {plan.manifest.key!r} changed while archiving."
                )
            if _compute_file_sha256(plan.manifest_source) != plan.manifest_content_hash:
                raise ValueError(
                    f"OutputSet manifest {plan.manifest.key!r} changed while archiving."
                )
            if destination.exists() or destination.is_symlink():
                raise FileExistsError(
                    f"OutputSet manifest archive destination already exists: {destination}"
                )
            os.rename(temporary_path, destination)
            return True
        finally:
            temporary_path.unlink(missing_ok=True)

    @staticmethod
    def _remove_output_set_archive_destination(destination: Path) -> None:
        """Best-effort cleanup of bytes created before OutputSet registration failed."""
        if destination.is_symlink():
            return
        if destination.is_dir():
            shutil.rmtree(destination)
        elif destination.exists():
            destination.unlink()

    @staticmethod
    def _remove_output_set_sources(plan: _OutputSetArchivePlan) -> None:
        """Apply move semantics after a complete OutputSet archive is registered."""
        for member in plan.members:
            if member.source_path != member.destination:
                member.source_path.unlink()
        if plan.manifest_source != plan.manifest_destination:
            plan.manifest_source.unlink()
        directories = {plan.parent_source}
        for member in plan.members:
            current = member.source_path.parent
            while current != plan.parent_source:
                directories.add(current)
                current = current.parent
        for directory in sorted(
            directories, key=lambda path: len(path.parts), reverse=True
        ):
            try:
                directory.rmdir()
            except OSError:
                pass

    def _archive_output_set_plan(
        self,
        plan: _OutputSetArchivePlan,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
        remove_sources: bool,
    ) -> Path:
        """Archive a validated OutputSet plan before registering it recoverable."""
        published_directory = False
        published_manifest = False
        try:
            published_directory = self._publish_output_set_directory(plan)
            published_manifest = self._publish_output_set_manifest(plan)
            self.tracker._set_artifact_recovery_roots_bulk(
                [
                    plan.parent,
                    *(member.artifact for member in plan.members),
                    plan.manifest,
                ],
                [Path(archive_root).expanduser().absolute()],
                append=append,
            )
        except Exception:
            if published_manifest:
                self._remove_output_set_archive_destination(plan.manifest_destination)
            if published_directory:
                self._remove_output_set_archive_destination(plan.destination)
            raise
        if mode == "move" and remove_sources:
            self._remove_output_set_sources(plan)
        return plan.destination

    def _archive_output_set(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
    ) -> Path:
        """Archive a manifest-backed OutputSet before registering it recoverable."""
        return self._archive_output_set_plan(
            self._build_output_set_archive_plan(artifact, archive_root),
            archive_root,
            mode=mode,
            append=append,
            remove_sources=True,
        )

    def _archive_directory_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
    ) -> Path:
        """Archive a manifest-verified immutable directory artifact."""
        manifest = _directory_artifact_manifest(artifact)
        relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI layout."
            )
        producing_run = self.get_run(str(artifact.run_id)) if artifact.run_id else None
        source_path: Path | None = None
        if producing_run is not None:
            from consist.core.materialize import find_existing_recovery_source_path

            _, source_path, _ = find_existing_recovery_source_path(
                self.tracker,
                artifact=artifact,
                run=producing_run,
                source_root=None,
                preserve_raw_paths=True,
            )
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        archive_root_path = Path(archive_root).expanduser().absolute()
        destination = archive_root_path / relative_path
        published = materialize_directory_tree(
            source_path,
            destination,
            manifest,
            preserve_existing=True,
        )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, [archive_root_path], append=append
            )
        except Exception:
            if published and destination.exists():
                shutil.rmtree(destination)
            raise
        if mode == "move" and source_path.resolve() != destination.resolve():
            shutil.rmtree(source_path)
            artifact.abs_path = str(destination.resolve())
        return destination

    def _archive_shapefile_bundle_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
    ) -> Path:
        """Archive a verified Shapefile sidecar bundle as one atomic directory."""
        entry, manifest = _shapefile_bundle_metadata(artifact)
        relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI layout."
            )
        producing_run = self.get_run(str(artifact.run_id)) if artifact.run_id else None
        source_path: Path | None = None
        if producing_run is not None:
            from consist.core.materialize import find_existing_recovery_source_path

            _, source_path, _ = find_existing_recovery_source_path(
                self.tracker,
                artifact=artifact,
                run=producing_run,
                source_root=None,
                preserve_raw_paths=True,
            )
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        archive_root_path = Path(archive_root).expanduser().absolute()
        destination = archive_root_path / relative_path
        published = materialize_shapefile_bundle(
            source_path.parent,
            destination,
            entry,
            manifest,
            preserve_existing=True,
        )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, [archive_root_path], append=append
            )
        except Exception:
            if published and destination.exists():
                shutil.rmtree(destination)
            raise
        if mode == "move" and source_path.parent.resolve() != destination.resolve():
            for member in manifest["entries"]:
                (source_path.parent / member["path"]).unlink()
            artifact.abs_path = str((destination / entry).resolve())
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
        Selected OutputSets are validated before any archive bytes are
        published. They are then published before ordinary outputs whose
        URI-relative destinations lie beneath their roots. Such nested outputs
        must be identical manifest members. Use
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
        if normalized_keys is None:
            selected = {
                key: artifact
                for key, artifact in selected.items()
                if _output_set_hydration_kind(self.tracker, artifact)
                not in {"member", "manifest"}
            }
        output_set_plans, nested_scalar_members = (
            self._build_selected_output_set_archive_plans(selected, archive_root)
        )
        archived_by_key: dict[str, Path] = {}
        for key, plan in output_set_plans.items():
            archived_by_key[key] = self._archive_output_set_plan(
                plan,
                archive_root,
                mode=mode,
                append=append,
                remove_sources=False,
            )
        for key, artifact in selected.items():
            if key in output_set_plans:
                continue
            nested_member = nested_scalar_members.get(key)
            if nested_member is not None:
                self.tracker.set_artifact_recovery_roots(
                    artifact,
                    [Path(archive_root).expanduser().absolute()],
                    append=append,
                )
                archived_by_key[key] = nested_member.destination
                continue
            archived_by_key[key] = self.tracker.archive_artifact(
                artifact, archive_root, mode=mode, append=append
            )
        if mode == "move":
            for plan in output_set_plans.values():
                self._remove_output_set_sources(plan)
        archived_paths = {key: archived_by_key[key] for key in selected}
        refreshed_outputs = self.get_run_outputs(run_id)
        refreshed_selected = self._select_required_output_keys(
            refreshed_outputs, tuple(selected), run_id=run_id
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
