"""Outbound archive and recovery-root registration support for ``Tracker``."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Literal, Mapping, Optional, Sequence

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.container_policy import validate_recovery_registration_policy
from consist.core.directory_artifacts import (
    directory_allows_extra_files,
    is_directory_artifact as _is_directory_artifact,
    is_file_bundle_artifact as _is_file_bundle_artifact,
    is_valid_directory_artifact_tree,
    is_valid_shapefile_bundle_source,
    materialize_directory_tree_with_rollback_info,
    materialize_shapefile_bundle,
    require_directory_artifact_manifest as _directory_artifact_manifest,
    require_shapefile_bundle_metadata as _shapefile_bundle_metadata,
    resolve_artifact_entry_path,
    resolve_file_bundle_root,
    rollback_published_directory_tree,
    validate_directory_tree,
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
    _normalize_output_set_relative_path,
    compute_output_set_manifest_digest,
)
from consist.core.tracker_recovery import (
    capture_artifact_witness,
    is_unvalidated_caller_digest,
    verify_candidate_against_artifact,
)
from consist.digest import file_sha256, verify_digest
from consist.location import ArtifactLocationRow, LocationSite, compute_location_id
from consist.models.artifact import Artifact, ArchivedOutputs


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


@dataclass(frozen=True, slots=True)
class _NestedOutputSetScalar:
    """One scalar output that exactly reuses an OutputSet member destination."""

    artifact: Artifact
    destination: Path
    output_set_key: str


@dataclass(frozen=True, slots=True)
class _NestedOutputSetDirectory:
    """One immutable directory artifact that reuses an OutputSet subtree."""

    artifact: Artifact
    destination: Path
    output_set_key: str


class TrackerArchiveService(_TrackerServiceBase):
    """Archive bytes and persist advisory recovery-root metadata.

    Notes
    -----
    This internal service is the implementation source for the corresponding
    :class:`Tracker` façade methods. Recovery roots are alternate byte sources;
    they do not change an artifact's canonical URI or identity.
    """

    _active_archive_replace_backup: tuple[Path, Path] | None = None

    def _has_verifiable_content_digest(self, artifact: Artifact) -> bool:
        return (
            self.identity.hashing_strategy == "full"
            and artifact.digest is not None
            and artifact.digest.scheme
            not in {"stat_file_v1:sha256", "stat_dir_v1:sha256"}
        )

    @staticmethod
    def _verify_file_against_artifact_digest(path: Path, artifact: Artifact) -> bool:
        if artifact.digest is None:
            return False
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        options: dict[str, Any] = {}
        member_paths = meta.get("output_set_member_paths")
        if isinstance(member_paths, Mapping):
            options["member_paths"] = member_paths
        if meta.get("file_bundle_artifact") is True:
            options["shapefile_bundle"] = True
        try:
            return verify_digest(
                path,
                artifact.digest,
                driver=artifact.driver,
                member=artifact.member,
                options=options or None,
            )
        except (ValueError, OSError):
            return False

    def _directory_archive_sort_key(
        self, index_and_item: tuple[int, tuple[str, Artifact]]
    ) -> tuple[int, int]:
        index, (_, artifact) = index_and_item
        relative_path = self.fs.get_remappable_relative_path(artifact.uri)
        depth = len(relative_path.parts) if relative_path is not None else 0
        return (depth, index)

    def is_virtual_unbacked_artifact(self, artifact: Artifact) -> bool:
        """Return ``True`` when an ingested artifact has no backing filesystem bytes."""
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        if meta.get("is_ingested") is not True:
            return False
        if artifact.abs_path and Path(artifact.abs_path).exists():
            return False
        try:
            resolved = Path(self.resolve_uri(artifact.uri))
            if resolved.exists():
                return False
        except (ValueError, RuntimeError, OSError, TypeError):
            pass
        from consist.core.materialize import find_existing_recovery_source_path

        producing_run = (
            self.get_run(str(artifact.run_id)) if artifact.run_id is not None else None
        )
        if producing_run is not None:
            _, source_path, _ = find_existing_recovery_source_path(
                self.tracker,
                artifact=artifact,
                run=producing_run,
                source_root=None,
            )
            if source_path is not None and source_path.exists():
                return False
        return True

    def _iter_live_couplers(self) -> list[Any]:
        active = self.tracker._active_coupler
        couplers = list(self.tracker._couplers or ())
        if active is not None and not any(c is active for c in couplers):
            return [active, *couplers]
        return couplers

    def _source_mount_recovery_root(self, artifact: Artifact) -> Path | None:
        if "://" not in artifact.uri:
            return None
        scheme, _ = artifact.uri.split("://", 1)
        mount_root = self.fs.mounts.get(scheme)
        if not mount_root:
            return None
        return Path(mount_root).expanduser().resolve()

    def _archive_recovery_roots_for_artifact(
        self,
        artifact: Artifact,
        archive_root_path: Path,
        *,
        mode: Literal["copy", "move"],
        append: bool,
        prepend_source_mount_root: bool,
    ) -> list[Path]:
        if mode == "copy" and append and prepend_source_mount_root:
            mount_root = self._source_mount_recovery_root(artifact)
            if mount_root is not None and mount_root != archive_root_path.resolve():
                return [mount_root, archive_root_path]
        return [archive_root_path]

    def _known_run_output_abs_paths(self, run_id: str) -> dict[str, str]:
        known: dict[str, str] = {}
        for record in (self.current_consist, self.tracker._last_consist):
            if record is None or record.run.id != run_id:
                continue
            for art in record.outputs:
                if art.key and art.abs_path is not None and art.key not in known:
                    known[art.key] = art.abs_path
        for coupler in self._iter_live_couplers():
            for key, art in coupler.items():
                if (
                    isinstance(art, Artifact)
                    and art.abs_path is not None
                    and key not in known
                    and art.run_id is not None
                    and str(art.run_id) == str(run_id)
                ):
                    known[key] = art.abs_path
        return known

    def _sync_active_run_artifacts_meta(
        self, updated_by_id: Mapping[str, dict[str, Any]]
    ) -> None:
        if not updated_by_id:
            return
        for record in (self.current_consist, self.tracker._last_consist):
            if record is None:
                continue
            for artifact_id, meta in updated_by_id.items():
                record.sync_output_artifact(artifact_id=artifact_id, meta=meta)
            record.outputs = [
                art.with_resolver(self.tracker)
                if art.id is not None and str(art.id) in updated_by_id
                else art
                for art in record.outputs
            ]
        for coupler in self._iter_live_couplers():
            for coupler_key, art in list(coupler.items()):
                if (
                    isinstance(art, Artifact)
                    and art.id is not None
                    and str(art.id) in updated_by_id
                ):
                    coupler._artifacts[coupler_key] = art.with_meta(
                        dict(updated_by_id[str(art.id)])
                    ).with_resolver(self.tracker)

    def _sync_active_run_artifact_abs_path(
        self,
        artifact: Artifact,
        abs_path: str | None,
        *,
        preserve_existing: bool = False,
    ) -> None:
        if artifact.id is None:
            return
        if not preserve_existing or artifact.abs_path is None:
            self.tracker._record_artifact_abs_path_override(artifact, abs_path)
        target_id = str(artifact.id)
        for record in (self.current_consist, self.tracker._last_consist):
            if record is None:
                continue
            record.sync_output_artifact(
                artifact_id=artifact.id,
                abs_path=abs_path,
                preserve_existing_abs_path=preserve_existing,
            )
            record.outputs = [
                art.with_resolver(self.tracker)
                if art.id is not None and str(art.id) == target_id
                else art
                for art in record.outputs
            ]
        for coupler in self._iter_live_couplers():
            for coupler_key, art in list(coupler.items()):
                if (
                    isinstance(art, Artifact)
                    and art.id is not None
                    and str(art.id) == target_id
                ):
                    next_art = art
                    if not preserve_existing or art.abs_path is None:
                        next_art = next_art.with_abs_path(abs_path)
                    coupler._artifacts[coupler_key] = next_art.with_resolver(
                        self.tracker
                    )

    def _sync_archived_run_outputs_to_runtime_state(
        self,
        *,
        run_id: str,
        refreshed_outputs: Mapping[str, Artifact],
        preserve_existing_abs_path: bool,
    ) -> None:
        for record in (self.current_consist, self.tracker._last_consist):
            if record is None or record.run is None:
                continue
            is_matching_run = str(record.run.id) == str(run_id)
            for key, refreshed in refreshed_outputs.items():
                record.sync_output_artifact(
                    key=key if is_matching_run else None,
                    artifact_id=refreshed.id,
                    meta=dict(refreshed.meta or {}),
                    abs_path=refreshed.abs_path,
                    preserve_existing_abs_path=preserve_existing_abs_path,
                )
            record.outputs = [art.with_resolver(self.tracker) for art in record.outputs]

        for coupler in self._iter_live_couplers():
            for coupler_key, coupler_art in list(coupler.items()):
                if not isinstance(coupler_art, Artifact):
                    continue
                matched_refreshed: Artifact | None = None
                if coupler_art.id is not None:
                    target_id = str(coupler_art.id)
                    matched_refreshed = next(
                        (
                            r
                            for r in refreshed_outputs.values()
                            if r.id is not None and str(r.id) == target_id
                        ),
                        None,
                    )
                if (
                    matched_refreshed is None
                    and coupler_art.run_id is not None
                    and str(coupler_art.run_id) == str(run_id)
                ):
                    matched_refreshed = refreshed_outputs.get(coupler_key)
                if matched_refreshed is None:
                    continue
                next_art = coupler_art.with_meta(dict(matched_refreshed.meta or {}))
                if matched_refreshed.abs_path is not None and (
                    not preserve_existing_abs_path or coupler_art.abs_path is None
                ):
                    next_art = next_art.with_abs_path(matched_refreshed.abs_path)
                coupler._artifacts[coupler_key] = next_art.with_resolver(self.tracker)

    def _build_recovery_location_rows_for_artifact(
        self,
        artifact: Artifact,
        roots: Sequence[str],
        *,
        existing_rows: Sequence[ArtifactLocationRow] = (),
        base_ns: Optional[int] = None,
    ) -> list[ArtifactLocationRow]:
        if not roots or artifact.id is None:
            return []
        digest_str = str(artifact.digest) if artifact.digest is not None else None
        member_str = artifact.member or ""
        now_base = time.time_ns() if base_ns is None else int(base_ns)
        existing_by_coord: dict[tuple[str, str, str], ArtifactLocationRow] = {
            (r.mount_name, r.mount_instance_id, r.uri): r for r in existing_rows
        }
        built: list[ArtifactLocationRow] = []
        for idx, root_str in enumerate(roots):
            mount_name, mount_inst, loc_uri, _target = (
                self.fs.archive_coordinate_for_root(artifact.uri, root_str)
            )
            prior = existing_by_coord.get((mount_name, mount_inst, loc_uri))
            site: LocationSite = prior.site if prior is not None else "advisory"
            witness = prior.witness if prior is not None else None
            built.append(
                ArtifactLocationRow(
                    location_id=compute_location_id(
                        digest=digest_str,
                        artifact_id=artifact.id,
                        site=site,
                        mount_name=mount_name,
                        mount_instance_id=mount_inst,
                        uri=loc_uri,
                        member=member_str,
                    ),
                    digest=digest_str,
                    artifact_id=artifact.id,
                    site=site,
                    mount_name=mount_name,
                    mount_instance_id=mount_inst,
                    uri=loc_uri,
                    member=member_str,
                    status="active",
                    witness=witness,
                    observed_at_ns=now_base + idx,
                )
            )
        return built

    def _promote_verified_archive_location(
        self,
        artifact: Artifact,
        archive_root: Path,
        destination: Path,
    ) -> None:
        if self.db is None or artifact.id is None:
            return
        mount_name, mount_inst, loc_uri, _ = self.fs.archive_coordinate_for_root(
            artifact.uri, archive_root
        )
        digest_str = str(artifact.digest) if artifact.digest is not None else None
        member_str = artifact.member or ""
        now_ns = time.time_ns()
        witness = capture_artifact_witness(destination, artifact, observed_at_ns=now_ns)
        existing = self.db.get_artifact_recovery_locations(artifact.id)
        obs_ns = now_ns
        evict_advisory_ids: list[uuid.UUID] = []
        for r in existing:
            if (r.mount_name, r.mount_instance_id, r.uri) == (
                mount_name,
                mount_inst,
                loc_uri,
            ):
                obs_ns = r.observed_at_ns
                if r.site != "archive":
                    evict_advisory_ids.append(r.location_id)
        if evict_advisory_ids:
            self.db.mark_artifact_locations_status_bulk(evict_advisory_ids, "evicted")
        archive_row = ArtifactLocationRow(
            location_id=compute_location_id(
                digest=digest_str,
                artifact_id=artifact.id,
                site="archive",
                mount_name=mount_name,
                mount_instance_id=mount_inst,
                uri=loc_uri,
                member=member_str,
            ),
            digest=digest_str,
            artifact_id=artifact.id,
            site="archive",
            mount_name=mount_name,
            mount_instance_id=mount_inst,
            uri=loc_uri,
            member=member_str,
            status="active",
            witness=witness,
            observed_at_ns=obs_ns,
        )
        self.db.upsert_artifact_locations([archive_row])

    def _mark_moved_artifact_source_evicted(
        self,
        artifact: Artifact,
        source_path: Optional[Path] = None,
    ) -> None:
        if self.db is None or artifact.id is None:
            return
        mount_name, mount_inst, loc_uri = self.fs.location_coordinate_for_uri(
            artifact.uri
        )
        site: LocationSite = (
            "workspace" if mount_name in ("workspace", "runs") else "external"
        )
        self.db.mark_coordinate_locations_status(
            site=site,
            mount_name=mount_name,
            mount_instance_id=mount_inst,
            uri=loc_uri,
            member=artifact.member or "",
            status="evicted",
        )
        if source_path is not None:
            src_real = Path(
                os.path.realpath(
                    resolve_file_bundle_root(source_path)
                    if _is_file_bundle_artifact(artifact)
                    else source_path
                )
            )
            active_rows = self.db.get_active_artifact_locations(
                artifact_id=artifact.id,
                digest=artifact.digest,
                member=artifact.member,
            )
            extra_ids: list[uuid.UUID] = []
            for row in active_rows:
                if row.site in ("archive", "advisory"):
                    continue
                cand = self.fs.resolve_location_row_path(row)
                if cand is None:
                    continue
                cand_real = Path(
                    os.path.realpath(
                        resolve_file_bundle_root(cand)
                        if _is_file_bundle_artifact(artifact)
                        else cand
                    )
                )
                if cand_real == src_real:
                    extra_ids.append(row.location_id)
            if extra_ids:
                self.db.mark_artifact_locations_status_bulk(extra_ids, "evicted")

    def evict_artifact(
        self,
        artifact: Artifact,
        *,
        path: Optional[str | os.PathLike[str]] = None,
    ) -> Path:
        """Safely evict a verified local scratch copy of ``artifact``."""
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if self.db is None:
            raise RuntimeError(
                "Cannot evict artifact: tracker has no database configured."
            )
        if (
            artifact.digest is None
            or artifact.digest.scheme
            in {
                "stat_file_v1:sha256",
                "stat_dir_v1:sha256",
                "caller_supplied_v1:sha256",
            }
            or is_unvalidated_caller_digest(artifact)
        ):
            raise ValueError(
                f"Cannot safely evict artifact {artifact.key!r}: "
                "no verifiable content digest is recorded."
            )

        target_path: Optional[Path] = None
        if path is not None:
            target_path = Path(path).expanduser()
        else:
            active_rows = self.db.get_active_artifact_locations(
                artifact_id=artifact.id,
                digest=artifact.digest,
                member=artifact.member,
            )
            for row in active_rows:
                if row.site in ("archive", "advisory"):
                    continue
                cand = self.fs.resolve_location_row_path(row)
                if cand is not None and cand.exists():
                    target_path = Path(resolve_artifact_entry_path(artifact, cand))
                    break
            if target_path is None:
                try:
                    target_path = Path(self.resolve_uri(artifact.uri))
                except (ValueError, RuntimeError, OSError, TypeError):
                    target_path = Path(artifact.abs_path) if artifact.abs_path else None

        if target_path is None:
            raise FileNotFoundError(
                f"Cannot resolve local scratch path for artifact {artifact.key!r}."
            )
        if target_path.is_symlink():
            raise ValueError(
                f"Refusing to evict symlink path {target_path} for artifact {artifact.key!r}."
            )
        if not target_path.exists():
            raise FileNotFoundError(
                f"Scratch path does not exist for artifact {artifact.key!r}: {target_path}"
            )

        # Safety Gate 1: Never delete user edits — verify full content/manifest digest first.
        if not verify_candidate_against_artifact(target_path, artifact):
            raise ValueError(
                f"Refusing to evict artifact {artifact.key!r} at {target_path}: "
                f"local bytes were modified after logging and do not match digest {artifact.digest}."
            )

        if artifact.member is not None:
            container_real = os.path.realpath(target_path)
            sibling_rows = self.db.fetchall(
                "SELECT id, uri FROM artifact WHERE id != ?",
                [artifact.id],
            )
            for _sib_id, sib_uri in sibling_rows:
                try:
                    sib_resolved = os.path.realpath(self.resolve_uri(str(sib_uri)))
                except (ValueError, RuntimeError, OSError, TypeError):
                    continue
                if sib_resolved == container_real:
                    raise ValueError(
                        f"Refusing to evict shared container {target_path} via member "
                        f"artifact {artifact.key!r} ({artifact.member!r})."
                    )

        # Safety Gate 2: Require >= 1 verified active archive on disk at a distinct realpath
        # OR ingested hot data in global_tables.
        target_compare = Path(
            os.path.realpath(
                resolve_file_bundle_root(target_path)
                if _is_file_bundle_artifact(artifact)
                else target_path
            )
        )
        verified_archive_entry: Optional[str] = None
        for row in self.db.get_active_artifact_locations(
            artifact_id=artifact.id,
            digest=artifact.digest,
            member=artifact.member,
        ):
            if row.site not in ("archive", "advisory"):
                continue
            cand = self.fs.resolve_location_row_path(row)
            if cand is None or not cand.exists():
                continue
            cand_compare = Path(
                os.path.realpath(
                    resolve_file_bundle_root(cand)
                    if _is_file_bundle_artifact(artifact)
                    else cand
                )
            )
            if cand_compare == target_compare:
                continue
            if verify_candidate_against_artifact(cand, artifact):
                verified_archive_entry = resolve_artifact_entry_path(artifact, cand)
                break

        has_hot_table = False
        if verified_archive_entry is None:
            meta = dict(artifact.meta or {})
            if artifact.id is not None:
                db_art = self.db.get_artifact(artifact.id)
                if db_art is not None and isinstance(db_art.meta, Mapping):
                    meta.update(db_art.meta)
            is_ingested = meta.get("is_ingested") is True
            dlt_table = meta.get("dlt_table_name") or artifact.key
            has_hot_table = (
                is_ingested
                and artifact.driver != "geoparquet"
                and isinstance(dlt_table, str)
                and bool(dlt_table)
                and self.db.has_table(dlt_table, table_schema="global_tables")
            )

        if verified_archive_entry is None and not has_hot_table:
            raise ValueError(
                f"Refusing to evict artifact {artifact.key!r} at {target_path}: "
                "no surviving verified archive location or ingested global_tables table survives."
            )

        evicted_resolved = Path(os.path.realpath(target_path))
        if _is_file_bundle_artifact(artifact):
            _entry, manifest = _shapefile_bundle_metadata(artifact)
            bundle_root = resolve_file_bundle_root(target_path)
            for member_item in manifest["entries"]:
                (bundle_root / member_item["path"]).unlink(missing_ok=True)
        elif target_path.is_dir():
            shutil.rmtree(target_path)
        else:
            target_path.unlink()

        self._mark_moved_artifact_source_evicted(artifact, evicted_resolved)
        self._sync_active_run_artifact_abs_path(artifact, verified_archive_entry)
        self._run_artifacts_cache.clear()
        return evicted_resolved

    def set_artifact_recovery_roots(
        self,
        artifact: Artifact,
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        *,
        append: bool = False,
    ) -> Artifact:
        """Persist ordered recovery locations for ``artifact`` in ``artifact_location``."""
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if self.db is None:
            raise RuntimeError(
                "Cannot update artifact recovery roots: tracker has no database configured."
            )
        if artifact.id is None:
            raise ValueError("artifact must have an id.")

        incoming = self.fs.normalize_recovery_roots(roots)
        existing_rows = self.db.get_artifact_recovery_locations(artifact.id)
        if append:
            existing_roots = [
                extracted
                for r in existing_rows
                if (
                    extracted := self.fs.extract_recovery_root_from_location_row(
                        artifact.uri, r
                    )
                )
                is not None
            ]
            normalized = self.fs.normalize_recovery_roots([*existing_roots, *incoming])
        else:
            normalized = incoming

        new_rows = self._build_recovery_location_rows_for_artifact(
            artifact,
            normalized,
            existing_rows=existing_rows,
        )
        self.db.replace_artifact_recovery_locations_bulk(
            {artifact.id: new_rows},
            require_all=True,
        )
        base_meta = dict(artifact.meta or {})
        base_meta.pop("recovery_roots", None)
        updated = artifact.with_meta(base_meta).with_resolver(self.tracker)
        self._sync_active_run_artifacts_meta({str(artifact.id): base_meta})
        self._run_artifacts_cache.clear()
        return updated

    def archive_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
        prepend_source_mount_root: bool = True,
        _deferred_directory_source_removals: list[tuple[Path, Path]] | None = None,
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
        prepend_source_mount_root : bool, default True
            When ``True`` and archiving with ``mode="copy"`` and ``append=True``,
            prepend the artifact's configured mount root ahead of ``archive_root``
            in ``recovery_roots`` if ``container_uri`` uses a scheme present in
            ``tracker.fs.mounts``.

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
            return self._archive_output_set_plan(
                self._build_output_set_archive_plan(artifact, archive_root),
                archive_root,
                mode=mode,
                append=append,
                prepend_source_mount_root=prepend_source_mount_root,
            )
        if _is_directory_artifact(artifact):
            return self._archive_directory_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
                prepend_source_mount_root=prepend_source_mount_root,
                _deferred_directory_source_removals=(
                    _deferred_directory_source_removals
                ),
            )
        if _is_file_bundle_artifact(artifact):
            return self._archive_shapefile_bundle_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
                prepend_source_mount_root=prepend_source_mount_root,
                _deferred_directory_source_removals=(
                    _deferred_directory_source_removals
                ),
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

        relative_path = self.fs.get_remappable_relative_path(artifact.uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI "
                "layout. Use managed output paths or preserve a stable relative "
                "layout before archiving. Absolute-path and file:// artifacts "
                "cannot be recovered from root-only recovery metadata."
            )

        archive_root_path = Path(archive_root).resolve()
        destination = (archive_root_path / relative_path).resolve()
        source_path = self._find_scalar_archive_source(artifact)

        destination_preexisted = destination.exists()
        matched_existing_destination = False
        moved_from: Path | None = None
        if destination.exists():
            if destination.is_symlink():
                raise ValueError(
                    f"Symlink detected in archive destination: {destination}"
                )
            if destination.resolve() != source_path.resolve():
                if not self._is_matching_archive_file(source_path, destination):
                    raise FileExistsError(
                        f"Archive destination already exists: {destination}"
                    )
                matched_existing_destination = True
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source_path.resolve() != destination.resolve():
                if mode == "copy" or _deferred_directory_source_removals is not None:
                    shutil.copy2(source_path, destination)
                    if file_sha256(destination) != file_sha256(source_path):
                        destination.unlink(missing_ok=True)
                        raise ValueError(
                            f"Archived copy for artifact {artifact.key!r} failed "
                            f"post-copy verification: {destination}"
                        )
                else:
                    moved_from = source_path
                    shutil.move(str(source_path), str(destination))

        recovery_roots = self._archive_recovery_roots_for_artifact(
            artifact,
            archive_root_path,
            mode=mode,
            append=append,
            prepend_source_mount_root=prepend_source_mount_root,
        )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, recovery_roots, append=append
            )
            self._promote_verified_archive_location(
                artifact, archive_root_path, destination
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
            if _deferred_directory_source_removals is not None:
                if source_path.resolve() != destination.resolve():
                    _deferred_directory_source_removals.append(
                        (source_path, destination)
                    )
            elif matched_existing_destination:
                source_path.unlink(missing_ok=True)
            self._mark_moved_artifact_source_evicted(artifact, source_path)
            resolved_dest = str(destination.resolve())
            self._sync_active_run_artifact_abs_path(artifact, resolved_dest)
        return destination

    def _find_current_managed_uri_source(self, artifact: Artifact) -> Path | None:
        """Return a validated current-mount source for an ordinary artifact.

        This is deliberately a fallback after producer historical and recovery
        locations. Only an explicitly configured URI scheme is eligible: the
        generic URI resolver also accepts run-relative and legacy local paths,
        which must not become a new source authority for run-owned artifacts.
        """
        if "://" not in artifact.uri:
            return None

        scheme, _ = artifact.uri.split("://", 1)
        if scheme not in self.fs.mounts:
            return None

        candidate = Path(self.fs.resolve_uri(artifact.uri))
        if not candidate.exists():
            return None
        if not candidate.is_file() or _is_file_bundle_artifact(artifact):
            return candidate
        if artifact.digest is None:
            raise ValueError(
                f"Cannot archive artifact {artifact.key!r} from its current "
                "configured mount: artifact hash is unavailable."
            )

        if not self._verify_file_against_artifact_digest(candidate, artifact):
            raise ValueError(
                f"Current configured mount source for artifact {artifact.key!r} "
                "does not match artifact hash."
            )
        return candidate

    @staticmethod
    def _is_matching_archive_file(source_path: Path, destination: Path) -> bool:
        """Return whether two regular files have identical size and SHA-256 content."""
        return (
            source_path.is_file()
            and destination.is_file()
            and source_path.stat().st_size == destination.stat().st_size
            and file_sha256(source_path) == file_sha256(destination)
        )

    @staticmethod
    def _is_output_set_parent(artifact: Artifact) -> bool:
        """Return whether an artifact is the logical parent of an OutputSet."""
        metadata = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        return artifact.driver == "artifact_set" or metadata.get("artifact_set") is True

    def _find_archive_source_candidate(
        self,
        artifact: Artifact,
        *,
        producing_run: Any | None = None,
        source_validator: Callable[[Path], bool] | None = None,
        fallback_validator: Callable[[Path], bool] | None = None,
        preserve_raw_paths: bool = True,
        allow_run_owned_uri_fallback: bool = False,
        include_current_managed_uri: bool = True,
    ) -> Path | None:
        """Locate an archive source candidate across recovery, runtime, and mount roots."""
        run = (
            producing_run
            if producing_run is not None
            else (
                self.get_run(str(artifact.run_id))
                if artifact.run_id is not None
                else None
            )
        )
        source_path: Path | None = None
        if run is not None:
            from consist.core.materialize import find_existing_recovery_source_path

            _, source_path, _ = find_existing_recovery_source_path(
                self.tracker,
                artifact=artifact,
                run=run,
                source_root=None,
                source_validator=source_validator,
                fallback_validator=fallback_validator,
                preserve_raw_paths=preserve_raw_paths,
            )
        allow_direct = allow_run_owned_uri_fallback or artifact.run_id is None
        if source_path is None and allow_direct:
            direct_candidates: list[Path] = []
            if artifact.abs_path:
                direct_candidates.append(Path(artifact.abs_path))
            try:
                direct_candidates.append(Path(self.resolve_uri(artifact.uri)))
            except (OSError, RuntimeError, TypeError, ValueError):
                pass
            validators = (
                (source_validator, fallback_validator)
                if fallback_validator is not None
                else (source_validator,)
            )
            for validator in validators:
                for raw_candidate in direct_candidates:
                    candidate = (
                        raw_candidate if preserve_raw_paths else raw_candidate.resolve()
                    )
                    if candidate.exists() and (
                        validator is None or validator(candidate)
                    ):
                        source_path = candidate
                        break
                if source_path is not None:
                    break
        if source_path is None and include_current_managed_uri:
            source_path = self._find_current_managed_uri_source(artifact)
        if (
            source_path is None
            and run is not None
            and self._active_archive_replace_backup is not None
        ):
            archive_root, backup_root = self._active_archive_replace_backup
            has_archive_root = any(
                Path(root).resolve() == archive_root
                for root in self.fs.normalize_recovery_roots(
                    self.tracker.artifact_recovery_roots(artifact)
                )
            ) or (
                isinstance(artifact.abs_path, str)
                and bool(artifact.abs_path)
                and Path(artifact.abs_path)
                .expanduser()
                .resolve()
                .is_relative_to(archive_root)
            )
            if has_archive_root:
                from consist.core.materialize import find_existing_recovery_source_path

                _, source_path, _ = find_existing_recovery_source_path(
                    self.tracker,
                    artifact=artifact,
                    run=run,
                    source_root=backup_root,
                    source_validator=source_validator,
                    fallback_validator=fallback_validator,
                    preserve_raw_paths=preserve_raw_paths,
                )
        return source_path

    def _find_output_set_archive_source(
        self,
        artifact: Artifact,
        *,
        producing_run: Any | None = None,
    ) -> Path:
        """Locate an OutputSet source without resolving through symlinks."""

        def reject_symlink(path: Path) -> bool:
            if self._has_symlink_component(path):
                raise ValueError(f"OutputSet source contains a symlink: {path}")
            return True

        source_path = self._find_archive_source_candidate(
            artifact,
            producing_run=producing_run,
            source_validator=reject_symlink,
            preserve_raw_paths=True,
            allow_run_owned_uri_fallback=True,
            include_current_managed_uri=False,
        )
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
        metadata = artifact.meta if isinstance(artifact.meta, Mapping) else {}
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
        manifest_metadata = manifest.meta if isinstance(manifest.meta, Mapping) else {}
        if (
            manifest_metadata.get("output_set_manifest") is not True
            or manifest_metadata.get("output_set_key") != artifact.key
        ):
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an invalid manifest artifact."
            )

        parent_relative = self.fs.get_remappable_relative_path(artifact.uri)
        manifest_relative = self.fs.get_remappable_relative_path(manifest.uri)
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
        if manifest.digest is None or not self._verify_file_against_artifact_digest(
            manifest_source, manifest
        ):
            raise ValueError(
                f"OutputSet manifest {manifest.key!r} bytes do not match its "
                "persisted identity."
            )
        manifest_content_hash = file_sha256(manifest_source)
        try:
            expected_parent_digest = compute_output_set_manifest_digest(
                manifest_contents
            )
        except ValueError as exc:
            raise ValueError(
                f"OutputSet parent {artifact.key!r} has an incomplete manifest."
            ) from exc
        if artifact.digest != expected_parent_digest:
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
            (child.meta if isinstance(child.meta, Mapping) else {}).get(
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
            child_metadata = child.meta if isinstance(child.meta, Mapping) else {}
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
                or manifest_member.get("uri") != child.uri
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
            child_relative = self.fs.get_remappable_relative_path(child.uri)
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
                file_sha256(member_source) != content_hash
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
        dict[str, _NestedOutputSetScalar],
        dict[str, _NestedOutputSetDirectory],
    ]:
        """Validate selected OutputSets and their nested-output overlap contract."""
        output_set_plans = {
            key: self._build_output_set_archive_plan(artifact, archive_root)
            for key, artifact in selected.items()
            if self._is_output_set_parent(artifact)
        }
        archive_root_path = Path(archive_root).expanduser().absolute()
        nested_scalars: dict[str, _NestedOutputSetScalar] = {}
        nested_directories: dict[str, _NestedOutputSetDirectory] = {}

        for key, artifact in selected.items():
            if key in output_set_plans:
                continue
            relative_path = self.fs.get_remappable_relative_path(artifact.uri)
            if relative_path is None:
                continue
            destination = archive_root_path / relative_path
            containing_output_sets: list[tuple[str, _OutputSetArchivePlan]] = []
            for output_set_key, plan in output_set_plans.items():
                try:
                    destination.relative_to(plan.destination)
                except ValueError:
                    continue
                containing_output_sets.append((output_set_key, plan))
            if not containing_output_sets:
                continue
            if len(containing_output_sets) > 1:
                raise ValueError(
                    f"Nested output {key!r} is contained by multiple selected "
                    "OutputSets."
                )
            output_set_key, plan = containing_output_sets[0]
            if destination == plan.destination:
                raise ValueError(
                    f"Nested output {key!r} conflicts with OutputSet "
                    f"{output_set_key!r} root."
                )
            if _is_directory_artifact(artifact):
                self._validate_nested_output_set_directory(
                    key, artifact, destination, output_set_key, plan
                )
                nested_directories[key] = _NestedOutputSetDirectory(
                    artifact=artifact,
                    destination=destination,
                    output_set_key=output_set_key,
                )
                continue
            matching_members = [
                member for member in plan.members if member.destination == destination
            ]
            if not matching_members:
                raise ValueError(
                    f"Nested output {key!r} is not a manifest member of "
                    f"OutputSet {output_set_key!r}."
                )
            member = matching_members[0]
            if (
                artifact.uri != member.artifact.uri
                or artifact.driver != member.artifact.driver
            ):
                raise ValueError(
                    f"Nested output {key!r} does not match immutable manifest "
                    f"member {member.artifact.key!r} of OutputSet "
                    f"{output_set_key!r}."
                )
            source = self._find_output_set_archive_source(artifact)
            if (
                source != member.source_path
                or not source.is_file()
                or file_sha256(source) != member.content_hash
            ):
                raise ValueError(
                    f"Nested output {key!r} does not match immutable manifest "
                    f"member {member.artifact.key!r} of OutputSet "
                    f"{output_set_key!r}."
                )
            nested_scalars[key] = _NestedOutputSetScalar(
                artifact=artifact,
                destination=destination,
                output_set_key=output_set_key,
            )

        for plan in output_set_plans.values():
            self._validate_output_set_archive_destination(
                plan.destination, plan.members
            )
            self._validate_output_set_manifest_archive_destination(plan)

        return output_set_plans, nested_scalars, nested_directories

    def _find_scalar_archive_source(self, artifact: Artifact) -> Path:
        """Resolve and validate the source file for archiving a scalar artifact."""
        mismatched_candidate: Path | None = None

        def _matches_artifact_hash(candidate: Path) -> bool:
            nonlocal mismatched_candidate
            if not candidate.is_file() or artifact.digest is None:
                return True
            if self._verify_file_against_artifact_digest(candidate, artifact):
                return True
            mismatched_candidate = candidate
            return False

        source_path = self._find_archive_source_candidate(
            artifact,
            source_validator=_matches_artifact_hash,
            preserve_raw_paths=False,
        )
        if source_path is None or not source_path.exists():
            if mismatched_candidate is not None:
                raise ValueError(
                    f"Source file for artifact {artifact.key!r} does not match "
                    f"artifact hash: {mismatched_candidate}"
                )
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )
        if source_path.is_dir():
            raise ValueError(
                "Directory archival requires an explicitly declared directory artifact."
            )
        return source_path

    def _validate_nested_output_set_directory(
        self,
        key: str,
        artifact: Artifact,
        destination: Path,
        output_set_key: str,
        plan: _OutputSetArchivePlan,
    ) -> None:
        """Require a nested directory artifact to equal an OutputSet subtree."""
        manifest = _directory_artifact_manifest(artifact)
        relative_subtree = _normalize_output_set_relative_path(
            destination.relative_to(plan.destination)
        )
        source = self._find_output_set_archive_source(artifact)
        expected_source = plan.parent_source / relative_subtree
        if source != expected_source or not source.is_dir():
            raise ValueError(
                f"Nested directory output {key!r} is outside OutputSet "
                f"{output_set_key!r} source root."
            )
        validate_directory_tree(source, manifest)
        expected_entries = self._output_set_directory_subtree_entries(plan, destination)
        if manifest["entries"] != expected_entries:
            raise ValueError(
                f"Nested directory output {key!r} does not match OutputSet "
                f"{output_set_key!r} manifest subtree."
            )

    @staticmethod
    def _output_set_directory_subtree_entries(
        plan: _OutputSetArchivePlan,
        destination: Path,
    ) -> list[dict[str, Any]]:
        """Derive the exact directory manifest entries represented by set members."""
        entries: list[dict[str, Any]] = []
        directories: set[str] = set()
        for member in plan.members:
            try:
                relative_path = _normalize_output_set_relative_path(
                    member.destination.relative_to(destination)
                )
            except ValueError:
                continue
            parent = Path(relative_path).parent
            while parent != Path("."):
                directories.add(parent.as_posix())
                parent = parent.parent
            entries.append(
                {
                    "kind": "file",
                    "path": relative_path,
                    "sha256": member.content_hash,
                    "size": member.source_path.stat().st_size,
                }
            )
        entries.extend({"kind": "directory", "path": path} for path in directories)
        return sorted(entries, key=lambda entry: (entry["path"], entry["kind"]))

    def _validate_output_set_archive_destination(
        self,
        destination: Path,
        members: Sequence[_OutputSetArchiveMember],
    ) -> None:
        """Require an existing OutputSet archive root to match the manifest."""
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
            if file_sha256(path) != member.content_hash:
                raise ValueError(
                    f"OutputSet archive destination member hash mismatch: {path}"
                )

    def _validate_output_set_manifest_archive_destination(
        self, plan: _OutputSetArchivePlan
    ) -> None:
        """Require an existing manifest destination to retain identical bytes."""
        destination = plan.manifest_destination
        if self._has_symlink_component(destination) or destination.is_symlink():
            raise ValueError(
                f"Symlink detected in OutputSet archive destination: {destination}"
            )
        if destination.exists() and (
            not destination.is_file()
            or file_sha256(destination) != plan.manifest_content_hash
        ):
            raise FileExistsError(
                f"OutputSet manifest archive destination already exists: {destination}"
            )

    def _publish_output_set_directory(self, plan: _OutputSetArchivePlan) -> bool:
        """Stage and publish one OutputSet member root without overwriting bytes."""
        self._validate_output_set_archive_destination(plan.destination, plan.members)
        if plan.destination.exists():
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
                if file_sha256(target) != member.content_hash:
                    raise ValueError(
                        f"OutputSet member {member.artifact.key!r} changed while archiving."
                    )
            for member in plan.members:
                if file_sha256(member.source_path) != member.content_hash:
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
                file_sha256(destination) != plan.manifest_content_hash
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
            if file_sha256(temporary_path) != plan.manifest_content_hash:
                raise ValueError(
                    f"OutputSet manifest {plan.manifest.key!r} changed while archiving."
                )
            if file_sha256(plan.manifest_source) != plan.manifest_content_hash:
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
        if plan.parent_source == plan.destination:
            return
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
        remove_sources: bool = True,
        dependent_artifacts: Sequence[Artifact] = (),
        prepend_source_mount_root: bool = True,
    ) -> Path:
        """Archive a validated OutputSet plan before registering it recoverable."""
        published_directory = False
        published_manifest = False
        archive_root_path = Path(archive_root).expanduser().absolute()
        recovery_roots = self._archive_recovery_roots_for_artifact(
            plan.parent,
            archive_root_path,
            mode=mode,
            append=append,
            prepend_source_mount_root=prepend_source_mount_root,
        )
        try:
            published_directory = self._publish_output_set_directory(plan)
            published_manifest = self._publish_output_set_manifest(plan)
            self.tracker._set_artifact_recovery_roots_bulk(
                [
                    plan.parent,
                    *(member.artifact for member in plan.members),
                    plan.manifest,
                    *dependent_artifacts,
                ],
                recovery_roots,
                append=append,
            )
            self._promote_verified_archive_location(
                plan.parent, archive_root_path, plan.destination
            )
            for member_item in plan.members:
                self._promote_verified_archive_location(
                    member_item.artifact, archive_root_path, member_item.destination
                )
            self._promote_verified_archive_location(
                plan.manifest, archive_root_path, plan.manifest_destination
            )
            for dep_art in dependent_artifacts:
                dep_rel = self.fs.get_remappable_relative_path(dep_art.uri)
                if dep_rel is not None:
                    self._promote_verified_archive_location(
                        dep_art,
                        archive_root_path,
                        (archive_root_path / dep_rel).resolve(),
                    )
        except Exception:
            if published_manifest:
                self._remove_output_set_archive_destination(plan.manifest_destination)
            if published_directory:
                self._remove_output_set_archive_destination(plan.destination)
            raise
        if mode == "move":
            if remove_sources:
                self._remove_output_set_sources(plan)
            self._mark_moved_artifact_source_evicted(plan.parent, plan.parent_source)
            self._sync_active_run_artifact_abs_path(
                plan.parent, str(plan.destination.resolve())
            )
            for member_item in plan.members:
                self._mark_moved_artifact_source_evicted(
                    member_item.artifact, member_item.source_path
                )
                self._sync_active_run_artifact_abs_path(
                    member_item.artifact, str(member_item.destination.resolve())
                )
            self._mark_moved_artifact_source_evicted(
                plan.manifest, plan.manifest_source
            )
            self._sync_active_run_artifact_abs_path(
                plan.manifest, str(plan.manifest_destination.resolve())
            )
            for dep_art in dependent_artifacts:
                self._mark_moved_artifact_source_evicted(dep_art)
                dep_rel = self.fs.get_remappable_relative_path(dep_art.uri)
                if dep_rel is not None:
                    self._sync_active_run_artifact_abs_path(
                        dep_art, str((archive_root_path / dep_rel).resolve())
                    )
        return plan.destination

    @staticmethod
    def _reject_symlink_under_archive_root(
        archive_root_path: Path, relative_path: Path
    ) -> None:
        current = archive_root_path
        if current.is_symlink():
            raise ValueError(
                f"archive destination contains a symlink component: {current}"
            )
        for part in relative_path.parts:
            current = current / part
            if current.is_symlink():
                raise ValueError(
                    f"archive destination contains a symlink component: {current}"
                )

    def _archive_directory_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
        prepend_source_mount_root: bool = True,
        _deferred_directory_source_removals: list[tuple[Path, Path]] | None = None,
    ) -> Path:
        """Archive a manifest-verified immutable directory artifact."""
        manifest = _directory_artifact_manifest(artifact)
        allow_extra_files = directory_allows_extra_files(artifact)
        relative_path = self.fs.get_remappable_relative_path(artifact.uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI layout."
            )
        source_path = self._find_archive_source_candidate(
            artifact,
            source_validator=lambda candidate: (
                is_valid_directory_artifact_tree(candidate, artifact) is not False
            ),
            fallback_validator=lambda _: True,
        )
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        archive_root_path = Path(archive_root).expanduser().absolute()
        self._reject_symlink_under_archive_root(archive_root_path, relative_path)
        destination = archive_root_path / relative_path
        published = materialize_directory_tree_with_rollback_info(
            source_path,
            destination,
            manifest,
            preserve_existing=True,
            allow_extra_files=allow_extra_files,
        )
        recovery_roots = self._archive_recovery_roots_for_artifact(
            artifact,
            archive_root_path,
            mode=mode,
            append=append,
            prepend_source_mount_root=prepend_source_mount_root,
        )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, recovery_roots, append=append
            )
            self._promote_verified_archive_location(
                artifact, archive_root_path, destination
            )
        except Exception:
            rollback_published_directory_tree(destination, published)
            raise
        if mode == "move":
            if source_path.resolve() != destination.resolve():
                if _deferred_directory_source_removals is not None:
                    _deferred_directory_source_removals.append(
                        (source_path, destination)
                    )
                elif source_path.exists():
                    shutil.rmtree(source_path)
            self._mark_moved_artifact_source_evicted(artifact, source_path)
            resolved_dest = str(destination.resolve())
            self._sync_active_run_artifact_abs_path(artifact, resolved_dest)
        return destination

    def _archive_shapefile_bundle_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"],
        append: bool,
        prepend_source_mount_root: bool = True,
        _deferred_directory_source_removals: list[tuple[Path, Path]] | None = None,
    ) -> Path:
        """Archive a verified Shapefile sidecar bundle as one atomic directory."""
        entry, manifest = _shapefile_bundle_metadata(artifact)
        relative_path = self.fs.get_remappable_relative_path(artifact.uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI layout."
            )
        source_path = self._find_archive_source_candidate(
            artifact,
            source_validator=lambda candidate: (
                is_valid_shapefile_bundle_source(candidate, artifact) is not False
            ),
            fallback_validator=lambda _: True,
        )
        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        archive_root_path = Path(archive_root).expanduser().absolute()
        self._reject_symlink_under_archive_root(archive_root_path, relative_path)
        destination = archive_root_path / relative_path
        source_bundle_root = resolve_file_bundle_root(source_path)
        published = materialize_shapefile_bundle(
            source_bundle_root,
            destination,
            entry,
            manifest,
            preserve_existing=True,
        )
        recovery_roots = self._archive_recovery_roots_for_artifact(
            artifact,
            archive_root_path,
            mode=mode,
            append=append,
            prepend_source_mount_root=prepend_source_mount_root,
        )
        try:
            self.tracker.set_artifact_recovery_roots(
                artifact, recovery_roots, append=append
            )
            self._promote_verified_archive_location(
                artifact, archive_root_path, destination
            )
        except Exception:
            if published and destination.exists():
                shutil.rmtree(destination)
            raise
        if mode == "move":
            if source_bundle_root.resolve() != destination.resolve():
                for member in manifest["entries"]:
                    member_source = source_bundle_root / member["path"]
                    if _deferred_directory_source_removals is not None:
                        _deferred_directory_source_removals.append(
                            (member_source, destination)
                        )
                    else:
                        member_source.unlink(missing_ok=True)
            self._mark_moved_artifact_source_evicted(artifact, source_path)
            resolved_dest = resolve_artifact_entry_path(artifact, destination)
            self._sync_active_run_artifact_abs_path(artifact, resolved_dest)
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
            if artifact.driver == "h5_table" or artifact.parent_artifact_id is not None
            else None
        )
        policy_validation = validate_recovery_registration_policy(
            artifact, parent=parent
        )
        if not policy_validation.allowed:
            return result(
                "blocked_by_container_policy", message=policy_validation.message
            )

        relative_path = self.fs.get_remappable_relative_path(artifact.uri)
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

        has_verifiable_digest = self._has_verifiable_content_digest(artifact)
        if verify and content_hash is None and not has_verifiable_digest:
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
            mismatches: list[str] = []
            try:
                if content_hash is not None:
                    if ":" in content_hash:
                        matched = verify_digest(
                            expected_path_resolved,
                            content_hash,
                            driver=artifact.driver,
                            member=artifact.member,
                        )
                    else:
                        matched = file_sha256(expected_path_resolved) == content_hash
                    if not matched:
                        mismatches.append("content_hash")
                elif artifact.digest is None or not verify_digest(
                    expected_path_resolved,
                    artifact.digest,
                    driver=artifact.driver,
                    member=artifact.member,
                ):
                    mismatches.append("artifact.digest")
            except Exception as exc:
                return result(
                    "failed",
                    expected_path=expected_path_resolved,
                    message=f"Could not hash recovery copy {expected_path_resolved}: {exc}",
                )
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
            artifact = self.tracker.set_artifact_recovery_roots(
                artifact, [recovery_root_path], append=append
            )
            if verify and expected_path_resolved is not None:
                self._promote_verified_archive_location(
                    artifact, recovery_root_path, expected_path_resolved
                )
        except Exception as exc:
            return result(
                "failed",
                expected_path=expected_path_resolved,
                message=f"Could not update recovery location state: {exc}",
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
        site: str = "advisory",
    ) -> dict[str, Artifact]:
        """Persist recovery locations for artifacts in one database transaction."""
        if self.db is None:
            raise RuntimeError(
                "Cannot update artifact recovery roots: tracker has no database configured."
            )

        incoming = self.fs.normalize_recovery_roots(roots)
        validated_artifacts: list[Artifact] = []
        for artifact in artifacts:
            if not isinstance(artifact, Artifact):
                raise TypeError("artifact must be an Artifact instance.")
            if artifact.id is None:
                raise ValueError("artifact must have an id.")
            validated_artifacts.append(artifact)

        if not validated_artifacts:
            return {}

        existing_by_id = self.db.get_artifacts_recovery_locations_bulk(
            [a.id for a in validated_artifacts]
        )
        base_ns = time.time_ns()
        locations_by_aid: dict[uuid.UUID, list[ArtifactLocationRow]] = {}
        cleaned_meta_by_id: dict[str, dict[str, Any]] = {}
        refreshed_by_id: dict[str, Artifact] = {}
        for artifact in validated_artifacts:
            existing_rows = existing_by_id.get(artifact.id, [])
            if append:
                existing_roots = [
                    extracted
                    for r in existing_rows
                    if (
                        extracted := self.fs.extract_recovery_root_from_location_row(
                            artifact.uri, r
                        )
                    )
                    is not None
                ]
                normalized = self.fs.normalize_recovery_roots(
                    [*existing_roots, *incoming]
                )
            else:
                normalized = incoming
            locations_by_aid[artifact.id] = (
                self._build_recovery_location_rows_for_artifact(
                    artifact,
                    normalized,
                    existing_rows=existing_rows,
                    base_ns=base_ns,
                )
            )
            clean_meta = dict(artifact.meta or {})
            clean_meta.pop("recovery_roots", None)
            cleaned_meta_by_id[str(artifact.id)] = clean_meta
            refreshed_by_id[str(artifact.id)] = artifact.with_meta(
                clean_meta
            ).with_resolver(self.tracker)

        self.db.replace_artifact_recovery_locations_bulk(
            locations_by_aid,
            require_all=True,
        )
        self._sync_active_run_artifacts_meta(cleaned_meta_by_id)
        self._run_artifacts_cache.clear()
        return refreshed_by_id

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
                refreshed_by_id = self.tracker._set_artifact_recovery_roots_bulk(
                    [artifact for _, artifact, _ in pending_metadata_updates],
                    [recovery_root_path],
                    append=append,
                )
            except Exception:
                for key, artifact, registration in pending_metadata_updates:
                    try:
                        updated_art = self.tracker.set_artifact_recovery_roots(
                            artifact, [recovery_root_path], append=append
                        )
                    except Exception as exc:
                        registered[key] = replace(
                            registration,
                            status="failed",
                            message=f"Could not update recovery location state: {exc}",
                            metadata_updated=False,
                        )
                    else:
                        if verify and registration.expected_path is not None:
                            self._promote_verified_archive_location(
                                updated_art,
                                recovery_root_path,
                                registration.expected_path,
                            )
                        registered[key] = replace(
                            registration,
                            artifact=updated_art,
                            message="Recovery copy verified and registered.",
                            metadata_updated=True,
                        )
            else:
                for key, artifact, registration in pending_metadata_updates:
                    updated_art = (
                        refreshed_by_id.get(str(artifact.id), artifact)
                        if isinstance(refreshed_by_id, Mapping)
                        else artifact
                    )
                    if verify and registration.expected_path is not None:
                        self._promote_verified_archive_location(
                            updated_art,
                            recovery_root_path,
                            registration.expected_path,
                        )
                    registered[key] = replace(
                        registration,
                        artifact=updated_art,
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
            assert candidate.artifact.digest is not None
            matches = verify_digest(
                candidate.target_path,
                candidate.artifact.digest,
                driver=candidate.artifact.driver,
                member=candidate.artifact.member,
            )
        except (OSError, ValueError) as exc:
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

        def validate_source(path: Path) -> bool:
            reject_symlink(path)
            if not path.is_file() or candidate.artifact.digest is None:
                return True
            return self._verify_file_against_artifact_digest(path, candidate.artifact)

        if producing_run is not None:
            try:
                source_path = self._find_archive_source_candidate(
                    candidate.artifact,
                    producing_run=producing_run,
                    source_validator=validate_source,
                    fallback_validator=reject_symlink,
                    preserve_raw_paths=True,
                    include_current_managed_uri=False,
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
            if verify and not self._verify_file_against_artifact_digest(
                candidate.target_path, candidate.artifact
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
            relative_path = self.fs.get_remappable_relative_path(artifact.uri)
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
            if verify and not self._has_verifiable_content_digest(artifact):
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
        prepend_source_mount_root: bool = True,
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
        prepend_source_mount_root : bool, default True
            When ``True`` and archiving with ``mode="copy"`` and ``append=True``,
            prepend the artifact's configured mount root ahead of ``archive_root``
            in ``recovery_roots`` if ``container_uri`` uses a scheme present in
            ``tracker.fs.mounts``.

        Returns
        -------
        ArchivedOutputs
            Read-only key-to-path mapping and refreshed output artifacts.

        Notes
        -----
        Selected OutputSets are validated before any archive bytes are
        published. They are then published before ordinary outputs whose
        URI-relative destinations lie beneath their roots. Such nested outputs
        must be identical manifest members or an immutable directory whose
        manifest exactly equals the represented set subtree. Use
        :meth:`archive_run_output_files` for report-oriented, no-overwrite
        regular-file archival.
        """
        if self.db is None:
            raise RuntimeError(
                "Cannot archive run outputs: tracker has no database configured."
            )
        normalized_keys = normalize_materialize_output_keys(
            keys, caller="archive_run_outputs"
        )
        known_abs_paths = self._known_run_output_abs_paths(run_id)
        outputs = {
            key: (
                artifact.with_abs_path(known_abs_paths[key])
                if artifact.abs_path is None and key in known_abs_paths
                else artifact
            )
            for key, artifact in self.get_run_outputs(run_id).items()
        }
        selected = self._select_required_output_keys(
            outputs, normalized_keys, run_id=run_id
        )
        if normalized_keys is None:
            selected = {
                key: artifact
                for key, artifact in selected.items()
                if _output_set_hydration_kind(self.tracker, artifact)
                not in {"member", "manifest"}
                and not self.is_virtual_unbacked_artifact(artifact)
            }
            if not selected:
                return ArchivedOutputs(paths={}, outputs={})
        if mode == "copy":
            for key, artifact in list(selected.items()):
                if artifact.abs_path is not None:
                    continue
                try:
                    resolved_candidate = Path(self.resolve_uri(artifact.uri))
                except (ValueError, RuntimeError, OSError, TypeError):
                    continue
                if resolved_candidate.exists():
                    selected[key] = artifact.with_abs_path(
                        str(resolved_candidate.resolve())
                    )
        output_set_plans, nested_scalars, nested_directories = (
            self._build_selected_output_set_archive_plans(selected, archive_root)
        )
        archived_by_key: dict[str, Path] = {}
        for key, plan in output_set_plans.items():
            dependent_artifacts = [
                nested.artifact
                for nested in (*nested_scalars.values(), *nested_directories.values())
                if nested.output_set_key == key
            ]
            archived_by_key[key] = self._archive_output_set_plan(
                plan,
                archive_root,
                mode=mode,
                append=append,
                remove_sources=False,
                dependent_artifacts=dependent_artifacts,
                prepend_source_mount_root=prepend_source_mount_root,
            )
            if mode == "move":
                selected[key] = plan.parent.with_abs_path(
                    str(plan.destination.resolve())
                )
        remaining_directory_items: list[tuple[int, tuple[str, Artifact]]] = []
        remaining_other_items: list[tuple[str, Artifact]] = []
        for idx, (key, artifact) in enumerate(selected.items()):
            if key in output_set_plans:
                continue
            nested_scalar = nested_scalars.get(key)
            if nested_scalar is not None:
                archived_by_key[key] = nested_scalar.destination
                if mode == "move":
                    selected[key] = artifact.with_abs_path(
                        str(nested_scalar.destination.resolve())
                    )
                continue
            nested_directory = nested_directories.get(key)
            if nested_directory is not None:
                archived_by_key[key] = nested_directory.destination
                if mode == "move":
                    selected[key] = artifact.with_abs_path(
                        str(nested_directory.destination.resolve())
                    )
                continue
            if _is_directory_artifact(artifact):
                remaining_directory_items.append((idx, (key, artifact)))
            else:
                remaining_other_items.append((key, artifact))

        ordered_remaining_items = [
            item
            for _, item in sorted(
                remaining_directory_items,
                key=self._directory_archive_sort_key,
            )
        ] + remaining_other_items

        deferred_removals: list[tuple[Path, Path]] | None = (
            [] if mode == "move" else None
        )
        for key, artifact in ordered_remaining_items:
            archived_by_key[key] = self.archive_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
                prepend_source_mount_root=prepend_source_mount_root,
                _deferred_directory_source_removals=deferred_removals,
            )
            if mode == "move":
                resolved_moved = (
                    resolve_artifact_entry_path(artifact, archived_by_key[key])
                    if _is_file_bundle_artifact(artifact)
                    else str(archived_by_key[key].resolve())
                )
                selected[key] = artifact.with_abs_path(resolved_moved)

        if mode == "move" and deferred_removals is not None:
            for source_path, destination in reversed(deferred_removals):
                if (
                    source_path.exists()
                    and source_path.resolve() != destination.resolve()
                ):
                    if source_path.is_dir():
                        shutil.rmtree(source_path)
                    else:
                        source_path.unlink(missing_ok=True)
            for plan in output_set_plans.values():
                self._remove_output_set_sources(plan)
        archived_paths = {key: archived_by_key[key] for key in selected}
        raw_refreshed = self.get_run_outputs(run_id)
        refreshed_outputs: dict[str, Artifact] = {}
        for key, artifact in raw_refreshed.items():
            abs_hint = artifact.abs_path
            if key in selected and selected[key].abs_path is not None:
                abs_hint = selected[key].abs_path
            elif abs_hint is None and key in known_abs_paths:
                abs_hint = known_abs_paths[key]
            refreshed_outputs[key] = (
                artifact.with_abs_path(abs_hint)
                if abs_hint != artifact.abs_path
                else artifact
            ).with_resolver(self.tracker)
        self._sync_archived_run_outputs_to_runtime_state(
            run_id=run_id,
            refreshed_outputs=refreshed_outputs,
            preserve_existing_abs_path=(mode == "copy"),
        )
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
        prepend_source_mount_root: bool = True,
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
        prepend_source_mount_root : bool, default True
            When ``True`` and archiving with ``mode="copy"`` and ``append=True``,
            prepend the artifact's configured mount root ahead of ``archive_root``
            in ``recovery_roots`` if ``container_uri`` uses a scheme present in
            ``tracker.fs.mounts``.

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
            prepend_source_mount_root=prepend_source_mount_root,
        )

    def _evict_superseded_recovery_root(
        self,
        archive_root: str | os.PathLike[str],
        *,
        active_run_id: str,
    ) -> None:
        """Mark an overwritten archive root evicted for superseded runs' artifacts."""
        if self.db is None:
            return
        normalized_roots = self.fs.normalize_recovery_roots(archive_root)
        if not normalized_roots:
            return
        target_root = normalized_roots[0]
        mount_name, mount_inst, root_uri, _ = self.fs.archive_coordinate_for_root(
            "", target_root
        )
        retained_ids = {
            art.id
            for art in self.get_run_outputs(active_run_id).values()
            if art.id is not None
        }
        updated_any = self.db.evict_recovery_root_locations(
            mount_name=mount_name,
            mount_instance_id=mount_inst,
            root_uri=root_uri,
            retained_ids=retained_ids,
        )
        if updated_any:
            self._run_artifacts_cache.clear()

    def _archive_root_has_expected_layout(
        self,
        archive_root_path: Path,
        outputs: Mapping[str, Artifact],
    ) -> bool:
        """Return whether ``archive_root_path`` has the exact file and directory layout for ``outputs`` with no symlinks or stale entries."""
        try:
            actual_files: set[Path] = set()
            actual_dirs: set[Path] = set()
            for path in archive_root_path.rglob("*"):
                if path.is_symlink():
                    return False
                rel_path = path.relative_to(archive_root_path)
                if path.is_file():
                    actual_files.add(rel_path)
                elif path.is_dir():
                    actual_dirs.add(rel_path)
                else:
                    return False

            expected_files: set[Path] = set()
            expected_dirs: set[Path] = set()

            def _add_dir(rel_dir: Path) -> None:
                cur = rel_dir
                while cur != Path(".") and cur != Path(""):
                    expected_dirs.add(cur)
                    cur = cur.parent

            def _add_file(rel_file: Path) -> None:
                expected_files.add(rel_file)
                _add_dir(rel_file.parent)

            for artifact in outputs.values():
                if self.is_virtual_unbacked_artifact(artifact):
                    continue
                relative_path = self.fs.get_remappable_relative_path(artifact.uri)
                if relative_path is None:
                    return False
                if self._is_output_set_parent(artifact):
                    _add_dir(relative_path)
                elif _is_directory_artifact(artifact) or _is_file_bundle_artifact(
                    artifact
                ):
                    manifest = (
                        _directory_artifact_manifest(artifact)
                        if _is_directory_artifact(artifact)
                        else _shapefile_bundle_metadata(artifact)[1]
                    )
                    _add_dir(relative_path)
                    for entry in manifest["entries"]:
                        entry_rel = relative_path / Path(entry["path"])
                        if entry["kind"] == "directory":
                            _add_dir(entry_rel)
                        elif entry["kind"] == "file":
                            _add_file(entry_rel)
                        else:
                            return False
                else:
                    _add_file(relative_path)

            return actual_files == expected_files and actual_dirs == expected_dirs
        except (ValueError, OSError):
            return False

    def archive_current_run_outputs_for_completion_policy(
        self,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"] = "copy",
        prepend_source_mount_root: bool = True,
    ) -> ArchivedOutputs:
        """Archive active-run outputs for ``RunCompletionArchivePolicy``, atomically replacing stale step-scoped archive roots."""
        if not self.current_consist or self.current_consist.run is None:
            raise RuntimeError(
                "archive_current_run_outputs_for_completion_policy(...) requires an active run context."
            )
        active_run_id = str(self.current_consist.run.id)
        archive_root_path = Path(archive_root).expanduser().absolute()
        preexisted_replaceable = (
            archive_root_path.exists()
            and not archive_root_path.is_symlink()
            and not self._has_symlink_component(archive_root_path)
            and archive_root_path.is_dir()
        )
        if not preexisted_replaceable:
            return self.tracker.archive_current_run_outputs(
                archive_root_path,
                mode=mode,
                append=True,
                prepend_source_mount_root=prepend_source_mount_root,
            )

        known_abs_paths = self._known_run_output_abs_paths(active_run_id)
        prior_outputs = {
            key: (
                artifact.with_abs_path(known_abs_paths[key])
                if artifact.abs_path is None and key in known_abs_paths
                else artifact
            )
            for key, artifact in self.get_run_outputs(active_run_id).items()
        }
        prior_state = [
            (
                artifact,
                self.fs.normalize_recovery_roots(
                    self.tracker.artifact_recovery_roots(artifact)
                ),
                artifact.abs_path,
            )
            for artifact in prior_outputs.values()
        ]

        def _restore_prior_state() -> None:
            for artifact, prior_roots, prior_abs_path in prior_state:
                restored = self.set_artifact_recovery_roots(
                    artifact, prior_roots, append=False
                )
                self._sync_active_run_artifact_abs_path(restored, prior_abs_path)

        if self._archive_root_has_expected_layout(archive_root_path, prior_outputs):
            try:
                return self.tracker.archive_current_run_outputs(
                    archive_root_path,
                    mode=mode,
                    append=True,
                    prepend_source_mount_root=prepend_source_mount_root,
                )
            except (FileExistsError, ValueError):
                _restore_prior_state()
            except Exception:
                _restore_prior_state()
                raise

        backup_root = Path(
            tempfile.mkdtemp(
                dir=str(archive_root_path.parent),
                prefix=f".consist-archive-replace-{archive_root_path.name}-",
            )
        )
        backup_target = backup_root / archive_root_path.name
        os.replace(archive_root_path, backup_target)
        self._active_archive_replace_backup = (
            archive_root_path.resolve(),
            backup_target.resolve(),
        )
        try:
            archived = self.tracker.archive_current_run_outputs(
                archive_root_path,
                mode=mode,
                append=True,
                prepend_source_mount_root=prepend_source_mount_root,
            )
        except Exception:
            if archive_root_path.exists() or archive_root_path.is_symlink():
                if archive_root_path.is_dir() and not archive_root_path.is_symlink():
                    shutil.rmtree(archive_root_path)
                else:
                    archive_root_path.unlink(missing_ok=True)
            if backup_target.exists():
                os.replace(backup_target, archive_root_path)
            shutil.rmtree(backup_root, ignore_errors=True)
            _restore_prior_state()
            raise
        finally:
            self._active_archive_replace_backup = None

        self._evict_superseded_recovery_root(
            archive_root_path, active_run_id=active_run_id
        )
        shutil.rmtree(backup_root, ignore_errors=True)
        return archived
