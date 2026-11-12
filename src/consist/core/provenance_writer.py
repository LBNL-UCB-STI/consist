from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import inspect
import logging
import os
from pathlib import Path
import tempfile
import time
import types
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
)
import uuid

from consist.core._performance_attribution import _track_begin_run_phase
from consist.location import ArtifactLocationRow, LocationSite, capture_witness
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import ConsistRecord, RunBindingInvocation
    from consist.models.run import Run as RunModel


def _accepts_locations_kwarg(fn: Callable[..., Any]) -> bool:
    if isinstance(fn, types.MethodType):
        return True
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return "locations" in params or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
    )


class ProvenanceWriter:
    """
    Persistence-side helper for run JSON snapshots and artifact DB sync.

    This keeps write-heavy provenance plumbing out of ``Tracker`` while leaving
    the tracker as the composition root and lifecycle orchestrator.
    """

    def __init__(self, tracker: "Tracker"):
        self._tracker = tracker
        self._artifact_batch_depth = 0
        self._batch_pending_json_flush = False
        self._batch_pending_artifacts: list[
            tuple["Artifact", str, Optional[tuple[str, ...]]]
        ] = []
        self._batch_pending_artifact_facet_bundles: list[
            tuple[
                "Artifact",
                ArtifactFacet,
                Dict[str, Any],
                Optional[List[ArtifactKV]],
            ]
        ] = []

    def _cleanup_temp_path(self, temp_path: Path) -> None:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            return

    @contextmanager
    def batch_artifact_writes(self) -> Iterator[None]:
        is_outermost = self._artifact_batch_depth == 0
        if is_outermost:
            self._batch_pending_json_flush = False
            self._batch_pending_artifacts = []
            self._batch_pending_artifact_facet_bundles = []
        self._artifact_batch_depth += 1
        try:
            yield
        finally:
            self._artifact_batch_depth -= 1
            if self._artifact_batch_depth != 0:
                return

            pending_json_flush = self._batch_pending_json_flush
            pending_artifacts = list(self._batch_pending_artifacts)
            pending_artifact_facet_bundles = list(
                self._batch_pending_artifact_facet_bundles
            )
            self._batch_pending_json_flush = False
            self._batch_pending_artifacts = []
            self._batch_pending_artifact_facet_bundles = []

            if pending_json_flush:
                with _track_begin_run_phase("input_binding.batch_flush_json"):
                    self._flush_json_now()
            if pending_artifacts:
                with _track_begin_run_phase("input_binding.batch_sync_artifacts"):
                    self._sync_artifacts_now(pending_artifacts)
            if pending_artifact_facet_bundles:
                with _track_begin_run_phase("input_binding.batch_persist_facets"):
                    self._persist_artifact_facet_bundles_now(
                        pending_artifact_facet_bundles
                    )

    def flush_json(self) -> None:
        if self._artifact_batch_depth > 0:
            self._batch_pending_json_flush = True
            return
        self._flush_json_now()

    def _flush_json_now(self) -> None:
        tracker = self._tracker
        if not tracker.current_consist:
            return
        self._write_record_json(tracker.current_consist)

    def flush_record_json(self, record: "ConsistRecord") -> None:
        self._write_record_json(record)

    def _write_record_json(self, record: "ConsistRecord") -> None:
        tracker = self._tracker
        json_str = record.model_dump_json(indent=2)

        run_id = record.run.id
        safe_run_id = "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )

        per_run_dir = tracker.fs.run_dir / "consist_runs"
        per_run_dir.mkdir(parents=True, exist_ok=True)
        per_run_target = per_run_dir / f"{safe_run_id}.json"
        self._write_text_atomic(per_run_target, json_str)

        latest_target = tracker.fs.run_dir / "consist.json"
        self._write_text_atomic(latest_target, json_str)

    def _write_text_atomic(self, target: Path, payload: str) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=target.parent,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                handle.write(payload)
                handle.flush()
                tmp_path = Path(handle.name)
            os.replace(tmp_path, target)
        except Exception:
            if tmp_path is not None:
                self._cleanup_temp_path(tmp_path)
            raise

    def sync_run(
        self,
        run: "RunModel",
        *,
        binding_invocation: "RunBindingInvocation | None" = None,
        require_success: bool = False,
    ) -> None:
        tracker = self._tracker
        if tracker.db:
            try:
                tracker.db.sync_run(
                    run,
                    binding_invocation=binding_invocation,
                    require_success=require_success,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
                if require_success:
                    raise

    def sync_run_with_links(
        self,
        run: "RunModel",
        *,
        artifact_ids: list[uuid.UUID],
        direction: str = "output",
        binding_invocation: "RunBindingInvocation | None" = None,
        require_success: bool = False,
    ) -> None:
        tracker = self._tracker
        if tracker.db:
            try:
                tracker.db.sync_run_with_links(
                    run=run,
                    artifact_ids=artifact_ids,
                    direction=direction,
                    binding_invocation=binding_invocation,
                    require_success=require_success,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
                if require_success:
                    raise

    def _build_artifact_location_rows(
        self,
        artifact: "Artifact",
        *,
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> list[ArtifactLocationRow]:
        tracker = self._tracker
        rows: list[ArtifactLocationRow] = []
        local_path: Optional[Path] = (
            Path(artifact._abs_path) if artifact._abs_path else None
        )
        if local_path is None:
            try:
                local_path = Path(tracker.fs.resolve_uri(artifact.uri))
            except (ValueError, RuntimeError, OSError, TypeError):
                local_path = None

        if local_path is not None:
            try:
                os.lstat(local_path)
                path_exists = True
            except OSError:
                path_exists = False
            if path_exists:
                mount_name, mount_instance_id, canonical_uri = (
                    tracker.fs.location_coordinate_for_uri(artifact.uri)
                )
                site: LocationSite = (
                    "workspace" if mount_name in ("workspace", "runs") else "external"
                )
                meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
                is_dir = bool(
                    meta.get("directory_artifact") is True
                    or meta.get("artifact_set") is True
                    or artifact.driver in ("zarr", "directory", "artifact_set")
                )
                is_shp = bool(
                    meta.get("file_bundle_artifact") is True
                    or artifact.driver == "shapefile"
                )
                raw_entry = meta.get("file_bundle_entry")
                bundle_entry = str(raw_entry) if isinstance(raw_entry, str) else None
                obs_ns = time.time_ns()
                try:
                    witness = capture_witness(
                        local_path,
                        observed_at_ns=obs_ns,
                        driver=artifact.driver,
                        is_directory=is_dir,
                        is_shapefile_bundle=is_shp,
                        bundle_entry=bundle_entry,
                    )
                except (OSError, ValueError):
                    witness = None
                rows.append(
                    ArtifactLocationRow.create(
                        digest=(
                            str(artifact.digest)
                            if artifact.digest is not None
                            else None
                        ),
                        artifact_id=artifact.id,
                        site=site,
                        mount_name=mount_name,
                        mount_instance_id=mount_instance_id,
                        uri=canonical_uri,
                        member=artifact.member or "",
                        status="active",
                        witness=witness,
                        observed_at_ns=obs_ns,
                    )
                )

        if recovery_roots:
            base_ns = time.time_ns()
            for idx, root in enumerate(recovery_roots):
                m_name, m_inst, r_uri, _ = tracker.fs.archive_coordinate_for_root(
                    artifact.uri, root
                )
                rows.append(
                    ArtifactLocationRow.create(
                        digest=(
                            str(artifact.digest)
                            if artifact.digest is not None
                            else None
                        ),
                        artifact_id=artifact.id,
                        site="advisory",
                        mount_name=m_name,
                        mount_instance_id=m_inst,
                        uri=r_uri,
                        member=artifact.member or "",
                        status="active",
                        witness=None,
                        observed_at_ns=base_ns + idx,
                    )
                )

        return rows

    def sync_artifact(
        self,
        artifact: "Artifact",
        direction: str,
        *,
        profile_label: Optional[str] = None,
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> None:
        roots_tuple = tuple(recovery_roots) if recovery_roots else None
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append((artifact, direction, roots_tuple))
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                sync_fn = tracker.db.sync_artifact
                if _accepts_locations_kwarg(sync_fn):
                    locations = self._build_artifact_location_rows(
                        artifact, recovery_roots=roots_tuple
                    )
                    sync_fn(
                        artifact,
                        tracker.current_consist.run.id,
                        direction,
                        profile_label=profile_label,
                        locations=locations or None,
                    )
                else:
                    sync_fn(
                        artifact,
                        tracker.current_consist.run.id,
                        direction,
                        profile_label=profile_label,
                    )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def sync_artifact_with_facet_bundle(
        self,
        artifact: "Artifact",
        direction: str,
        *,
        facet: ArtifactFacet,
        meta_updates: Dict[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> "Artifact":
        roots_tuple = tuple(recovery_roots) if recovery_roots else None
        updated_artifact = artifact.with_meta(meta_updates, merge=True).with_facet_id(
            facet.id
        )
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append(
                (updated_artifact, direction, roots_tuple)
            )
            self._batch_pending_artifact_facet_bundles.append(
                (updated_artifact, facet, meta_updates, kv_rows)
            )
            return updated_artifact

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                sync_fn = tracker.db.sync_artifact_with_facet_bundle
                if _accepts_locations_kwarg(sync_fn):
                    locations = self._build_artifact_location_rows(
                        updated_artifact, recovery_roots=roots_tuple
                    )
                    return sync_fn(
                        artifact,
                        tracker.current_consist.run.id,
                        direction,
                        facet=facet,
                        meta_updates=meta_updates,
                        kv_rows=kv_rows,
                        locations=locations or None,
                    )
                return sync_fn(
                    artifact,
                    tracker.current_consist.run.id,
                    direction,
                    facet=facet,
                    meta_updates=meta_updates,
                    kv_rows=kv_rows,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
        return updated_artifact

    def _sync_artifacts_now(
        self,
        artifacts_with_direction: Sequence[
            tuple["Artifact", str, Optional[tuple[str, ...]]]
        ],
    ) -> None:
        tracker = self._tracker
        if not tracker.db or not tracker.current_consist:
            return

        run_id = tracker.current_consist.run.id
        artifacts_by_direction: Dict[
            str, List[tuple["Artifact", Optional[tuple[str, ...]]]]
        ] = {}
        for artifact, direction, roots_tuple in artifacts_with_direction:
            artifacts_by_direction.setdefault(direction, []).append(
                (artifact, roots_tuple)
            )

        for direction, items in artifacts_by_direction.items():
            try:
                artifacts = [art for art, _ in items]
                sync_fn = tracker.db.sync_artifacts
                if _accepts_locations_kwarg(sync_fn):
                    locations: list[ArtifactLocationRow] = []
                    for art, roots_tuple in items:
                        locations.extend(
                            self._build_artifact_location_rows(
                                art, recovery_roots=roots_tuple
                            )
                        )
                    sync_fn(
                        artifacts=artifacts,
                        run_id=run_id,
                        direction=direction,
                        locations=locations or None,
                    )
                else:
                    sync_fn(
                        artifacts=artifacts,
                        run_id=run_id,
                        direction=direction,
                    )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def _persist_artifact_facet_bundles_now(
        self,
        artifact_facet_bundles: Sequence[
            tuple["Artifact", ArtifactFacet, Dict[str, Any], Optional[List[ArtifactKV]]]
        ],
    ) -> None:
        tracker = self._tracker
        if not tracker.db:
            return

        for artifact, facet, meta_updates, kv_rows in artifact_facet_bundles:
            try:
                tracker.db.persist_artifact_facet_bundle(
                    artifact=artifact,
                    facet=facet,
                    meta_updates=meta_updates,
                    kv_rows=kv_rows,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
