from __future__ import annotations

from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import tempfile
import time
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
)
import uuid

from consist.core._performance_attribution import _track_begin_run_phase
from consist.core.fs import build_verified_location_row
from consist.location import ArtifactLocationRow
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import ConsistRecord, RunBindingInvocation
    from consist.models.run import Run as RunModel


def safe_snapshot_run_id(run_id: str) -> str:
    return "".join(
        c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in str(run_id)
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
            tuple["Artifact", str, Optional[tuple[str, ...]], Optional[str]]
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

        safe_run_id = safe_snapshot_run_id(record.run.id)

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

    def persist_snapshot_recovery_roots(
        self,
        roots_by_run_and_artifact: Dict[str, Dict[uuid.UUID, Sequence[str]]],
    ) -> None:
        """Persist post-run recovery roots to ``consist_runs/<run>.locations.json`` when snapshot exists."""
        per_run_dir = self._tracker.fs.run_dir / "consist_runs"
        if not per_run_dir.is_dir():
            return
        for run_id, by_aid in roots_by_run_and_artifact.items():
            if not run_id or not by_aid:
                continue
            safe_id = safe_snapshot_run_id(run_id)
            snap_file = per_run_dir / f"{safe_id}.json"
            if not snap_file.is_file():
                continue
            sidecar = per_run_dir / f"{safe_id}.locations.json"
            existing: dict[str, list[str]] = {}
            if sidecar.is_file():
                try:
                    loaded = json.loads(sidecar.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict) and isinstance(
                        loaded.get("recovery_roots_by_artifact_id"), dict
                    ):
                        existing = {
                            str(k): [str(x) for x in v]
                            for k, v in loaded["recovery_roots_by_artifact_id"].items()
                            if isinstance(v, list)
                        }
                except (OSError, ValueError):
                    existing = {}
            for aid, roots in by_aid.items():
                aid_key = str(aid)
                if roots:
                    existing[aid_key] = list(roots)
                else:
                    existing.pop(aid_key, None)
            try:
                self._write_text_atomic(
                    sidecar,
                    json.dumps(
                        {"version": 1, "recovery_roots_by_artifact_id": existing},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
            except OSError:
                continue

    def _build_artifact_location_rows(
        self,
        artifact: "Artifact",
        *,
        direction: str = "output",
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> list[ArtifactLocationRow]:
        tracker = self._tracker
        db = tracker.db
        if db is not None:
            db.sync_location_epoch_caches()
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
                loc = build_verified_location_row(
                    tracker.fs, artifact, local_path=local_path
                )
                skip_unchanged = False
                if direction == "input" and db is not None:
                    coord = (
                        loc.site,
                        loc.mount_name,
                        loc.mount_instance_id,
                        loc.uri,
                        loc.member,
                    )
                    cached = db._active_coord_witness.get(coord)
                    if (
                        cached is not None
                        and cached[0] == loc.digest
                        and db._witness_matches_or_settled(cached[1], loc.witness)
                    ):
                        skip_unchanged = True
                if not skip_unchanged:
                    rows.append(loc)

        if recovery_roots:
            base_ns = time.time_ns()
            for idx, root in enumerate(recovery_roots):
                rows.append(
                    build_verified_location_row(
                        tracker.fs,
                        artifact,
                        archive_root=root,
                        site="advisory",
                        capture_local_witness=False,
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
        role: Optional[str] = None,
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> None:
        roots_tuple = tuple(recovery_roots) if recovery_roots else None
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append(
                (artifact, direction, roots_tuple, role)
            )
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                locations = self._build_artifact_location_rows(
                    artifact, direction=direction, recovery_roots=roots_tuple
                )
                extra_kwargs: Dict[str, Any] = {}
                if role is not None:
                    extra_kwargs["role"] = role
                tracker.db.sync_artifact(
                    artifact,
                    tracker.current_consist.run.id,
                    direction,
                    profile_label=profile_label,
                    locations=locations or None,
                    **extra_kwargs,
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
        role: Optional[str] = None,
        recovery_roots: Optional[Sequence[str]] = None,
    ) -> "Artifact":
        roots_tuple = tuple(recovery_roots) if recovery_roots else None
        updated_artifact = artifact.with_meta(meta_updates, merge=True).with_facet_id(
            facet.id
        )
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append(
                (updated_artifact, direction, roots_tuple, role)
            )
            self._batch_pending_artifact_facet_bundles.append(
                (updated_artifact, facet, meta_updates, kv_rows)
            )
            return updated_artifact

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                locations = self._build_artifact_location_rows(
                    updated_artifact, direction=direction, recovery_roots=roots_tuple
                )
                extra_kwargs: Dict[str, Any] = {}
                if role is not None:
                    extra_kwargs["role"] = role
                return tracker.db.sync_artifact_with_facet_bundle(
                    artifact,
                    tracker.current_consist.run.id,
                    direction,
                    facet=facet,
                    meta_updates=meta_updates,
                    kv_rows=kv_rows,
                    locations=locations or None,
                    **extra_kwargs,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
        return updated_artifact

    def _sync_artifacts_now(
        self,
        artifacts_with_direction: Sequence[
            tuple["Artifact", str, Optional[tuple[str, ...]], Optional[str]]
        ],
    ) -> None:
        tracker = self._tracker
        if not tracker.db or not tracker.current_consist:
            return

        run_id = tracker.current_consist.run.id
        artifacts_by_direction: Dict[
            str,
            List[tuple["Artifact", Optional[tuple[str, ...]], Optional[str]]],
        ] = {}
        for artifact, direction, roots_tuple, role in artifacts_with_direction:
            artifacts_by_direction.setdefault(direction, []).append(
                (artifact, roots_tuple, role)
            )

        for direction, items in artifacts_by_direction.items():
            try:
                artifacts = [art for art, _, _ in items]
                locations: list[ArtifactLocationRow] = []
                for art, roots_tuple, _ in items:
                    locations.extend(
                        self._build_artifact_location_rows(
                            art, direction=direction, recovery_roots=roots_tuple
                        )
                    )
                extra_kwargs: Dict[str, Any] = {}
                if any(role is not None for _, _, role in items):
                    extra_kwargs["link_roles"] = [role for _, _, role in items]
                tracker.db.sync_artifacts(
                    artifacts=artifacts,
                    run_id=run_id,
                    direction=direction,
                    locations=locations or None,
                    **extra_kwargs,
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
