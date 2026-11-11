from __future__ import annotations

from contextlib import contextmanager
import logging
import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Sequence
import uuid

from consist.core._performance_attribution import _track_begin_run_phase

if TYPE_CHECKING:
    from consist.artifact import Artifact
    from consist.core.tracker import Tracker
    from consist.models.run import ConsistRecord, RunBindingInvocation
    from consist.models.run import Run as RunModel


class ProvenanceWriter:
    """
    Persistence-side helper for run JSON snapshots and artifact ``StoreLedger`` sync.

    Keeps write-heavy provenance plumbing out of ``Tracker`` while leaving
    the tracker as the composition root and lifecycle orchestrator.
    """

    def __init__(self, tracker: Tracker) -> None:
        self._tracker = tracker
        self._artifact_batch_depth = 0
        self._batch_pending_json_flush = False
        self._batch_pending_artifacts: list[
            tuple[Artifact, str, str | None, str | None, int, bool | None]
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
        self._artifact_batch_depth += 1
        try:
            yield
        finally:
            self._artifact_batch_depth -= 1
            if self._artifact_batch_depth != 0:
                return

            pending_json_flush = self._batch_pending_json_flush
            pending_artifacts = list(self._batch_pending_artifacts)
            self._batch_pending_json_flush = False
            self._batch_pending_artifacts = []

            if pending_json_flush:
                with _track_begin_run_phase("input_binding.batch_flush_json"):
                    self._flush_json_now()
            if pending_artifacts:
                with _track_begin_run_phase("input_binding.batch_sync_artifacts"):
                    self._sync_artifacts_now(pending_artifacts)

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

    def flush_record_json(self, record: ConsistRecord) -> None:
        self._write_record_json(record)

    def _write_record_json(self, record: ConsistRecord) -> None:
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
        run: RunModel,
        *,
        binding_invocation: RunBindingInvocation | None = None,
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
        run: RunModel,
        *,
        artifact_ids: list[uuid.UUID],
        direction: str = "output",
        binding_invocation: RunBindingInvocation | None = None,
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

    def sync_artifact(
        self,
        artifact: Artifact,
        direction: str,
        *,
        slot: str | None = None,
        role: str | None = None,
        ordinal: int = 0,
        facet_index: bool | None = None,
        profile_label: Optional[str] = None,
    ) -> None:
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append(
                (artifact, direction, slot, role, ordinal, facet_index)
            )
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                tracker.db.sync_artifact(
                    artifact,
                    tracker.current_consist.run.id,
                    direction,
                    slot=slot,
                    role=role,
                    ordinal=ordinal,
                    facet_index=facet_index,
                    profile_label=profile_label,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def sync_artifact_with_facet_bundle(
        self,
        artifact: Artifact,
        direction: str,
        *,
        meta_updates: Dict[str, Any] | None = None,
        index_kv: bool | None = None,
        profile_label: Optional[str] = None,
    ) -> Artifact:
        updated = (
            artifact.with_meta(meta_updates, merge=True) if meta_updates else artifact
        )
        bound = artifact.resolve_bound_tracker()
        if bound is not None:
            updated.bind_tracker(bound)
        self.sync_artifact(
            updated,
            direction,
            facet_index=index_kv,
            profile_label=profile_label,
        )
        return updated

    def _sync_artifacts_now(
        self,
        artifacts_with_direction: Sequence[
            tuple[Artifact, str, str | None, str | None, int, bool | None]
        ],
    ) -> None:
        tracker = self._tracker
        if not tracker.db or not tracker.current_consist:
            return

        run_id = tracker.current_consist.run.id
        grouped_simple: Dict[str, List[Artifact]] = {}
        custom_entries: list[
            tuple[Artifact, str, str | None, str | None, int, bool | None]
        ] = []
        for (
            artifact,
            direction,
            slot,
            role,
            ordinal,
            facet_index,
        ) in artifacts_with_direction:
            if slot is None and role is None and ordinal == 0 and facet_index is None:
                grouped_simple.setdefault(direction, []).append(artifact)
            else:
                custom_entries.append(
                    (artifact, direction, slot, role, ordinal, facet_index)
                )

        for direction, artifacts in grouped_simple.items():
            try:
                tracker.db.sync_artifacts(
                    artifacts=artifacts,
                    run_id=run_id,
                    direction=direction,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

        for (
            artifact,
            direction,
            slot,
            role,
            ordinal,
            facet_index,
        ) in custom_entries:
            try:
                tracker.db.sync_artifact(
                    artifact,
                    run_id,
                    direction,
                    slot=slot,
                    role=role,
                    ordinal=ordinal,
                    facet_index=facet_index,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
