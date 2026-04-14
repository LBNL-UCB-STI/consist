from __future__ import annotations

from contextlib import contextmanager
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Sequence, TYPE_CHECKING
import uuid

from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import ConsistRecord
    from consist.models.run import Run as RunModel


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
        self._batch_pending_artifacts: list[tuple["Artifact", str]] = []
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
                self._flush_json_now()
            if pending_artifacts:
                self._sync_artifacts_now(pending_artifacts)
            if pending_artifact_facet_bundles:
                self._persist_artifact_facet_bundles_now(pending_artifact_facet_bundles)

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

    def sync_run(self, run: "RunModel") -> None:
        tracker = self._tracker
        if tracker.db:
            try:
                tracker.db.sync_run(run)
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def sync_run_with_links(
        self,
        run: "RunModel",
        *,
        artifact_ids: list[uuid.UUID],
        direction: str = "output",
    ) -> None:
        tracker = self._tracker
        if tracker.db:
            try:
                tracker.db.sync_run_with_links(
                    run=run, artifact_ids=artifact_ids, direction=direction
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def sync_artifact(
        self,
        artifact: "Artifact",
        direction: str,
        *,
        profile_label: Optional[str] = None,
    ) -> None:
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append((artifact, direction))
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                tracker.db.sync_artifact(
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
    ) -> None:
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append((artifact, direction))
            self._batch_pending_artifact_facet_bundles.append(
                (artifact, facet, meta_updates, kv_rows)
            )
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                tracker.db.sync_artifact_with_facet_bundle(
                    artifact,
                    tracker.current_consist.run.id,
                    direction,
                    facet=facet,
                    meta_updates=meta_updates,
                    kv_rows=kv_rows,
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def _sync_artifacts_now(
        self, artifacts_with_direction: Sequence[tuple["Artifact", str]]
    ) -> None:
        tracker = self._tracker
        if not tracker.db or not tracker.current_consist:
            return

        run_id = tracker.current_consist.run.id
        artifacts_by_direction: Dict[str, List["Artifact"]] = {}
        for artifact, direction in artifacts_with_direction:
            artifacts_by_direction.setdefault(direction, []).append(artifact)

        for direction, artifacts in artifacts_by_direction.items():
            try:
                tracker.db.sync_artifacts(
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
