from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Literal, Optional
import uuid

from sqlalchemy.engine import Engine
from sqlmodel import col, select

from consist.models.run import Run, RunArtifactLink

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

GlobalTableMode = Literal["run_scoped", "run_link", "unscoped_cache"]


@dataclass(slots=True)
class PurgePlan:
    """Deletion preview for a run purge request."""

    run_ids: list[str]
    child_run_ids: list[str]
    orphaned_artifact_ids: list[uuid.UUID]
    json_files: list[Path]
    disk_files: list[Path]
    ingested_data: dict[str, int]
    ingested_table_modes: dict[str, str]


@dataclass(slots=True)
class PurgeResult:
    """Outcome metadata for a purge operation."""

    plan: PurgePlan
    executed: bool
    ingested_data_skipped: bool


@dataclass(slots=True)
class ExportResult:
    """Outcome metadata for shard export operations."""

    run_ids: list[str]
    artifact_count: int
    out_path: Path
    ingested_rows: dict[str, int]
    ingested_table_modes: dict[str, str]
    unscoped_cache_tables_skipped: list[str]
    snapshots_copied: int


@dataclass(slots=True)
class MergeResult:
    """Outcome metadata for shard merge operations."""

    shard_path: Path
    runs_merged: list[str]
    runs_skipped: list[str]
    artifacts_merged: int
    ingested_tables_merged: list[str]
    unscoped_cache_tables_skipped: list[str]
    conflicts_detected: list[str]
    snapshots_merged: int


@dataclass(slots=True)
class InspectReport:
    """Read-only database health summary."""

    total_runs: int
    runs_by_status: dict[str, int]
    total_artifacts: int
    orphaned_artifact_count: int
    zombie_run_ids: list[str]
    global_table_sizes: dict[str, int]
    db_file_size_mb: float
    json_snapshot_count: int
    json_db_parity: bool


@dataclass(slots=True)
class DoctorReport:
    """Read-only invariant diagnostics for maintenance checks."""

    zombie_run_ids: list[str]
    completed_without_end_time: list[str]
    dangling_parent_run_ids: list[str]
    artifacts_with_missing_producing_run: list[uuid.UUID]
    global_table_schema_drift: dict[str, str]


@dataclass(slots=True)
class RebuildResult:
    """Result payload for JSON-to-DB rebuild operations."""

    json_files_scanned: int
    runs_inserted: int
    runs_already_present: int
    artifacts_inserted: int
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False


class DatabaseMaintenance:
    """
    Maintenance service for Consist provenance databases.

    This class intentionally avoids depending on `Tracker` so maintenance routines
    can be tested and executed in isolation using only `DatabaseManager` and a
    run directory root.
    """

    def __init__(self, db: DatabaseManager, run_dir: Path):
        """
        Initialize the maintenance service.

        Parameters
        ----------
        db
            Active database manager for the target provenance DB.
        run_dir
            Base run directory used by maintenance operations that touch filesystem
            state (snapshots, audit log, artifact files).
        """
        self.db = db
        self.run_dir = Path(run_dir)

    def inspect(self) -> InspectReport:
        raise NotImplementedError("Step 1 only: inspect() is not implemented yet.")

    def doctor(self) -> DoctorReport:
        raise NotImplementedError("Step 1 only: doctor() is not implemented yet.")

    def plan_purge(
        self, run_ids: Iterable[str] | str, *, include_children: bool
    ) -> PurgePlan:
        raise NotImplementedError("Step 1 only: plan_purge() is not implemented yet.")

    def purge(
        self,
        run_ids: Iterable[str] | str,
        *,
        include_children: bool,
        delete_files: bool,
        delete_ingested_data: bool,
        dry_run: bool,
    ) -> PurgeResult:
        raise NotImplementedError("Step 1 only: purge() is not implemented yet.")

    def fix_status(self, run_id: str, new_status: str, *, reason: Optional[str]) -> Run:
        raise NotImplementedError("Step 1 only: fix_status() is not implemented yet.")

    def export(
        self,
        run_ids: Iterable[str] | str,
        out_path: Path,
        *,
        include_data: bool,
        include_snapshots: bool,
    ) -> ExportResult:
        raise NotImplementedError("Step 1 only: export() is not implemented yet.")

    def merge(
        self, shard_path: Path, *, conflict: str, include_snapshots: bool
    ) -> MergeResult:
        raise NotImplementedError("Step 1 only: merge() is not implemented yet.")

    def compact(self) -> None:
        raise NotImplementedError("Step 1 only: compact() is not implemented yet.")

    def rebuild_from_json(self, json_dir: Path, *, dry_run: bool) -> RebuildResult:
        raise NotImplementedError(
            "Step 1 only: rebuild_from_json() is not implemented yet."
        )

    def snapshot(
        self, dest_path: Path, *, checkpoint: bool = True, metadata: Optional[dict] = None
    ) -> Path:
        raise NotImplementedError("Step 1 only: snapshot() is not implemented yet.")

    def _expand_run_ids(self, run_ids: Iterable[str] | str) -> list[str]:
        """
        Expand run IDs to include all descendant runs.

        Traversal is breadth-first and cycle-safe, preserving user-provided seed
        order and deduplicating repeated IDs.
        """
        seed_ids = self._normalize_run_ids(run_ids)
        if not seed_ids:
            return []

        with self.db.session_scope() as session:
            rows = session.exec(
                select(Run.id, Run.parent_run_id).where(col(Run.parent_run_id).is_not(None))
            ).all()

        children_by_parent: dict[str, list[str]] = {}
        for child_id, parent_id in rows:
            if parent_id is None:
                continue
            children_by_parent.setdefault(parent_id, []).append(child_id)

        for children in children_by_parent.values():
            children.sort()

        expanded: list[str] = []
        seen: set[str] = set()
        queue: deque[str] = deque(seed_ids)

        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            expanded.append(current)
            for child_id in children_by_parent.get(current, []):
                if child_id not in seen:
                    queue.append(child_id)

        return expanded

    def _find_orphaned_artifacts(
        self, run_ids: Iterable[str] | str
    ) -> list[uuid.UUID]:
        """
        Return artifact IDs linked exclusively to the selected run IDs.

        An artifact is considered orphaned by purge when it has at least one link
        and every link belongs to a run in `run_ids`.
        """
        selected_ids = set(self._normalize_run_ids(run_ids))
        if not selected_ids:
            return []

        with self.db.session_scope() as session:
            links = session.exec(
                select(RunArtifactLink.artifact_id, RunArtifactLink.run_id)
            ).all()

        linked_runs_by_artifact: dict[uuid.UUID, set[str]] = {}
        for artifact_id, run_id in links:
            parsed_id = self._coerce_uuid(artifact_id)
            if parsed_id is None:
                continue
            linked_runs_by_artifact.setdefault(parsed_id, set()).add(str(run_id))

        orphaned = [
            artifact_id
            for artifact_id, linked_runs in linked_runs_by_artifact.items()
            if linked_runs and linked_runs.issubset(selected_ids)
        ]
        orphaned.sort(key=str)
        return orphaned

    def _discover_global_tables(self, engine: Optional[Engine] = None) -> list[str]:
        """
        Discover table names under the `global_tables` schema.

        Returns names in deterministic sorted order.
        """
        active_engine = engine or self.db.engine

        def _query() -> list[str]:
            with active_engine.begin() as conn:
                rows = conn.exec_driver_sql(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'global_tables'
                    ORDER BY table_name
                    """
                ).fetchall()
            return [str(row[0]) for row in rows]

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_discover_global_tables"
        )

    def _classify_global_table(
        self, table: str, engine: Optional[Engine] = None
    ) -> GlobalTableMode:
        raise NotImplementedError(
            "Step 1 only: _classify_global_table() is not implemented yet."
        )

    def _global_table_row_counts(
        self,
        tables: Iterable[str],
        run_ids: Iterable[str] | str,
        engine: Optional[Engine] = None,
    ) -> dict[str, int]:
        raise NotImplementedError(
            "Step 1 only: _global_table_row_counts() is not implemented yet."
        )

    def _resolve_global_table_filter_sql(
        self,
        table: str,
        run_ids: Iterable[str] | str,
        *,
        table_alias: str = "",
    ) -> Optional[str]:
        raise NotImplementedError(
            "Step 1 only: _resolve_global_table_filter_sql() is not implemented yet."
        )

    def _build_intersection_column_list(
        self,
        table: str,
        *,
        source_schema: str,
        target_schema: str,
        engine: Optional[Engine] = None,
    ) -> list[str]:
        raise NotImplementedError(
            "Step 1 only: _build_intersection_column_list() is not implemented yet."
        )

    def _resolve_artifact_disk_paths(
        self, artifact_ids: Iterable[uuid.UUID]
    ) -> list[Path]:
        raise NotImplementedError(
            "Step 1 only: _resolve_artifact_disk_paths() is not implemented yet."
        )

    def _init_shard_schema(self, shard_path: Path) -> None:
        raise NotImplementedError(
            "Step 1 only: _init_shard_schema() is not implemented yet."
        )

    def _resolve_run_snapshot_path(self, run_id: str, run: Optional[Run]) -> Path:
        raise NotImplementedError(
            "Step 1 only: _resolve_run_snapshot_path() is not implemented yet."
        )

    def _log_audit(self, operation: str, summary: str) -> None:
        raise NotImplementedError("Step 1 only: _log_audit() is not implemented yet.")

    @staticmethod
    def _normalize_run_ids(run_ids: Iterable[str] | str) -> list[str]:
        """Normalize run IDs into a de-duplicated list while preserving order."""
        items = [run_ids] if isinstance(run_ids, str) else list(run_ids)
        normalized: list[str] = []
        seen: set[str] = set()
        for run_id in items:
            value = str(run_id).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    @staticmethod
    def _coerce_uuid(value: object) -> Optional[uuid.UUID]:
        """Coerce mixed UUID/string DB values into UUID objects."""
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None
