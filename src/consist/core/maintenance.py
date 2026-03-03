from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Literal, Optional
import re
import uuid

from sqlalchemy.engine import Engine
from sqlmodel import col, select

from consist.models.run import Run, RunArtifactLink

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

GlobalTableMode = Literal["run_scoped", "run_link", "unscoped_cache"]
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
        def _query() -> tuple[int, dict[str, int], int, int, list[str], list[str]]:
            with self.db.engine.begin() as conn:
                total_runs_row = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM run"
                ).fetchone()
                total_runs = int(total_runs_row[0] if total_runs_row else 0)

                status_rows = conn.exec_driver_sql(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM run
                    GROUP BY status
                    ORDER BY status
                    """
                ).fetchall()
                runs_by_status = {
                    str(status): int(count) for status, count in status_rows if status
                }

                total_artifacts_row = conn.exec_driver_sql(
                    "SELECT COUNT(*) FROM artifact"
                ).fetchone()
                total_artifacts = int(total_artifacts_row[0] if total_artifacts_row else 0)

                orphaned_row = conn.exec_driver_sql(
                    """
                    SELECT COUNT(*)
                    FROM artifact a
                    LEFT JOIN run_artifact_link ral
                      ON a.id = ral.artifact_id
                    WHERE ral.artifact_id IS NULL
                    """
                ).fetchone()
                orphaned_artifact_count = int(orphaned_row[0] if orphaned_row else 0)

                zombie_rows = conn.exec_driver_sql(
                    """
                    SELECT id
                    FROM run
                    WHERE status = 'running'
                      AND ended_at IS NOT NULL
                    ORDER BY id
                    """
                ).fetchall()
                zombie_run_ids = [str(row[0]) for row in zombie_rows]

                db_run_rows = conn.exec_driver_sql(
                    """
                    SELECT id
                    FROM run
                    ORDER BY id
                    """
                ).fetchall()
                db_run_ids = [str(row[0]) for row in db_run_rows]

            return (
                total_runs,
                runs_by_status,
                total_artifacts,
                orphaned_artifact_count,
                zombie_run_ids,
                db_run_ids,
            )

        (
            total_runs,
            runs_by_status,
            total_artifacts,
            orphaned_artifact_count,
            zombie_run_ids,
            db_run_ids,
        ) = self.db.execute_with_retry(_query, operation_name="maintenance_inspect")

        global_tables = self._discover_global_tables()
        global_table_sizes = self._global_table_row_counts(
            global_tables, [], engine=self.db.engine
        )

        db_file_size_mb = 0.0
        db_path_raw = getattr(self.db, "db_path", None)
        if isinstance(db_path_raw, str) and db_path_raw:
            db_path = Path(db_path_raw)
            if db_path.exists() and db_path.is_file():
                db_file_size_mb = round(db_path.stat().st_size / (1024 * 1024), 6)

        snapshot_dir = self.run_dir / "consist_runs"
        snapshot_files = sorted(snapshot_dir.glob("*.json")) if snapshot_dir.exists() else []
        snapshot_run_ids: set[str] = set()
        for snapshot_file in snapshot_files:
            try:
                payload = json.loads(snapshot_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            run_payload = payload.get("run")
            if not isinstance(run_payload, dict):
                continue
            run_id = run_payload.get("id")
            if run_id is None:
                continue
            parsed_run_id = str(run_id).strip()
            if parsed_run_id:
                snapshot_run_ids.add(parsed_run_id)

        db_run_id_set = set(db_run_ids)

        return InspectReport(
            total_runs=total_runs,
            runs_by_status=runs_by_status,
            total_artifacts=total_artifacts,
            orphaned_artifact_count=orphaned_artifact_count,
            zombie_run_ids=zombie_run_ids,
            global_table_sizes=global_table_sizes,
            db_file_size_mb=db_file_size_mb,
            json_snapshot_count=len(snapshot_files),
            json_db_parity=db_run_id_set == snapshot_run_ids,
        )

    def doctor(self) -> DoctorReport:
        def _query() -> tuple[list[str], list[str], list[str], list[uuid.UUID]]:
            with self.db.engine.begin() as conn:
                zombie_rows = conn.exec_driver_sql(
                    """
                    SELECT id
                    FROM run
                    WHERE status = 'running'
                      AND ended_at IS NOT NULL
                    ORDER BY id
                    """
                ).fetchall()
                zombie_run_ids = [str(row[0]) for row in zombie_rows]

                completed_without_end_rows = conn.exec_driver_sql(
                    """
                    SELECT id
                    FROM run
                    WHERE status IN ('completed', 'failed')
                      AND ended_at IS NULL
                    ORDER BY id
                    """
                ).fetchall()
                completed_without_end_time = [
                    str(row[0]) for row in completed_without_end_rows
                ]

                dangling_parent_rows = conn.exec_driver_sql(
                    """
                    SELECT DISTINCT child.parent_run_id
                    FROM run child
                    LEFT JOIN run parent
                      ON child.parent_run_id = parent.id
                    WHERE child.parent_run_id IS NOT NULL
                      AND parent.id IS NULL
                    ORDER BY child.parent_run_id
                    """
                ).fetchall()
                dangling_parent_run_ids = [
                    str(row[0]) for row in dangling_parent_rows if row[0]
                ]

                missing_producer_rows = conn.exec_driver_sql(
                    """
                    SELECT artifact.id
                    FROM artifact
                    LEFT JOIN run
                      ON artifact.run_id = run.id
                    WHERE artifact.run_id IS NOT NULL
                      AND run.id IS NULL
                    ORDER BY CAST(artifact.id AS VARCHAR)
                    """
                ).fetchall()
                artifacts_with_missing_producing_run: list[uuid.UUID] = []
                for row in missing_producer_rows:
                    parsed_uuid = self._coerce_uuid(row[0])
                    if parsed_uuid is not None:
                        artifacts_with_missing_producing_run.append(parsed_uuid)

            return (
                zombie_run_ids,
                completed_without_end_time,
                dangling_parent_run_ids,
                artifacts_with_missing_producing_run,
            )

        (
            zombie_run_ids,
            completed_without_end_time,
            dangling_parent_run_ids,
            artifacts_with_missing_producing_run,
        ) = self.db.execute_with_retry(_query, operation_name="maintenance_doctor")

        return DoctorReport(
            zombie_run_ids=zombie_run_ids,
            completed_without_end_time=completed_without_end_time,
            dangling_parent_run_ids=dangling_parent_run_ids,
            artifacts_with_missing_producing_run=artifacts_with_missing_producing_run,
            global_table_schema_drift={},
        )

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
        snapshot_metadata: dict = {"operation": "maintenance_snapshot"}
        if metadata:
            snapshot_metadata.update(metadata)
        return self.db.snapshot_to(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata=snapshot_metadata,
        )

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
        safe_table = self._validate_identifier(table, label="table")
        active_engine = engine or self.db.engine

        def _query() -> GlobalTableMode:
            with active_engine.begin() as conn:
                rows = conn.exec_driver_sql(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'global_tables'
                      AND table_name = ?
                    """,
                    (safe_table,),
                ).fetchall()
            column_names = {str(row[0]) for row in rows}
            if "consist_run_id" in column_names:
                return "run_scoped"
            if "run_id" in column_names:
                return "run_link"
            return "unscoped_cache"

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_classify_global_table"
        )

    def _global_table_row_counts(
        self,
        tables: Iterable[str],
        run_ids: Iterable[str] | str,
        engine: Optional[Engine] = None,
    ) -> dict[str, int]:
        safe_tables = sorted(
            {self._validate_identifier(table, label="table") for table in tables}
        )
        normalized_run_ids = self._normalize_run_ids(run_ids)
        active_engine = engine or self.db.engine

        def _query() -> dict[str, int]:
            counts: dict[str, int] = {}
            with active_engine.begin() as conn:
                for table in safe_tables:
                    quoted_table = self._quote_ident(table)
                    if not normalized_run_ids:
                        row = conn.exec_driver_sql(
                            f"SELECT COUNT(*) FROM global_tables.{quoted_table}"
                        ).fetchone()
                        counts[table] = int(row[0] if row else 0)
                        continue

                    filter_sql = self._resolve_global_table_filter_sql(
                        table,
                        normalized_run_ids,
                        table_alias="gt",
                        engine=active_engine,
                    )
                    if filter_sql is None:
                        counts[table] = 0
                        continue

                    row = conn.exec_driver_sql(
                        f"""
                        SELECT COUNT(*)
                        FROM global_tables.{quoted_table} AS "gt"
                        WHERE {filter_sql}
                        """
                    ).fetchone()
                    counts[table] = int(row[0] if row else 0)

            return {table: counts[table] for table in safe_tables}

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_global_table_row_counts"
        )

    def _resolve_global_table_filter_sql(
        self,
        table: str,
        run_ids: Iterable[str] | str,
        *,
        table_alias: str = "",
        engine: Optional[Engine] = None,
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        mode = self._classify_global_table(safe_table, engine=engine)
        if mode == "unscoped_cache":
            return None

        normalized_run_ids = self._normalize_run_ids(run_ids)
        if not normalized_run_ids:
            return "1 = 0"

        run_column = "consist_run_id" if mode == "run_scoped" else "run_id"
        quoted_column = self._quote_ident(run_column)
        if table_alias:
            safe_alias = self._validate_identifier(table_alias, label="table_alias")
            column_ref = f'{self._quote_ident(safe_alias)}.{quoted_column}'
        else:
            column_ref = quoted_column

        run_id_literals = ", ".join(
            self._quote_sql_string_literal(run_id) for run_id in normalized_run_ids
        )
        return f"{column_ref} = ANY([{run_id_literals}])"

    def _build_intersection_column_list(
        self,
        table: str,
        *,
        source_schema: str,
        target_schema: str,
        engine: Optional[Engine] = None,
    ) -> list[str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_source_schema = self._validate_identifier(
            source_schema, label="source_schema"
        )
        safe_target_schema = self._validate_identifier(
            target_schema, label="target_schema"
        )
        active_engine = engine or self.db.engine

        def _query() -> list[str]:
            with active_engine.begin() as conn:
                rows = conn.exec_driver_sql(
                    """
                    SELECT table_schema, column_name, ordinal_position
                    FROM information_schema.columns
                    WHERE table_name = ?
                      AND table_schema IN (?, ?)
                    ORDER BY table_schema, ordinal_position
                    """,
                    (safe_table, safe_source_schema, safe_target_schema),
                ).fetchall()

            source_columns: set[str] = set()
            target_columns_in_order: list[str] = []

            for schema_name, column_name, _ in rows:
                schema_value = str(schema_name)
                column_value = str(column_name)
                if schema_value == safe_source_schema:
                    source_columns.add(column_value)
                if schema_value == safe_target_schema:
                    target_columns_in_order.append(column_value)

            if not source_columns or not target_columns_in_order:
                return []

            return [
                self._quote_ident(column_name)
                for column_name in target_columns_in_order
                if column_name in source_columns
            ]

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_build_intersection_column_list"
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

    @staticmethod
    def _validate_identifier(identifier: str, *, label: str) -> str:
        value = str(identifier).strip()
        if not _SAFE_IDENTIFIER_RE.fullmatch(value):
            raise ValueError(
                f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
                "and the identifier must not start with a number."
            )
        return value

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _quote_sql_string_literal(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"
