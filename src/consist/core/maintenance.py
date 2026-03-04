from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
import shutil
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional
import re
import uuid
from urllib.parse import unquote, urlsplit

from sqlalchemy.engine import Engine
from sqlmodel import col, select

from consist.core.facet_common import flatten_facet_values
from consist.core.identity import IdentityManager
from consist.models.artifact import Artifact
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from consist.models.run import ConsistRecord, Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

GlobalTableMode = Literal["run_scoped", "run_link", "unscoped_cache"]
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
UTC = timezone.utc


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
    incompatible_global_tables_skipped: dict[str, str] = field(default_factory=dict)


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
        seed_run_ids = self._normalize_run_ids(run_ids)
        expanded_run_ids = (
            self._expand_run_ids(seed_run_ids) if include_children else seed_run_ids
        )
        seed_run_id_set = set(seed_run_ids)
        child_run_ids = [
            run_id for run_id in expanded_run_ids if run_id not in seed_run_id_set
        ]
        orphaned_artifact_ids = self._find_orphaned_artifacts(expanded_run_ids)

        runs_by_id: dict[str, Run] = {}
        if expanded_run_ids:
            with self.db.session_scope() as session:
                runs = session.exec(
                    select(Run).where(col(Run.id).in_(expanded_run_ids))
                ).all()
            runs_by_id = {run.id: run for run in runs}

        json_files: list[Path] = []
        seen_json_paths: set[str] = set()
        for run_id in expanded_run_ids:
            snapshot_path = self._resolve_run_snapshot_path(run_id, runs_by_id.get(run_id))
            if not snapshot_path.exists():
                continue
            path_key = str(snapshot_path)
            if path_key in seen_json_paths:
                continue
            seen_json_paths.add(path_key)
            json_files.append(snapshot_path)

        discovered_tables = self._discover_global_tables()
        if expanded_run_ids:
            ingested_data = self._global_table_row_counts(
                discovered_tables,
                expanded_run_ids,
            )
        else:
            ingested_data = {table: 0 for table in discovered_tables}

        ingested_table_modes = {
            table: self._classify_global_table(table) for table in discovered_tables
        }

        return PurgePlan(
            run_ids=expanded_run_ids,
            child_run_ids=child_run_ids,
            orphaned_artifact_ids=orphaned_artifact_ids,
            json_files=json_files,
            disk_files=self._resolve_artifact_disk_paths(orphaned_artifact_ids),
            ingested_data=ingested_data,
            ingested_table_modes=ingested_table_modes,
        )

    def purge(
        self,
        run_ids: Iterable[str] | str,
        *,
        include_children: bool,
        delete_files: bool,
        delete_ingested_data: bool,
        dry_run: bool,
        prune_cache: bool = False,
    ) -> PurgeResult:
        plan = self.plan_purge(run_ids, include_children=include_children)
        ingested_candidates_exist = any(count > 0 for count in plan.ingested_data.values())
        ingested_data_skipped = ingested_candidates_exist and not delete_ingested_data

        if dry_run:
            return PurgeResult(
                plan=plan,
                executed=False,
                ingested_data_skipped=ingested_data_skipped,
            )

        selected_run_ids = plan.run_ids
        selected_orphaned_artifact_ids = [str(value) for value in plan.orphaned_artifact_ids]

        def _any_sql(values: list[str]) -> str:
            literals = ", ".join(self._quote_sql_string_literal(value) for value in values)
            return f"ANY([{literals}])"

        def _execute() -> None:
            clear_artifact_ids: list[str] = []
            preserved_links: list[tuple[str, str, str, bool]] = []
            preserved_artifact_kv_rows: list[tuple] = []
            preserved_observation_rows: list[tuple] = []
            with self.db.engine.begin() as conn:
                run_filter = (
                    f'{self._quote_ident("run_id")} = {_any_sql(selected_run_ids)}'
                    if selected_run_ids
                    else None
                )
                orphaned_artifact_filter = (
                    f'CAST({self._quote_ident("artifact_id")} AS VARCHAR) = '
                    f"{_any_sql(selected_orphaned_artifact_ids)}"
                    if selected_orphaned_artifact_ids
                    else None
                )

                observation_predicates = [
                    predicate
                    for predicate in (run_filter, orphaned_artifact_filter)
                    if predicate is not None
                ]
                if observation_predicates:
                    conn.exec_driver_sql(
                        f"""
                        DELETE FROM {self._quote_ident("artifact_schema_observation")}
                        WHERE {" OR ".join(observation_predicates)}
                        """
                    )

                if selected_run_ids:
                    conn.exec_driver_sql(
                        f"""
                        DELETE FROM {self._quote_ident("run_config_kv")}
                        WHERE {self._quote_ident("run_id")} = {_any_sql(selected_run_ids)}
                        """
                    )

                if selected_orphaned_artifact_ids:
                    conn.exec_driver_sql(
                        f"""
                        DELETE FROM {self._quote_ident("artifact_kv")}
                        WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                              = {_any_sql(selected_orphaned_artifact_ids)}
                        """
                    )

                if selected_run_ids:
                    clear_rows = conn.exec_driver_sql(
                        f"""
                        SELECT CAST({self._quote_ident("id")} AS VARCHAR) AS artifact_id
                        FROM {self._quote_ident("artifact")}
                        WHERE {self._quote_ident("run_id")} = {_any_sql(selected_run_ids)}
                          AND NOT (
                              CAST({self._quote_ident("id")} AS VARCHAR)
                              = {_any_sql(selected_orphaned_artifact_ids)}
                          )
                        ORDER BY CAST({self._quote_ident("id")} AS VARCHAR)
                        """
                        if selected_orphaned_artifact_ids
                        else f"""
                        SELECT CAST({self._quote_ident("id")} AS VARCHAR) AS artifact_id
                        FROM {self._quote_ident("artifact")}
                        WHERE {self._quote_ident("run_id")} = {_any_sql(selected_run_ids)}
                        ORDER BY CAST({self._quote_ident("id")} AS VARCHAR)
                        """
                    ).fetchall()
                    clear_artifact_ids = [str(row[0]) for row in clear_rows]

                    if clear_artifact_ids:
                        preserved_rows = conn.exec_driver_sql(
                            f"""
                            SELECT
                                {self._quote_ident("run_id")},
                                CAST({self._quote_ident("artifact_id")} AS VARCHAR),
                                {self._quote_ident("direction")},
                                {self._quote_ident("is_implicit")}
                            FROM {self._quote_ident("run_artifact_link")}
                            WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                              AND NOT (
                                  {self._quote_ident("run_id")}
                                  = {_any_sql(selected_run_ids)}
                              )
                            ORDER BY
                                {self._quote_ident("run_id")},
                                CAST({self._quote_ident("artifact_id")} AS VARCHAR),
                                {self._quote_ident("direction")}
                            """
                        ).fetchall()
                        preserved_links = [
                            (str(run_id), str(artifact_id), str(direction), bool(is_implicit))
                            for run_id, artifact_id, direction, is_implicit in preserved_rows
                        ]
                        preserved_artifact_kv_rows = conn.exec_driver_sql(
                            f"""
                            SELECT
                                CAST({self._quote_ident("artifact_id")} AS VARCHAR),
                                {self._quote_ident("facet_id")},
                                {self._quote_ident("key_path")},
                                {self._quote_ident("namespace")},
                                {self._quote_ident("value_type")},
                                {self._quote_ident("value_str")},
                                {self._quote_ident("value_num")},
                                {self._quote_ident("value_bool")},
                                {self._quote_ident("value_json")},
                                {self._quote_ident("created_at")}
                            FROM {self._quote_ident("artifact_kv")}
                            WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                            ORDER BY
                                CAST({self._quote_ident("artifact_id")} AS VARCHAR),
                                {self._quote_ident("facet_id")},
                                {self._quote_ident("key_path")}
                            """
                        ).fetchall()
                        preserved_observation_rows = conn.exec_driver_sql(
                            f"""
                            SELECT
                                CAST({self._quote_ident("id")} AS VARCHAR),
                                CAST({self._quote_ident("artifact_id")} AS VARCHAR),
                                {self._quote_ident("schema_id")},
                                {self._quote_ident("run_id")},
                                {self._quote_ident("source")},
                                {self._quote_ident("sample_rows")},
                                {self._quote_ident("observed_at")}
                            FROM {self._quote_ident("artifact_schema_observation")}
                            WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                              AND (
                                  {self._quote_ident("run_id")} IS NULL
                                  OR NOT (
                                      {self._quote_ident("run_id")}
                                      = {_any_sql(selected_run_ids)}
                                  )
                              )
                            ORDER BY CAST({self._quote_ident("id")} AS VARCHAR)
                            """
                        ).fetchall()

                    conn.exec_driver_sql(
                        f"""
                        DELETE FROM {self._quote_ident("run_artifact_link")}
                        WHERE {self._quote_ident("run_id")} = {_any_sql(selected_run_ids)}
                        """
                    )
                    if clear_artifact_ids:
                        conn.exec_driver_sql(
                            f"""
                            DELETE FROM {self._quote_ident("run_artifact_link")}
                            WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                              AND NOT (
                                  {self._quote_ident("run_id")}
                                  = {_any_sql(selected_run_ids)}
                              )
                            """
                        )
                        conn.exec_driver_sql(
                            f"""
                            DELETE FROM {self._quote_ident("artifact_kv")}
                            WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                            """
                        )
                        preserved_observation_ids = [
                            str(row[0]) for row in preserved_observation_rows
                        ]
                        if preserved_observation_ids:
                            conn.exec_driver_sql(
                                f"""
                                DELETE FROM {self._quote_ident("artifact_schema_observation")}
                                WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                      = {_any_sql(preserved_observation_ids)}
                                """
                            )

                if selected_run_ids:
                    conn.exec_driver_sql(
                        f"""
                        DELETE FROM {self._quote_ident("run")}
                        WHERE {self._quote_ident("id")} = {_any_sql(selected_run_ids)}
                        """
                    )

                if delete_ingested_data and selected_run_ids:
                    run_id_filter_values = _any_sql(selected_run_ids)
                    derivable_run_link_tables: list[str] = []
                    prunable_cache_tables: list[str] = []
                    purged_content_hashes: set[str] = set()

                    if prune_cache:
                        table_columns_by_name: dict[str, set[str]] = {}
                        for table in plan.ingested_data:
                            safe_table = self._validate_identifier(table, label="table")
                            mode = plan.ingested_table_modes.get(table)
                            if mode not in {"run_link", "unscoped_cache"}:
                                continue
                            column_rows = conn.exec_driver_sql(
                                """
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = 'global_tables'
                                  AND table_name = ?
                                """,
                                (safe_table,),
                            ).fetchall()
                            table_columns_by_name[safe_table] = {
                                str(row[0]) for row in column_rows
                            }

                            columns = table_columns_by_name[safe_table]
                            if mode == "run_link" and {
                                "run_id",
                                "content_hash",
                            }.issubset(columns):
                                derivable_run_link_tables.append(safe_table)
                            if mode == "unscoped_cache" and "content_hash" in columns:
                                prunable_cache_tables.append(safe_table)

                        if derivable_run_link_tables and prunable_cache_tables:
                            for table in derivable_run_link_tables:
                                quoted_table = self._quote_ident(table)
                                hash_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT DISTINCT CAST({self._quote_ident("content_hash")} AS VARCHAR)
                                    FROM global_tables.{quoted_table}
                                    WHERE {self._quote_ident("run_id")} = {run_id_filter_values}
                                      AND {self._quote_ident("content_hash")} IS NOT NULL
                                    """
                                ).fetchall()
                                purged_content_hashes.update(
                                    str(row[0])
                                    for row in hash_rows
                                    if row[0] is not None
                                )

                    for table in plan.ingested_data:
                        mode = plan.ingested_table_modes.get(table)
                        if mode not in {"run_scoped", "run_link"}:
                            continue
                        safe_table = self._validate_identifier(table, label="table")
                        quoted_table = self._quote_ident(safe_table)
                        run_column = (
                            self._quote_ident("consist_run_id")
                            if mode == "run_scoped"
                            else self._quote_ident("run_id")
                        )
                        conn.exec_driver_sql(
                            f"""
                            DELETE FROM global_tables.{quoted_table}
                            WHERE {run_column} = {run_id_filter_values}
                            """
                        )

                    if (
                        prune_cache
                        and derivable_run_link_tables
                        and prunable_cache_tables
                        and purged_content_hashes
                    ):
                        sorted_purged_hashes = sorted(purged_content_hashes)
                        purged_hash_filter = _any_sql(sorted_purged_hashes)
                        surviving_content_hashes: set[str] = set()
                        for table in derivable_run_link_tables:
                            quoted_table = self._quote_ident(table)
                            surviving_hash_rows = conn.exec_driver_sql(
                                f"""
                                SELECT DISTINCT CAST({self._quote_ident("content_hash")} AS VARCHAR)
                                FROM global_tables.{quoted_table}
                                WHERE CAST({self._quote_ident("content_hash")} AS VARCHAR)
                                      = {purged_hash_filter}
                                  AND {self._quote_ident("content_hash")} IS NOT NULL
                                """
                            ).fetchall()
                            surviving_content_hashes.update(
                                str(row[0])
                                for row in surviving_hash_rows
                                if row[0] is not None
                            )

                        unreferenced_hashes = sorted(
                            purged_content_hashes - surviving_content_hashes
                        )
                        if unreferenced_hashes:
                            unreferenced_hash_filter = _any_sql(unreferenced_hashes)
                            for table in prunable_cache_tables:
                                quoted_table = self._quote_ident(table)
                                conn.exec_driver_sql(
                                    f"""
                                    DELETE FROM global_tables.{quoted_table}
                                    WHERE CAST({self._quote_ident("content_hash")} AS VARCHAR)
                                          = {unreferenced_hash_filter}
                                    """
                                )

            if clear_artifact_ids or selected_orphaned_artifact_ids:
                with self.db.engine.begin() as conn:
                    if selected_orphaned_artifact_ids:
                        conn.exec_driver_sql(
                            f"""
                            DELETE FROM {self._quote_ident("artifact")}
                            WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                  = {_any_sql(selected_orphaned_artifact_ids)}
                            """
                        )
                    if clear_artifact_ids:
                        conn.exec_driver_sql(
                            f"""
                            UPDATE {self._quote_ident("artifact")}
                            SET {self._quote_ident("run_id")} = NULL
                            WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                  = {_any_sql(clear_artifact_ids)}
                            """
                        )
                    for (
                        preserved_run_id,
                        preserved_artifact_id,
                        preserved_direction,
                        preserved_is_implicit,
                    ) in preserved_links:
                        conn.exec_driver_sql(
                            f"""
                            INSERT INTO {self._quote_ident("run_artifact_link")}
                            (
                                {self._quote_ident("run_id")},
                                {self._quote_ident("artifact_id")},
                                {self._quote_ident("direction")},
                                {self._quote_ident("is_implicit")}
                            )
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                preserved_run_id,
                                preserved_artifact_id,
                                preserved_direction,
                                preserved_is_implicit,
                            ),
                        )
                    for (
                        kv_artifact_id,
                        kv_facet_id,
                        kv_key_path,
                        kv_namespace,
                        kv_value_type,
                        kv_value_str,
                        kv_value_num,
                        kv_value_bool,
                        kv_value_json,
                        kv_created_at,
                    ) in preserved_artifact_kv_rows:
                        conn.exec_driver_sql(
                            f"""
                            INSERT INTO {self._quote_ident("artifact_kv")}
                            (
                                {self._quote_ident("artifact_id")},
                                {self._quote_ident("facet_id")},
                                {self._quote_ident("key_path")},
                                {self._quote_ident("namespace")},
                                {self._quote_ident("value_type")},
                                {self._quote_ident("value_str")},
                                {self._quote_ident("value_num")},
                                {self._quote_ident("value_bool")},
                                {self._quote_ident("value_json")},
                                {self._quote_ident("created_at")}
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                kv_artifact_id,
                                kv_facet_id,
                                kv_key_path,
                                kv_namespace,
                                kv_value_type,
                                kv_value_str,
                                kv_value_num,
                                kv_value_bool,
                                kv_value_json,
                                kv_created_at,
                            ),
                        )
                    for (
                        obs_id,
                        obs_artifact_id,
                        obs_schema_id,
                        obs_run_id,
                        obs_source,
                        obs_sample_rows,
                        obs_observed_at,
                    ) in preserved_observation_rows:
                        conn.exec_driver_sql(
                            f"""
                            INSERT INTO {self._quote_ident("artifact_schema_observation")}
                            (
                                {self._quote_ident("id")},
                                {self._quote_ident("artifact_id")},
                                {self._quote_ident("schema_id")},
                                {self._quote_ident("run_id")},
                                {self._quote_ident("source")},
                                {self._quote_ident("sample_rows")},
                                {self._quote_ident("observed_at")}
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                obs_id,
                                obs_artifact_id,
                                obs_schema_id,
                                obs_run_id,
                                obs_source,
                                obs_sample_rows,
                                obs_observed_at,
                            ),
                        )

        self.db.execute_with_retry(_execute, operation_name="maintenance_purge")

        for json_path in plan.json_files:
            try:
                json_path.unlink(missing_ok=True)
            except OSError:
                continue

        if delete_files:
            for disk_path in plan.disk_files:
                try:
                    if not disk_path.exists() and not disk_path.is_symlink():
                        continue
                    if disk_path.is_dir() and not disk_path.is_symlink():
                        shutil.rmtree(disk_path, ignore_errors=True)
                    else:
                        disk_path.unlink(missing_ok=True)
                except OSError:
                    continue

        self._log_audit(
            "purge",
            (
                f"runs={len(plan.run_ids)} "
                f"children={len(plan.child_run_ids)} "
                f"orphaned_artifacts={len(plan.orphaned_artifact_ids)} "
                f"json_files={len(plan.json_files)} "
                f"delete_ingested_data={delete_ingested_data} "
                f"prune_cache={prune_cache} "
                f"delete_files={delete_files}"
            ),
        )

        return PurgeResult(
            plan=plan,
            executed=True,
            ingested_data_skipped=ingested_data_skipped,
        )

    def fix_status(
        self,
        run_id: str,
        new_status: str,
        *,
        reason: Optional[str],
        force: bool = False,
    ) -> Run:
        normalized_run_id = str(run_id).strip()
        normalized_status = str(new_status).strip().lower()
        allowed_statuses = {"running", "completed", "failed"}
        if normalized_status not in allowed_statuses:
            allowed_values = ", ".join(sorted(allowed_statuses))
            raise ValueError(
                f"Invalid status {new_status!r}. Expected one of: {allowed_values}."
            )

        now = datetime.now(UTC)

        def _update() -> Run:
            with self.db.session_scope() as session:
                run = session.get(Run, normalized_run_id)
                if run is None:
                    raise ValueError(f"Run not found: {normalized_run_id}")

                previous_status = str(run.status).strip().lower()
                if (
                    normalized_status == "running"
                    and previous_status in {"completed", "failed"}
                    and not force
                ):
                    raise ValueError(
                        "Use --force to transition a terminal run back to running."
                    )

                run.status = normalized_status
                run.updated_at = now
                if normalized_status in {"completed", "failed"}:
                    if run.ended_at is None:
                        run.ended_at = now
                else:
                    run.ended_at = None

                if reason is not None:
                    run_meta = dict(run.meta) if isinstance(run.meta, dict) else {}
                    run_meta["status_fix_reason"] = reason
                    run.meta = run_meta

                session.add(run)
                session.commit()
                session.refresh(run)
                return run

        updated_run = self.db.execute_with_retry(
            _update, operation_name="maintenance_fix_status"
        )
        self._log_audit(
            "fix_status",
            (
                f"run_id={updated_run.id} "
                f"status={updated_run.status} "
                f"reason={reason or ''}"
            ),
        )
        return updated_run

    def export(
        self,
        run_ids: Iterable[str] | str,
        out_path: Path,
        *,
        include_data: bool,
        include_snapshots: bool,
        include_children: bool = True,
        dry_run: bool = False,
    ) -> ExportResult:
        normalized_run_ids = self._normalize_run_ids(run_ids)
        expanded_run_ids = (
            self._expand_run_ids(normalized_run_ids)
            if include_children
            else normalized_run_ids
        )
        out_path = Path(out_path)

        runs_by_id: dict[str, Run] = {}
        if expanded_run_ids:
            with self.db.session_scope() as session:
                runs = session.exec(
                    select(Run).where(col(Run.id).in_(expanded_run_ids))
                ).all()
            runs_by_id = {run.id: run for run in runs}
        selected_run_ids = [run_id for run_id in expanded_run_ids if run_id in runs_by_id]

        discovered_tables = self._discover_global_tables()
        ingested_table_modes = {
            table: self._classify_global_table(table) for table in discovered_tables
        }
        ingested_rows: dict[str, int] = {}
        unscoped_cache_tables_skipped: list[str] = []
        artifact_count = 0

        if dry_run:
            if selected_run_ids:
                with self.db.engine.begin() as conn:
                    artifact_rows = conn.exec_driver_sql(
                        f"""
                        SELECT DISTINCT CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                        FROM {self._qualified_table_sql("run_artifact_link")}
                        WHERE {self._quote_ident("run_id")} = ANY(
                            [{", ".join(self._quote_sql_string_literal(value) for value in selected_run_ids)}]
                        )
                        ORDER BY CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                        """
                    ).fetchall()
                artifact_count = len(artifact_rows)

            if include_data and selected_run_ids:
                candidate_rows = self._global_table_row_counts(
                    discovered_tables,
                    selected_run_ids,
                )
                for table in discovered_tables:
                    mode = ingested_table_modes.get(table)
                    if mode == "unscoped_cache":
                        unscoped_cache_tables_skipped.append(table)
                        continue
                    ingested_rows[table] = candidate_rows.get(table, 0)

            return ExportResult(
                run_ids=selected_run_ids,
                artifact_count=artifact_count,
                out_path=out_path,
                ingested_rows=ingested_rows,
                ingested_table_modes=ingested_table_modes,
                unscoped_cache_tables_skipped=unscoped_cache_tables_skipped,
                snapshots_copied=0,
            )

        self._init_shard_schema(out_path)

        def _any_sql(values: list[str]) -> str:
            literals = ", ".join(self._quote_sql_string_literal(value) for value in values)
            return f"ANY([{literals}])"

        def _write() -> None:
            nonlocal artifact_count, ingested_rows, unscoped_cache_tables_skipped
            alias = "shard_export"
            with self.db.engine.begin() as conn:
                conn.exec_driver_sql(
                    f"ATTACH {self._quote_sql_string_literal(str(out_path))} AS {self._quote_ident(alias)}"
                )
                try:
                    conn.exec_driver_sql(
                        f"CREATE SCHEMA IF NOT EXISTS {self._quote_ident(alias)}.global_tables"
                    )
                    source_schema = "main"
                    target_schema = "main"

                    def _core_columns(table_name: str) -> list[str]:
                        return self._build_table_intersection_columns(
                            table_name,
                            source_schema=source_schema,
                            source_catalog=None,
                            target_schema=target_schema,
                            target_catalog=alias,
                            engine=self.db.engine,
                        )

                    if selected_run_ids:
                        run_id_filter = _any_sql(selected_run_ids)
                        run_columns = _core_columns("run")
                        run_source_columns = set(
                            self._list_table_columns(
                                "run",
                                schema=source_schema,
                                engine=self.db.engine,
                            )
                        )
                        if run_columns and "id" in run_source_columns:
                            run_column_sql = ", ".join(run_columns)
                            conn.exec_driver_sql(
                                f"""
                                INSERT INTO {self._qualified_table_sql("run", catalog=alias)} ({run_column_sql})
                                SELECT {run_column_sql}
                                FROM {self._qualified_table_sql("run")}
                                WHERE {self._quote_ident("id")} = {run_id_filter}
                                """
                            )

                        run_artifact_link_columns = _core_columns("run_artifact_link")
                        run_artifact_link_source_columns = self._list_table_columns(
                            "run_artifact_link",
                            schema=source_schema,
                            engine=self.db.engine,
                        )
                        run_artifact_link_source_set = set(run_artifact_link_source_columns)
                        if (
                            run_artifact_link_columns
                            and "run_id" in run_artifact_link_source_set
                        ):
                            run_artifact_link_column_sql = ", ".join(run_artifact_link_columns)
                            conn.exec_driver_sql(
                                f"""
                                INSERT INTO {self._qualified_table_sql("run_artifact_link", catalog=alias)} ({run_artifact_link_column_sql})
                                SELECT {run_artifact_link_column_sql}
                                FROM {self._qualified_table_sql("run_artifact_link")}
                                WHERE {self._quote_ident("run_id")} = {run_id_filter}
                                """
                            )

                        if {
                            "run_id",
                            "artifact_id",
                        }.issubset(run_artifact_link_source_set):
                            artifact_rows = conn.exec_driver_sql(
                                f"""
                                SELECT DISTINCT CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                FROM {self._qualified_table_sql("run_artifact_link")}
                                WHERE {self._quote_ident("run_id")} = {run_id_filter}
                                ORDER BY CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                """
                            ).fetchall()
                        else:
                            artifact_rows = []
                        artifact_ids = [str(row[0]) for row in artifact_rows]
                        artifact_count = len(artifact_ids)

                        run_config_kv_columns = _core_columns("run_config_kv")
                        run_config_source_columns = set(
                            self._list_table_columns(
                                "run_config_kv",
                                schema=source_schema,
                                engine=self.db.engine,
                            )
                        )
                        if run_config_kv_columns and "run_id" in run_config_source_columns:
                            run_config_column_sql = ", ".join(run_config_kv_columns)
                            conn.exec_driver_sql(
                                f"""
                                INSERT INTO {self._qualified_table_sql("run_config_kv", catalog=alias)} ({run_config_column_sql})
                                SELECT {run_config_column_sql}
                                FROM {self._qualified_table_sql("run_config_kv")}
                                WHERE {self._quote_ident("run_id")} = {run_id_filter}
                                """
                            )

                        observation_columns = _core_columns("artifact_schema_observation")
                        observation_source_columns = set(
                            self._list_table_columns(
                                "artifact_schema_observation",
                                schema=source_schema,
                                engine=self.db.engine,
                            )
                        )
                        observation_filters: list[str] = []
                        if "run_id" in observation_source_columns:
                            observation_filters.append(
                                f'{self._quote_ident("run_id")} = {run_id_filter}'
                            )
                        if artifact_ids and "artifact_id" in observation_source_columns:
                            observation_filters.append(
                                f'CAST({self._quote_ident("artifact_id")} AS VARCHAR)'
                                f" = {_any_sql(artifact_ids)}"
                            )
                        observation_where = (
                            " OR ".join(observation_filters) if observation_filters else None
                        )

                        if artifact_ids:
                            artifact_columns = _core_columns("artifact")
                            artifact_source_columns = set(
                                self._list_table_columns(
                                    "artifact",
                                    schema=source_schema,
                                    engine=self.db.engine,
                                )
                            )
                            if artifact_columns and "id" in artifact_source_columns:
                                artifact_column_sql = ", ".join(artifact_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("artifact", catalog=alias)} ({artifact_column_sql})
                                    SELECT {artifact_column_sql}
                                    FROM {self._qualified_table_sql("artifact")}
                                    WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                          = {_any_sql(artifact_ids)}
                                    """
                                )

                            artifact_kv_columns = _core_columns("artifact_kv")
                            artifact_kv_source_columns = set(
                                self._list_table_columns(
                                    "artifact_kv",
                                    schema=source_schema,
                                    engine=self.db.engine,
                                )
                            )
                            if artifact_kv_columns and "artifact_id" in artifact_kv_source_columns:
                                artifact_kv_column_sql = ", ".join(artifact_kv_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("artifact_kv", catalog=alias)} ({artifact_kv_column_sql})
                                    SELECT {artifact_kv_column_sql}
                                    FROM {self._qualified_table_sql("artifact_kv")}
                                    WHERE CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                          = {_any_sql(artifact_ids)}
                                    """
                                )

                        if (
                            observation_columns
                            and observation_where
                            and not (
                                "artifact_id" in observation_source_columns and not artifact_ids
                            )
                        ):
                            schemas_ready = True
                            if "schema_id" in observation_source_columns:
                                schema_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT DISTINCT CAST({self._quote_ident("schema_id")} AS VARCHAR)
                                    FROM {self._qualified_table_sql("artifact_schema_observation")}
                                    WHERE ({observation_where})
                                      AND {self._quote_ident("schema_id")} IS NOT NULL
                                    ORDER BY CAST({self._quote_ident("schema_id")} AS VARCHAR)
                                    """
                                ).fetchall()
                                schema_ids = [str(row[0]) for row in schema_rows]
                                if schema_ids:
                                    artifact_schema_columns = _core_columns(
                                        "artifact_schema"
                                    )
                                    artifact_schema_source_columns = set(
                                        self._list_table_columns(
                                            "artifact_schema",
                                            schema=source_schema,
                                            engine=self.db.engine,
                                        )
                                    )
                                    if artifact_schema_columns and "id" in (
                                        artifact_schema_source_columns
                                    ):
                                        artifact_schema_column_sql = ", ".join(
                                            artifact_schema_columns
                                        )
                                        conn.exec_driver_sql(
                                            f"""
                                            INSERT INTO {self._qualified_table_sql("artifact_schema", catalog=alias)} ({artifact_schema_column_sql})
                                            SELECT {artifact_schema_column_sql}
                                            FROM {self._qualified_table_sql("artifact_schema")}
                                            WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                                  = {_any_sql(schema_ids)}
                                            """
                                        )
                                    else:
                                        schemas_ready = False

                            if schemas_ready:
                                observation_column_sql = ", ".join(observation_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} ({observation_column_sql})
                                    SELECT {observation_column_sql}
                                    FROM {self._qualified_table_sql("artifact_schema_observation")}
                                    WHERE {observation_where}
                                    """
                                )
                    else:
                        artifact_count = 0

                    if include_data and selected_run_ids:
                        for table in discovered_tables:
                            mode = ingested_table_modes.get(table)
                            if mode == "unscoped_cache":
                                unscoped_cache_tables_skipped.append(table)
                                continue
                            safe_table = self._validate_identifier(table, label="table")
                            quoted_table = self._quote_ident(safe_table)
                            filter_sql = self._resolve_global_table_filter_sql(
                                safe_table,
                                selected_run_ids,
                                table_alias="src_gt",
                            )
                            if filter_sql is None:
                                continue
                            conn.exec_driver_sql(
                                f"""
                                CREATE TABLE IF NOT EXISTS
                                    {self._quote_ident(alias)}.global_tables.{quoted_table}
                                AS
                                SELECT *
                                FROM global_tables.{quoted_table}
                                WHERE 1 = 0
                                """
                            )
                            count_row = conn.exec_driver_sql(
                                f"""
                                SELECT COUNT(*)
                                FROM global_tables.{quoted_table} AS "src_gt"
                                WHERE {filter_sql}
                                """
                            ).fetchone()
                            row_count = int(count_row[0] if count_row else 0)
                            ingested_rows[safe_table] = row_count
                            if row_count <= 0:
                                continue
                            conn.exec_driver_sql(
                                f"""
                                INSERT INTO {self._quote_ident(alias)}.global_tables.{quoted_table}
                                SELECT *
                                FROM global_tables.{quoted_table} AS "src_gt"
                                WHERE {filter_sql}
                                """
                            )
                finally:
                    conn.exec_driver_sql(f"DETACH {self._quote_ident(alias)}")

        self.db.execute_with_retry(_write, operation_name="maintenance_export")

        snapshots_copied = 0
        if include_snapshots:
            shard_snapshot_dir = out_path.parent / "shard_snapshots"
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            for run_id in selected_run_ids:
                source_snapshot = self._resolve_run_snapshot_path(run_id, runs_by_id.get(run_id))
                if not source_snapshot.exists():
                    continue
                destination_snapshot = shard_snapshot_dir / source_snapshot.name
                shutil.copy2(source_snapshot, destination_snapshot)
                snapshots_copied += 1

        self._log_audit(
            "export",
            (
                f"out_path={out_path} "
                f"runs={len(selected_run_ids)} "
                f"artifacts={artifact_count} "
                f"include_data={include_data} "
                f"include_snapshots={include_snapshots}"
            ),
        )

        return ExportResult(
            run_ids=selected_run_ids,
            artifact_count=artifact_count,
            out_path=out_path,
            ingested_rows=ingested_rows,
            ingested_table_modes=ingested_table_modes,
            unscoped_cache_tables_skipped=unscoped_cache_tables_skipped,
            snapshots_copied=snapshots_copied,
        )

    def merge(
        self,
        shard_path: Path,
        *,
        conflict: str,
        include_snapshots: bool,
        dry_run: bool = False,
    ) -> MergeResult:
        shard_path = Path(shard_path)
        if not shard_path.exists():
            raise ValueError(f"Shard database does not exist: {shard_path}")

        conflict_mode = str(conflict).strip().lower()
        if conflict_mode not in {"error", "skip"}:
            raise ValueError("conflict must be one of: error, skip")

        shard_db = self.db.__class__(str(shard_path))
        try:
            shard_run_columns = self._list_table_columns("run", engine=shard_db.engine)
            if shard_run_columns and "id" in shard_run_columns:
                with shard_db.engine.begin() as conn:
                    shard_run_rows = conn.exec_driver_sql(
                        f"""
                        SELECT {self._quote_ident("id")}
                        FROM {self._quote_ident("run")}
                        ORDER BY {self._quote_ident("id")}
                        """
                    ).fetchall()
                shard_run_ids = [str(row[0]) for row in shard_run_rows]
            else:
                shard_run_ids = []
            shard_global_tables = self._discover_global_tables(engine=shard_db.engine)
            shard_table_modes = {
                table: self._classify_global_table(table, engine=shard_db.engine)
                for table in shard_global_tables
            }

            with self.db.session_scope() as session:
                existing_rows = session.exec(
                    select(Run.id).where(col(Run.id).in_(shard_run_ids))
                ).all()
            existing_run_ids = {str(value) for value in existing_rows}
            conflicts_detected = sorted(existing_run_ids.intersection(shard_run_ids))
            conflict_set = set(conflicts_detected)

            if conflicts_detected and conflict_mode == "error":
                raise ValueError(
                    "Run ID conflicts detected: " + ", ".join(conflicts_detected)
                )

            if conflict_mode == "skip":
                runs_skipped = conflicts_detected
                runs_to_merge = [
                    run_id for run_id in shard_run_ids if run_id not in conflict_set
                ]
            else:
                runs_skipped = []
                runs_to_merge = list(shard_run_ids)

            incompatible_global_tables_skipped: dict[str, str] = {}
            if runs_to_merge:
                compatibility_issues: dict[str, str] = {}
                for table, mode in shard_table_modes.items():
                    if mode == "unscoped_cache":
                        continue
                    compatibility_issue = self._global_table_merge_compatibility_issue(
                        table,
                        mode=mode,
                        source_engine=shard_db.engine,
                        target_engine=self.db.engine,
                    )
                    if compatibility_issue:
                        compatibility_issues[table] = compatibility_issue

                if compatibility_issues:
                    incompatible_global_tables_skipped = {
                        table: compatibility_issues[table]
                        for table in sorted(compatibility_issues)
                    }
                    if conflict_mode == "error":
                        details = "; ".join(
                            (
                                f"{table}: {reason}"
                                for table, reason in incompatible_global_tables_skipped.items()
                            )
                        )
                        raise ValueError(
                            "Global table schema compatibility check failed: " + details
                        )

            shard_table_filters: dict[str, Optional[str]] = {}
            if runs_to_merge:
                for table, mode in shard_table_modes.items():
                    if mode == "unscoped_cache":
                        continue
                    if table in incompatible_global_tables_skipped:
                        continue
                    shard_table_filters[table] = self._resolve_global_table_filter_sql(
                        table,
                        runs_to_merge,
                        table_alias="src_gt",
                        engine=shard_db.engine,
                    )

            artifacts_merged = 0
            ingested_tables_merged: list[str] = []
            unscoped_cache_tables_skipped: list[str] = []

            def _any_sql(values: list[str]) -> str:
                literals = ", ".join(
                    self._quote_sql_string_literal(value) for value in values
                )
                return f"ANY([{literals}])"

            if dry_run:
                if runs_to_merge:
                    shard_link_columns = self._list_table_columns(
                        "run_artifact_link",
                        engine=shard_db.engine,
                    )
                    if {"run_id", "artifact_id"}.issubset(set(shard_link_columns)):
                        with shard_db.engine.begin() as conn:
                            artifact_rows = conn.exec_driver_sql(
                                f"""
                                SELECT DISTINCT CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                FROM {self._qualified_table_sql("run_artifact_link")}
                                WHERE {self._quote_ident("run_id")} = {_any_sql(runs_to_merge)}
                                ORDER BY CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                """
                            ).fetchall()
                        artifact_ids = [str(row[0]) for row in artifact_rows]
                    else:
                        artifact_ids = []

                    if artifact_ids:
                        with self.db.engine.begin() as conn:
                            existing_artifact_rows = conn.exec_driver_sql(
                                f"""
                                SELECT DISTINCT CAST({self._quote_ident("id")} AS VARCHAR)
                                FROM {self._qualified_table_sql("artifact")}
                                WHERE CAST({self._quote_ident("id")} AS VARCHAR)
                                      = {_any_sql(artifact_ids)}
                                """
                            ).fetchall()
                        existing_artifacts = {str(row[0]) for row in existing_artifact_rows}
                        artifacts_merged = len(
                            [value for value in artifact_ids if value not in existing_artifacts]
                        )
                    else:
                        artifacts_merged = 0

                    for table in shard_global_tables:
                        mode = shard_table_modes.get(table)
                        if mode == "unscoped_cache":
                            unscoped_cache_tables_skipped.append(table)
                            continue
                        if table in incompatible_global_tables_skipped:
                            continue
                        filter_sql = shard_table_filters.get(table)
                        if filter_sql is None:
                            continue
                        quoted_table = self._quote_ident(
                            self._validate_identifier(table, label="table")
                        )
                        with shard_db.engine.begin() as conn:
                            row = conn.exec_driver_sql(
                                f"""
                                SELECT COUNT(*)
                                FROM global_tables.{quoted_table} AS "src_gt"
                                WHERE {filter_sql}
                                """
                            ).fetchone()
                        candidate_rows = int(row[0] if row else 0)
                        if candidate_rows > 0:
                            ingested_tables_merged.append(table)
                else:
                    artifacts_merged = 0

                snapshots_merged = 0
                if include_snapshots and runs_to_merge:
                    source_snapshot_dir = shard_path.parent / "shard_snapshots"
                    for run_id in runs_to_merge:
                        source_name = self._resolve_run_snapshot_path(run_id, None).name
                        source_snapshot = source_snapshot_dir / source_name
                        if not source_snapshot.exists():
                            continue
                        destination_snapshot = self._resolve_run_snapshot_path(run_id, None)
                        if destination_snapshot.exists():
                            continue
                        snapshots_merged += 1

                return MergeResult(
                    shard_path=shard_path,
                    runs_merged=runs_to_merge,
                    runs_skipped=runs_skipped,
                    artifacts_merged=artifacts_merged,
                    ingested_tables_merged=sorted(set(ingested_tables_merged)),
                    unscoped_cache_tables_skipped=sorted(
                        set(unscoped_cache_tables_skipped)
                    ),
                    conflicts_detected=conflicts_detected,
                    snapshots_merged=snapshots_merged,
                    incompatible_global_tables_skipped=incompatible_global_tables_skipped,
                )

            def _write() -> None:
                nonlocal artifacts_merged, ingested_tables_merged, unscoped_cache_tables_skipped
                alias = "shard_merge"
                merge_source_schema = "merge_source"
                with self.db.engine.begin() as conn:
                    conn.exec_driver_sql(
                        f"ATTACH {self._quote_sql_string_literal(str(shard_path))} AS {self._quote_ident(alias)}"
                    )
                    try:
                        conn.exec_driver_sql(
                            f"""
                            CREATE SCHEMA IF NOT EXISTS {self._quote_ident(merge_source_schema)}
                            """
                        )
                        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
                        if runs_to_merge:
                            run_filter = _any_sql(runs_to_merge)
                            source_schema = "main"
                            target_schema = "main"

                            def _core_columns(table_name: str) -> list[str]:
                                return self._build_table_intersection_columns(
                                    table_name,
                                    source_schema=source_schema,
                                    source_catalog=alias,
                                    target_schema=target_schema,
                                    target_catalog=None,
                                    engine=self.db.engine,
                                )

                            run_columns = _core_columns("run")
                            run_source_columns = self._list_table_columns(
                                "run",
                                schema=source_schema,
                                catalog=alias,
                                engine=self.db.engine,
                            )
                            run_target_columns = self._list_table_columns(
                                "run",
                                schema=target_schema,
                                engine=self.db.engine,
                            )
                            if run_columns and {"id"}.issubset(
                                set(run_source_columns).intersection(run_target_columns)
                            ):
                                run_column_sql = ", ".join(run_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("run")} ({run_column_sql})
                                    SELECT {run_column_sql}
                                    FROM {self._qualified_table_sql("run", catalog=alias)} AS "src_run"
                                    WHERE "src_run".{self._quote_ident("id")} = {run_filter}
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM {self._qualified_table_sql("run")} AS "dst_run"
                                          WHERE "dst_run".{self._quote_ident("id")}
                                                = "src_run".{self._quote_ident("id")}
                                      )
                                    """
                                )

                            run_artifact_link_columns = _core_columns("run_artifact_link")
                            run_artifact_link_source_columns = self._list_table_columns(
                                "run_artifact_link",
                                schema=source_schema,
                                catalog=alias,
                                engine=self.db.engine,
                            )
                            run_artifact_link_target_columns = self._list_table_columns(
                                "run_artifact_link",
                                schema=target_schema,
                                engine=self.db.engine,
                            )
                            artifact_rows = []
                            if {"run_id", "artifact_id"}.issubset(
                                set(run_artifact_link_source_columns)
                            ):
                                artifact_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT DISTINCT
                                        CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                    FROM {self._qualified_table_sql("run_artifact_link", catalog=alias)}
                                    WHERE {self._quote_ident("run_id")} = {run_filter}
                                    ORDER BY CAST({self._quote_ident("artifact_id")} AS VARCHAR)
                                    """
                                ).fetchall()

                            if run_artifact_link_columns and {"run_id", "artifact_id"}.issubset(
                                set(run_artifact_link_source_columns).intersection(
                                    run_artifact_link_target_columns
                                )
                            ):
                                run_artifact_link_column_sql = ", ".join(
                                    run_artifact_link_columns
                                )
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("run_artifact_link")} ({run_artifact_link_column_sql})
                                    SELECT {run_artifact_link_column_sql}
                                    FROM {self._qualified_table_sql("run_artifact_link", catalog=alias)} AS "src_link"
                                    WHERE "src_link".{self._quote_ident("run_id")} = {run_filter}
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM {self._qualified_table_sql("run_artifact_link")} AS "dst_link"
                                          WHERE "dst_link".{self._quote_ident("run_id")}
                                                    = "src_link".{self._quote_ident("run_id")}
                                            AND CAST("dst_link".{self._quote_ident("artifact_id")} AS VARCHAR)
                                                    = CAST("src_link".{self._quote_ident("artifact_id")} AS VARCHAR)
                                      )
                                    """
                                )

                            artifact_ids = [str(row[0]) for row in artifact_rows]
                            if artifact_ids:
                                artifact_columns = _core_columns("artifact")
                                artifact_source_columns = self._list_table_columns(
                                    "artifact",
                                    schema=source_schema,
                                    catalog=alias,
                                    engine=self.db.engine,
                                )
                                artifact_target_columns = self._list_table_columns(
                                    "artifact",
                                    schema=target_schema,
                                    engine=self.db.engine,
                                )
                                new_artifact_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT COUNT(*)
                                    FROM {self._qualified_table_sql("artifact", catalog=alias)} AS "src_art"
                                    WHERE CAST("src_art".{self._quote_ident("id")} AS VARCHAR)
                                          = {_any_sql(artifact_ids)}
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM {self._qualified_table_sql("artifact")} AS "dst_art"
                                          WHERE CAST("dst_art".{self._quote_ident("id")} AS VARCHAR)
                                                = CAST("src_art".{self._quote_ident("id")} AS VARCHAR)
                                      )
                                    """
                                ).fetchone()
                                artifacts_merged = int(
                                    new_artifact_rows[0] if new_artifact_rows else 0
                                )
                                if artifact_columns and {"id"}.issubset(
                                    set(artifact_source_columns).intersection(
                                        artifact_target_columns
                                    )
                                ):
                                    artifact_column_sql = ", ".join(artifact_columns)
                                    conn.exec_driver_sql(
                                        f"""
                                        INSERT INTO {self._qualified_table_sql("artifact")} ({artifact_column_sql})
                                        SELECT {artifact_column_sql}
                                        FROM {self._qualified_table_sql("artifact", catalog=alias)} AS "src_art"
                                        WHERE CAST("src_art".{self._quote_ident("id")} AS VARCHAR)
                                              = {_any_sql(artifact_ids)}
                                          AND NOT EXISTS (
                                              SELECT 1
                                              FROM {self._qualified_table_sql("artifact")} AS "dst_art"
                                              WHERE CAST("dst_art".{self._quote_ident("id")} AS VARCHAR)
                                                    = CAST("src_art".{self._quote_ident("id")} AS VARCHAR)
                                          )
                                        """
                                    )

                            run_config_kv_columns = _core_columns("run_config_kv")
                            run_config_source_columns = self._list_table_columns(
                                "run_config_kv",
                                schema=source_schema,
                                catalog=alias,
                                engine=self.db.engine,
                            )
                            run_config_target_columns = self._list_table_columns(
                                "run_config_kv",
                                schema=target_schema,
                                engine=self.db.engine,
                            )
                            if run_config_kv_columns and {
                                "run_id",
                                "facet_id",
                                "namespace",
                                "key",
                            }.issubset(
                                set(run_config_source_columns).intersection(
                                    run_config_target_columns
                                )
                            ):
                                run_config_column_sql = ", ".join(run_config_kv_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("run_config_kv")} ({run_config_column_sql})
                                    SELECT {run_config_column_sql}
                                    FROM {self._qualified_table_sql("run_config_kv", catalog=alias)} AS "src_cfg"
                                    WHERE "src_cfg".{self._quote_ident("run_id")} = {run_filter}
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM {self._qualified_table_sql("run_config_kv")} AS "dst_cfg"
                                          WHERE "dst_cfg".{self._quote_ident("run_id")}
                                                    = "src_cfg".{self._quote_ident("run_id")}
                                            AND "dst_cfg".{self._quote_ident("facet_id")}
                                                    = "src_cfg".{self._quote_ident("facet_id")}
                                            AND "dst_cfg".{self._quote_ident("namespace")}
                                                    = "src_cfg".{self._quote_ident("namespace")}
                                            AND "dst_cfg".{self._quote_ident("key")}
                                                    = "src_cfg".{self._quote_ident("key")}
                                      )
                                    """
                                )

                            if artifact_ids:
                                artifact_kv_columns = _core_columns("artifact_kv")
                                artifact_kv_source_columns = self._list_table_columns(
                                    "artifact_kv",
                                    schema=source_schema,
                                    catalog=alias,
                                    engine=self.db.engine,
                                )
                                artifact_kv_target_columns = self._list_table_columns(
                                    "artifact_kv",
                                    schema=target_schema,
                                    engine=self.db.engine,
                                )
                                if artifact_kv_columns and {
                                    "artifact_id",
                                    "facet_id",
                                    "key_path",
                                }.issubset(
                                    set(artifact_kv_source_columns).intersection(
                                        artifact_kv_target_columns
                                    )
                                ):
                                    artifact_kv_column_sql = ", ".join(artifact_kv_columns)
                                    conn.exec_driver_sql(
                                        f"""
                                        INSERT INTO {self._qualified_table_sql("artifact_kv")} ({artifact_kv_column_sql})
                                        SELECT {artifact_kv_column_sql}
                                        FROM {self._qualified_table_sql("artifact_kv", catalog=alias)} AS "src_akv"
                                        WHERE CAST("src_akv".{self._quote_ident("artifact_id")} AS VARCHAR)
                                              = {_any_sql(artifact_ids)}
                                          AND NOT EXISTS (
                                              SELECT 1
                                              FROM {self._qualified_table_sql("artifact_kv")} AS "dst_akv"
                                              WHERE CAST("dst_akv".{self._quote_ident("artifact_id")} AS VARCHAR)
                                                        = CAST("src_akv".{self._quote_ident("artifact_id")} AS VARCHAR)
                                                AND "dst_akv".{self._quote_ident("facet_id")}
                                                        = "src_akv".{self._quote_ident("facet_id")}
                                                AND "dst_akv".{self._quote_ident("key_path")}
                                                        = "src_akv".{self._quote_ident("key_path")}
                                          )
                                        """
                                    )

                            observation_columns = _core_columns("artifact_schema_observation")
                            observation_source_columns = set(
                                self._list_table_columns(
                                    "artifact_schema_observation",
                                    schema=source_schema,
                                    catalog=alias,
                                    engine=self.db.engine,
                                )
                            )
                            observation_target_columns = set(
                                self._list_table_columns(
                                    "artifact_schema_observation",
                                    schema=target_schema,
                                    engine=self.db.engine,
                                )
                            )
                            shared_observation_columns = (
                                observation_source_columns.intersection(
                                    observation_target_columns
                                )
                            )
                            observation_filters: list[str] = []
                            if "run_id" in observation_source_columns:
                                observation_filters.append(
                                    f'"src_obs".{self._quote_ident("run_id")} = {run_filter}'
                                )
                            if "artifact_id" in observation_source_columns and artifact_ids:
                                observation_filters.append(
                                    f'CAST("src_obs".{self._quote_ident("artifact_id")} AS VARCHAR)'
                                    f" = {_any_sql(artifact_ids)}"
                                )
                            observation_where = (
                                " OR ".join(observation_filters)
                                if observation_filters
                                else None
                            )
                            if (
                                observation_columns
                                and "id" in shared_observation_columns
                                and observation_where is not None
                            ):
                                if "schema_id" in shared_observation_columns:
                                    schema_rows = conn.exec_driver_sql(
                                        f"""
                                        SELECT DISTINCT CAST("src_obs".{self._quote_ident("schema_id")} AS VARCHAR)
                                        FROM {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} AS "src_obs"
                                        WHERE ({observation_where})
                                          AND "src_obs".{self._quote_ident("schema_id")} IS NOT NULL
                                        ORDER BY CAST("src_obs".{self._quote_ident("schema_id")} AS VARCHAR)
                                        """
                                    ).fetchall()
                                    schema_ids = [str(row[0]) for row in schema_rows]
                                    if schema_ids:
                                        artifact_schema_columns = _core_columns(
                                            "artifact_schema"
                                        )
                                        artifact_schema_source_columns = set(
                                            self._list_table_columns(
                                                "artifact_schema",
                                                schema=source_schema,
                                                catalog=alias,
                                                engine=self.db.engine,
                                            )
                                        )
                                        artifact_schema_target_columns = set(
                                            self._list_table_columns(
                                                "artifact_schema",
                                                schema=target_schema,
                                                engine=self.db.engine,
                                            )
                                        )
                                        if artifact_schema_columns and {"id"}.issubset(
                                            artifact_schema_source_columns.intersection(
                                                artifact_schema_target_columns
                                            )
                                        ):
                                            artifact_schema_column_sql = ", ".join(
                                                artifact_schema_columns
                                            )
                                            conn.exec_driver_sql(
                                                f"""
                                                INSERT INTO {self._qualified_table_sql("artifact_schema")} ({artifact_schema_column_sql})
                                                SELECT {artifact_schema_column_sql}
                                                FROM {self._qualified_table_sql("artifact_schema", catalog=alias)} AS "src_schema"
                                                WHERE CAST("src_schema".{self._quote_ident("id")} AS VARCHAR)
                                                      = {_any_sql(schema_ids)}
                                                  AND NOT EXISTS (
                                                      SELECT 1
                                                      FROM {self._qualified_table_sql("artifact_schema")} AS "dst_schema"
                                                      WHERE CAST("dst_schema".{self._quote_ident("id")} AS VARCHAR)
                                                            = CAST("src_schema".{self._quote_ident("id")} AS VARCHAR)
                                                  )
                                                """
                                            )

                                observation_column_sql = ", ".join(observation_columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO {self._qualified_table_sql("artifact_schema_observation")} ({observation_column_sql})
                                    SELECT {observation_column_sql}
                                    FROM {self._qualified_table_sql("artifact_schema_observation", catalog=alias)} AS "src_obs"
                                    WHERE ({observation_where})
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM {self._qualified_table_sql("artifact_schema_observation")} AS "dst_obs"
                                          WHERE CAST("dst_obs".{self._quote_ident("id")} AS VARCHAR)
                                                = CAST("src_obs".{self._quote_ident("id")} AS VARCHAR)
                                      )
                                    """
                                )

                        if runs_to_merge:
                            for table in shard_global_tables:
                                mode = shard_table_modes.get(table)
                                if mode == "unscoped_cache":
                                    unscoped_cache_tables_skipped.append(table)
                                    continue
                                if table in incompatible_global_tables_skipped:
                                    continue
                                safe_table = self._validate_identifier(table, label="table")
                                quoted_table = self._quote_ident(safe_table)
                                conn.exec_driver_sql(
                                    f"""
                                    CREATE TABLE IF NOT EXISTS global_tables.{quoted_table}
                                    AS
                                    SELECT *
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table}
                                    WHERE 1 = 0
                                    """
                                )
                                conn.exec_driver_sql(
                                    f"""
                                    CREATE OR REPLACE TABLE {self._quote_ident(merge_source_schema)}.{quoted_table}
                                    AS
                                    SELECT *
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table}
                                    WHERE 1 = 0
                                    """
                                )
                                filter_sql = shard_table_filters.get(safe_table)
                                if filter_sql is None:
                                    continue
                                count_row = conn.exec_driver_sql(
                                    f"""
                                    SELECT COUNT(*)
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table} AS "src_gt"
                                    WHERE {filter_sql}
                                    """
                                ).fetchone()
                                candidate_rows = int(count_row[0] if count_row else 0)
                                if candidate_rows <= 0:
                                    continue
                                source_lookup_name = self._qualified_table_lookup_name(
                                    safe_table, schema=merge_source_schema
                                )
                                target_lookup_name = self._qualified_table_lookup_name(
                                    safe_table, schema="global_tables"
                                )
                                source_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT name
                                    FROM pragma_table_info(
                                        {self._quote_sql_string_literal(source_lookup_name)}
                                    )
                                    ORDER BY cid
                                    """
                                ).fetchall()
                                target_rows = conn.exec_driver_sql(
                                    f"""
                                    SELECT name
                                    FROM pragma_table_info(
                                        {self._quote_sql_string_literal(target_lookup_name)}
                                    )
                                    ORDER BY cid
                                    """
                                ).fetchall()
                                source_columns = {str(row[0]) for row in source_rows}
                                columns: list[str] = []
                                seen_columns: set[str] = set()
                                for row in target_rows:
                                    column_name = str(row[0])
                                    if (
                                        column_name in source_columns
                                        and column_name not in seen_columns
                                    ):
                                        columns.append(self._quote_ident(column_name))
                                        seen_columns.add(column_name)
                                if not columns:
                                    continue
                                column_sql = ", ".join(columns)
                                conn.exec_driver_sql(
                                    f"""
                                    INSERT INTO global_tables.{quoted_table} ({column_sql})
                                    SELECT {column_sql}
                                    FROM {self._quote_ident(alias)}.global_tables.{quoted_table} AS "src_gt"
                                    WHERE {filter_sql}
                                    """
                                )
                                ingested_tables_merged.append(safe_table)
                    finally:
                        conn.exec_driver_sql(
                            f"DROP SCHEMA IF EXISTS {self._quote_ident(merge_source_schema)} CASCADE"
                        )
                        conn.exec_driver_sql(f"DETACH {self._quote_ident(alias)}")

            self.db.execute_with_retry(_write, operation_name="maintenance_merge")

            snapshots_merged = 0
            if include_snapshots and runs_to_merge:
                source_snapshot_dir = shard_path.parent / "shard_snapshots"
                for run_id in runs_to_merge:
                    source_name = self._resolve_run_snapshot_path(run_id, None).name
                    source_snapshot = source_snapshot_dir / source_name
                    if not source_snapshot.exists():
                        continue
                    destination_snapshot = self._resolve_run_snapshot_path(run_id, None)
                    if destination_snapshot.exists():
                        continue
                    destination_snapshot.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_snapshot, destination_snapshot)
                    snapshots_merged += 1

            self._log_audit(
                "merge",
                (
                    f"shard_path={shard_path} "
                    f"runs_merged={len(runs_to_merge)} "
                    f"conflicts={len(conflicts_detected)} "
                    f"conflict_mode={conflict_mode} "
                    f"include_snapshots={include_snapshots}"
                ),
            )

            return MergeResult(
                shard_path=shard_path,
                runs_merged=runs_to_merge,
                runs_skipped=runs_skipped,
                artifacts_merged=artifacts_merged,
                ingested_tables_merged=sorted(set(ingested_tables_merged)),
                unscoped_cache_tables_skipped=sorted(set(unscoped_cache_tables_skipped)),
                conflicts_detected=conflicts_detected,
                snapshots_merged=snapshots_merged,
                incompatible_global_tables_skipped=incompatible_global_tables_skipped,
            )
        finally:
            shard_db.engine.dispose()

    def compact(self) -> None:
        def _vacuum() -> None:
            with self.db.engine.begin() as conn:
                conn.exec_driver_sql("VACUUM")

        self.db.execute_with_retry(_vacuum, operation_name="maintenance_compact")
        self._log_audit("compact", "operation=vacuum")

    def rebuild_from_json(
        self,
        json_dir: Path,
        *,
        dry_run: bool,
        mode: Literal["minimal", "full"] = "minimal",
    ) -> RebuildResult:
        snapshot_dir = Path(json_dir)
        errors: list[str] = []
        runs_inserted = 0
        runs_already_present = 0
        artifacts_inserted = 0
        run_exists_cache: dict[str, bool] = {}
        artifact_exists_cache: dict[str, bool] = {}
        rebuild_mode = str(mode).strip().lower()
        if rebuild_mode not in {"minimal", "full"}:
            raise ValueError("mode must be one of: minimal, full")
        identity = IdentityManager(project_root=str(self.run_dir))
        supports_full_restore = (
            self._build_rebuild_full_table_support()
            if rebuild_mode == "full" and not dry_run
            else {}
        )

        if not snapshot_dir.exists() or not snapshot_dir.is_dir():
            errors.append(f"{snapshot_dir}: JSON directory does not exist or is not a directory.")
            return RebuildResult(
                json_files_scanned=0,
                runs_inserted=0,
                runs_already_present=0,
                artifacts_inserted=0,
                errors=errors,
                dry_run=dry_run,
            )

        snapshot_files = sorted(
            [path for path in snapshot_dir.glob("*.json") if path.is_file()],
            key=lambda path: path.name,
        )

        for snapshot_path in snapshot_files:
            try:
                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(f"{snapshot_path.name}: failed to read/parse JSON ({exc})")
                continue

            try:
                record = ConsistRecord.model_validate(payload)
            except Exception as exc:
                errors.append(f"{snapshot_path.name}: invalid snapshot payload ({exc})")
                continue

            run = record.run
            run_id = str(run.id).strip()
            if not run_id:
                errors.append(f"{snapshot_path.name}: missing run.id")
                continue

            artifact_rows: dict[str, Artifact] = {}
            snapshot_artifact_meta: dict[str, tuple[uuid.UUID, dict[str, Any]]] = {}
            for artifact in [*record.inputs, *record.outputs]:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None:
                    errors.append(
                        f"{snapshot_path.name}: invalid artifact id {artifact.id!r}"
                    )
                    continue
                artifact_id = str(parsed_uuid)
                if artifact_id not in artifact_rows:
                    artifact_rows[artifact_id] = Artifact(
                        id=parsed_uuid,
                        key=artifact.key,
                        container_uri=artifact.container_uri,
                        table_path=artifact.table_path,
                        array_path=artifact.array_path,
                        driver=artifact.driver,
                        hash=artifact.hash,
                        run_id=artifact.run_id,
                        meta=dict(artifact.meta)
                        if isinstance(artifact.meta, dict)
                        else {},
                        created_at=artifact.created_at,
                    )
                if artifact_id not in snapshot_artifact_meta:
                    snapshot_artifact_meta[artifact_id] = (
                        parsed_uuid,
                        dict(artifact.meta) if isinstance(artifact.meta, dict) else {},
                    )

            link_rows: dict[str, str] = {}
            for artifact in record.inputs:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None:
                    continue
                link_rows.setdefault(str(parsed_uuid), "input")
            for artifact in record.outputs:
                parsed_uuid = self._coerce_uuid(artifact.id)
                if parsed_uuid is None:
                    continue
                link_rows[str(parsed_uuid)] = "output"

            if dry_run:
                with self.db.session_scope() as session:
                    if run_id not in run_exists_cache:
                        run_exists_cache[run_id] = session.get(Run, run_id) is not None
                    if run_exists_cache[run_id]:
                        runs_already_present += 1
                    else:
                        runs_inserted += 1
                        run_exists_cache[run_id] = True

                    for artifact_id, artifact in artifact_rows.items():
                        if artifact_id not in artifact_exists_cache:
                            artifact_exists_cache[artifact_id] = (
                                session.get(Artifact, artifact.id) is not None
                            )
                        if not artifact_exists_cache[artifact_id]:
                            artifacts_inserted += 1
                            artifact_exists_cache[artifact_id] = True
                continue

            def _upsert_snapshot() -> tuple[bool, int]:
                with self.db.session_scope() as session:
                    run_exists = session.get(Run, run_id) is not None
                    if not run_exists:
                        session.add(
                            Run(
                                id=run_id,
                                parent_run_id=run.parent_run_id,
                                status=run.status,
                                model_name=run.model_name,
                                description=run.description,
                                year=run.year,
                                iteration=run.iteration,
                                tags=list(run.tags or []),
                                config_hash=getattr(run, "config_hash", None),
                                git_hash=getattr(run, "git_hash", None),
                                input_hash=run.input_hash,
                                signature=run.signature,
                                meta=dict(run.meta) if isinstance(run.meta, dict) else {},
                                started_at=run.started_at,
                                ended_at=run.ended_at,
                                created_at=run.created_at,
                                updated_at=run.updated_at,
                            )
                        )

                    inserted_artifacts_for_file = 0
                    for artifact in artifact_rows.values():
                        if session.get(Artifact, artifact.id) is not None:
                            continue
                        session.add(artifact)
                        inserted_artifacts_for_file += 1

                    for artifact_id, direction in link_rows.items():
                        parsed_uuid = self._coerce_uuid(artifact_id)
                        if parsed_uuid is None:
                            continue
                        if session.get(RunArtifactLink, (run_id, parsed_uuid)) is not None:
                            continue
                        session.add(
                            RunArtifactLink(
                                run_id=run_id,
                                artifact_id=parsed_uuid,
                                direction=direction,
                            )
                        )

                    session.commit()
                    return run_exists, inserted_artifacts_for_file

            try:
                run_exists, inserted_artifacts_for_file = self.db.execute_with_retry(
                    _upsert_snapshot,
                    operation_name="maintenance_rebuild_from_json_upsert",
                )
            except Exception as exc:
                errors.append(f"{snapshot_path.name}: rebuild write failed ({exc})")
                continue

            if run_exists:
                runs_already_present += 1
            else:
                runs_inserted += 1
            artifacts_inserted += inserted_artifacts_for_file

            if rebuild_mode == "full":
                try:
                    self.db.execute_with_retry(
                        lambda: self._restore_full_snapshot_metadata(
                            run_id=run_id,
                            run=run,
                            run_facet=record.facet,
                            snapshot_artifact_meta=snapshot_artifact_meta,
                            supports_full_restore=supports_full_restore,
                            identity=identity,
                        ),
                        operation_name="maintenance_rebuild_from_json_full_restore",
                    )
                except Exception as exc:
                    errors.append(
                        f"{snapshot_path.name}: optional full restore failed ({exc})"
                    )
                    continue

        result = RebuildResult(
            json_files_scanned=len(snapshot_files),
            runs_inserted=runs_inserted,
            runs_already_present=runs_already_present,
            artifacts_inserted=artifacts_inserted,
            errors=errors,
            dry_run=dry_run,
        )

        if not dry_run:
            self._log_audit(
                "rebuild_from_json",
                (
                    f"json_dir={snapshot_dir} "
                    f"json_files_scanned={result.json_files_scanned} "
                    f"runs_inserted={result.runs_inserted} "
                    f"runs_already_present={result.runs_already_present} "
                    f"artifacts_inserted={result.artifacts_inserted} "
                    f"mode={rebuild_mode} "
                    f"errors={len(result.errors)}"
                ),
            )

        return result

    def _build_rebuild_full_table_support(self) -> dict[str, bool]:
        return {
            "run_config_kv": self._table_supports_model("run_config_kv", RunConfigKV),
            "artifact_kv": self._table_supports_model("artifact_kv", ArtifactKV),
            "artifact_schema": self._table_supports_model(
                "artifact_schema", ArtifactSchema
            ),
            "artifact_schema_observation": self._table_supports_model(
                "artifact_schema_observation", ArtifactSchemaObservation
            ),
        }

    def _restore_full_snapshot_metadata(
        self,
        *,
        run_id: str,
        run: Run,
        run_facet: Any,
        snapshot_artifact_meta: dict[str, tuple[uuid.UUID, dict[str, Any]]],
        supports_full_restore: dict[str, bool],
        identity: IdentityManager,
    ) -> None:
        with self.db.session_scope() as session:
            run_meta = dict(run.meta) if isinstance(run.meta, dict) else {}
            run_namespace = self._coerce_non_empty_str(
                run_meta.get("config_facet_namespace")
            ) or run.model_name

            if supports_full_restore.get("run_config_kv"):
                self._restore_run_config_kv_rows(
                    session=session,
                    run_id=run_id,
                    run_facet=run_facet,
                    run_meta=run_meta,
                    namespace=run_namespace,
                    identity=identity,
                )

            restore_schema = bool(
                supports_full_restore.get("artifact_schema")
                and supports_full_restore.get("artifact_schema_observation")
            )
            for artifact_id, artifact_meta in snapshot_artifact_meta.values():
                if supports_full_restore.get("artifact_kv"):
                    self._restore_artifact_kv_rows(
                        session=session,
                        artifact_id=artifact_id,
                        artifact_meta=artifact_meta,
                        default_namespace=run.model_name,
                        identity=identity,
                    )

                if restore_schema:
                    self._restore_artifact_schema_rows(
                        session=session,
                        artifact_id=artifact_id,
                        artifact_meta=artifact_meta,
                        run_id=run_id,
                    )

            session.commit()

    def _restore_run_config_kv_rows(
        self,
        *,
        session: Any,
        run_id: str,
        run_facet: Any,
        run_meta: dict[str, Any],
        namespace: str,
        identity: IdentityManager,
    ) -> None:
        if not isinstance(run_facet, dict) or not run_facet:
            return

        facet_id = self._coerce_non_empty_str(run_meta.get("config_facet_id"))
        if facet_id is None:
            facet_id = identity.canonical_json_sha256(run_facet)

        flattened = flatten_facet_values(
            facet_dict=run_facet,
            include_json_leaves=True,
        )
        for row in flattened:
            pk = (run_id, facet_id, namespace, row.key_path)
            if session.get(RunConfigKV, pk) is not None:
                continue
            session.add(
                RunConfigKV(
                    run_id=run_id,
                    facet_id=facet_id,
                    namespace=namespace,
                    key=row.key_path,
                    value_type=row.value_type,
                    value_str=row.value_str,
                    value_num=row.value_num,
                    value_bool=row.value_bool,
                    value_json=row.value_json,
                )
            )

    def _restore_artifact_kv_rows(
        self,
        *,
        session: Any,
        artifact_id: uuid.UUID,
        artifact_meta: dict[str, Any],
        default_namespace: str,
        identity: IdentityManager,
    ) -> None:
        facet_payload = artifact_meta.get("artifact_facet")
        if not isinstance(facet_payload, dict) or not facet_payload:
            fallback_payload = artifact_meta.get("facet")
            if isinstance(fallback_payload, dict) and fallback_payload:
                facet_payload = fallback_payload
            else:
                return

        facet_id = self._coerce_non_empty_str(artifact_meta.get("artifact_facet_id"))
        if facet_id is None:
            facet_id = identity.canonical_json_sha256(facet_payload)
        namespace = self._coerce_non_empty_str(
            artifact_meta.get("artifact_facet_namespace")
        ) or default_namespace

        flattened = flatten_facet_values(
            facet_dict=facet_payload,
            include_json_leaves=False,
        )
        for row in flattened:
            pk = (artifact_id, facet_id, row.key_path)
            if session.get(ArtifactKV, pk) is not None:
                continue
            session.add(
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id=facet_id,
                    key_path=row.key_path,
                    namespace=namespace,
                    value_type=row.value_type,
                    value_str=row.value_str,
                    value_num=row.value_num,
                    value_bool=row.value_bool,
                )
            )

    def _restore_artifact_schema_rows(
        self,
        *,
        session: Any,
        artifact_id: uuid.UUID,
        artifact_meta: dict[str, Any],
        run_id: str,
    ) -> None:
        schema_id = self._coerce_non_empty_str(artifact_meta.get("schema_id"))
        if schema_id is None:
            return

        summary = artifact_meta.get("schema_summary")
        summary_json = summary if isinstance(summary, dict) else {}

        profile = artifact_meta.get("schema_profile")
        profile_json = profile if isinstance(profile, dict) else None

        existing_schema = session.get(ArtifactSchema, schema_id)
        if existing_schema is None:
            profile_version = summary_json.get("profile_version")
            if not isinstance(profile_version, int) or profile_version <= 0:
                profile_version = 1
            session.add(
                ArtifactSchema(
                    id=schema_id,
                    profile_version=profile_version,
                    summary_json=summary_json,
                    profile_json=profile_json,
                )
            )
        else:
            updated = False
            if not existing_schema.summary_json and summary_json:
                existing_schema.summary_json = summary_json
                updated = True
            if existing_schema.profile_json is None and profile_json is not None:
                existing_schema.profile_json = profile_json
                updated = True
            if updated:
                session.add(existing_schema)

        source = self._schema_observation_source(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )
        sample_rows = self._schema_observation_sample_rows(
            summary_json=summary_json,
            artifact_meta=artifact_meta,
        )

        existing_rows = session.exec(
            select(ArtifactSchemaObservation).where(
                ArtifactSchemaObservation.run_id == run_id,
                ArtifactSchemaObservation.artifact_id == artifact_id,
                ArtifactSchemaObservation.schema_id == schema_id,
            )
        ).all()
        if any(
            row.source == source and row.sample_rows == sample_rows
            for row in existing_rows
        ):
            return

        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_id,
                run_id=run_id,
                source=source,
                sample_rows=sample_rows,
            )
        )

    def _table_supports_model(self, table_name: str, model: type[Any]) -> bool:
        table_columns = set(self._list_table_columns(table_name))
        if not table_columns:
            return False
        model_table = getattr(model, "__table__", None)
        if model_table is None:
            return False
        model_columns = {str(column.name) for column in model_table.columns}
        return model_columns.issubset(table_columns)

    @staticmethod
    def _coerce_non_empty_str(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        return candidate if candidate else None

    @classmethod
    def _schema_observation_source(
        cls, *, summary_json: dict[str, Any], artifact_meta: dict[str, Any]
    ) -> str:
        source = cls._coerce_non_empty_str(summary_json.get("source"))
        if source is not None:
            return source
        source = cls._coerce_non_empty_str(artifact_meta.get("schema_source"))
        if source is not None:
            return source
        source = cls._coerce_non_empty_str(artifact_meta.get("source"))
        if source is not None:
            return source
        return "snapshot_rebuild"

    @staticmethod
    def _schema_observation_sample_rows(
        *, summary_json: dict[str, Any], artifact_meta: dict[str, Any]
    ) -> Optional[int]:
        summary_rows = summary_json.get("sample_rows")
        if isinstance(summary_rows, int):
            return summary_rows
        meta_rows = artifact_meta.get("sample_rows")
        if isinstance(meta_rows, int):
            return meta_rows
        schema_meta_rows = artifact_meta.get("schema_sample_rows")
        if isinstance(schema_meta_rows, int):
            return schema_meta_rows
        return None

    def snapshot(
        self, dest_path: Path, *, checkpoint: bool = True, metadata: Optional[dict] = None
    ) -> Path:
        snapshot_metadata: dict = {"operation": "maintenance_snapshot"}
        if metadata:
            snapshot_metadata.update(metadata)
        snapshot_path = self.db.snapshot_to(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata=snapshot_metadata,
        )
        self._log_audit(
            "snapshot",
            f"dest_path={snapshot_path} checkpoint={checkpoint}",
        )
        return snapshot_path

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

    @staticmethod
    def _required_global_table_column(mode: GlobalTableMode) -> Optional[str]:
        if mode == "run_scoped":
            return "consist_run_id"
        if mode == "run_link":
            return "run_id"
        return None

    @staticmethod
    def _normalize_sql_type(type_name: str) -> str:
        return " ".join(str(type_name).strip().upper().split())

    def _global_table_merge_compatibility_issue(
        self,
        table: str,
        *,
        mode: GlobalTableMode,
        source_engine: Engine,
        target_engine: Engine,
    ) -> Optional[str]:
        safe_table = self._validate_identifier(table, label="table")
        source_column_types = self._list_table_columns_with_types(
            safe_table,
            schema="global_tables",
            engine=source_engine,
        )
        if not source_column_types:
            return "source table missing in global_tables schema"

        target_column_types = self._list_table_columns_with_types(
            safe_table,
            schema="global_tables",
            engine=target_engine,
        )
        target_table_exists = bool(target_column_types)

        required_column = self._required_global_table_column(mode)
        if required_column and required_column not in source_column_types:
            return (
                f"missing required source column '{required_column}' for mode '{mode}'"
            )
        if required_column and target_table_exists and required_column not in target_column_types:
            return (
                f"missing required target column '{required_column}' for mode '{mode}'"
            )

        shared_columns = sorted(set(source_column_types).intersection(target_column_types))
        incompatible_columns: list[str] = []
        for column_name in shared_columns:
            source_type = source_column_types[column_name]
            target_type = target_column_types[column_name]
            if not self._types_compatible_for_merge(
                source_type, target_type, engine=target_engine
            ):
                incompatible_columns.append(
                    f"{column_name} (source={source_type}, target={target_type})"
                )

        if incompatible_columns:
            return "incompatible shared column type(s): " + ", ".join(
                incompatible_columns
            )
        return None

    def _types_compatible_for_merge(
        self,
        source_type: str,
        target_type: str,
        *,
        engine: Optional[Engine] = None,
    ) -> bool:
        normalized_source = self._normalize_sql_type(source_type)
        normalized_target = self._normalize_sql_type(target_type)
        if not normalized_source or not normalized_target:
            return False
        if normalized_source == normalized_target:
            return True

        active_engine = engine or self.db.engine

        def _query() -> bool:
            with active_engine.begin() as conn:
                try:
                    row = conn.exec_driver_sql(
                        f"""
                        SELECT can_cast_implicitly(
                            NULL::{normalized_source},
                            NULL::{normalized_target}
                        )
                        """
                    ).fetchone()
                except Exception:
                    return False
            return bool(row[0]) if row else False

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_types_compatible_for_merge"
        )

    def _list_table_columns_with_types(
        self,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
        engine: Optional[Engine] = None,
    ) -> dict[str, str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_schema = self._validate_identifier(schema, label="schema")
        safe_catalog = (
            self._validate_identifier(catalog, label="catalog")
            if catalog is not None
            else None
        )
        active_engine = engine or self.db.engine
        lookup_name = self._qualified_table_lookup_name(
            safe_table, schema=safe_schema, catalog=safe_catalog
        )

        def _query() -> dict[str, str]:
            with active_engine.begin() as conn:
                try:
                    rows = conn.exec_driver_sql(
                        f"""
                        SELECT name, type
                        FROM pragma_table_info({self._quote_sql_string_literal(lookup_name)})
                        ORDER BY cid
                        """
                    ).fetchall()
                except Exception:
                    return {}
            column_types: dict[str, str] = {}
            for row in rows:
                column_name = str(row[0])
                if column_name in column_types:
                    continue
                column_types[column_name] = str(row[1] or "").strip()
            return column_types

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_list_table_columns_with_types"
        )

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
            seen_target_columns: set[str] = set()

            for schema_name, column_name, _ in rows:
                schema_value = str(schema_name)
                column_value = str(column_name)
                if schema_value == safe_source_schema:
                    source_columns.add(column_value)
                if (
                    schema_value == safe_target_schema
                    and column_value not in seen_target_columns
                ):
                    target_columns_in_order.append(column_value)
                    seen_target_columns.add(column_value)

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

    def _list_table_columns(
        self,
        table: str,
        *,
        schema: str = "main",
        catalog: Optional[str] = None,
        engine: Optional[Engine] = None,
    ) -> list[str]:
        safe_table = self._validate_identifier(table, label="table")
        safe_schema = self._validate_identifier(schema, label="schema")
        safe_catalog = (
            self._validate_identifier(catalog, label="catalog")
            if catalog is not None
            else None
        )
        active_engine = engine or self.db.engine
        lookup_name = self._qualified_table_lookup_name(
            safe_table, schema=safe_schema, catalog=safe_catalog
        )

        def _query() -> list[str]:
            with active_engine.begin() as conn:
                try:
                    rows = conn.exec_driver_sql(
                        f"""
                        SELECT name
                        FROM pragma_table_info({self._quote_sql_string_literal(lookup_name)})
                        ORDER BY cid
                        """
                    ).fetchall()
                except Exception:
                    return []
            seen: set[str] = set()
            columns: list[str] = []
            for row in rows:
                value = str(row[0])
                if value in seen:
                    continue
                seen.add(value)
                columns.append(value)
            return columns

        return self.db.execute_with_retry(
            _query, operation_name="maintenance_list_table_columns"
        )

    def _build_table_intersection_columns(
        self,
        table: str,
        *,
        source_schema: str = "main",
        source_catalog: Optional[str] = None,
        target_schema: str = "main",
        target_catalog: Optional[str] = None,
        engine: Optional[Engine] = None,
    ) -> list[str]:
        source_columns = set(
            self._list_table_columns(
                table,
                schema=source_schema,
                catalog=source_catalog,
                engine=engine,
            )
        )
        target_columns = self._list_table_columns(
            table,
            schema=target_schema,
            catalog=target_catalog,
            engine=engine,
        )
        return [
            self._quote_ident(column_name)
            for column_name in target_columns
            if column_name in source_columns
        ]

    def _resolve_artifact_disk_paths(
        self, artifact_ids: Iterable[uuid.UUID]
    ) -> list[Path]:
        normalized_artifact_ids: list[uuid.UUID] = []
        seen_artifact_ids: set[uuid.UUID] = set()
        for value in artifact_ids:
            parsed_uuid = self._coerce_uuid(value)
            if parsed_uuid is None or parsed_uuid in seen_artifact_ids:
                continue
            seen_artifact_ids.add(parsed_uuid)
            normalized_artifact_ids.append(parsed_uuid)

        if not normalized_artifact_ids:
            return []

        with self.db.session_scope() as session:
            artifacts = session.exec(
                select(Artifact).where(col(Artifact.id).in_(normalized_artifact_ids))
            ).all()
            output_links = session.exec(
                select(RunArtifactLink.artifact_id, RunArtifactLink.run_id)
                .where(col(RunArtifactLink.artifact_id).in_(normalized_artifact_ids))
                .where(col(RunArtifactLink.direction) == "output")
                .order_by(RunArtifactLink.run_id)
            ).all()

        artifacts_by_id: dict[uuid.UUID, Artifact] = {artifact.id: artifact for artifact in artifacts}
        run_id_by_artifact: dict[uuid.UUID, str] = {}
        run_ids: set[str] = set()

        for artifact in artifacts:
            if artifact.run_id:
                run_id_by_artifact[artifact.id] = artifact.run_id
                run_ids.add(artifact.run_id)

        for artifact_id_raw, run_id_raw in output_links:
            parsed_uuid = self._coerce_uuid(artifact_id_raw)
            if parsed_uuid is None or parsed_uuid in run_id_by_artifact:
                continue
            run_id = str(run_id_raw).strip()
            if not run_id:
                continue
            run_id_by_artifact[parsed_uuid] = run_id
            run_ids.add(run_id)

        runs_by_id: dict[str, Run] = {}
        if run_ids:
            with self.db.session_scope() as session:
                runs = session.exec(select(Run).where(col(Run.id).in_(run_ids))).all()
            runs_by_id = {run.id: run for run in runs}

        resolved_paths: list[Path] = []
        seen_paths: set[str] = set()
        for artifact_id in normalized_artifact_ids:
            artifact = artifacts_by_id.get(artifact_id)
            if artifact is None:
                continue
            run_id = run_id_by_artifact.get(artifact_id)
            run = runs_by_id.get(run_id) if run_id else None
            for candidate in self._resolve_artifact_uri_candidates(artifact, run):
                if not candidate.exists():
                    continue
                path_key = str(candidate)
                if path_key in seen_paths:
                    continue
                seen_paths.add(path_key)
                resolved_paths.append(candidate)

        return resolved_paths

    def _resolve_artifact_uri_candidates(
        self, artifact: Artifact, run: Optional[Run]
    ) -> list[Path]:
        uri = str(getattr(artifact, "container_uri", "") or "").strip()
        if not uri:
            return []

        run_dir = self._resolve_run_directory_for_artifact(run)
        run_meta = run.meta if run and isinstance(run.meta, dict) else {}
        artifact_meta = artifact.meta if isinstance(artifact.meta, dict) else {}
        candidate_paths: list[Path] = []
        seen_candidates: set[str] = set()

        def _append_candidate(path: Optional[Path]) -> None:
            if path is None:
                return
            path_key = str(path)
            if path_key in seen_candidates:
                return
            seen_candidates.add(path_key)
            candidate_paths.append(path)

        if uri.startswith("./"):
            _append_candidate(self._resolve_relative_path_under_root(uri[2:], run_dir))
            return candidate_paths

        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            normalized_scheme = scheme.strip()

            if normalized_scheme == "file":
                _append_candidate(self._resolve_file_uri(uri))
                return candidate_paths

            if normalized_scheme == "workspace":
                _append_candidate(
                    self._resolve_relative_path_under_root(rel_path, run_dir)
                )
                return candidate_paths

            safe_relative = self._normalize_relative_uri_path(rel_path)
            if safe_relative is None:
                return candidate_paths

            for root in self._mount_roots_for_uri(
                normalized_scheme, run_meta=run_meta, artifact_meta=artifact_meta
            ):
                _append_candidate(
                    self._resolve_relative_path_under_root(safe_relative, root)
                )
            return candidate_paths

        raw_path = Path(uri)
        if raw_path.is_absolute():
            try:
                _append_candidate(raw_path.resolve())
            except OSError:
                return candidate_paths

        return candidate_paths

    def _resolve_run_directory_for_artifact(self, run: Optional[Run]) -> Path:
        if run and isinstance(run.meta, dict):
            physical_run_dir = run.meta.get("_physical_run_dir")
            if isinstance(physical_run_dir, str) and physical_run_dir:
                try:
                    return Path(physical_run_dir).resolve()
                except OSError:
                    pass
        try:
            return self.run_dir.resolve()
        except OSError:
            return self.run_dir

    def _mount_roots_for_uri(
        self,
        scheme: str,
        *,
        run_meta: dict[str, Any],
        artifact_meta: dict[str, Any],
    ) -> list[Path]:
        roots: list[Path] = []
        seen_roots: set[str] = set()
        mounts_raw = run_meta.get("mounts")
        if isinstance(mounts_raw, dict):
            mount_root = mounts_raw.get(scheme)
            if isinstance(mount_root, str) and mount_root:
                normalized_root = self._normalize_mount_root_path(mount_root)
                if normalized_root is not None:
                    root_key = str(normalized_root)
                    seen_roots.add(root_key)
                    roots.append(normalized_root)

        artifact_mount_root = artifact_meta.get("mount_root")
        if isinstance(artifact_mount_root, str) and artifact_mount_root:
            normalized_root = self._normalize_mount_root_path(artifact_mount_root)
            if normalized_root is not None:
                root_key = str(normalized_root)
                if root_key not in seen_roots:
                    seen_roots.add(root_key)
                    roots.append(normalized_root)

        return roots

    @staticmethod
    def _normalize_mount_root_path(root: str) -> Optional[Path]:
        try:
            root_path = Path(root)
            if not root_path.is_absolute():
                return None
            return root_path.resolve()
        except OSError:
            return None

    @staticmethod
    def _normalize_relative_uri_path(value: str) -> Optional[str]:
        rel_path = PurePosixPath(value.strip())
        if not rel_path.parts or rel_path.is_absolute():
            return None
        if any(part == ".." for part in rel_path.parts):
            return None
        return rel_path.as_posix()

    @staticmethod
    def _resolve_relative_path_under_root(relative_path: str, root: Path) -> Optional[Path]:
        safe_rel_path = DatabaseMaintenance._normalize_relative_uri_path(relative_path)
        if safe_rel_path is None:
            return None
        try:
            root_resolved = root.resolve()
            resolved_path = (root_resolved / safe_rel_path).resolve()
            resolved_path.relative_to(root_resolved)
        except (OSError, ValueError):
            return None
        return resolved_path

    @staticmethod
    def _resolve_file_uri(uri: str) -> Optional[Path]:
        parsed = urlsplit(uri)
        if parsed.scheme != "file":
            return None
        if parsed.netloc not in {"", "localhost"}:
            return None
        path_text = unquote(parsed.path or "")
        if not path_text:
            return None
        path = Path(path_text)
        if not path.is_absolute():
            return None
        try:
            return path.resolve()
        except OSError:
            return None

    def _init_shard_schema(self, shard_path: Path) -> None:
        target_path = Path(shard_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        for path in (target_path, Path(f"{target_path}.wal")):
            if path.exists():
                path.unlink()
        shard_db = self.db.__class__(str(target_path))
        try:
            with shard_db.engine.begin() as conn:
                conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        finally:
            shard_db.engine.dispose()

    def _resolve_run_snapshot_path(self, run_id: str, run: Optional[Run]) -> Path:
        run_dir = self.run_dir
        if run and isinstance(run.meta, dict):
            physical_run_dir = run.meta.get("_physical_run_dir")
            if isinstance(physical_run_dir, str) and physical_run_dir:
                run_dir = Path(physical_run_dir)
        safe_run_id = "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )
        return run_dir / "consist_runs" / f"{safe_run_id}.json"

    def _log_audit(self, operation: str, summary: str) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        audit_path = self.run_dir / ".consist_audit.log"
        with audit_path.open("a", encoding="utf-8") as file:
            file.write(f"{timestamp}\t{operation}\t{summary}\n")

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

    @staticmethod
    def _qualified_table_lookup_name(
        table: str, *, schema: str = "main", catalog: Optional[str] = None
    ) -> str:
        parts = [schema, table]
        if catalog:
            parts.insert(0, catalog)
        return ".".join(parts)

    @staticmethod
    def _qualified_table_sql(
        table: str, *, schema: str = "main", catalog: Optional[str] = None
    ) -> str:
        parts = [DatabaseMaintenance._quote_ident(schema), DatabaseMaintenance._quote_ident(table)]
        if catalog:
            parts.insert(0, DatabaseMaintenance._quote_ident(catalog))
        return ".".join(parts)
