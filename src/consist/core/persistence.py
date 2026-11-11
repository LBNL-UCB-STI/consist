from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    overload,
)
import uuid

import pandas as pd

from consist.artifact import Artifact
from consist.core.db_runtime import DB_CALL_PROFILER, DatabaseRuntimeOps
from consist.core.db_snapshot import DatabaseSnapshotOps, snapshot_sidecar_path
from consist.core.provenance_writer import ProvenanceWriter as ProvenanceWriter
from consist.digest import Digest
from consist.location import DirTreeStat, MemberOf, Mount
from consist.meta import FrozenMeta, strip_system_meta
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.run import (
    CouplerSnapshotPhase,
    Run,
    RunArtifacts,
    RunBindingInvocation,
    RunCouplerSnapshot,
)
from consist.models.run_config_kv import RunConfigKV
from consist.protocols import ParameterizedSQLProtocol
from consist.store.detect import (
    ConsistError as ConsistError,
    DatabaseMigrationRequired as DatabaseMigrationRequired,
    IncompatibleSchemaError as IncompatibleSchemaError,
)
from consist.store.ledger import StoreLedger, _UNSET, _UnsetType, open_v2_store

if TYPE_CHECKING:
    import duckdb
    from sqlmodel import SQLModel

_ModelT = TypeVar("_ModelT")
LIKE_ESCAPE_CHAR = "!"
SchemaProfileSource = Literal["file", "duckdb", "user_provided"]


def _escape_like_literal(value: str, *, escape_char: str = LIKE_ESCAPE_CHAR) -> str:
    return (
        value.replace(escape_char, escape_char + escape_char)
        .replace("%", escape_char + "%")
        .replace("_", escape_char + "_")
    )


def _coerce_uuid(val: uuid.UUID | str) -> uuid.UUID:
    return val if isinstance(val, uuid.UUID) else uuid.UUID(str(val))


def _try_parse_uuid(val: Any) -> uuid.UUID | None:
    if isinstance(val, uuid.UUID):
        return val
    if not isinstance(val, str):
        return None
    try:
        return uuid.UUID(val)
    except ValueError:
        return None


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _infer_repr_kind(artifact: Artifact) -> str:
    for obs in artifact.hints:
        if obs.location.uri.startswith("db://"):
            continue
        if obs.location.member is not None or isinstance(obs.witness, MemberOf):
            return "member"
        if isinstance(obs.witness, DirTreeStat):
            return "tree"
        return "file"
    for obs in artifact.hints:
        if obs.location.uri.startswith("db://"):
            return "table"
    first_uri, _ = artifact._first_candidate_location()
    if first_uri is not None and first_uri.startswith("db://"):
        return "table"
    if artifact.member is not None:
        return "member"
    if artifact.driver in (
        "directory",
        "zarr",
    ) or artifact.identity.anchor.scheme.startswith("sha256:manifest"):
        return "tree"
    return "file"


def _kv_row_python_value(
    *,
    value_type: str,
    value_str: str | None,
    value_num: float | None,
    value_bool: bool | None,
    value_json: Any,
) -> Any:
    if value_type == "null":
        return None
    if value_type == "bool":
        return value_bool
    if value_type == "int":
        return int(value_num) if value_num is not None else None
    if value_type == "float":
        return value_num
    if value_type == "str":
        return value_str
    return value_json


_RUN_COLUMNS_SQL: str = (
    "r.id, r.model_name, r.description, r.year, r.iteration, r.stage, r.phase, "
    "r.parent_run_id, r.status, r.code_hash, r.git_hash, r.config_hash, "
    "r.input_hash, r.signature, r.signature_scheme, r.cache_status, r.tags, "
    "r.meta, r.started_at, r.ended_at, r.created_at, r.updated_at"
)
_hydrate_run_row = StoreLedger._hydrate_run_row


@dataclass(frozen=True)
class ConfigFacetRecord:
    """Read-only DTO representing a config facet stored in ``main.run.meta``."""

    id: str
    namespace: str
    schema_name: str
    schema_version: Optional[str]
    facet_json: Dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class ArtifactSchemaSelection:
    schema_id: str
    source: str
    candidate_count: int
    selection_rule: str


class DatabaseManager:
    """
    Domain persistence and query facade backed by a single ``StoreLedger`` connection.
    """

    def __init__(
        self,
        db_path: str | os.PathLike[str],
        *,
        read_only: bool = False,
        skip_schema_init: bool = False,
        auto_migrate: Optional[bool | Literal["quick", "full", "off"] | str] = None,
        lock_retries: int = 20,
        lock_base_sleep_seconds: float = 0.1,
        lock_max_sleep_seconds: float = 2.0,
        mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
        site: str = "local",
    ) -> None:
        _ = skip_schema_init
        raw_db_path = os.fspath(db_path)
        self.db_path: str = raw_db_path
        self.read_only: bool = bool(read_only)
        self._lock_retries: int = int(lock_retries)
        self._lock_base_sleep_seconds: float = float(lock_base_sleep_seconds)
        self._lock_max_sleep_seconds: float = float(lock_max_sleep_seconds)
        self._site: str = site
        self._runtime_ops = DatabaseRuntimeOps(self)
        self._snapshot_ops = DatabaseSnapshotOps(self)

        stripped = raw_db_path.strip()
        if not stripped or stripped == ":memory:":
            self.ledger: StoreLedger = StoreLedger(
                ":memory:",
                read_only=self.read_only,
                mounts=mounts,
            )
        else:
            fs_path = Path(stripped).expanduser()
            if self.read_only and not fs_path.exists():
                raise FileNotFoundError(
                    f"Database file does not exist for read-only access: {fs_path}"
                )
            if not self.read_only:
                fs_path.parent.mkdir(parents=True, exist_ok=True)

            def _open_store() -> StoreLedger:
                return open_v2_store(
                    fs_path,
                    read_only=self.read_only,
                    auto_migrate=auto_migrate,
                    mounts=mounts,
                    site=site,
                )

            if self.read_only:
                self.ledger = _open_store()
            else:
                self.ledger = self.execute_with_retry(
                    _open_store,
                    operation_name="open_v2_store",
                )
            if self.ledger.db_path is not None:
                self.db_path = str(self.ledger.db_path)

    # ------------------------------------------------------------------
    # Lifecycle, Snapshot, and Read-Only Query Helpers
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying ``StoreLedger`` connection."""
        self.ledger.close()

    def dispose(self) -> None:
        """Alias for ``close()``."""
        self.close()

    def execute_with_retry(
        self,
        func: Callable[[], Any],
        operation_name: str = "db_op",
        retries: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        return self._runtime_ops.execute_with_retry(
            func,
            operation_name=operation_name,
            retries=retries,
            **kwargs,
        )

    def _snapshot_sidecar_path(self, destination: Path) -> Path:
        return snapshot_sidecar_path(destination)

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return self._snapshot_ops.snapshot_to(
            dest_path,
            checkpoint=checkpoint,
            metadata=metadata,
        )

    @staticmethod
    def _unwrap_sql_and_params(
        sql: str | ParameterizedSQLProtocol,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> tuple[str, Sequence[Any] | Mapping[str, Any] | None]:
        if isinstance(sql, ParameterizedSQLProtocol):
            if params is not None:
                raise ValueError(
                    "Cannot supply separate 'params' when passing a ParameterizedSQL query"
                )
            return (sql.sql, sql.params)
        return (sql, params)

    def query_df(
        self,
        sql: str | ParameterizedSQLProtocol,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        return self.ledger.query_df(raw_sql, raw_params)

    def query_rows(
        self,
        sql: str | ParameterizedSQLProtocol,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        model: type[_ModelT],
    ) -> list[_ModelT]:
        raw_sql, raw_params = self._unwrap_sql_and_params(sql, params)
        return self.ledger.query_rows(raw_sql, raw_params, model=model)

    def _ensure_schema_links_view(self) -> None:
        if self.read_only:
            return
        try:
            with self.ledger.exec_cursor() as cur:
                cur.execute(
                    """
                    CREATE OR REPLACE VIEW main.consist_schema_links AS
                    SELECT
                        rel.schema_id,
                        json_extract_string(src.summary_json, '$.table_name') AS from_table,
                        rel.from_field,
                        rel.to_table,
                        rel.to_field,
                        rel.relationship_type,
                        rel.cardinality
                    FROM main.artifact_schema_relation rel
                    JOIN main.artifact_schema src ON rel.schema_id = src.id
                    """
                )
        except Exception as exc:
            logging.warning("Failed to create consist_schema_links view: %s", exc)

    # ------------------------------------------------------------------
    # Run Persistence and Queries
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_run_fields(run: Run) -> None:
        if not isinstance(run.meta, dict):
            return
        if run.stage is None and isinstance(run.meta.get("stage"), str):
            run.stage = run.meta["stage"]
        elif run.stage is not None:
            run.meta["stage"] = run.stage
        if run.phase is None and isinstance(run.meta.get("phase"), str):
            run.phase = run.meta["phase"]
        elif run.phase is not None:
            run.meta["phase"] = run.phase

    def sync_run(
        self,
        run: Run,
        *,
        binding_invocation: RunBindingInvocation | None = None,
        require_success: bool = False,
    ) -> None:
        self._normalize_run_fields(run)

        def _sync() -> None:
            with self.ledger.transaction():
                self.ledger.upsert_run(run)
                if binding_invocation is not None:
                    self.ledger.record_binding_invocation(binding_invocation)

        try:
            with DB_CALL_PROFILER.track("sync_run"):
                self.execute_with_retry(_sync, operation_name="sync_run")
        except Exception as exc:
            logging.error("DB Sync Run Error: %s", exc)
            if require_success:
                raise

    def sync_run_with_links(
        self,
        run: Run,
        *,
        artifact_ids: Sequence[uuid.UUID | str],
        direction: str = "output",
        binding_invocation: RunBindingInvocation | None = None,
        require_success: bool = False,
    ) -> None:
        self._normalize_run_fields(run)
        unique_ids = [_coerce_uuid(aid) for aid in dict.fromkeys(artifact_ids)]

        def _sync() -> None:
            with self.ledger.transaction() as cur:
                self.ledger.upsert_run(run)
                if unique_ids:
                    id_strs = [str(aid) for aid in unique_ids]
                    found_rows = cur.execute(
                        "SELECT id, key, anchor FROM main.artifact "
                        "WHERE id IN (SELECT CAST(UNNEST(?) AS UUID))",
                        [id_strs],
                    ).fetchall()
                    art_info = {
                        _coerce_uuid(r[0]): (str(r[1]), str(r[2])) for r in found_rows
                    }
                    for aid in unique_ids:
                        info = art_info.get(aid)
                        if info is None:
                            logging.warning(
                                "[Consist] Cannot bind missing artifact_id=%s to run_id=%s",
                                aid,
                                run.id,
                            )
                            continue
                        art_key, art_anchor = info
                        self._insert_run_binding_checked(
                            cur,
                            run_id=run.id,
                            artifact_id=aid,
                            anchor=art_anchor,
                            artifact_key=art_key,
                            direction=direction,
                        )
                if binding_invocation is not None:
                    self.ledger.record_binding_invocation(binding_invocation)

        try:
            with DB_CALL_PROFILER.track("sync_run_with_links"):
                self.execute_with_retry(_sync, operation_name="sync_run_with_links")
        except Exception as exc:
            logging.error("DB Sync Run With Links Error: %s", exc)
            if require_success:
                raise

    def get_run(self, run_id: str) -> Optional[Run]:
        return self.execute_with_retry(
            lambda: self.ledger.get_run(run_id),
            operation_name="get_run",
        )

    def update_run_meta(
        self,
        run_id: str,
        meta_updates: Mapping[str, Any],
    ) -> None:
        def _update() -> None:
            existing = self.ledger.get_run(run_id)
            if existing is None:
                return
            merged = dict(existing.meta or {})
            merged.update(meta_updates)
            existing.meta = merged
            existing.updated_at = datetime.now(timezone.utc)
            self.ledger.upsert_run(existing)

        self.execute_with_retry(_update, operation_name="update_run_meta")

    def update_run_signature(
        self,
        run_id: str,
        signature: Optional[str],
    ) -> None:
        def _update() -> None:
            existing = self.ledger.get_run(run_id)
            if existing is None:
                return
            existing.signature = signature
            existing.updated_at = datetime.now(timezone.utc)
            self.ledger.upsert_run(existing)

        self.execute_with_retry(_update, operation_name="update_run_signature")

    def get_runs_by_parent(self, parent_run_id: str) -> List[Run]:
        def _query() -> List[Run]:
            runs = self.ledger.find_runs(parent_run_id=parent_run_id, limit=None)
            runs.sort(key=lambda r: (r.created_at, r.started_at, r.id))
            return runs

        return self.execute_with_retry(_query, operation_name="get_runs_by_parent")

    def get_recent_runs(
        self,
        limit: int = 20,
        tags: Optional[Sequence[str]] = None,
    ) -> List[Run]:
        return self.find_runs(tags=list(tags) if tags else None, limit=limit)

    def find_runs(
        self,
        tags: Optional[List[str]] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        facet: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        limit: Optional[int] = 100,
        name: Optional[str] = None,
        run_scope: Optional[str] = None,
        raise_on_error: bool = False,
        *,
        parent_run_id: Optional[str] = None,
        signature: Optional[str] = None,
        cache_status: Optional[str] = None,
        offset: int = 0,
    ) -> List[Run]:
        effective_parent = parent_run_id if parent_run_id is not None else parent_id

        try:
            return self.execute_with_retry(
                lambda: self.ledger._query_runs(
                    model=model,
                    status=status,
                    year=year,
                    iteration=iteration,
                    stage=stage,
                    phase=phase,
                    parent_run_id=effective_parent,
                    signature=signature,
                    cache_status=cache_status,
                    name=name,
                    run_scope=run_scope,
                    tags=tags,
                    facet=facet,
                    metadata=metadata,
                    limit=limit,
                    offset=offset,
                    scope_facet_to_model=True,
                    lifecycle_order=True,
                ),
                operation_name="find_runs",
            )
        except Exception as exc:
            if raise_on_error:
                raise
            logging.warning("Failed to find runs: %s", exc)
            return []

    def find_cached_run(
        self,
        signature: str,
        *,
        model_name: Optional[str] = None,
        signature_scheme: Optional[str] = None,
    ) -> Optional[Run]:
        try:
            return self.execute_with_retry(
                lambda: self.ledger.find_cached_run(
                    signature,
                    model_name=model_name,
                    signature_scheme=signature_scheme,
                ),
                operation_name="find_cached_run",
            )
        except Exception as exc:
            logging.warning("Cached run lookup failed: %s", exc)
            return None

    def find_run_by_signature(self, signature: str) -> Optional[Run]:
        return self.find_cached_run(signature)

    def find_runs_by_signature(self, signature: str) -> List[Run]:
        def _query() -> List[Run]:
            runs = self.ledger.find_runs(
                signature=signature,
                status="completed",
                cache_status="live",
                limit=None,
            )
            runs.sort(
                key=lambda r: (
                    r.ended_at or r.started_at or r.created_at,
                    r.created_at,
                    r.id,
                ),
                reverse=True,
            )
            return runs

        try:
            return self.execute_with_retry(
                _query, operation_name="cache_signature_lookup_candidates"
            )
        except Exception as exc:
            logging.warning("Cache signature candidate lookup failed: %s", exc)
            return []

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        candidates = self.find_matching_runs(config_hash, input_hash, git_hash, limit=1)
        return candidates[0] if candidates else None

    def find_matching_runs(
        self,
        config_hash: str,
        input_hash: str,
        git_hash: str,
        *,
        limit: Optional[int] = None,
    ) -> List[Run]:
        def _query() -> List[Run]:
            limit_sql = f" LIMIT {max(0, int(limit))}" if limit is not None else ""
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    f"SELECT {StoreLedger._RUN_SELECT_COLUMNS} FROM main.run "
                    "WHERE status = 'completed' AND cache_status = 'live' "
                    "AND config_hash = ? AND input_hash = ? AND git_hash = ? "
                    "ORDER BY COALESCE(ended_at, started_at, created_at) DESC, "
                    f"created_at DESC, id DESC{limit_sql}",
                    [config_hash, input_hash, git_hash],
                ).fetchall()
            return [StoreLedger._hydrate_run_row(r) for r in rows]

        try:
            return self.execute_with_retry(
                _query, operation_name="cache_lookup_candidates"
            )
        except Exception as exc:
            logging.warning("Cache candidate lookup failed: %s", exc)
            return []

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> List[Run]:
        return self.find_runs(model=model_name, status="completed", limit=limit)

    def get_history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        try:
            runs = self.find_runs(tags=tags, limit=limit)
            if not runs:
                return pd.DataFrame()
            records = [run.model_dump() for run in runs]
            df = pd.DataFrame(records)
            if "started_at" in df.columns and "ended_at" in df.columns:
                start = pd.to_datetime(df["started_at"], utc=True)
                end = pd.to_datetime(df["ended_at"], utc=True)
                df["duration_seconds"] = (end - start).dt.total_seconds()
            return df
        except Exception as exc:
            logging.warning("Failed to fetch history: %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Binding Invocations
    # ------------------------------------------------------------------

    def record_binding_invocation(
        self,
        invocation: RunBindingInvocation | None = None,
        *,
        invocation_id: uuid.UUID | None = None,
        requested_run_id: str | None = None,
        execution_run_id: str | None = None,
        cache_source_run_id: str | None = None,
        cache_outcome: str | None = None,
        binding_json: str | None = None,
        created_at: datetime | None = None,
    ) -> uuid.UUID:
        return self.execute_with_retry(
            lambda: self.ledger.record_binding_invocation(
                invocation,
                invocation_id=invocation_id,
                requested_run_id=requested_run_id,
                execution_run_id=execution_run_id,
                cache_source_run_id=cache_source_run_id,
                cache_outcome=cache_outcome,
                binding_json=binding_json,
                created_at=created_at,
            ),
            operation_name="record_binding_invocation",
        )

    def get_binding_invocation(
        self, invocation_id: uuid.UUID | str
    ) -> Optional[RunBindingInvocation]:
        inv_uuid = _coerce_uuid(invocation_id)
        with self.ledger.exec_cursor() as cur:
            row = cur.execute(
                "SELECT id, requested_run_id, execution_run_id, cache_source_run_id, "
                "cache_outcome, binding_json, created_at "
                "FROM main.run_binding_invocation WHERE id = ?",
                [inv_uuid],
            ).fetchone()
        if row is None:
            return None
        return RunBindingInvocation(
            id=_coerce_uuid(row[0]),
            requested_run_id=str(row[1]),
            execution_run_id=str(row[2]),
            cache_source_run_id=str(row[3]) if row[3] is not None else None,
            cache_outcome=str(row[4]),
            binding_json=str(row[5]),
            created_at=(
                row[6] if isinstance(row[6], datetime) else datetime.now(timezone.utc)
            ),
        )

    def get_latest_binding_invocation(
        self,
        *,
        requested_run_id: str | None = None,
        execution_run_id: str | None = None,
    ) -> Optional[RunBindingInvocation]:
        items = self.ledger.list_binding_invocations(
            requested_run_id=requested_run_id,
            execution_run_id=execution_run_id,
        )
        return items[-1] if items else None

    def get_binding_invocations(
        self,
        *,
        requested_run_id: str | None = None,
        execution_run_id: str | None = None,
        cache_source_run_id: str | None = None,
        limit: int | None = None,
    ) -> List[RunBindingInvocation]:
        return self.ledger.list_binding_invocations(
            requested_run_id=requested_run_id,
            execution_run_id=execution_run_id,
            cache_source_run_id=cache_source_run_id,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Artifact & Binding Persistence and Queries
    # ------------------------------------------------------------------

    def _persist_artifact_and_hints(
        self,
        artifact: Artifact,
        *,
        facet_index: bool | None = None,
    ) -> None:
        repr_kind = _infer_repr_kind(artifact)
        basis: Literal["computed", "implied", "declared", "trusted_foreign"] = (
            "computed" if artifact.identity.verifiable else "declared"
        )
        self.ledger.record_identity(
            artifact.identity,
            repr_kind=repr_kind,
            schema_id=artifact.schema_id,
            basis=basis,
        )

        if artifact.hints:
            existing_places = {
                (
                    str(r["site"]),
                    str(r["scope_key"]) if r.get("scope_key") is not None else "",
                    str(r["mount_instance_id"])
                    if r.get("mount_instance_id") is not None
                    else "",
                    str(r["uri"]),
                    str(r["member"]) if r.get("member") is not None else "",
                )
                for r in self.ledger.locate_current(
                    [artifact.identity.anchor],
                    include_evicted=True,
                )
            }
            for obs in artifact.hints:
                aligned_obs = (
                    obs
                    if obs.identity.anchor == artifact.identity.anchor
                    else replace(
                        obs,
                        identity=artifact.identity,
                        basis="declared"
                        if not artifact.identity.verifiable
                        else obs.basis,
                    )
                )
                if aligned_obs.place_key not in existing_places:
                    obs_repr = (
                        "table"
                        if aligned_obs.location.uri.startswith("db://")
                        else (
                            "member"
                            if aligned_obs.location.member is not None
                            else (
                                "tree"
                                if isinstance(aligned_obs.witness, DirTreeStat)
                                else "file"
                            )
                        )
                    )
                    self.ledger.record_observation(
                        aligned_obs,
                        repr_kind=obs_repr,
                        schema_id=artifact.schema_id,
                    )
                    existing_places.add(aligned_obs.place_key)

        self.ledger.record_artifact(
            artifact_id=artifact.id,
            key=artifact.key,
            driver=artifact.driver,
            anchor=artifact.identity.anchor,
            run_id=artifact.run_id,
            parent_artifact_id=artifact.parent_artifact_id,
            schema_id=artifact.schema_id,
            meta=artifact.meta,
            facet_index=facet_index,
            created_at=artifact.created_at,
        )

    def _insert_run_binding_checked(
        self,
        cur: duckdb.DuckDBPyConnection,
        *,
        run_id: str,
        artifact_id: uuid.UUID,
        anchor: Digest | str,
        artifact_key: str,
        direction: str,
        slot: str | None = None,
        role: str | None = None,
        ordinal: int | None = None,
        contract_scheme: str | None = None,
    ) -> None:
        if direction not in ("input", "output", "depends_on"):
            raise ValueError(f"Unsupported binding direction: {direction!r}")

        all_run_rows = cur.execute(
            "SELECT direction, slot, ordinal, occurrence_index, artifact_id "
            "FROM main.run_binding WHERE run_id = ?",
            [run_id],
        ).fetchall()

        existing_rows = [
            (r[0], r[1], r[2])
            for r in all_run_rows
            if _coerce_uuid(r[4]) == artifact_id
        ]

        for existing_dir_raw, _existing_slot_raw, _existing_ord_raw in existing_rows:
            existing_dir = str(existing_dir_raw)
            if existing_dir != direction:
                if {existing_dir, direction} <= {"input", "output"}:
                    logging.warning(
                        "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s "
                        "as '%s' because it is already linked as '%s'. Pass-through runs "
                        "should log separate output artifacts or transform before logging outputs.",
                        artifact_id,
                        run_id,
                        direction,
                        existing_dir,
                    )
                    return

        effective_slot = slot if (slot is not None and slot) else (role or artifact_key)
        effective_role = role if role is not None else artifact_key

        for existing_dir_raw, existing_slot_raw, existing_ord_raw in existing_rows:
            if (
                str(existing_dir_raw) == direction
                and str(existing_slot_raw) == effective_slot
            ):
                if ordinal is None or int(existing_ord_raw) == int(ordinal):
                    return

        same_dir_rows = [r for r in all_run_rows if str(r[0]) == direction]
        same_slot_rows = [r for r in same_dir_rows if str(r[1]) == effective_slot]
        max_slot_ord = max((int(r[2]) for r in same_slot_rows), default=-1)

        if ordinal is not None:
            conflict_aid = next(
                (
                    _coerce_uuid(r[4])
                    for r in same_slot_rows
                    if int(r[2]) == int(ordinal)
                ),
                None,
            )
            if conflict_aid is not None:
                if conflict_aid == artifact_id:
                    return
                effective_ordinal = max_slot_ord + 1
            else:
                effective_ordinal = int(ordinal)
        else:
            effective_ordinal = max_slot_ord + 1

        occurrence_index = max((int(r[3]) for r in same_dir_rows), default=-1) + 1

        self.ledger.record_run_binding(
            run_id=run_id,
            direction=direction,  # type: ignore[arg-type]
            occurrence_index=occurrence_index,
            slot=effective_slot,
            ordinal=effective_ordinal,
            artifact_id=artifact_id,
            role=effective_role,
            anchor=anchor,
            contract_scheme=contract_scheme,
        )

    def _insert_run_binding_for_artifact_id(
        self,
        cur: duckdb.DuckDBPyConnection,
        *,
        run_id: str,
        artifact_id: uuid.UUID,
        direction: str,
        slot: str | None = None,
        role: str | None = None,
        ordinal: int | None = None,
    ) -> None:
        row = cur.execute(
            "SELECT key, anchor FROM main.artifact WHERE id = ?",
            [artifact_id],
        ).fetchone()
        if row is None:
            logging.warning(
                "[Consist] Cannot bind missing artifact_id=%s to run_id=%s",
                artifact_id,
                run_id,
            )
            return
        art_key, art_anchor = str(row[0]), str(row[1])
        self._insert_run_binding_checked(
            cur,
            run_id=run_id,
            artifact_id=artifact_id,
            anchor=art_anchor,
            artifact_key=art_key,
            direction=direction,
            slot=slot,
            role=role,
            ordinal=ordinal,
        )

    def sync_artifact(
        self,
        artifact: Artifact,
        run_id: Optional[str] = None,
        direction: str = "output",
        *,
        slot: str | None = None,
        role: str | None = None,
        ordinal: int | None = None,
        facet_index: bool | None = None,
        profile_label: Optional[str] = None,
    ) -> None:
        metric_name = (
            f"sync_artifact.{profile_label}" if profile_label else "sync_artifact"
        )

        def _sync() -> None:
            with self.ledger.transaction() as cur:
                self._persist_artifact_and_hints(artifact, facet_index=facet_index)
                if run_id is not None:
                    self._insert_run_binding_checked(
                        cur,
                        run_id=run_id,
                        artifact_id=artifact.id,
                        anchor=artifact.identity.anchor,
                        artifact_key=artifact.key,
                        direction=direction,
                        slot=slot,
                        role=role,
                        ordinal=ordinal,
                    )

        with DB_CALL_PROFILER.track(metric_name):
            self.execute_with_retry(_sync, operation_name="sync_artifact")

    def sync_artifacts(
        self,
        *,
        artifacts: Sequence[Artifact],
        run_id: Optional[str] = None,
        direction: str = "output",
    ) -> None:
        if not artifacts:
            return

        def _sync_many() -> None:
            with self.ledger.transaction() as cur:
                for artifact in artifacts:
                    self._persist_artifact_and_hints(artifact)
                    if run_id is not None:
                        self._insert_run_binding_checked(
                            cur,
                            run_id=run_id,
                            artifact_id=artifact.id,
                            anchor=artifact.identity.anchor,
                            artifact_key=artifact.key,
                            direction=direction,
                        )

        with DB_CALL_PROFILER.track("sync_artifacts"):
            self.execute_with_retry(_sync_many, operation_name="sync_artifacts")

    def upsert_artifact(self, artifact: Artifact) -> Artifact:
        def _upsert() -> Artifact:
            with self.ledger.transaction():
                self._persist_artifact_and_hints(artifact)
            loaded = self.ledger.load_artifact(artifact.id)
            return loaded if loaded is not None else artifact

        return self.execute_with_retry(_upsert, operation_name="upsert_artifact")

    def update_artifact_meta(
        self,
        artifact: Artifact | uuid.UUID | str,
        updates: Mapping[str, Any] | FrozenMeta,
        *,
        merge: bool = True,
        preserve_system_keys: bool = True,
        schema_id: str | None | _UnsetType = _UNSET,
        facet_index: bool | None = None,
        raise_on_error: bool = False,
    ) -> Artifact | None:
        def _update() -> Artifact:
            return self.ledger.update_artifact_meta(
                artifact,
                updates,
                merge=merge,
                preserve_system_keys=preserve_system_keys,
                schema_id=schema_id,
                facet_index=facet_index,
            )

        try:
            with DB_CALL_PROFILER.track("update_artifact_meta"):
                return self.execute_with_retry(
                    _update, operation_name="update_artifact_meta"
                )
        except Exception as exc:
            logging.warning("Failed to update artifact metadata in DB: %s", exc)
            if raise_on_error:
                raise
            return artifact if isinstance(artifact, Artifact) else None

    def link_artifact_to_run(
        self,
        artifact_id: uuid.UUID | str,
        run_id: str,
        direction: str = "input",
        *,
        slot: str | None = None,
        role: str | None = None,
        ordinal: int | None = None,
    ) -> None:
        aid = _coerce_uuid(artifact_id)

        def _link() -> None:
            with self.ledger.transaction() as cur:
                self._insert_run_binding_for_artifact_id(
                    cur,
                    run_id=run_id,
                    artifact_id=aid,
                    direction=direction,
                    slot=slot,
                    role=role,
                    ordinal=ordinal,
                )

        with DB_CALL_PROFILER.track("link_artifact"):
            self.execute_with_retry(_link, operation_name="link_artifact")

    def link_artifacts_to_run_bulk(
        self,
        *,
        artifact_ids: Sequence[uuid.UUID | str],
        run_id: str,
        direction: str = "input",
        roles_by_id: Optional[
            Mapping[uuid.UUID, tuple[str, str | None, int | None]]
        ] = None,
    ) -> None:
        unique_ids = [_coerce_uuid(aid) for aid in dict.fromkeys(artifact_ids)]
        if not unique_ids:
            return

        def _link_bulk() -> None:
            with self.ledger.transaction() as cur:
                for aid in unique_ids:
                    slot_override: str | None = None
                    role_override: str | None = None
                    ord_override: int | None = None
                    if roles_by_id is not None and aid in roles_by_id:
                        slot_override, role_override, ord_override = roles_by_id[aid]
                    self._insert_run_binding_for_artifact_id(
                        cur,
                        run_id=run_id,
                        artifact_id=aid,
                        direction=direction,
                        slot=slot_override,
                        role=role_override,
                        ordinal=ord_override,
                    )

        with DB_CALL_PROFILER.track("link_artifacts_bulk"):
            self.execute_with_retry(_link_bulk, operation_name="link_artifacts_bulk")

    def get_artifact(
        self,
        key_or_id: str | uuid.UUID,
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Artifact]:
        def _query() -> Optional[Artifact]:
            parsed_uuid = _try_parse_uuid(key_or_id)
            if parsed_uuid is not None and run_id is not None:
                loaded = self.ledger.load_artifact(parsed_uuid)
                if loaded is None:
                    return None
                if loaded.run_id == run_id:
                    return loaded
                with self.ledger.exec_cursor() as cur:
                    bound = cur.execute(
                        "SELECT 1 FROM main.run_binding WHERE run_id = ? AND artifact_id = ? LIMIT 1",
                        [run_id, parsed_uuid],
                    ).fetchone()
                return loaded if bound is not None else None
            return self.ledger.get_artifact(key_or_id, run_id=run_id)

        try:
            return self.execute_with_retry(_query, operation_name="get_artifact")
        except Exception:
            return None

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts:
        try:
            return self.execute_with_retry(
                lambda: self.ledger.get_artifacts_for_run(run_id),
                operation_name="get_artifacts_for_run",
            )
        except Exception:
            return RunArtifacts()

    def get_output_artifacts_for_run(self, run_id: str) -> List[Artifact]:
        def _query() -> List[Artifact]:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT DISTINCT a.id, a.key, a.driver, a.anchor, a.run_id, "
                    "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                    "FROM main.artifact a "
                    "JOIN main.run_binding rb ON rb.artifact_id = a.id "
                    "WHERE rb.run_id = ? AND rb.direction = 'output' "
                    "ORDER BY a.created_at ASC, a.key ASC, a.id ASC",
                    [run_id],
                ).fetchall()
            return self.ledger._hydrate_artifact_rows(rows)

        try:
            return self.execute_with_retry(
                _query, operation_name="get_output_artifacts_for_run"
            )
        except Exception:
            return []

    def get_output_artifacts_by_run_ids(
        self,
        run_ids: Sequence[str],
        *,
        keys: Optional[Sequence[str]] = None,
    ) -> Tuple[Dict[str, Run], Dict[str, Dict[str, Artifact]]]:
        unique_run_ids = list(dict.fromkeys(run_ids))
        if not unique_run_ids:
            return {}, {}
        key_filter = list(dict.fromkeys(keys)) if keys is not None else None
        if key_filter is not None and not key_filter:
            return {}, {rid: {} for rid in unique_run_ids}

        def _query() -> Tuple[Dict[str, Run], Dict[str, Dict[str, Artifact]]]:
            with self.ledger.exec_cursor() as cur:
                run_rows = cur.execute(
                    f"SELECT {StoreLedger._RUN_SELECT_COLUMNS} FROM main.run "
                    "WHERE id IN (SELECT UNNEST(?))",
                    [unique_run_ids],
                ).fetchall()
                runs_by_id = {
                    run.id: run
                    for run in (StoreLedger._hydrate_run_row(r) for r in run_rows)
                }

                key_clause = ""
                params: list[Any] = [unique_run_ids]
                if key_filter is not None:
                    key_clause = (
                        " AND COALESCE(rb.role, rb.slot, a.key) IN (SELECT UNNEST(?))"
                    )
                    params.append(key_filter)

                bind_rows = cur.execute(
                    "SELECT rb.run_id, COALESCE(rb.role, rb.slot, a.key) AS bind_key, "
                    "a.id, a.key, a.driver, a.anchor, a.run_id, "
                    "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                    "FROM main.run_binding rb "
                    "JOIN main.artifact a ON a.id = rb.artifact_id "
                    "WHERE rb.run_id IN (SELECT UNNEST(?)) "
                    f"AND rb.direction = 'output'{key_clause} "
                    "ORDER BY rb.occurrence_index ASC, rb.ordinal ASC, a.created_at ASC",
                    params,
                ).fetchall()

            outputs_by_run: Dict[str, Dict[str, Artifact]] = {
                rid: {} for rid in unique_run_ids
            }
            if bind_rows:
                art_tuples = [r[2:] for r in bind_rows]
                hydrated = self.ledger._hydrate_artifact_rows(art_tuples)
                for (linked_run_id, bind_key, *_), art in zip(bind_rows, hydrated):
                    rid_str = str(linked_run_id)
                    if rid_str in outputs_by_run:
                        outputs_by_run[rid_str][str(bind_key)] = art
            return runs_by_id, outputs_by_run

        try:
            return self.execute_with_retry(
                _query, operation_name="get_output_artifacts_by_run_ids"
            )
        except Exception as exc:
            logging.warning(
                "[Consist] Failed to bulk-fetch output artifacts for runs: %s", exc
            )
            return {}, {rid: {} for rid in unique_run_ids}

    def get_child_artifacts(self, parent_id: uuid.UUID | str) -> List[Artifact]:
        parent_uuid = _coerce_uuid(parent_id)

        def _query() -> List[Artifact]:
            direct = self.ledger.get_child_artifacts(parent_uuid)
            with self.ledger.exec_cursor() as cur:
                legacy_rows = cur.execute(
                    "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                    "schema_id, meta, created_at FROM main.artifact "
                    "WHERE parent_artifact_id IS NULL "
                    "AND json_extract_string(meta, '$.parent_id') = ? "
                    "ORDER BY created_at ASC, key ASC, id ASC",
                    [str(parent_uuid)],
                ).fetchall()
            legacy = (
                self.ledger._hydrate_artifact_rows(legacy_rows) if legacy_rows else []
            )
            combined = {art.id: art for art in direct}
            for art in legacy:
                combined.setdefault(art.id, art)
            return sorted(
                combined.values(),
                key=lambda a: (
                    a.created_at or datetime.min.replace(tzinfo=timezone.utc),
                    a.key,
                    a.id,
                ),
            )

        try:
            return self.execute_with_retry(_query, operation_name="get_child_artifacts")
        except Exception:
            return []

    def get_parent_artifact(self, child_id: uuid.UUID | str) -> Optional[Artifact]:
        child_uuid = _coerce_uuid(child_id)

        def _query() -> Optional[Artifact]:
            child = self.ledger.load_artifact(child_uuid)
            if child is None:
                return None
            parent_uuid = child.parent_artifact_id
            if parent_uuid is None:
                raw_parent = child.meta.get("parent_id")
                parent_uuid = _try_parse_uuid(raw_parent)
            if parent_uuid is None:
                return None
            return self.ledger.load_artifact(parent_uuid)

        try:
            return self.execute_with_retry(_query, operation_name="get_parent_artifact")
        except Exception:
            return None

    def find_artifacts(
        self,
        *,
        creator: Optional[str] = None,
        consumer: Optional[str] = None,
        key: Optional[str] = None,
        driver: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        def _query() -> List[Artifact]:
            clauses: list[str] = []
            params: list[Any] = []
            if creator:
                clauses.append(
                    "EXISTS (SELECT 1 FROM main.run_binding out_rb "
                    "WHERE out_rb.artifact_id = a.id AND out_rb.run_id = ? "
                    "AND out_rb.direction = 'output')"
                )
                params.append(creator)
            if consumer:
                clauses.append(
                    "EXISTS (SELECT 1 FROM main.run_binding in_rb "
                    "WHERE in_rb.artifact_id = a.id AND in_rb.run_id = ? "
                    "AND in_rb.direction IN ('input', 'depends_on'))"
                )
                params.append(consumer)
            if key is not None:
                clauses.append("a.key = ?")
                params.append(key)
            if driver is not None:
                clauses.append("a.driver = ?")
                params.append(driver)

            where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                    "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                    f"FROM main.artifact a{where_sql} "
                    "ORDER BY a.created_at DESC, a.id DESC LIMIT ?",
                    [*params, max(0, int(limit))],
                ).fetchall()
            return self.ledger._hydrate_artifact_rows(rows)

        try:
            return self.execute_with_retry(_query, operation_name="find_artifacts")
        except Exception:
            return []

    def _find_latest_at_uri_internal(
        self,
        uri: str,
        *,
        run_id: Optional[str] = None,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        include_inputs: bool = True,
    ) -> Optional[Artifact]:
        eff_member = member if member is not None else (table_path or array_path)
        clauses = ["al.uri = ?"]
        params: list[Any] = [uri]
        if run_id is not None:
            clauses.append("a.run_id = ?")
            params.append(run_id)
        elif not include_inputs:
            clauses.append("a.run_id IS NOT NULL")
        if driver is not None:
            clauses.append("a.driver = ?")
            params.append(driver)
        if eff_member is not None:
            clauses.append("al.member = ?")
            params.append(eff_member)
        else:
            clauses.append("al.member IS NULL")

        with self.ledger.exec_cursor() as cur:
            row = cur.execute(
                "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                "FROM main.artifact a "
                "JOIN main.artifact_location_current al ON al.anchor = a.anchor "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY a.created_at DESC, al.observed_at_ns DESC LIMIT 1",
                params,
            ).fetchone()
        if row is None:
            return None
        return self.ledger._hydrate_artifact_row(row)

    def get_artifact_by_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        *,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        include_inputs: bool = True,
    ) -> Optional[Artifact]:
        try:
            return self.execute_with_retry(
                lambda: self._find_latest_at_uri_internal(
                    uri,
                    run_id=run_id,
                    driver=driver,
                    member=member,
                    table_path=table_path,
                    array_path=array_path,
                    include_inputs=include_inputs,
                ),
                operation_name="get_artifact_by_uri",
            )
        except Exception:
            return None

    def find_latest_artifact_at_uri(
        self,
        uri: str,
        *,
        driver: Optional[str] = None,
        member: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        include_inputs: bool = True,
    ) -> Optional[Artifact]:
        return self.get_artifact_by_uri(
            uri,
            driver=driver,
            member=member,
            table_path=table_path,
            array_path=array_path,
            include_inputs=include_inputs,
        )

    def find_latest_artifacts_at_uris(
        self,
        uris: Sequence[Any],
        *,
        driver: Optional[str] = None,
        include_inputs: bool = True,
    ) -> Dict[Any, Artifact]:
        if not uris:
            return {}
        has_tuple_keys = any(isinstance(item, tuple) for item in uris)
        normalized_lookups: list[tuple[Any, str, Optional[str], Optional[str]]] = []
        seen_keys: set[Any] = set()
        unique_uri_strs: list[str] = []
        seen_uri_strs: set[str] = set()
        for item in uris:
            if item in seen_keys:
                continue
            if isinstance(item, tuple):
                u_str = str(item[0]) if len(item) >= 1 and item[0] else ""
                drv_val = (
                    str(item[1]) if len(item) >= 2 and item[1] is not None else driver
                )
                tp_val = (
                    str(item[2]) if len(item) >= 3 and item[2] is not None else None
                )
                ap_val = (
                    str(item[3]) if len(item) >= 4 and item[3] is not None else None
                )
                mem_val = tp_val if tp_val is not None else ap_val
            else:
                u_str = str(item) if item else ""
                drv_val = driver
                mem_val = None
            if not u_str:
                continue
            seen_keys.add(item)
            normalized_lookups.append((item, u_str, drv_val, mem_val))
            if u_str not in seen_uri_strs:
                seen_uri_strs.add(u_str)
                unique_uri_strs.append(u_str)

        if not normalized_lookups:
            return {}

        def _query() -> Dict[Any, Artifact]:
            run_clause = "" if include_inputs else " AND a.run_id IS NOT NULL"
            drv_clause = (
                " AND a.driver = ?"
                if (driver is not None and not has_tuple_keys)
                else ""
            )
            params: list[Any] = [unique_uri_strs]
            if driver is not None and not has_tuple_keys:
                params.append(driver)
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT al.uri, al.member, a.id, a.key, a.driver, a.anchor, "
                    "a.run_id, a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                    "FROM main.artifact a "
                    "JOIN main.artifact_location_current al ON al.anchor = a.anchor "
                    f"WHERE al.uri IN (SELECT UNNEST(?)){run_clause}{drv_clause} "
                    "ORDER BY a.created_at DESC, al.observed_at_ns DESC",
                    params,
                ).fetchall()
            if not rows:
                return {}
            matched_keys: list[Any] = []
            selected_rows: list[tuple[Any, ...]] = []
            for orig_key, exp_uri, exp_drv, exp_mem in normalized_lookups:
                for r in rows:
                    r_uri = str(r[0])
                    r_mem = str(r[1]) if r[1] is not None else None
                    r_drv = str(r[4])
                    if r_uri != exp_uri:
                        continue
                    if r_mem != exp_mem:
                        continue
                    if exp_drv is not None and r_drv != exp_drv:
                        continue
                    matched_keys.append(orig_key)
                    selected_rows.append(r[2:])
                    break
            if not selected_rows:
                return {}
            hydrated = self.ledger._hydrate_artifact_rows(selected_rows)
            return dict(zip(matched_keys, hydrated))

        try:
            return self.execute_with_retry(
                _query, operation_name="find_latest_artifacts_at_uris"
            )
        except Exception:
            return {}

    def find_latest_artifact_by_hash(
        self,
        digest_or_hash: Digest | str,
        *,
        driver: Optional[str] = None,
    ) -> Optional[Artifact]:
        raw = str(digest_or_hash).strip()
        if not raw:
            return None

        def _query() -> Optional[Artifact]:
            drv_clause = " AND a.driver = ?" if driver is not None else ""
            drv_params = [driver] if driver is not None else []
            with self.ledger.exec_cursor() as cur:
                if ":" in raw:
                    row = cur.execute(
                        "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                        "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                        "FROM main.artifact a "
                        f"WHERE a.anchor = ?{drv_clause} "
                        "ORDER BY a.created_at DESC LIMIT 1",
                        [raw, *drv_params],
                    ).fetchone()
                    if row is None:
                        scheme, value = raw.split(":", 1)
                        row = cur.execute(
                            "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                            "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                            "FROM main.artifact a "
                            "JOIN main.content_digest cd ON cd.anchor = a.anchor "
                            f"WHERE cd.scheme = ? AND cd.value = ?{drv_clause} "
                            "ORDER BY a.created_at DESC LIMIT 1",
                            [scheme, value, *drv_params],
                        ).fetchone()
                else:
                    row = cur.execute(
                        "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                        "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                        "FROM main.artifact a "
                        "JOIN main.content_digest cd ON cd.anchor = a.anchor "
                        f"WHERE cd.value = ?{drv_clause} "
                        "ORDER BY a.created_at DESC LIMIT 1",
                        [raw, *drv_params],
                    ).fetchone()
            if row is None:
                return None
            return self.ledger._hydrate_artifact_row(row)

        try:
            return self.execute_with_retry(
                _query, operation_name="find_latest_artifact_by_hash"
            )
        except Exception:
            return None

    def find_artifacts_by_anchor(self, anchor: Digest | str) -> List[Artifact]:
        anchor_str = str(anchor)

        def _query() -> List[Artifact]:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                    "schema_id, meta, created_at FROM main.artifact "
                    "WHERE anchor = ? OR anchor IN ("
                    "SELECT historical_anchor FROM main.anchor_promotion WHERE verified_anchor = ? "
                    "UNION "
                    "SELECT verified_anchor FROM main.anchor_promotion WHERE historical_anchor = ?"
                    ") ORDER BY created_at ASC, id ASC",
                    [anchor_str, anchor_str, anchor_str],
                ).fetchall()
            return self.ledger._hydrate_artifact_rows(rows)

        try:
            return self.execute_with_retry(
                _query, operation_name="find_artifacts_by_anchor"
            )
        except Exception:
            return []

    def find_runs_producing_anchor(self, anchor: Digest | str) -> List[Run]:
        anchor_str = str(anchor)

        def _query() -> List[Run]:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    f"SELECT DISTINCT {StoreLedger._RUN_SELECT_COLUMNS} FROM main.run r "
                    "WHERE r.id IN ("
                    "SELECT rb.run_id FROM main.run_binding rb "
                    "WHERE rb.direction = 'output' AND rb.anchor = ? "
                    "UNION "
                    "SELECT a.run_id FROM main.artifact a "
                    "WHERE a.run_id IS NOT NULL AND a.anchor = ?"
                    ") ORDER BY COALESCE(r.ended_at, r.started_at, r.created_at) DESC, "
                    "r.created_at DESC, r.id DESC",
                    [anchor_str, anchor_str],
                ).fetchall()
            return [StoreLedger._hydrate_run_row(r) for r in rows]

        try:
            return self.execute_with_retry(
                _query, operation_name="find_runs_producing_anchor"
            )
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Facet KV Read Views
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_config_facet_record(
        row: tuple[Any, ...],
    ) -> Optional[ConfigFacetRecord]:
        raw_meta, raw_model_name, raw_created_at = row
        parsed = (
            json.loads(raw_meta) if isinstance(raw_meta, str) and raw_meta else raw_meta
        )
        if not isinstance(parsed, Mapping):
            return None
        raw_facet = parsed.get("facet")
        if not isinstance(raw_facet, Mapping):
            return None
        fid = str(parsed.get("config_facet_id") or "")
        if not fid:
            return None
        namespace = str(
            parsed.get("config_facet_namespace")
            or parsed.get("facet_namespace")
            or raw_model_name
            or ""
        )
        schema_name = str(
            parsed.get("config_facet_schema_name")
            or parsed.get("config_facet_schema")
            or "dict"
        )
        raw_ver = parsed.get("config_facet_schema_version")
        if raw_ver is None:
            raw_ver = parsed.get("facet_schema_version")
        schema_version = str(raw_ver) if raw_ver is not None else None
        created_at = (
            raw_created_at
            if isinstance(raw_created_at, datetime)
            else datetime.now(timezone.utc)
        )
        return ConfigFacetRecord(
            id=fid,
            namespace=namespace,
            schema_name=schema_name,
            schema_version=schema_version,
            facet_json=dict(raw_facet),
            created_at=created_at,
        )

    def get_config_facet(self, facet_id: str) -> Optional[ConfigFacetRecord]:
        if not facet_id:
            return None
        try:
            with self.ledger.exec_cursor() as cur:
                row = cur.execute(
                    "SELECT meta, model_name, created_at FROM main.run "
                    "WHERE json_extract_string(meta, '$.config_facet_id') = ? "
                    "ORDER BY created_at DESC LIMIT 1",
                    [facet_id],
                ).fetchone()
            if row is None:
                return None
            return self._row_to_config_facet_record(row)
        except Exception:
            return None

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[ConfigFacetRecord]:
        clauses = ["json_extract_string(meta, '$.config_facet_id') IS NOT NULL"]
        params: list[Any] = []
        if namespace is not None:
            clauses.append(
                "COALESCE("
                "json_extract_string(meta, '$.config_facet_namespace'), "
                "json_extract_string(meta, '$.facet_namespace'), "
                "model_name"
                ") = ?"
            )
            params.append(namespace)
        if schema_name is not None:
            clauses.append(
                "COALESCE("
                "json_extract_string(meta, '$.config_facet_schema_name'), "
                "json_extract_string(meta, '$.config_facet_schema'), "
                "'dict'"
                ") = ?"
            )
            params.append(schema_name)
        sql = (
            "SELECT meta, model_name, created_at FROM main.run "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY created_at DESC"
        )
        try:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(sql, params).fetchall()
            seen: set[str] = set()
            out: List[ConfigFacetRecord] = []
            max_items = max(0, int(limit))
            for r in rows:
                if len(out) >= max_items:
                    break
                rec = self._row_to_config_facet_record(r)
                if rec is not None and rec.id not in seen:
                    seen.add(rec.id)
                    out.append(rec)
            return out
        except Exception:
            return []

    def get_run_config_kv(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ) -> List[RunConfigKV]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        if prefix:
            clauses.append(f"key LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
            params.append(f"{_escape_like_literal(prefix)}%")
        sql = (
            "SELECT run_id, facet_id, namespace, key, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM main.run_config_kv WHERE {' AND '.join(clauses)} "
            f"ORDER BY key ASC LIMIT {max(0, int(limit))}"
        )
        try:
            return self.ledger.query_rows(sql, params, model=RunConfigKV)
        except Exception:
            return []

    def get_run_config_kv_for_runs(
        self,
        run_ids: Sequence[str],
        *,
        namespace: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        limit: int = 100_000,
    ) -> List[RunConfigKV]:
        if not run_ids:
            return []
        if keys is not None and len(keys) == 0:
            return []
        clauses = ["run_id IN (SELECT UNNEST(?))"]
        params: list[Any] = [list(run_ids)]
        if namespace:
            clauses.append("namespace = ?")
            params.append(namespace)
        if keys is not None:
            clauses.append("key IN (SELECT UNNEST(?))")
            params.append(list(keys))
        sql = (
            "SELECT run_id, facet_id, namespace, key, value_type, "
            "value_str, value_num, value_bool, value_json, created_at "
            f"FROM main.run_config_kv WHERE {' AND '.join(clauses)} "
            f"ORDER BY run_id ASC, key ASC LIMIT {max(0, int(limit))}"
        )
        try:
            return self.ledger.query_rows(sql, params, model=RunConfigKV)
        except Exception:
            return []

    def get_facet_values_for_runs(
        self,
        run_ids: Sequence[str],
        *,
        key: str,
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        rows = self.get_run_config_kv_for_runs(run_ids, namespace=namespace, keys=[key])
        return {
            row.run_id: _kv_row_python_value(
                value_type=row.value_type,
                value_str=row.value_str,
                value_num=row.value_num,
                value_bool=row.value_bool,
                value_json=row.value_json,
            )
            for row in rows
        }

    def get_artifact_kv(
        self,
        artifact_id: uuid.UUID | str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ) -> List[ArtifactKV]:
        art_uuid = _coerce_uuid(artifact_id)
        clauses = ["artifact_id = ?"]
        params: list[Any] = [art_uuid]
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)
        if prefix:
            clauses.append(f"key_path LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
            params.append(f"{_escape_like_literal(prefix)}%")
        sql = (
            "SELECT artifact_id, facet_id, namespace, key_path, value_type, "
            "value_str, value_num, value_bool, created_at "
            f"FROM main.artifact_kv WHERE {' AND '.join(clauses)} "
            f"ORDER BY key_path ASC LIMIT {max(0, int(limit))}"
        )
        return self.execute_with_retry(
            lambda: self.ledger.query_rows(sql, params, model=ArtifactKV),
            operation_name="get_artifact_kv",
        )

    def get_artifact_kv_values_bulk(
        self,
        *,
        artifact_ids: Sequence[uuid.UUID | str],
        key_paths: Sequence[str],
        namespace: Optional[str] = None,
    ) -> Dict[uuid.UUID, Dict[str, Dict[str, Any]]]:
        if not artifact_ids or not key_paths:
            return {}
        id_strs = [str(_coerce_uuid(aid)) for aid in artifact_ids]
        clauses = [
            "artifact_id IN (SELECT CAST(UNNEST(?) AS UUID))",
            "key_path IN (SELECT UNNEST(?))",
        ]
        params: list[Any] = [id_strs, list(key_paths)]
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)
        sql = (
            "SELECT artifact_id, facet_id, namespace, key_path, value_type, "
            "value_str, value_num, value_bool, created_at "
            f"FROM main.artifact_kv WHERE {' AND '.join(clauses)} "
            "ORDER BY created_at DESC, facet_id DESC"
        )
        rows = self.execute_with_retry(
            lambda: self.ledger.query_rows(sql, params, model=ArtifactKV),
            operation_name="get_artifact_kv_values_bulk",
        )

        out: Dict[uuid.UUID, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            by_key = out.setdefault(row.artifact_id, {})
            if row.key_path in by_key:
                continue
            val = _kv_row_python_value(
                value_type=row.value_type,
                value_str=row.value_str,
                value_num=row.value_num,
                value_bool=row.value_bool,
                value_json=row.value_json,
            )
            by_key[row.key_path] = {"value": val, "type": row.value_type}
        return out

    def find_runs_by_facet_kv(
        self,
        *,
        namespace: str,
        key: str,
        value_type: Optional[str] = None,
        value_str: Optional[str] = None,
        value_num: Optional[float] = None,
        value_bool: Optional[bool] = None,
        limit: int = 100,
    ) -> List[Run]:
        def _query() -> List[Run]:
            kv_clauses = ["kv.run_id = r.id", "kv.namespace = ?", "kv.key = ?"]
            params: list[Any] = [namespace, key]
            if value_type is not None:
                kv_clauses.append("kv.value_type = ?")
                params.append(value_type)
            if value_str is not None:
                kv_clauses.append("kv.value_str = ?")
                params.append(value_str)
            elif value_num is not None:
                kv_clauses.append("kv.value_num = ?")
                params.append(float(value_num))
            elif value_bool is not None:
                kv_clauses.append("kv.value_bool = ?")
                params.append(value_bool)

            sql = (
                f"SELECT {StoreLedger._RUN_SELECT_COLUMNS} FROM main.run r "
                "WHERE r.status = 'completed' "
                f"AND EXISTS (SELECT 1 FROM main.run_config_kv kv WHERE {' AND '.join(kv_clauses)}) "
                "ORDER BY COALESCE(r.ended_at, r.started_at, r.created_at) DESC, "
                "r.created_at DESC, r.id DESC LIMIT ?"
            )
            params.append(max(0, int(limit)))
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(sql, params).fetchall()
            return [StoreLedger._hydrate_run_row(r) for r in rows]

        try:
            return self.execute_with_retry(
                _query, operation_name="find_runs_by_facet_kv"
            )
        except Exception as exc:
            logging.warning("Failed to find runs by facet kv: %s", exc)
            return []

    @staticmethod
    def _build_artifact_kv_exists_clauses(
        *,
        alias: str,
        predicates: Sequence[Mapping[str, Any]],
        namespace: Optional[str],
    ) -> tuple[list[str], list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for pred in predicates:
            sub = [f"kv.artifact_id = {alias}.id", "kv.key_path = ?"]
            params.append(str(pred["key_path"]))
            pred_ns = pred.get("namespace")
            eff_ns = pred_ns if pred_ns is not None else namespace
            if eff_ns is not None:
                sub.append("kv.namespace = ?")
                params.append(str(eff_ns))
            op = str(pred["op"])
            kind = str(pred["kind"])
            val = pred["value"]
            if op == "=":
                if kind == "str":
                    sub.extend(["kv.value_type = 'str'", "kv.value_str = ?"])
                    params.append(str(val))
                elif kind == "bool":
                    sub.extend(["kv.value_type = 'bool'", "kv.value_bool = ?"])
                    params.append(bool(val))
                elif kind == "null":
                    sub.append("kv.value_type = 'null'")
                else:
                    sub.extend(
                        ["kv.value_type IN ('int', 'float')", "kv.value_num = ?"]
                    )
                    params.append(float(val))
            elif op == ">=":
                sub.extend(["kv.value_type IN ('int', 'float')", "kv.value_num >= ?"])
                params.append(float(val))
            elif op == "<=":
                sub.extend(["kv.value_type IN ('int', 'float')", "kv.value_num <= ?"])
                params.append(float(val))
            clauses.append(
                f"EXISTS (SELECT 1 FROM main.artifact_kv kv WHERE {' AND '.join(sub)})"
            )
        return clauses, params

    def find_artifacts_by_facet_params(
        self,
        *,
        predicates: List[Dict[str, Any]],
        namespace: Optional[str] = None,
        key_prefix: Optional[str] = None,
        artifact_family_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        def _query() -> List[Artifact]:
            clauses: list[str] = []
            params: list[Any] = []
            if key_prefix:
                clauses.append(f"a.key LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'")
                params.append(f"{_escape_like_literal(key_prefix)}%")
            if artifact_family_prefix:
                fam_sub = [
                    "fkv.artifact_id = a.id",
                    "fkv.key_path = 'artifact_family'",
                    "fkv.value_type = 'str'",
                    f"fkv.value_str LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'",
                ]
                params.append(f"{_escape_like_literal(artifact_family_prefix)}%")
                if namespace is not None:
                    fam_sub.append("fkv.namespace = ?")
                    params.append(namespace)
                clauses.append(
                    f"EXISTS (SELECT 1 FROM main.artifact_kv fkv WHERE {' AND '.join(fam_sub)})"
                )
            pred_clauses, pred_params = self._build_artifact_kv_exists_clauses(
                alias="a",
                predicates=predicates,
                namespace=namespace,
            )
            clauses.extend(pred_clauses)
            params.extend(pred_params)

            where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                    "a.parent_artifact_id, a.schema_id, a.meta, a.created_at "
                    f"FROM main.artifact a{where_sql} "
                    "ORDER BY a.created_at DESC, a.id DESC LIMIT ?",
                    [*params, max(0, int(limit))],
                ).fetchall()
            return self.ledger._hydrate_artifact_rows(rows)

        try:
            return self.execute_with_retry(
                _query, operation_name="find_artifacts_by_facet_params"
            )
        except Exception as exc:
            logging.warning("Failed to find artifacts by facet params: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Schema Persistence, Observations, Grouped Views, and Physical FKs
    # ------------------------------------------------------------------

    def persist_artifact_schema(
        self,
        schema: ArtifactSchema,
        fields: Sequence[ArtifactSchemaField] = (),
        relations: Optional[Sequence[ArtifactSchemaRelation]] = None,
        *,
        observation: Optional[ArtifactSchemaObservation] = None,
    ) -> str:
        def _persist() -> str:
            schema_id = self.ledger.upsert_artifact_schema(
                schema,
                fields,
                relations=relations,
                observation=observation,
            )
            if relations:
                self._ensure_schema_links_view()
            return schema_id

        return self.execute_with_retry(
            _persist, operation_name="persist_artifact_schema"
        )

    def persist_artifact_schema_profile(
        self,
        *,
        artifact: Artifact,
        schema: ArtifactSchema,
        fields: Sequence[ArtifactSchemaField],
        observation: ArtifactSchemaObservation,
        meta_updates: Optional[Mapping[str, Any]] = None,
        relations: Optional[Sequence[ArtifactSchemaRelation]] = None,
    ) -> Artifact:
        def _persist() -> Artifact:
            with self.ledger.transaction():
                self.ledger.upsert_artifact_schema(
                    schema,
                    fields,
                    relations=relations,
                    observation=observation,
                )
                cleaned_meta = strip_system_meta(meta_updates or {})
                existing = self.ledger.load_artifact(
                    artifact.id, include_locations=False
                )
                if existing is not None:
                    updated = self.ledger.update_artifact_meta(
                        artifact,
                        cleaned_meta,
                        merge=True,
                        schema_id=schema.id,
                    )
                else:
                    updated = artifact.with_meta(cleaned_meta, merge=True).with_schema(
                        schema.id
                    )
            if relations:
                self._ensure_schema_links_view()
            return updated

        return self.execute_with_retry(
            _persist, operation_name="persist_artifact_schema_profile"
        )

    def persist_artifact_schema_observation_bundle(
        self,
        *,
        artifact: Artifact,
        observation: ArtifactSchemaObservation,
        meta_updates: Optional[Mapping[str, Any]] = None,
    ) -> Artifact:
        def _persist() -> Artifact:
            with self.ledger.transaction() as cur:
                obs_ts = (
                    observation.observed_at
                    if isinstance(observation.observed_at, datetime)
                    else datetime.now(timezone.utc)
                )
                cur.execute(
                    "INSERT INTO main.artifact_schema_observation ("
                    "id, artifact_id, schema_id, run_id, source, sample_rows, observed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                    [
                        _coerce_uuid(observation.id),
                        _coerce_uuid(observation.artifact_id),
                        observation.schema_id,
                        observation.run_id,
                        observation.source,
                        observation.sample_rows,
                        obs_ts,
                    ],
                )
                cleaned_meta = strip_system_meta(meta_updates or {})
                existing = self.ledger.load_artifact(
                    artifact.id, include_locations=False
                )
                if existing is not None:
                    return self.ledger.update_artifact_meta(
                        artifact,
                        cleaned_meta,
                        merge=True,
                        schema_id=observation.schema_id,
                    )
                return artifact.with_meta(cleaned_meta, merge=True).with_schema(
                    observation.schema_id
                )

        return self.execute_with_retry(
            _persist, operation_name="persist_artifact_schema_observation_bundle"
        )

    def backfill_artifact_schema_field_ordinals(self, *, schema_id: str) -> None:
        if self.read_only:
            return

        def _backfill() -> None:
            fetched = self.ledger.get_artifact_schema(
                schema_id, include_relations=False
            )
            if fetched is None:
                return
            schema_obj, field_rows = fetched
            if not field_rows or all(
                f.ordinal_position is not None for f in field_rows
            ):
                return

            ordered_names: list[str] = []
            profile = schema_obj.profile_json
            if isinstance(profile, dict):
                raw_fields = profile.get("fields")
                if isinstance(raw_fields, list):
                    for entry in raw_fields:
                        if isinstance(entry, dict) and isinstance(
                            entry.get("name"), str
                        ):
                            ordered_names.append(entry["name"])
            ordinal_by_name = {name: i + 1 for i, name in enumerate(ordered_names)}

            updated_fields: list[ArtifactSchemaField] = []
            sorted_by_name = sorted(field_rows, key=lambda r: r.name)
            for idx, fld in enumerate(sorted_by_name, start=1):
                if fld.ordinal_position is None:
                    new_ord = ordinal_by_name.get(fld.name, idx)
                    updated_fields.append(
                        fld.model_copy(update={"ordinal_position": new_ord})
                    )
                else:
                    updated_fields.append(fld)
            self.ledger.upsert_artifact_schema(schema_obj, updated_fields)

        try:
            self.execute_with_retry(
                _backfill, operation_name="backfill_artifact_schema_ordinals"
            )
        except Exception as exc:
            logging.warning(
                "Failed to backfill artifact_schema_field.ordinal_position for schema_id=%s: %s",
                schema_id,
                exc,
            )

    @overload
    def get_artifact_schema(
        self,
        *,
        schema_id: str,
        backfill_ordinals: bool = True,
        include_relations: Literal[False] = False,
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]: ...

    @overload
    def get_artifact_schema(
        self,
        *,
        schema_id: str,
        backfill_ordinals: bool = True,
        include_relations: Literal[True],
    ) -> Optional[
        Tuple[ArtifactSchema, List[ArtifactSchemaField], List[ArtifactSchemaRelation]]
    ]: ...

    def get_artifact_schema(
        self,
        schema_id: str | None = None,
        *,
        backfill_ordinals: bool = True,
        include_relations: bool = False,
    ) -> (
        Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]
        | Optional[
            Tuple[
                ArtifactSchema,
                List[ArtifactSchemaField],
                List[ArtifactSchemaRelation],
            ]
        ]
    ):
        if not schema_id:
            return None
        if include_relations:
            res_rel = self.ledger.get_artifact_schema(schema_id, include_relations=True)
            if res_rel is None:
                return None
            schema_obj, fields, relations = res_rel
            if (
                backfill_ordinals
                and not self.read_only
                and any(f.ordinal_position is None for f in fields)
            ):
                self.backfill_artifact_schema_field_ordinals(schema_id=schema_id)
                res_rel2 = self.ledger.get_artifact_schema(
                    schema_id, include_relations=True
                )
                if res_rel2 is not None:
                    schema_obj, fields, relations = res_rel2
            return (schema_obj, fields, relations)

        res = self.ledger.get_artifact_schema(schema_id, include_relations=False)
        if res is None:
            return None
        schema_obj, fields = res
        if (
            backfill_ordinals
            and not self.read_only
            and any(f.ordinal_position is None for f in fields)
        ):
            self.backfill_artifact_schema_field_ordinals(schema_id=schema_id)
            res2 = self.ledger.get_artifact_schema(schema_id, include_relations=False)
            if res2 is not None:
                schema_obj, fields = res2
        return (schema_obj, fields)

    def get_artifact_schema_relations(
        self, *, schema_id: str
    ) -> List[ArtifactSchemaRelation]:
        fetched = self.get_artifact_schema(
            schema_id=schema_id, backfill_ordinals=False, include_relations=True
        )
        if fetched is None:
            return []
        return list(fetched[2])

    def select_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID | str,
        prefer_source: Optional[SchemaProfileSource] = None,
        strict_source: bool = False,
    ) -> Optional[ArtifactSchemaSelection]:
        art_uuid = _coerce_uuid(artifact_id)

        def _query() -> Optional[ArtifactSchemaSelection]:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT id, artifact_id, schema_id, run_id, source, sample_rows, observed_at "
                    "FROM main.artifact_schema_observation WHERE artifact_id = ? "
                    "ORDER BY observed_at DESC",
                    [art_uuid],
                ).fetchall()
            if not rows:
                return None

            observations = [
                ArtifactSchemaObservation(
                    id=_coerce_uuid(r[0]),
                    artifact_id=_coerce_uuid(r[1]),
                    schema_id=str(r[2]),
                    run_id=str(r[3]) if r[3] is not None else None,
                    source=str(r[4]),
                    sample_rows=int(r[5]) if r[5] is not None else None,
                    observed_at=(
                        r[6]
                        if isinstance(r[6], datetime)
                        else datetime.now(timezone.utc)
                    ),
                )
                for r in rows
            ]
            candidate_count = len(observations)

            if strict_source and prefer_source is not None:
                explicit = next(
                    (obs for obs in observations if obs.source == prefer_source),
                    None,
                )
                if explicit is None:
                    raise ValueError(
                        f"No schema observation with source '{prefer_source}' "
                        f"for artifact '{art_uuid}'. "
                        f"Available candidates: {candidate_count}."
                    )
                return ArtifactSchemaSelection(
                    schema_id=explicit.schema_id,
                    source=explicit.source,
                    candidate_count=candidate_count,
                    selection_rule=f"explicit source={prefer_source}",
                )

            user_provided = next(
                (obs for obs in observations if obs.source == "user_provided"),
                None,
            )
            if user_provided is not None:
                return ArtifactSchemaSelection(
                    schema_id=user_provided.schema_id,
                    source=user_provided.source,
                    candidate_count=candidate_count,
                    selection_rule="user_provided override",
                )

            if prefer_source is not None:
                preferred = next(
                    (obs for obs in observations if obs.source == prefer_source),
                    None,
                )
                if preferred is not None:
                    return ArtifactSchemaSelection(
                        schema_id=preferred.schema_id,
                        source=preferred.source,
                        candidate_count=candidate_count,
                        selection_rule=f"preferred source={prefer_source}",
                    )

            for src in ("file", "duckdb"):
                fallback = next(
                    (obs for obs in observations if obs.source == src), None
                )
                if fallback is not None:
                    return ArtifactSchemaSelection(
                        schema_id=fallback.schema_id,
                        source=fallback.source,
                        candidate_count=candidate_count,
                        selection_rule="default source order file>duckdb",
                    )

            most_recent = observations[0]
            return ArtifactSchemaSelection(
                schema_id=most_recent.schema_id,
                source=most_recent.source,
                candidate_count=candidate_count,
                selection_rule="most recent observation fallback",
            )

        try:
            return self.execute_with_retry(
                _query, operation_name="select_artifact_schema_for_artifact"
            )
        except ValueError:
            raise
        except Exception:
            return None

    def get_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID | str,
        backfill_ordinals: bool = True,
        prefer_source: Optional[SchemaProfileSource] = None,
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
        selection = self.select_artifact_schema_for_artifact(
            artifact_id=artifact_id,
            prefer_source=prefer_source,
            strict_source=False,
        )
        if selection is None:
            return None
        return self.get_artifact_schema(
            schema_id=selection.schema_id,
            backfill_ordinals=backfill_ordinals,
        )

    def find_schema_observation_for_anchor(
        self,
        anchor: Digest | str,
        *,
        prefer_source: Optional[str] = None,
    ) -> Optional[Tuple[ArtifactSchemaObservation, Optional[Dict[str, Any]]]]:
        anchor_str = str(anchor)

        def _query() -> Optional[
            Tuple[ArtifactSchemaObservation, Optional[Dict[str, Any]]]
        ]:
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT o.id, o.artifact_id, o.schema_id, o.run_id, o.source, "
                    "o.sample_rows, o.observed_at, s.summary_json "
                    "FROM main.artifact_schema_observation o "
                    "JOIN main.artifact a ON a.id = o.artifact_id "
                    "JOIN main.artifact_schema s ON s.id = o.schema_id "
                    "WHERE a.anchor = ? ORDER BY o.observed_at DESC",
                    [anchor_str],
                ).fetchall()
            if not rows:
                return None

            parsed: list[tuple[ArtifactSchemaObservation, dict[str, Any]]] = []
            for r in rows:
                obs = ArtifactSchemaObservation(
                    id=_coerce_uuid(r[0]),
                    artifact_id=_coerce_uuid(r[1]),
                    schema_id=str(r[2]),
                    run_id=str(r[3]) if r[3] is not None else None,
                    source=str(r[4]),
                    sample_rows=int(r[5]) if r[5] is not None else None,
                    observed_at=(
                        r[6]
                        if isinstance(r[6], datetime)
                        else datetime.now(timezone.utc)
                    ),
                )
                raw_sum = r[7]
                summary = (
                    json.loads(raw_sum)
                    if isinstance(raw_sum, str) and raw_sum
                    else (dict(raw_sum) if isinstance(raw_sum, Mapping) else {})
                )
                parsed.append((obs, summary))

            for target_src in (
                ("user_provided", prefer_source, "file", "duckdb")
                if prefer_source
                else ("user_provided", "file", "duckdb")
            ):
                if target_src is None:
                    continue
                found = next(
                    (item for item in parsed if item[0].source == target_src), None
                )
                if found is not None:
                    return found
            return parsed[0]

        try:
            return self.execute_with_retry(
                _query, operation_name="find_schema_observation_for_anchor"
            )
        except Exception:
            return None

    def backfill_artifact_schemas(self) -> int:
        if self.read_only:
            return 0
        schemas = self.ledger.list_artifact_schemas()
        count = 0
        for s in schemas:
            self.backfill_artifact_schema_field_ordinals(schema_id=s.id)
            count += 1
        return count

    @staticmethod
    def _schema_model_table_name(schema_model: Type[SQLModel]) -> str:
        raw_dict = schema_model.__dict__
        if "__tablename__" in raw_dict and isinstance(raw_dict["__tablename__"], str):
            return raw_dict["__tablename__"]
        return schema_model.__name__

    @staticmethod
    def _schema_model_field_names(schema_model: Type[SQLModel]) -> Set[str]:
        from sqlalchemy.sql.schema import Column as SAColumn

        field_names: Set[str] = set()
        for attr_name, field_info in schema_model.model_fields.items():
            sql_name = attr_name
            try:
                sa_col = field_info.sa_column  # type: ignore[attr-defined]
            except AttributeError:
                sa_col = None
            if (
                isinstance(sa_col, SAColumn)
                and isinstance(sa_col.name, str)
                and sa_col.name
            ):
                sql_name = sa_col.name
            if sql_name.startswith("consist_"):
                continue
            field_names.add(sql_name)
        return field_names

    def _load_schema_field_names(self) -> Dict[str, Set[str]]:
        with self.ledger.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT schema_id, name FROM main.artifact_schema_field"
            ).fetchall()
        fields_by_schema: Dict[str, Set[str]] = {}
        for sid, fname in rows:
            if sid is not None and fname is not None:
                fields_by_schema.setdefault(str(sid), set()).add(str(fname))
        return fields_by_schema

    def _resolve_schema_compatible_ids(
        self,
        *,
        schema_id: str,
        all_schemas: Sequence[ArtifactSchema],
        schema_fields_by_id: Dict[str, Set[str]],
    ) -> Set[str]:
        schemas_by_id = {s.id: s for s in all_schemas}
        base_schema = schemas_by_id.get(schema_id)
        if base_schema is None:
            return {schema_id}
        base_fields = schema_fields_by_id.get(schema_id, set())
        if not base_fields:
            return {schema_id}
        base_table = base_schema.summary_json.get("table_name")
        compatible_ids: Set[str] = {schema_id}
        for cand_id, cand_schema in schemas_by_id.items():
            cand_table = cand_schema.summary_json.get("table_name")
            if (
                isinstance(base_table, str)
                and isinstance(cand_table, str)
                and base_table != cand_table
            ):
                continue
            cand_fields = schema_fields_by_id.get(cand_id, set())
            if cand_fields and (
                base_fields.issubset(cand_fields) or cand_fields.issubset(base_fields)
            ):
                compatible_ids.add(cand_id)
        return compatible_ids

    def find_schema_ids_for_model(
        self, *, schema_model: Type[SQLModel], compatible: bool = True
    ) -> List[str]:
        def _query() -> List[str]:
            model_table_name = self._schema_model_table_name(schema_model)
            model_fields = self._schema_model_field_names(schema_model)
            if not model_fields:
                return []
            all_schemas = self.ledger.list_artifact_schemas()
            schema_fields_by_id = self._load_schema_field_names()
            matched: list[str] = []
            for schema_row in all_schemas:
                cand_table = schema_row.summary_json.get("table_name")
                if isinstance(cand_table, str) and cand_table != model_table_name:
                    continue
                cand_fields = schema_fields_by_id.get(schema_row.id, set())
                if not cand_fields:
                    continue
                if compatible:
                    if model_fields.issubset(cand_fields) or cand_fields.issubset(
                        model_fields
                    ):
                        matched.append(schema_row.id)
                elif cand_fields == model_fields:
                    matched.append(schema_row.id)
            return sorted(set(matched))

        try:
            return self.execute_with_retry(
                _query, operation_name="find_schema_ids_for_model"
            )
        except Exception as exc:
            logging.warning("Failed to resolve schema ids for model: %s", exc)
            return []

    def find_artifacts_for_grouped_view(
        self,
        *,
        schema_id: Optional[str] = None,
        schema_ids: Optional[List[str]] = None,
        schema_compatible: bool = False,
        predicates: Optional[List[Dict[str, Any]]] = None,
        namespace: Optional[str] = None,
        drivers: Optional[List[str]] = None,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        limit: int = 1_000_000,
    ) -> List[Tuple[Artifact, Optional[Run]]]:
        if schema_id is None and not schema_ids:
            raise ValueError("Provide schema_id or schema_ids for grouped view lookup.")
        resolved_predicates = predicates or []

        def _query() -> List[Tuple[Artifact, Optional[Run]]]:
            base_ids = list(schema_ids or [])
            if schema_id is not None:
                base_ids.append(schema_id)
            base_ids = sorted(set(base_ids))

            if schema_compatible:
                all_schemas = self.ledger.list_artifact_schemas()
                field_map = self._load_schema_field_names()
                target_schema_ids: Set[str] = set()
                for bid in base_ids:
                    target_schema_ids.update(
                        self._resolve_schema_compatible_ids(
                            schema_id=bid,
                            all_schemas=all_schemas,
                            schema_fields_by_id=field_map,
                        )
                    )
            else:
                target_schema_ids = set(base_ids)

            if not target_schema_ids:
                return []

            clauses = [
                "("
                "a.schema_id IN (SELECT UNNEST(?)) OR EXISTS ("
                "SELECT 1 FROM main.artifact_schema_observation o "
                "WHERE o.artifact_id = a.id AND o.schema_id IN (SELECT UNNEST(?))"
                ")"
                ")"
            ]
            sorted_sids = sorted(target_schema_ids)
            params: list[Any] = [sorted_sids, sorted_sids]

            if drivers:
                clauses.append("a.driver IN (SELECT UNNEST(?))")
                params.append(list(drivers))
            if run_id is not None:
                clauses.append("r.id = ?")
                params.append(run_id)
            if parent_run_id is not None:
                clauses.append("r.parent_run_id = ?")
                params.append(parent_run_id)
            if model is not None:
                clauses.append("r.model_name = ?")
                params.append(model)
            if status is not None:
                clauses.append("r.status = ?")
                params.append(status)
            if year is not None:
                clauses.append("r.year = ?")
                params.append(int(year))
            if iteration is not None:
                clauses.append("r.iteration = ?")
                params.append(int(iteration))

            pred_clauses, pred_params = self._build_artifact_kv_exists_clauses(
                alias="a",
                predicates=resolved_predicates,
                namespace=namespace,
            )
            clauses.extend(pred_clauses)
            params.extend(pred_params)

            run_cols = ", ".join(
                f"r.{c.strip()}" for c in StoreLedger._RUN_SELECT_COLUMNS.split(",")
            )
            sql = (
                "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                "a.parent_artifact_id, a.schema_id, a.meta, a.created_at, "
                f"{run_cols} "
                "FROM main.artifact a "
                "LEFT JOIN main.run r ON a.run_id = r.id "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY a.created_at ASC, a.id ASC LIMIT ?"
            )
            params.append(max(0, int(limit)))
            with self.ledger.exec_cursor() as cur:
                rows = cur.execute(sql, params).fetchall()
            if not rows:
                return []

            art_rows = [row[:9] for row in rows]
            run_rows = [row[9:] for row in rows]
            hydrated_arts = self.ledger._hydrate_artifact_rows(art_rows)
            hydrated_runs = [
                StoreLedger._hydrate_run_row(r) if r[0] is not None else None
                for r in run_rows
            ]
            return list(zip(hydrated_arts, hydrated_runs))

        try:
            return self.execute_with_retry(
                _query, operation_name="find_artifacts_for_grouped_view"
            )
        except Exception as exc:
            logging.warning("Failed to resolve grouped-view artifacts: %s", exc)
            return []

    def apply_physical_fks(self) -> List[str]:
        if self.read_only:
            return []
        self._ensure_schema_links_view()
        applied: list[str] = []
        try:
            with self.ledger.exec_cursor() as cur:
                links = cur.execute(
                    "SELECT DISTINCT from_table, from_field, to_table, to_field "
                    "FROM main.consist_schema_links "
                    "WHERE from_table IS NOT NULL AND to_table IS NOT NULL"
                ).fetchall()
                existing_tables = {
                    (str(r[0]), str(r[1]))
                    for r in cur.execute(
                        "SELECT schema_name, table_name FROM duckdb_tables()"
                    ).fetchall()
                }
                for from_tbl, from_fld, to_tbl, to_fld in links:
                    src_s = (
                        "global_tables"
                        if ("global_tables", str(from_tbl)) in existing_tables
                        else (
                            "main"
                            if ("main", str(from_tbl)) in existing_tables
                            else None
                        )
                    )
                    dst_s = (
                        "global_tables"
                        if ("global_tables", str(to_tbl)) in existing_tables
                        else (
                            "main" if ("main", str(to_tbl)) in existing_tables else None
                        )
                    )
                    if src_s is None or dst_s is None:
                        continue
                    desc = (
                        f"{src_s}.{from_tbl}({from_fld}) -> {dst_s}.{to_tbl}({to_fld})"
                    )
                    try:
                        import duckdb

                        cur.execute(
                            f"ALTER TABLE {_quote_ident(src_s)}.{_quote_ident(str(from_tbl))} "
                            f"ADD FOREIGN KEY ({_quote_ident(str(from_fld))}) "
                            f"REFERENCES {_quote_ident(dst_s)}.{_quote_ident(str(to_tbl))}({_quote_ident(str(to_fld))})"
                        )
                        applied.append(desc)
                    except duckdb.Error:
                        continue
        except Exception as exc:
            logging.warning("Failed to apply physical FKs: %s", exc)
        return applied

    # ------------------------------------------------------------------
    # Coupler Snapshots
    # ------------------------------------------------------------------

    def upsert_coupler_snapshots(
        self,
        *,
        scenario_run_id: str | None = None,
        run_id: str | None = None,
        phase: CouplerSnapshotPhase | None = None,
        step_ordinal: int = 0,
        artifacts: Optional[Mapping[str, Artifact | uuid.UUID]] = None,
        snapshots: Optional[Sequence[RunCouplerSnapshot]] = None,
    ) -> None:
        def _do_sync() -> None:
            with self.ledger.transaction():
                if artifacts:
                    for item in artifacts.values():
                        if isinstance(item, Artifact):
                            self._persist_artifact_and_hints(item)
                self.ledger.record_coupler_snapshots(
                    scenario_run_id=scenario_run_id,
                    run_id=run_id,
                    phase=phase,
                    step_ordinal=step_ordinal,
                    artifacts=artifacts,
                    snapshots=snapshots,
                )

        self.execute_with_retry(_do_sync, operation_name="upsert_coupler_snapshots")

    def sync_run_coupler_snapshot(
        self,
        *,
        scenario_run_id: str,
        run_id: str,
        phase: CouplerSnapshotPhase,
        step_ordinal: int,
        artifacts: Optional[Mapping[str, Artifact]] = None,
    ) -> None:
        self.upsert_coupler_snapshots(
            scenario_run_id=scenario_run_id,
            run_id=run_id,
            phase=phase,
            step_ordinal=step_ordinal,
            artifacts=artifacts or {},
        )

    def get_coupler_snapshots(
        self,
        run_id: str | None = None,
        *,
        scenario_run_id: str | None = None,
        phase: CouplerSnapshotPhase | None = "post",
        must_exist: bool = False,
    ) -> List[RunCouplerSnapshot]:
        return self.execute_with_retry(
            lambda: self.ledger.get_coupler_snapshots(
                run_id,
                scenario_run_id=scenario_run_id,
                phase=phase,
                must_exist=must_exist,
            ),
            operation_name="get_coupler_snapshots",
        )

    def get_run_coupler_snapshot_details(
        self,
        run_id: str,
        phase: CouplerSnapshotPhase = "post",
        *,
        must_exist: bool = False,
    ) -> Tuple[Dict[str, Artifact], int]:
        def _do_get() -> Tuple[Dict[str, Artifact], int]:
            snaps = self.ledger.get_coupler_snapshots(
                run_id, phase=phase, must_exist=must_exist
            )
            if not snaps:
                return {}, 0
            max_ord = max(s.step_ordinal for s in snaps)
            with self.ledger.exec_cursor() as cur:
                art_rows = cur.execute(
                    "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                    "schema_id, meta, created_at FROM main.artifact "
                    "WHERE id IN (SELECT CAST(UNNEST(?) AS UUID))",
                    [[str(s.artifact_id) for s in snaps]],
                ).fetchall()
            hydrated_by_id = {
                art.id: art for art in self.ledger._hydrate_artifact_rows(art_rows)
            }
            result_map: Dict[str, Artifact] = {}
            for s in snaps:
                art = hydrated_by_id.get(s.artifact_id)
                if art is not None:
                    result_map[s.role_key] = art
            if not result_map and must_exist:
                raise KeyError(
                    f"No coupler snapshot found for run_id={run_id!r}, phase={phase!r}"
                )
            return result_map, max_ord

        return self.execute_with_retry(
            _do_get, operation_name="get_run_coupler_snapshot_details"
        )

    def get_run_coupler_snapshot(
        self,
        run_id: str,
        phase: CouplerSnapshotPhase = "post",
        *,
        must_exist: bool = False,
    ) -> Dict[str, Artifact]:
        artifacts, _ = self.get_run_coupler_snapshot_details(
            run_id=run_id, phase=phase, must_exist=must_exist
        )
        return artifacts

    def get_max_coupler_snapshot_ordinal(self, scenario_run_id: str) -> int:
        def _do_get() -> int:
            with self.ledger.exec_cursor() as cur:
                row = cur.execute(
                    "SELECT COALESCE(MAX(step_ordinal), 0) "
                    "FROM main.run_coupler_snapshot WHERE scenario_run_id = ?",
                    [scenario_run_id],
                ).fetchone()
            return int(row[0]) if row is not None and row[0] is not None else 0

        return self.execute_with_retry(
            _do_get, operation_name="get_max_coupler_snapshot_ordinal"
        )

    def find_latest_scenario_coupler_snapshot(
        self,
        scenario_run_id: str,
        *,
        phase: Optional[CouplerSnapshotPhase] = "post",
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
    ) -> Optional[Tuple[str, int, Dict[str, Artifact]]]:
        def _do_find() -> Optional[Tuple[str, int, Dict[str, Artifact]]]:
            clauses = ["cs.scenario_run_id = ?"]
            params: list[Any] = [scenario_run_id]
            if phase is not None:
                clauses.append("cs.phase = ?")
                params.append(phase)
            if year is not None:
                clauses.append("r.year = ?")
                params.append(int(year))
            if iteration is not None:
                clauses.append("r.iteration = ?")
                params.append(int(iteration))
            if stage is not None:
                clauses.append("r.stage = ?")
                params.append(stage)
            if model_name is not None:
                clauses.append("r.model_name = ?")
                params.append(model_name)
            if before_step_ordinal is not None:
                clauses.append("cs.step_ordinal < ?")
                params.append(int(before_step_ordinal))

            with self.ledger.exec_cursor() as cur:
                row = cur.execute(
                    "SELECT cs.run_id, cs.phase, cs.step_ordinal "
                    "FROM main.run_coupler_snapshot cs "
                    "LEFT JOIN main.run r ON cs.run_id = r.id "
                    f"WHERE {' AND '.join(clauses)} "
                    "ORDER BY cs.step_ordinal DESC, cs.updated_at DESC LIMIT 1",
                    params,
                ).fetchone()
            if row is None:
                return None
            matched_run_id = str(row[0])
            raw_phase = str(row[1])
            matched_phase: CouplerSnapshotPhase = (
                raw_phase if raw_phase in ("init", "pre", "post") else "post"
            )
            matched_ordinal = int(row[2])
            artifacts_map, _ = self.get_run_coupler_snapshot_details(
                matched_run_id, phase=matched_phase, must_exist=False
            )
            return (matched_run_id, matched_ordinal, artifacts_map)

        return self.execute_with_retry(
            _do_find, operation_name="find_latest_scenario_coupler_snapshot"
        )

    def synthesize_coupler_snapshot_from_history(
        self,
        *,
        scenario_run_id: Optional[str] = None,
        run_id: Optional[str] = None,
        phase: CouplerSnapshotPhase = "post",
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        model_name: Optional[str] = None,
        before_step_ordinal: Optional[int] = None,
        backfill: bool = True,
    ) -> Tuple[Dict[str, Artifact], int]:
        def _do_synthesize() -> Tuple[
            Dict[str, Artifact], int, Optional[str], Optional[str], bool
        ]:
            target_run = self.ledger.get_run(run_id) if run_id is not None else None
            if run_id is not None and target_run is None:
                return {}, 0, None, None, False

            if target_run is not None:
                if target_run.parent_run_id is not None:
                    parent_id = target_run.parent_run_id
                    backfill_scenario_id = target_run.parent_run_id
                    backfill_run_id: Optional[str] = target_run.id
                    is_child_target = True
                else:
                    parent_id = target_run.id
                    backfill_scenario_id = target_run.id
                    backfill_run_id = target_run.id
                    is_child_target = False
            else:
                if scenario_run_id is None:
                    return {}, 0, None, None, False
                parent_id = scenario_run_id
                backfill_scenario_id = scenario_run_id
                backfill_run_id = None
                is_child_target = False

            if not parent_id:
                return {}, 0, None, None, False

            all_child_runs = self.get_runs_by_parent(parent_id)
            target_index: Optional[int] = None
            if is_child_target and run_id is not None:
                for idx, r in enumerate(all_child_runs):
                    if r.id == run_id:
                        target_index = idx
                        break

            can_backfill = (
                year is None
                and iteration is None
                and stage is None
                and model_name is None
                and before_step_ordinal is None
            )

            if is_child_target and target_index is not None:
                inferred_ordinal = target_index + 1
                candidate_runs = (
                    all_child_runs[:target_index]
                    if phase == "pre"
                    else all_child_runs[: target_index + 1]
                )
            else:
                candidate_runs = all_child_runs
                inferred_ordinal = 0

            candidate_ids = {r.id for r in candidate_runs}
            selected_runs: list[Run] = []
            for idx, r in enumerate(all_child_runs):
                if r.id not in candidate_ids or r.status != "completed":
                    continue
                ord_val = idx + 1
                if before_step_ordinal is not None and ord_val >= before_step_ordinal:
                    continue
                if year is not None and r.year != year:
                    continue
                if iteration is not None and r.iteration != iteration:
                    continue
                if stage is not None and r.stage != stage:
                    continue
                if model_name is not None and r.model_name != model_name:
                    continue
                selected_runs.append(r)

            if not (is_child_target and target_index is not None):
                if selected_runs:
                    last_run_id = selected_runs[-1].id
                    inferred_ordinal = next(
                        (
                            idx + 1
                            for idx, r in enumerate(all_child_runs)
                            if r.id == last_run_id
                        ),
                        len(selected_runs),
                    )
                else:
                    inferred_ordinal = 0

            if not selected_runs:
                return (
                    {},
                    inferred_ordinal,
                    backfill_scenario_id,
                    backfill_run_id,
                    can_backfill,
                )

            selected_run_ids = [r.id for r in selected_runs]
            _, outputs_by_run = self.get_output_artifacts_by_run_ids(selected_run_ids)
            artifacts: Dict[str, Artifact] = {}
            for rid in selected_run_ids:
                for role_key, art in outputs_by_run.get(rid, {}).items():
                    if art.key:
                        artifacts[art.key] = art
                    elif role_key:
                        artifacts[role_key] = art

            return (
                artifacts,
                inferred_ordinal,
                backfill_scenario_id,
                backfill_run_id,
                can_backfill,
            )

        (
            artifacts,
            inferred_ordinal,
            backfill_scenario_id,
            backfill_run_id,
            can_backfill,
        ) = self.execute_with_retry(
            _do_synthesize,
            operation_name="synthesize_coupler_snapshot_from_history",
        )

        if (
            not self.read_only
            and backfill
            and can_backfill
            and backfill_run_id is not None
            and artifacts
            and backfill_scenario_id is not None
        ):
            self.sync_run_coupler_snapshot(
                scenario_run_id=backfill_scenario_id,
                run_id=backfill_run_id,
                phase=phase,
                step_ordinal=inferred_ordinal,
                artifacts=artifacts,
            )

        return artifacts, inferred_ordinal

    def diff_coupler_snapshots(
        self,
        left_run_id: str,
        right_run_id: str,
        *,
        left_phase: CouplerSnapshotPhase = "post",
        right_phase: CouplerSnapshotPhase = "post",
    ) -> pd.DataFrame:
        left_artifacts = self.get_run_coupler_snapshot(
            left_run_id, phase=left_phase, must_exist=False
        )
        right_artifacts = self.get_run_coupler_snapshot(
            right_run_id, phase=right_phase, must_exist=False
        )
        columns = [
            "role_key",
            "status",
            "left_artifact_id",
            "right_artifact_id",
            "left_hash",
            "right_hash",
            "left_producer_run_id",
            "right_producer_run_id",
        ]
        all_keys = sorted(set(left_artifacts.keys()) | set(right_artifacts.keys()))
        rows: list[dict[str, Any]] = []
        for key in all_keys:
            la = left_artifacts.get(key)
            ra = right_artifacts.get(key)
            left_id = str(la.id) if la is not None else None
            right_id = str(ra.id) if ra is not None else None
            left_hash = str(la.identity.anchor) if la is not None else None
            right_hash = str(ra.identity.anchor) if ra is not None else None
            left_prod = la.run_id if la is not None else None
            right_prod = ra.run_id if ra is not None else None
            if la is None and ra is not None:
                status = "added"
            elif la is not None and ra is None:
                status = "removed"
            elif left_id != right_id or left_hash != right_hash:
                status = "modified"
            else:
                status = "unchanged"
            rows.append(
                {
                    "role_key": key,
                    "status": status,
                    "left_artifact_id": left_id,
                    "right_artifact_id": right_id,
                    "left_hash": left_hash,
                    "right_hash": right_hash,
                    "left_producer_run_id": left_prod,
                    "right_producer_run_id": right_prod,
                }
            )
        return pd.DataFrame(rows, columns=columns)
