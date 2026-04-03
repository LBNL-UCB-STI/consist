import time
import random
import logging
import re
import os
import uuid
import json
import shutil
import tempfile
import contextvars
from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Optional,
    List,
    Sequence,
    Callable,
    Any,
    Dict,
    Tuple,
    Set,
    Type,
    TYPE_CHECKING,
    Literal,
    Iterator,
)

import pandas as pd
from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import ConcurrentModificationError
from sqlalchemy.orm import aliased
from sqlalchemy.pool import NullPool
from sqlmodel import create_engine, Session, select, SQLModel, col, delete

from consist.core.schema_compat import (
    apply_content_identity_compatibility,
    apply_run_stage_phase_compatibility,
    backfill_artifact_content_ids as compat_backfill_artifact_content_ids,
)
from consist.core.facet_common import flatten_facet_values
from consist.models.artifact import Artifact, ArtifactContent
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import (
    Run,
    RunArtifactLink,
    resolve_canonical_run_meta_field,
)
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import ConsistRecord
    from consist.models.run import Run as RunModel


MAX_JSON_DEPTH = 50
_RETRYABLE_DB_ERROR_MARKERS = (
    "database is locked",
    "database is busy",
    "io error",
    "lock",
    "already active",
    "already open",
    "another connection",
    "another process",
)
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

SchemaProfileSource = Literal["file", "duckdb", "user_provided"]


@dataclass(frozen=True)
class ArtifactSchemaSelection:
    schema_id: str
    source: str
    candidate_count: int
    selection_rule: str


def _is_retryable_db_error(message: str) -> bool:
    normalized = message.lower()
    return any(marker in normalized for marker in _RETRYABLE_DB_ERROR_MARKERS)


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _SAFE_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
            "and the identifier must not start with a number."
        )
    return identifier


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_qualified_identifier(name: str, *, label: str) -> str:
    parts = name.split(".")
    if not parts or any(part == "" for part in parts):
        raise ValueError(f"Invalid {label}: empty identifier component.")
    safe_parts = [_validate_identifier(part, label=label) for part in parts]
    return ".".join(_quote_ident(part) for part in safe_parts)


def _assert_json_depth(obj: Any, *, max_depth: int) -> None:
    stack: list[tuple[Any, int]] = [(obj, 1)]
    while stack:
        value, depth = stack.pop()
        if depth > max_depth:
            raise ValueError(f"JSON nesting depth exceeds limit of {max_depth}.")
        if isinstance(value, dict):
            stack.extend((v, depth + 1) for v in value.values())
        elif isinstance(value, list):
            stack.extend((v, depth + 1) for v in value)


def load_json_safe(json_string: str, *, max_depth: int = MAX_JSON_DEPTH) -> Any:
    try:
        parsed = json.loads(json_string)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    _assert_json_depth(parsed, max_depth=max_depth)
    return parsed


class DatabaseManager:
    """
    Service responsible for Database Persistence.

    It handles:
    1. Connection pooling (Engine creation).

    2. Schema initialization.

    3. Concurrency control (Retries/Locking).

    4. CRUD operations for Runs and Artifacts.
    """

    def __init__(
        self,
        db_path: str,
        *,
        lock_retries: int = 20,
        lock_base_sleep_seconds: float = 0.1,
        lock_max_sleep_seconds: float = 2.0,
    ):
        self.db_path = db_path
        self._lock_retries = max(1, int(lock_retries))
        self._lock_base_sleep_seconds = max(0.0, float(lock_base_sleep_seconds))
        self._lock_max_sleep_seconds = max(0.0, float(lock_max_sleep_seconds))
        # Using NullPool ensures the file lock is released when the session closes
        self.engine = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)
        self._session_ctx: contextvars.ContextVar[Session | None] = (
            contextvars.ContextVar("consist_session", default=None)
        )
        self._init_schema()

    def _init_schema(self):
        """Creates tables if they don't exist, wrapped in retry logic."""

        def _create():
            SQLModel.metadata.create_all(
                self.engine,
                tables=[
                    getattr(Run, "__table__"),
                    getattr(Artifact, "__table__"),
                    getattr(RunArtifactLink, "__table__"),
                    getattr(ConfigFacet, "__table__"),
                    getattr(RunConfigKV, "__table__"),
                    getattr(ArtifactFacet, "__table__"),
                    getattr(ArtifactKV, "__table__"),
                    getattr(ArtifactSchema, "__table__"),
                    getattr(ArtifactSchemaField, "__table__"),
                    getattr(ArtifactSchemaObservation, "__table__"),
                    getattr(ArtifactSchemaRelation, "__table__"),
                    getattr(ArtifactContent, "__table__"),
                ],
            )
            # DuckDB self-referential FK on run.parent_run_id blocks status updates.
            self._relax_run_parent_fk()
            # Lightweight compatibility hooks for older DBs. Keep these isolated so
            # they can be removed without changing steady-state persistence behavior.
            apply_content_identity_compatibility(self)
            apply_run_stage_phase_compatibility(self)
            self._ensure_artifact_schema_field_ordinal_position()
            self._ensure_schema_links_view()

        self.execute_with_retry(_create, operation_name="init_schema")

    def _table_has_column(self, *, table_name: str, column_name: str) -> bool:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name or ""):
            return False
        try:
            with self.engine.begin() as conn:
                rows = conn.exec_driver_sql(
                    f"PRAGMA table_info('{table_name}')"
                ).fetchall()
            # DuckDB pragma_table_info returns: (cid, name, type, notnull, dflt_value, pk)
            return any(str(row[1]) == column_name for row in rows)
        except Exception:
            return False

    def _table_has_index_on_column(self, *, table_name: str, column_name: str) -> bool:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name or ""):
            return False
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column_name or ""):
            return False
        try:
            with self.engine.begin() as conn:
                rows = conn.exec_driver_sql(
                    """
                    SELECT expressions
                    FROM duckdb_indexes()
                    WHERE table_name = ?
                    """,
                    (table_name,),
                ).fetchall()
            target_expression = f"[{column_name}]"
            return any(str(row[0]) == target_expression for row in rows)
        except Exception:
            return False

    def _ensure_artifact_schema_field_ordinal_position(self) -> None:
        """
        Ensure `artifact_schema_field.ordinal_position` exists.

        SQLModel metadata changes do not automatically migrate existing DuckDB DBs,
        so we apply small additive migrations here.
        """
        if self._table_has_column(
            table_name="artifact_schema_field", column_name="ordinal_position"
        ):
            return
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(
                    "ALTER TABLE artifact_schema_field ADD COLUMN ordinal_position INTEGER"
                )
        except Exception as e:
            logging.warning(
                "Failed to add artifact_schema_field.ordinal_position column: %s", e
            )

    @staticmethod
    def _sync_run_stage_phase(run: Run) -> None:
        """Keep `Run.stage`/`Run.phase` aligned with mirrored JSON metadata."""
        if run.meta is None:
            run.meta = {}
        if not isinstance(run.meta, dict):
            return

        for field in ("stage", "phase"):
            value = resolve_canonical_run_meta_field(run, field)
            if value is not None:
                setattr(run, field, value)
                run.meta[field] = value
                continue

            if field not in run.meta:
                continue

            meta_value = run.meta.get(field)
            if isinstance(meta_value, str) or meta_value is None:
                setattr(run, field, meta_value)

    def _ensure_schema_links_view(self) -> None:
        """Create or replace the portable schema link view."""
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(
                    """
                    CREATE OR REPLACE VIEW consist_schema_links AS
                    SELECT
                        rel.schema_id,
                        src.summary_json->>'$.table_name' AS from_table,
                        rel.from_field,
                        rel.to_table,
                        rel.to_field,
                        rel.relationship_type,
                        rel.cardinality
                    FROM artifact_schema_relation rel
                    JOIN artifact_schema src ON rel.schema_id = src.id
                    """
                )
        except Exception as e:
            logging.warning("Failed to create consist_schema_links view: %s", e)

    def backfill_artifact_schema_field_ordinals(self, *, schema_id: str) -> None:
        """
        Best-effort backfill of missing `ordinal_position` for a schema.

        Preference order:
        1) `ArtifactSchema.profile_json["fields"]` order when present and non-empty.
        2) Alphabetical by `ArtifactSchemaField.name`.
        """

        def _backfill():
            with self.session_scope() as session:
                schema = session.get(ArtifactSchema, schema_id)
                if schema is None:
                    return

                field_rows = session.exec(
                    select(ArtifactSchemaField)
                    .where(ArtifactSchemaField.schema_id == schema_id)
                    .order_by(ArtifactSchemaField.name)
                ).all()
                if not field_rows:
                    return
                if all(row.ordinal_position is not None for row in field_rows):
                    return

                ordered_names: list[str] = []
                profile = getattr(schema, "profile_json", None)
                if isinstance(profile, dict):
                    fields = profile.get("fields")
                    if isinstance(fields, list) and fields:
                        for entry in fields:
                            if isinstance(entry, dict) and isinstance(
                                entry.get("name"), str
                            ):
                                ordered_names.append(entry["name"])

                ordinal_by_name: Dict[str, int] = {}
                if ordered_names:
                    ordinal_by_name = {
                        name: i + 1 for i, name in enumerate(ordered_names)
                    }

                changed = False
                if ordinal_by_name:
                    for row in field_rows:
                        if row.ordinal_position is None:
                            ord_val = ordinal_by_name.get(row.name)
                            if ord_val is not None:
                                row.ordinal_position = ord_val
                                changed = True
                else:
                    for i, row in enumerate(field_rows, start=1):
                        if row.ordinal_position is None:
                            row.ordinal_position = i
                            changed = True

                if changed:
                    session.add_all(field_rows)
                    session.commit()

        try:
            self.execute_with_retry(
                _backfill, operation_name="backfill_artifact_schema_ordinals"
            )
        except Exception as e:
            logging.warning(
                "Failed to backfill artifact_schema_field.ordinal_position for schema_id=%s: %s",
                schema_id,
                e,
            )

    def _relax_run_parent_fk(self):
        """Ensure run.parent_run_id FK is NOT ENFORCED (DuckDB self-FK workaround)."""
        try:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(
                    "ALTER TABLE run DROP CONSTRAINT IF EXISTS fk_run_parent"
                )
                conn.exec_driver_sql(
                    "ALTER TABLE run ADD CONSTRAINT fk_run_parent "
                    "FOREIGN KEY (parent_run_id) REFERENCES run(id) NOT ENFORCED"
                )
        except Exception as e:
            msg = str(e)
            if "Not implemented" in msg and "ALTER TABLE" in msg:
                logging.debug(
                    "DuckDB does not yet support ALTER TABLE for FK relaxation: %s",
                    msg,
                )
                return
            logging.warning(f"Failed to relax run.parent_run_id FK: {e}")

    def _atomic_copy_file(self, src: Path, dest: Path) -> None:
        """Copy a file via temp path and atomic rename in destination directory."""
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            shutil.copy2(src, temp_path)
            temp_path.replace(dest)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass

    def _atomic_write_json_file(self, payload: Dict[str, Any], dest: Path) -> None:
        """Write JSON to a temp file then atomically replace target."""
        temp_path = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.tmp"
        try:
            temp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temp_path.replace(dest)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass

    def _snapshot_sidecar_path(self, destination: Path) -> Path:
        """
        Build a sidecar metadata path paired to a snapshot DB file.

        Example: provenance.duckdb -> provenance.snapshot_meta.json
        """
        base_name = destination.stem if destination.suffix else destination.name
        return destination.with_name(f"{base_name}.snapshot_meta.json")

    def snapshot_to(
        self,
        dest_path: str | os.PathLike[str],
        checkpoint: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """
        Create a crash-safe snapshot of the configured DuckDB database.

        Parameters
        ----------
        dest_path : str | os.PathLike[str]
            Destination path for the snapshot database file.
        checkpoint : bool, default True
            If True, issue DuckDB CHECKPOINT before copying.
        metadata : Optional[Dict[str, Any]], optional
            Optional metadata payload written to a paired sidecar JSON file.

        Returns
        -------
        Path
            The destination snapshot database path.
        """
        if self.db_path == ":memory:":
            raise ValueError("Cannot snapshot an in-memory DuckDB database.")

        source_db_path = Path(self.db_path)
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if checkpoint:

            def _checkpoint() -> None:
                with self.engine.begin() as conn:
                    conn.exec_driver_sql("CHECKPOINT")

            self.execute_with_retry(_checkpoint, operation_name="snapshot_checkpoint")

        self._atomic_copy_file(source_db_path, destination)

        source_wal_path = Path(f"{source_db_path}.wal")
        destination_wal_path = Path(f"{destination}.wal")
        if checkpoint:
            try:
                if destination_wal_path.exists():
                    destination_wal_path.unlink()
            except OSError as exc:
                logging.warning(
                    "Failed to remove stale snapshot WAL at %s: %s",
                    destination_wal_path,
                    exc,
                )
        elif source_wal_path.exists():
            self._atomic_copy_file(source_wal_path, destination_wal_path)

        if metadata is not None:
            snapshot_metadata = dict(metadata)
            snapshot_metadata["snapshot_ts_utc"] = datetime.now(
                timezone.utc
            ).isoformat()
            snapshot_metadata["source_db_path"] = str(source_db_path)
            self._atomic_write_json_file(
                snapshot_metadata,
                self._snapshot_sidecar_path(destination),
            )

        return destination

    def execute_with_retry(
        self,
        func: Callable,
        operation_name: str = "db_op",
        retries: Optional[int] = None,
        **kwargs,  # <--- Add this to absorb extra arguments
    ) -> Any:
        """Executes a function with exponential backoff for DB locks."""
        default_retries = getattr(self, "_lock_retries", 20)
        base_sleep_seconds = getattr(self, "_lock_base_sleep_seconds", 0.1)
        max_sleep_seconds = getattr(self, "_lock_max_sleep_seconds", 2.0)
        retry_count = default_retries if retries is None else max(1, int(retries))
        for i in range(retry_count):
            try:
                return func()
            except (OperationalError, DatabaseError) as e:
                if _is_retryable_db_error(str(e)):
                    if i == retry_count - 1:
                        raise e
                    sleep_time = min(
                        (base_sleep_seconds * (1.5**i)) + random.uniform(0.05, 0.2),
                        max_sleep_seconds,
                    )
                    time.sleep(sleep_time)
                else:
                    raise e
        raise ConcurrentModificationError(f"Concurrency problem in {operation_name}")

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """
        Provide a Session, reusing a shared session within a command when present.

        This keeps CLI commands to a single DuckDB connection while preserving
        existing behavior for library usage.
        """
        session = self._session_ctx.get()
        if session is not None:
            yield session
            return
        session = Session(self.engine)
        token = self._session_ctx.set(session)
        try:
            yield session
        finally:
            session.close()
            self._session_ctx.reset(token)

    # --- Write Operations ---

    def update_artifact_meta(self, artifact: Artifact, updates: Dict[str, Any]) -> None:
        """Updates artifact metadata safely with retries."""

        def _update():
            with self.session_scope() as session:
                # Merge ensures we are attached to this session
                db_art = session.merge(artifact)
                # Ensure meta is a dict (handle potential None)
                current_meta = db_art.meta or {}
                current_meta.update(updates)
                db_art.meta = current_meta

                session.add(db_art)
                session.commit()
                # Update the local object to match
                artifact.meta = current_meta

        try:
            self.execute_with_retry(_update, operation_name="update_meta")
        except Exception as e:
            logging.warning(f"Failed to update artifact metadata: {e}")

    def sync_run(self, run: Run) -> None:
        """Upserts a Run object."""

        def _do_sync():
            with self.session_scope() as session:
                self._sync_run_stage_phase(run)
                # Use merge for a simple upsert. DuckDB's MERGE semantics via SQLModel
                # handle inserts and updates in one call and avoid stale state issues.
                logging.debug(
                    f"[DB sync_run] upserting run={run.id} status={run.status}"
                )
                session.merge(run)
                session.commit()
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    # Only pay for the diagnostic read-back when debug logging is enabled.
                    persisted = session.get(Run, run.id)
                    logging.debug(
                        f"[DB sync_run] persisted run={run.id} status={getattr(persisted, 'status', None)}"
                    )

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_run")
        except Exception as e:
            logging.warning("Database sync failed: %s", e)

    def update_run_meta(self, run_id: str, meta_updates: Dict[str, Any]) -> None:
        """Updates a run's meta field with a partial dict."""

        def _update():
            with self.session_scope() as session:
                db_run = session.get(Run, run_id)
                if not db_run:
                    return
                current = db_run.meta or {}
                current.update(meta_updates)
                db_run.meta = current
                self._sync_run_stage_phase(db_run)
                session.add(db_run)
                session.commit()

        try:
            self.execute_with_retry(_update, operation_name="update_run_meta")
        except Exception as e:
            logging.warning(f"Failed to update run meta for {run_id}: {e}")

    def update_run_signature(self, run_id: str, signature: str) -> None:
        """Sets the signature field for a run."""

        def _update():
            with self.session_scope() as session:
                db_run = session.get(Run, run_id)
                if not db_run:
                    return
                db_run.signature = signature
                session.add(db_run)
                session.commit()
                logging.debug(
                    "[db] update_run_signature run=%s signature=%s", run_id, signature
                )

        try:
            self.execute_with_retry(_update, operation_name="update_run_signature")
        except Exception as e:
            logging.warning(f"Failed to update signature for {run_id}: {e}")

    def upsert_config_facet(self, facet: ConfigFacet) -> None:
        """Upserts a ConfigFacet (deduped by facet.id)."""

        def _upsert():
            with self.session_scope() as session:
                session.merge(facet)
                session.commit()

        try:
            self.execute_with_retry(_upsert, operation_name="upsert_config_facet")
        except Exception as e:
            logging.warning(f"Failed to upsert config facet {facet.id}: {e}")

    def persist_config_facet_bundle(
        self,
        *,
        facet: ConfigFacet,
        kv_rows: Optional[List[RunConfigKV]] = None,
    ) -> None:
        """
        Persist a config facet row and optional flattened KV rows together.

        This keeps the common run/step facet path to a single DB transaction
        instead of one upsert plus a second KV replace/insert pass.
        """

        def _persist():
            with self.session_scope() as session:
                existing = session.get(ConfigFacet, facet.id)
                if existing is None:
                    session.add(facet)
                else:
                    session.merge(facet)

                if kv_rows:
                    combos = {
                        (row.run_id, row.facet_id, row.namespace)
                        for row in kv_rows
                        if row.run_id and row.facet_id and row.namespace
                    }
                    for run_id, facet_id, namespace in combos:
                        session.exec(
                            delete(RunConfigKV).where(
                                col(RunConfigKV.run_id) == run_id,
                                col(RunConfigKV.facet_id) == facet_id,
                                col(RunConfigKV.namespace) == namespace,
                            )
                        )
                    session.add_all(kv_rows)

                session.commit()

        try:
            self.execute_with_retry(
                _persist, operation_name="persist_config_facet_bundle"
            )
        except Exception as e:
            logging.warning(
                "Failed to persist config facet bundle facet=%s: %s",
                getattr(facet, "id", None),
                e,
            )

    def upsert_artifact_facet(self, facet: ArtifactFacet) -> None:
        """Upserts an ArtifactFacet (deduped by facet.id)."""

        def _upsert():
            with self.session_scope() as session:
                existing = session.get(ArtifactFacet, facet.id)
                if existing is None:
                    session.add(facet)
                else:
                    if (
                        existing.namespace is None
                        and isinstance(facet.namespace, str)
                        and facet.namespace
                    ):
                        existing.namespace = facet.namespace
                    if (
                        existing.schema_name is None
                        and isinstance(facet.schema_name, str)
                        and facet.schema_name
                    ):
                        existing.schema_name = facet.schema_name
                    if (
                        existing.schema_version is None
                        and facet.schema_version is not None
                    ):
                        existing.schema_version = facet.schema_version
                    session.add(existing)
                session.commit()

        try:
            self.execute_with_retry(_upsert, operation_name="upsert_artifact_facet")
        except Exception as e:
            logging.warning(f"Failed to upsert artifact facet {facet.id}: {e}")

    def persist_artifact_facet_bundle(
        self,
        *,
        artifact: Artifact,
        facet: ArtifactFacet,
        meta_updates: Dict[str, Any],
        kv_rows: Optional[List[ArtifactKV]] = None,
    ) -> None:
        """
        Persist artifact facet rows, artifact metadata, and optional KV index together.

        This is intended for the common hot path where a logged artifact attaches a
        facet payload and optional flattened KV index immediately after the artifact
        row itself has already been persisted.
        """

        def _persist():
            with self.session_scope() as session:
                db_art = session.merge(artifact)

                existing = session.get(ArtifactFacet, facet.id)
                if existing is None:
                    session.add(facet)
                else:
                    if (
                        existing.namespace is None
                        and isinstance(facet.namespace, str)
                        and facet.namespace
                    ):
                        existing.namespace = facet.namespace
                    if (
                        existing.schema_name is None
                        and isinstance(facet.schema_name, str)
                        and facet.schema_name
                    ):
                        existing.schema_name = facet.schema_name
                    if (
                        existing.schema_version is None
                        and facet.schema_version is not None
                    ):
                        existing.schema_version = facet.schema_version
                    session.add(existing)

                current_meta = db_art.meta or {}
                current_meta.update(meta_updates)
                db_art.meta = current_meta
                session.add(db_art)

                if kv_rows:
                    combos = {
                        (row.artifact_id, row.facet_id, row.namespace)
                        for row in kv_rows
                        if row.artifact_id and row.facet_id
                    }
                    for artifact_id, facet_id, namespace in combos:
                        filters = [
                            col(ArtifactKV.artifact_id) == artifact_id,
                            col(ArtifactKV.facet_id) == facet_id,
                        ]
                        if namespace is None:
                            filters.append(col(ArtifactKV.namespace).is_(None))
                        else:
                            filters.append(col(ArtifactKV.namespace) == namespace)
                        session.exec(delete(ArtifactKV).where(*filters))
                    session.add_all(kv_rows)

                session.commit()
                artifact.meta = current_meta

        try:
            self.execute_with_retry(
                _persist, operation_name="persist_artifact_facet_bundle"
            )
        except Exception as e:
            logging.warning(
                "Failed to persist artifact facet bundle artifact=%s facet=%s: %s",
                getattr(artifact, "id", None),
                getattr(facet, "id", None),
                e,
            )

    def insert_run_config_kv_bulk(self, rows: List[RunConfigKV]) -> None:
        """Bulk inserts RunConfigKV rows."""
        if not rows:
            return

        def _insert():
            with self.session_scope() as session:
                # Ensure idempotency for repeated runs with identical run_id/facet_id/namespace.
                combos = {
                    (row.run_id, row.facet_id, row.namespace)
                    for row in rows
                    if row.run_id and row.facet_id and row.namespace
                }
                for run_id, facet_id, namespace in combos:
                    session.exec(
                        delete(RunConfigKV).where(
                            col(RunConfigKV.run_id) == run_id,
                            col(RunConfigKV.facet_id) == facet_id,
                            col(RunConfigKV.namespace) == namespace,
                        )
                    )
                session.add_all(rows)
                session.commit()

        try:
            self.execute_with_retry(_insert, operation_name="insert_run_config_kv_bulk")
        except Exception as e:
            logging.warning(f"Failed to insert run config kv rows: {e}")

    def insert_artifact_kv_bulk(self, rows: List[ArtifactKV]) -> None:
        """Bulk inserts ArtifactKV rows."""
        if not rows:
            return

        def _insert():
            with self.session_scope() as session:
                combos = {
                    (row.artifact_id, row.facet_id, row.namespace)
                    for row in rows
                    if row.artifact_id and row.facet_id
                }
                for artifact_id, facet_id, namespace in combos:
                    filters = [
                        col(ArtifactKV.artifact_id) == artifact_id,
                        col(ArtifactKV.facet_id) == facet_id,
                    ]
                    if namespace is None:
                        filters.append(col(ArtifactKV.namespace).is_(None))
                    else:
                        filters.append(col(ArtifactKV.namespace) == namespace)
                    session.exec(delete(ArtifactKV).where(*filters))
                session.add_all(rows)
                session.commit()

        try:
            self.execute_with_retry(_insert, operation_name="insert_artifact_kv_bulk")
        except Exception as e:
            logging.warning(f"Failed to insert artifact kv rows: {e}")

    def upsert_artifact_schema(
        self,
        schema: ArtifactSchema,
        fields: List[ArtifactSchemaField],
        relations: Optional[List[ArtifactSchemaRelation]] = None,
    ) -> None:
        """
        Upsert a deduped artifact schema and its per-field rows.

        Notes
        -----
        - `ArtifactSchema.id` is expected to be a stable hash of the canonical schema JSON.
        - Field rows are keyed by (schema_id, name) to prevent duplication.
        """

        def _upsert():
            with self.session_scope() as session:
                session.merge(schema)
                # Idempotency: schema profiling may run multiple times for the same
                # schema_id (hash). Replace the normalized field rows in one shot.
                session.exec(
                    delete(ArtifactSchemaField).where(
                        ArtifactSchemaField.schema_id == schema.id  # ty: ignore[invalid-argument-type]
                    )
                )
                if fields:
                    session.add_all(fields)

                # Idempotency for relational metadata
                session.exec(
                    delete(ArtifactSchemaRelation).where(
                        ArtifactSchemaRelation.schema_id == schema.id  # ty: ignore[invalid-argument-type]
                    )
                )
                if relations:
                    session.add_all(relations)
                session.commit()

        try:
            self.execute_with_retry(_upsert, operation_name="upsert_artifact_schema")
        except Exception as e:
            logging.warning(f"Failed to upsert artifact schema {schema.id}: {e}")

    def persist_artifact_schema_profile(
        self,
        *,
        artifact: Artifact,
        schema: ArtifactSchema,
        fields: List[ArtifactSchemaField],
        observation: ArtifactSchemaObservation,
        meta_updates: Dict[str, Any],
        relations: Optional[List[ArtifactSchemaRelation]] = None,
    ) -> None:
        """
        Persist schema rows, observation, and artifact metadata in one transaction.

        This is intended for schema profiling paths that always perform these writes
        together, reducing round trips compared with calling the individual helpers
        one-by-one.
        """

        def _persist():
            with self.session_scope() as session:
                db_art = session.merge(artifact)
                session.merge(schema)
                session.exec(
                    delete(ArtifactSchemaField).where(
                        ArtifactSchemaField.schema_id == schema.id  # ty: ignore[invalid-argument-type]
                    )
                )
                if fields:
                    session.add_all(fields)

                session.exec(
                    delete(ArtifactSchemaRelation).where(
                        ArtifactSchemaRelation.schema_id == schema.id  # ty: ignore[invalid-argument-type]
                    )
                )
                if relations:
                    session.add_all(relations)

                session.add(observation)

                current_meta = db_art.meta or {}
                current_meta.update(meta_updates)
                db_art.meta = current_meta
                session.add(db_art)
                session.commit()

                artifact.meta = current_meta

        try:
            self.execute_with_retry(
                _persist, operation_name="persist_artifact_schema_profile"
            )
        except Exception as e:
            logging.warning(
                "Failed to persist artifact schema profile artifact=%s schema=%s: %s",
                getattr(artifact, "id", None),
                getattr(schema, "id", None),
                e,
            )

    def persist_artifact_schema_observation_bundle(
        self,
        *,
        artifact: Artifact,
        observation: ArtifactSchemaObservation,
        meta_updates: Dict[str, Any],
    ) -> None:
        """
        Persist a schema observation plus artifact metadata in one transaction.

        This is useful when schema reuse resolves to an existing schema row and only
        the new observation + artifact metadata need to be recorded.
        """

        def _persist():
            with self.session_scope() as session:
                db_art = session.merge(artifact)
                session.add(observation)

                current_meta = db_art.meta or {}
                current_meta.update(meta_updates)
                db_art.meta = current_meta
                session.add(db_art)
                session.commit()

                artifact.meta = current_meta

        try:
            self.execute_with_retry(
                _persist, operation_name="persist_artifact_schema_observation_bundle"
            )
        except Exception as e:
            logging.warning(
                "Failed to persist artifact schema observation bundle artifact=%s schema=%s: %s",
                getattr(artifact, "id", None),
                getattr(observation, "schema_id", None),
                e,
            )

    def insert_artifact_schema_observation(
        self, observation: ArtifactSchemaObservation
    ) -> None:
        """Insert a new schema observation row."""

        def _insert():
            with self.session_scope() as session:
                session.add(observation)
                session.commit()

        try:
            self.execute_with_retry(
                _insert, operation_name="insert_artifact_schema_observation"
            )
        except Exception as e:
            logging.warning(
                "Failed to insert artifact schema observation artifact=%s schema=%s: %s",
                getattr(observation, "artifact_id", None),
                getattr(observation, "schema_id", None),
                e,
            )

    def get_artifact_schema_relations(
        self, *, schema_id: str
    ) -> List[ArtifactSchemaRelation]:
        def _query() -> List[ArtifactSchemaRelation]:
            with self.session_scope() as session:
                return list(
                    session.exec(
                        select(ArtifactSchemaRelation).where(
                            ArtifactSchemaRelation.schema_id == schema_id
                        )
                    ).all()
                )

        try:
            return self.execute_with_retry(
                _query, operation_name="get_artifact_schema_relations"
            )
        except Exception as e:
            logging.warning(
                "Failed to fetch artifact schema relations schema_id=%s: %s",
                schema_id,
                e,
            )
            return []

    def apply_physical_fks(self) -> int:
        """
        Best-effort creation of physical FOREIGN KEY constraints in DuckDB.

        Returns the number of constraints successfully applied.
        """

        def _apply() -> int:
            with self.session_scope() as session:
                rows = session.exec(
                    select(ArtifactSchemaRelation, ArtifactSchema).where(
                        ArtifactSchemaRelation.schema_id == ArtifactSchema.id
                    )
                ).all()

            applied = 0
            with self.engine.begin() as conn:
                for rel, schema in rows:
                    summary = getattr(schema, "summary_json", None) or {}
                    table_name = summary.get("table_name")
                    table_schema = summary.get("table_schema")
                    if not isinstance(table_name, str) or not table_name:
                        continue
                    if not isinstance(rel.from_field, str) or not rel.from_field:
                        continue
                    if not isinstance(rel.to_table, str) or not rel.to_table:
                        continue
                    if not isinstance(rel.to_field, str) or not rel.to_field:
                        continue

                    try:
                        from_parts = []
                        if isinstance(table_schema, str) and table_schema:
                            from_parts.append(
                                _validate_identifier(table_schema, label="table_schema")
                            )
                        from_parts.append(
                            _validate_identifier(table_name, label="table_name")
                        )
                        quoted_from_table = ".".join(
                            _quote_ident(part) for part in from_parts
                        )
                        from_table = ".".join(from_parts)

                        safe_from_field = _validate_identifier(
                            rel.from_field, label="from_field"
                        )
                        safe_to_field = _validate_identifier(
                            rel.to_field, label="to_field"
                        )
                        quoted_to_table = _quote_qualified_identifier(
                            rel.to_table, label="to_table"
                        )

                        safe_from = "_".join(from_parts)
                        constraint = f"fk_{safe_from}_{safe_from_field}"
                        constraint = _validate_identifier(
                            constraint, label="constraint_name"
                        )
                    except ValueError as e:
                        logging.warning(
                            "Skipping FK creation due to invalid identifier(s) "
                            "(table=%s, from_field=%s, to_table=%s, to_field=%s): %s",
                            table_name,
                            rel.from_field,
                            rel.to_table,
                            rel.to_field,
                            e,
                        )
                        continue

                    stmt = (
                        "ALTER TABLE "
                        f"{quoted_from_table} "
                        "ADD CONSTRAINT "
                        f"{_quote_ident(constraint)} "
                        f"FOREIGN KEY ({_quote_ident(safe_from_field)}) "
                        f"REFERENCES {quoted_to_table}({_quote_ident(safe_to_field)})"
                    )
                    try:
                        conn.exec_driver_sql(stmt)
                        applied += 1
                    except Exception as e:
                        logging.warning(
                            "Failed to apply FK %s (%s.%s -> %s.%s): %s",
                            constraint,
                            from_table,
                            rel.from_field,
                            rel.to_table,
                            rel.to_field,
                            e,
                        )
            return applied

        try:
            return self.execute_with_retry(_apply, operation_name="apply_physical_fks")
        except Exception as e:
            logging.warning("Failed to apply physical FKs: %s", e)
            return 0

    def link_artifact_to_run(
        self, artifact_id: uuid.UUID, run_id: str, direction: str
    ) -> None:
        """
        Create a link between an artifact and a run without syncing the artifact itself.

        Notes
        -----
        `run_artifact_link` is keyed by (run_id, artifact_id). This means the same Artifact
        cannot be linked to the same Run as both an input and an output. If callers attempt
        to do so (commonly via "pass-through" steps that re-log an input Artifact as output),
        we keep the first direction and emit a targeted warning instead of silently overwriting.
        """

        def _do_link():
            with self.session_scope() as session:
                existing = session.exec(
                    select(RunArtifactLink)
                    .where(RunArtifactLink.run_id == run_id)
                    .where(RunArtifactLink.artifact_id == artifact_id)
                ).first()

                if existing is not None:
                    if existing.direction != direction:
                        logging.warning(
                            "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                            "because it is already linked as '%s'. "
                            "If this step truly produces a new output, write to a new path (preferred), "
                            "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                            artifact_id,
                            run_id,
                            direction,
                            existing.direction,
                        )
                    return

                link = RunArtifactLink(
                    run_id=run_id, artifact_id=artifact_id, direction=direction
                )
                session.add(link)
                session.commit()

        try:
            self.execute_with_retry(_do_link, operation_name="link_artifact")
        except Exception as e:
            logging.warning(
                f"Failed to link artifact {artifact_id} to run {run_id}: {e}"
            )

    def link_artifacts_to_run_bulk(
        self, *, artifact_ids: List[uuid.UUID], run_id: str, direction: str
    ) -> None:
        """
        Bulk variant of `link_artifact_to_run`.

        This executes in a single transaction to avoid per-artifact session overhead.
        The same direction conflict behavior applies: if a link already exists with a
        different direction, we keep the first direction and emit a warning.
        """
        if not artifact_ids:
            return

        def _do_link():
            with self.session_scope() as session:
                existing = session.exec(
                    select(RunArtifactLink)
                    .where(RunArtifactLink.run_id == run_id)
                    .where(col(RunArtifactLink.artifact_id).in_(artifact_ids))
                ).all()

                existing_by_id = {row.artifact_id: row for row in existing}

                new_links: List[RunArtifactLink] = []
                for artifact_id in artifact_ids:
                    row = existing_by_id.get(artifact_id)
                    if row is not None:
                        if row.direction != direction:
                            logging.warning(
                                "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                                "because it is already linked as '%s'. "
                                "If this step truly produces a new output, write to a new path (preferred), "
                                "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                                artifact_id,
                                run_id,
                                direction,
                                row.direction,
                            )
                        continue
                    new_links.append(
                        RunArtifactLink(
                            run_id=run_id, artifact_id=artifact_id, direction=direction
                        )
                    )

                if not new_links:
                    return

                session.add_all(new_links)
                session.commit()

        try:
            self.execute_with_retry(_do_link, operation_name="link_artifacts_bulk")
        except Exception as e:
            logging.warning(
                "Failed to link artifacts to run run_id=%s count=%s: %s",
                run_id,
                len(artifact_ids),
                e,
            )

    def sync_run_with_links(
        self, *, run: Run, artifact_ids: List[uuid.UUID], direction: str = "output"
    ) -> None:
        """
        Upsert a Run and link artifacts in a single transaction.

        Parameters
        ----------
        run : Run
            Run to upsert.
        artifact_ids : List[uuid.UUID]
            Artifact ids to link to the run.
        direction : str, default "output"
            Link direction for all artifacts.
        """
        if not artifact_ids:
            self.sync_run(run)
            return

        def _do_sync():
            with self.session_scope() as session:
                session.merge(run)

                existing = session.exec(
                    select(RunArtifactLink)
                    .where(RunArtifactLink.run_id == run.id)
                    .where(col(RunArtifactLink.artifact_id).in_(artifact_ids))
                ).all()

                existing_by_id = {row.artifact_id: row for row in existing}
                new_links: List[RunArtifactLink] = []
                for artifact_id in artifact_ids:
                    row = existing_by_id.get(artifact_id)
                    if row is not None:
                        if row.direction != direction:
                            logging.warning(
                                "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                                "because it is already linked as '%s'. "
                                "If this step truly produces a new output, write to a new path (preferred), "
                                "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                                artifact_id,
                                run.id,
                                direction,
                                row.direction,
                            )
                        continue
                    new_links.append(
                        RunArtifactLink(
                            run_id=run.id, artifact_id=artifact_id, direction=direction
                        )
                    )

                if new_links:
                    session.add_all(new_links)

                session.commit()

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_run_with_links")
        except Exception as e:
            logging.warning("Database sync failed: %s", e)

    def sync_artifact(self, artifact: Artifact, run_id: str, direction: str) -> None:
        """Upserts an Artifact and links it to the Run."""

        def _do_sync():
            with self.session_scope() as session:
                existing_link = session.exec(
                    select(RunArtifactLink)
                    .where(RunArtifactLink.run_id == run_id)
                    .where(RunArtifactLink.artifact_id == artifact.id)
                ).first()

                # Merge artifact (create or update)
                session.merge(artifact)

                if existing_link is not None:
                    if existing_link.direction != direction:
                        logging.warning(
                            "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                            "because it is already linked as '%s'. "
                            "If this step truly produces a new output, write to a new path (preferred), "
                            "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                            artifact.id,
                            run_id,
                            direction,
                            existing_link.direction,
                        )
                    session.commit()
                    return

                session.add(
                    RunArtifactLink(
                        run_id=run_id, artifact_id=artifact.id, direction=direction
                    )
                )
                session.commit()

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_artifact")
        except Exception as e:
            logging.warning("Artifact sync failed: %s", e)
            logging.warning("Database sync failed: %s", e)

    def sync_artifacts(
        self,
        *,
        artifacts: Sequence[Artifact],
        run_id: str,
        direction: str,
    ) -> None:
        """
        Upsert multiple Artifacts and link them to a Run in one transaction.

        Parameters
        ----------
        artifacts : Sequence[Artifact]
            Artifacts to sync.
        run_id : str
            Run id to link artifacts to.
        direction : str
            Link direction for all artifacts ("input" or "output").
        """
        if not artifacts:
            return

        def _do_sync():
            with self.session_scope() as session:
                deduped: Dict[uuid.UUID, Artifact] = {}
                for artifact in artifacts:
                    deduped[artifact.id] = artifact

                for artifact in deduped.values():
                    session.merge(artifact)

                artifact_ids = list(deduped.keys())
                existing = session.exec(
                    select(RunArtifactLink)
                    .where(RunArtifactLink.run_id == run_id)
                    .where(col(RunArtifactLink.artifact_id).in_(artifact_ids))
                ).all()
                existing_by_id = {row.artifact_id: row for row in existing}

                new_links: List[RunArtifactLink] = []
                for artifact_id in artifact_ids:
                    row = existing_by_id.get(artifact_id)
                    if row is not None:
                        if row.direction != direction:
                            logging.warning(
                                "[Consist] Ignoring attempt to link artifact_id=%s to run_id=%s as '%s' "
                                "because it is already linked as '%s'. "
                                "If this step truly produces a new output, write to a new path (preferred), "
                                "or log a distinct Artifact instance rather than reusing the same Artifact reference.",
                                artifact_id,
                                run_id,
                                direction,
                                row.direction,
                            )
                        continue
                    new_links.append(
                        RunArtifactLink(
                            run_id=run_id, artifact_id=artifact_id, direction=direction
                        )
                    )

                if new_links:
                    session.add_all(new_links)
                session.commit()

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_artifacts")
        except Exception as e:
            logging.warning("Artifacts sync failed: %s", e)
            logging.warning("Database sync failed: %s", e)

    # --- Read Operations ---

    def find_latest_artifact_at_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        driver: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        include_inputs: bool = False,
    ) -> Optional[Artifact]:
        """
        Finds the most recent artifact created at this location by a run.

        Parameters
        ----------
        uri : str
            The URI to search for artifacts.
        run_id : Optional[str]
            If provided, only return artifacts from this specific run (prevents cross-run artifact retrieval).
        driver : Optional[str]
            If provided, only return artifacts matching the driver.
        table_path : Optional[str]
            If provided, only return artifacts matching the table_path.
        array_path : Optional[str]
            If provided, only return artifacts matching the array_path.
        include_inputs : bool
            If True, include artifacts without a producing run_id.

        Returns
        -------
        Optional[Artifact]
            The most recent artifact at the URI (optionally filtered by run_id), or None if not found.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(Artifact).where(Artifact.container_uri == uri)
                if not include_inputs:
                    statement = statement.where(
                        col(Artifact.run_id).is_not(None)
                    )  # Must be produced by a run
                # If run_id is specified, filter to only that run (prevents cross-run artifact confusion)
                if run_id is not None:
                    statement = statement.where(Artifact.run_id == run_id)
                if driver is not None:
                    statement = statement.where(Artifact.driver == driver)
                if table_path is not None:
                    statement = statement.where(Artifact.table_path == table_path)
                if array_path is not None:
                    statement = statement.where(Artifact.array_path == array_path)

                statement = statement.order_by(col(Artifact.created_at).desc()).limit(1)
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def find_latest_artifact_by_hash(
        self,
        content_hash: str,
        *,
        driver: Optional[str] = None,
        include_inputs: bool = False,
    ) -> Optional[Artifact]:
        """
        Finds the most recent artifact with the given content hash.

        This helper is transitional. New same-content behavior inside Consist
        should prefer ``content_id`` when available, but hash-based lookup
        remains necessary for legacy rows and compatibility paths.

        Parameters
        ----------
        content_hash : str
            Hash to match against ``Artifact.hash``.
        driver : Optional[str]
            If provided, only return artifacts matching the driver.
        include_inputs : bool
            If True, include artifacts without a producing run_id.

        Returns
        -------
        Optional[Artifact]
            The most recent artifact with the given hash, or None if not found.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(Artifact).where(Artifact.hash == content_hash)
                if not include_inputs:
                    statement = statement.where(
                        col(Artifact.run_id).is_not(None)
                    )  # Must be produced by a run
                if driver is not None:
                    statement = statement.where(Artifact.driver == driver)
                statement = statement.order_by(col(Artifact.created_at).desc()).limit(1)
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_or_create_artifact_content(
        self,
        *,
        content_hash: str,
        driver: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> ArtifactContent:
        """
        Lookup or create an ArtifactContent row for the given hash+driver pair.
        """

        def _query() -> ArtifactContent:
            with self.session_scope() as session:
                statement = (
                    select(ArtifactContent)
                    .where(ArtifactContent.content_hash == content_hash)
                    .where(ArtifactContent.driver == driver)
                )
                existing = session.exec(statement.limit(1)).first()
                if existing:
                    return existing
                row = ArtifactContent(
                    content_hash=content_hash,
                    driver=driver,
                    meta=meta or {},
                )
                session.add(row)
                session.commit()
                session.refresh(row)
                return row

        try:
            return self.execute_with_retry(_query)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to get or create ArtifactContent for hash={content_hash}: {exc}"
            ) from exc

    def find_artifact_content(
        self,
        *,
        content_hash: str,
        driver: Optional[str] = None,
    ) -> Optional[ArtifactContent]:
        """
        Return an ArtifactContent row if one exists for the given hash (and optional driver).
        """

        def _query() -> Optional[ArtifactContent]:
            with self.session_scope() as session:
                statement = select(ArtifactContent).where(
                    ArtifactContent.content_hash == content_hash
                )
                if driver is not None:
                    statement = statement.where(ArtifactContent.driver == driver)
                statement = statement.order_by(
                    col(ArtifactContent.created_at).desc()
                ).limit(1)
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def backfill_artifact_content_ids(self) -> None:
        """
        Explicitly backfill content identities for older artifact rows.

        This is kept outside of normal DB startup so compatibility work remains
        easy to remove once legacy DB support is dropped.
        """
        compat_backfill_artifact_content_ids(self)

    def find_artifacts_by_content_id(self, content_id: uuid.UUID) -> list[Artifact]:
        """
        Return all artifact occurrences referencing the supplied content identity.
        """

        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Artifact).where(Artifact.content_id == content_id)
                ).order_by(col(Artifact.created_at).desc())
                return session.exec(statement).all()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def find_runs_producing_content(self, content_id: uuid.UUID) -> list[str]:
        """
        Return run IDs that produced the supplied content identity.
        """

        def _query():
            with self.session_scope() as session:
                statement = (
                    select(RunArtifactLink.run_id)
                    .join(
                        Artifact,
                        col(Artifact.id) == col(RunArtifactLink.artifact_id),
                    )
                    .where(RunArtifactLink.direction == "output")
                    .where(Artifact.content_id == content_id)
                    .distinct()
                )
                return list(session.exec(statement).all())

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def find_schema_observation_for_hash(
        self,
        content_hash: str,
        *,
        prefer_source: Optional[str] = None,
    ) -> Optional[ArtifactSchemaObservation]:
        """
        Find the best schema observation for artifacts matching the given hash.

        Preference order mirrors ``get_artifact_schema_for_artifact``:
        user_provided > prefer_source (if set) > file > duckdb.
        """

        def _query() -> Optional[ArtifactSchemaObservation]:
            with self.session_scope() as session:
                observations = session.exec(
                    select(ArtifactSchemaObservation)
                    .join(
                        Artifact,
                        col(ArtifactSchemaObservation.artifact_id) == col(Artifact.id),
                    )
                    .where(Artifact.hash == content_hash)
                    .order_by(col(ArtifactSchemaObservation.observed_at).desc())
                ).all()

                if not observations:
                    return None

                user_provided_obs = next(
                    (obs for obs in observations if obs.source == "user_provided"), None
                )
                if user_provided_obs:
                    return user_provided_obs

                if prefer_source:
                    matching = next(
                        (obs for obs in observations if obs.source == prefer_source),
                        None,
                    )
                    if matching:
                        return matching

                for source in ("file", "duckdb"):
                    matching = next(
                        (obs for obs in observations if obs.source == source), None
                    )
                    if matching:
                        return matching

                return observations[0]

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def find_schema_observation_for_content_id(
        self,
        content_id: uuid.UUID,
        *,
        prefer_source: Optional[str] = None,
    ) -> Optional[ArtifactSchemaObservation]:
        """
        Find the best schema observation for artifacts sharing a given content identity.

        Preference order mirrors ``get_artifact_schema_for_artifact``:
        user_provided > prefer_source (if set) > file > duckdb.
        """

        def _query() -> Optional[ArtifactSchemaObservation]:
            with self.session_scope() as session:
                observations = session.exec(
                    select(ArtifactSchemaObservation)
                    .join(
                        Artifact,
                        col(ArtifactSchemaObservation.artifact_id) == col(Artifact.id),
                    )
                    .where(Artifact.content_id == content_id)
                    .order_by(col(ArtifactSchemaObservation.observed_at).desc())
                ).all()

                if not observations:
                    return None

                user_provided_obs = next(
                    (obs for obs in observations if obs.source == "user_provided"), None
                )
                if user_provided_obs:
                    return user_provided_obs

                if prefer_source:
                    matching = next(
                        (obs for obs in observations if obs.source == prefer_source),
                        None,
                    )
                    if matching:
                        return matching

                for source in ("file", "duckdb"):
                    matching = next(
                        (obs for obs in observations if obs.source == source), None
                    )
                    if matching:
                        return matching

                return observations[0]

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_run(self, run_id: str) -> Optional[Run]:
        def _query():
            with self.session_scope() as session:
                return session.get(Run, run_id)

        return self.execute_with_retry(_query)

    def get_run_signatures(self, run_ids: List[str]) -> Dict[str, str]:
        """
        Bulk lookup of run signatures by run id.

        Returns a mapping of run_id -> signature for runs that exist and have a signature.
        """
        if not run_ids:
            return {}

        def _query():
            with self.session_scope() as session:
                rows = session.exec(
                    select(Run.id, Run.signature).where(col(Run.id).in_(run_ids))
                ).all()
                return {row[0]: row[1] for row in rows if row[1]}

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return {}

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.config_hash == config_hash)
                    .where(Run.input_hash == input_hash)
                    .where(Run.git_hash == git_hash)
                    .order_by(col(Run.created_at).desc())
                    .limit(1)
                )
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query, operation_name="cache_lookup")
        except Exception as e:
            logging.warning(f"Cache lookup failed: {e}")
            return None

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]:
        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.model_name == model_name)
                    .order_by(col(Run.created_at).desc())
                    .limit(limit)
                )
                results = session.exec(statement).all()
                for run in results:
                    _ = run.meta
                    _ = run.tags
                    session.expunge(run)
                return results

        try:
            return self.execute_with_retry(
                _query, operation_name="find_recent_completed_runs_for_model"
            )
        except Exception as e:
            logging.warning(
                "Failed to find recent completed runs for model %s: %s",
                model_name,
                e,
            )
            return []

    def find_run_by_signature(self, signature: str) -> Optional[Run]:
        """Find a completed run by its composite signature."""

        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.signature == signature)
                    .order_by(col(Run.created_at).desc())
                    .limit(1)
                )
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(
                _query, operation_name="find_run_by_signature"
            )
        except Exception as e:
            logging.warning(f"Signature lookup failed: {e}")
            return None

    def get_artifact(
        self,
        key_or_id: str | uuid.UUID,
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Artifact]:
        def _query():
            with self.session_scope() as session:
                try:
                    # Try UUID lookup first
                    uuid_obj = uuid.UUID(str(key_or_id))
                    artifact = session.get(Artifact, uuid_obj)
                    if artifact is None:
                        return None
                    if run_id is not None:
                        link = session.exec(
                            select(RunArtifactLink)
                            .where(RunArtifactLink.run_id == run_id)
                            .where(RunArtifactLink.artifact_id == artifact.id)
                            .limit(1)
                        ).first()
                        if link is None:
                            return None
                    return artifact
                except ValueError:
                    # Fallback to key lookup (most recent), optionally scoped to
                    # artifacts linked to the requested run.
                    statement = select(Artifact).where(Artifact.key == key_or_id)
                    if run_id is not None:
                        statement = statement.join(
                            RunArtifactLink,
                            Artifact.id == RunArtifactLink.artifact_id,  # ty: ignore[invalid-argument-type]
                        ).where(RunArtifactLink.run_id == run_id)
                    statement = statement.order_by(
                        col(Artifact.created_at).desc()
                    ).limit(1)
                    return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def select_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        prefer_source: Optional[SchemaProfileSource] = None,
        strict_source: bool = False,
    ) -> Optional[ArtifactSchemaSelection]:
        """
        Select a schema profile for an artifact and return explainable selection metadata.

        Selection order (default):
        user_provided > prefer_source (if set) > file > duckdb > most-recent fallback.
        """

        def _query() -> Optional[ArtifactSchemaSelection]:
            with self.session_scope() as session:
                observations = session.exec(
                    select(ArtifactSchemaObservation)
                    .where(ArtifactSchemaObservation.artifact_id == artifact_id)
                    .order_by(col(ArtifactSchemaObservation.observed_at).desc())
                ).all()

                if not observations:
                    return None

                candidate_count = len(observations)

                if strict_source and prefer_source is not None:
                    explicit = next(
                        (obs for obs in observations if obs.source == prefer_source),
                        None,
                    )
                    if explicit is None:
                        raise ValueError(
                            f"No schema observation with source '{prefer_source}' "
                            f"for artifact '{artifact_id}'. "
                            f"Available candidates: {candidate_count}."
                        )
                    return ArtifactSchemaSelection(
                        schema_id=explicit.schema_id,
                        source=explicit.source,
                        candidate_count=candidate_count,
                        selection_rule=f"explicit source={prefer_source}",
                    )

                user_provided_obs = next(
                    (obs for obs in observations if obs.source == "user_provided"), None
                )
                if user_provided_obs is not None:
                    return ArtifactSchemaSelection(
                        schema_id=user_provided_obs.schema_id,
                        source=user_provided_obs.source,
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

                for source in ("file", "duckdb"):
                    fallback = next(
                        (obs for obs in observations if obs.source == source), None
                    )
                    if fallback is not None:
                        return ArtifactSchemaSelection(
                            schema_id=fallback.schema_id,
                            source=fallback.source,
                            candidate_count=candidate_count,
                            selection_rule="default source order file>duckdb",
                        )

                # Unknown source values only: use most recent observation.
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

    def get_artifact_schema(
        self, *, schema_id: str, backfill_ordinals: bool = True
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
        def _query():
            with self.session_scope() as session:
                schema = session.get(ArtifactSchema, schema_id)
                if schema is None:
                    return None
                fields = session.exec(
                    select(ArtifactSchemaField).where(
                        ArtifactSchemaField.schema_id == schema_id
                    )
                ).all()
                return schema, fields

        result = self.execute_with_retry(_query)
        if result is None:
            return None
        schema, fields = result

        if backfill_ordinals and any(row.ordinal_position is None for row in fields):
            self.backfill_artifact_schema_field_ordinals(schema_id=schema_id)
            result2 = self.execute_with_retry(_query)
            if result2 is not None:
                schema, fields = result2

        fields_sorted = sorted(
            fields,
            key=lambda r: (
                r.ordinal_position is None,
                r.ordinal_position if r.ordinal_position is not None else 0,
                r.name,
            ),
        )
        return schema, fields_sorted

    def get_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        backfill_ordinals: bool = True,
        prefer_source: Optional[SchemaProfileSource] = None,
    ) -> Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
        """
        Fetch the schema for an artifact, optionally preferring a specific profiling source.

        This method queries the artifact_schema_observation table to find all recorded
        schema profiles for the artifact, then selects one based on source preference.

        Parameters
        ----------
        artifact_id : uuid.UUID
            The artifact to fetch the schema for.
        backfill_ordinals : bool, default True
            If True, automatically infer missing column ordinal positions from column order.
        prefer_source : {"file", "duckdb", "user_provided"}, optional
            Preference hint for when user_provided schema does not exist. If specified
            and no user_provided schema is available, prefer schemas from this source.
            If the preferred source has no observations, falls back to the default order
            (file > duckdb). If None, uses the default preference order.
            NOTE: prefer_source does NOT override user_provided schemas. User-provided
            schemas are unconditionally preferred because they represent manually-curated
            production-ready definitions.

        Returns
        -------
        Optional[Tuple[ArtifactSchema, List[ArtifactSchemaField]]]
            The selected schema and its fields, or None if no schema is found.

        Notes
        -----
        Multiple profiling sources may have observed the same artifact:
        - "user_provided": Manual schema defined by the user (ALWAYS preferred)
        - "file": Schema inferred from the raw file (CSV/Parquet) using pandas dtypes
        - "duckdb": Schema from the DuckDB table after dlt ingestion

        Selection order:
        1. If user_provided exists, return it (unconditional, cannot be overridden)
        2. If prefer_source is specified and available, return it
        3. Otherwise, use default order: file > duckdb

        This ensures that manually-curated schemas with FK constraints and indexes
        are never accidentally replaced by auto-profiled schemas.
        """

        selection = self.select_artifact_schema_for_artifact(
            artifact_id=artifact_id,
            prefer_source=prefer_source,
            strict_source=False,
        )
        if selection is None:
            return None

        return self.get_artifact_schema(
            schema_id=selection.schema_id, backfill_ordinals=backfill_ordinals
        )

    def get_artifacts_for_run(self, run_id: str) -> List[Tuple[Artifact, str]]:
        def _query():
            with self.session_scope() as session:
                return session.exec(
                    select(Artifact, RunArtifactLink.direction)
                    .join(
                        RunArtifactLink,
                        Artifact.id == RunArtifactLink.artifact_id,  # ty: ignore[invalid-argument-type]
                    )
                    .where(RunArtifactLink.run_id == run_id)
                ).all()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def get_output_artifacts_for_run(self, run_id: str) -> List[Artifact]:
        """
        Return raw output-linked artifacts for a run without key collapsing.

        This preserves duplicate output rows so higher-level planning code can
        detect ambiguous duplicate keys before projecting into dict form.
        """

        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Artifact)
                    .join(
                        RunArtifactLink,
                        Artifact.id == RunArtifactLink.artifact_id,  # ty: ignore[invalid-argument-type]
                    )
                    .where(RunArtifactLink.run_id == run_id)
                    .where(RunArtifactLink.direction == "output")
                    .order_by(
                        col(Artifact.created_at), col(Artifact.key), col(Artifact.id)
                    )
                )
                return list(session.exec(statement).all())

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def find_artifacts(
        self,
        *,
        creator: Optional[str] = None,
        consumer: Optional[str] = None,
        key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        def _query():
            with self.session_scope() as session:
                statement = select(Artifact).distinct()

                if creator:
                    output_link = aliased(RunArtifactLink)
                    statement = statement.join(
                        output_link,
                        Artifact.id == output_link.artifact_id,  # ty: ignore[invalid-argument-type]
                    )
                    statement = statement.where(output_link.run_id == creator)
                    statement = statement.where(output_link.direction == "output")

                if consumer:
                    input_link = aliased(RunArtifactLink)
                    statement = statement.join(
                        input_link,
                        Artifact.id == input_link.artifact_id,  # ty: ignore[invalid-argument-type]
                    )
                    statement = statement.where(input_link.run_id == consumer)
                    statement = statement.where(input_link.direction == "input")

                if key:
                    statement = statement.where(Artifact.key == key)

                results = session.exec(
                    statement.order_by(col(Artifact.created_at).desc()).limit(limit)
                ).all()

                for artifact in results:
                    _ = artifact.meta
                    session.expunge(artifact)

                return results

        try:
            return self.execute_with_retry(_query, operation_name="find_artifacts")
        except Exception:
            return []

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
        facet: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        name: Optional[str] = None,
    ) -> List[Run]:
        def _apply_facet_predicates(statement: Any) -> Any:
            if not facet:
                return statement

            for index, predicate in enumerate(
                flatten_facet_values(facet_dict=facet, include_json_leaves=True)
            ):
                kv = aliased(RunConfigKV, name=f"run_config_kv_{index}")
                statement = statement.join(
                    kv,
                    Run.id == kv.run_id,
                )
                statement = statement.where(kv.key == predicate.key_path)
                if model is not None:
                    statement = statement.where(kv.namespace == model)

                if predicate.value_type == "null":
                    statement = statement.where(kv.value_type == "null")
                elif predicate.value_type == "bool":
                    statement = statement.where(kv.value_type == "bool")
                    statement = statement.where(kv.value_bool == predicate.value_bool)
                elif predicate.value_type == "int":
                    value_num = predicate.value_num
                    assert value_num is not None
                    statement = statement.where(kv.value_type == "int")
                    statement = statement.where(kv.value_num == float(value_num))
                elif predicate.value_type == "float":
                    value_num = predicate.value_num
                    assert value_num is not None
                    statement = statement.where(kv.value_type == "float")
                    statement = statement.where(kv.value_num == float(value_num))
                elif predicate.value_type == "str":
                    statement = statement.where(kv.value_type == "str")
                    statement = statement.where(kv.value_str == predicate.value_str)
                else:
                    statement = statement.where(kv.value_type == "json")
                    statement = statement.where(kv.value_json == predicate.value_json)

            return statement

        def _query():
            with self.session_scope() as session:
                statement = select(Run).order_by(col(Run.created_at).desc())

                if status:
                    statement = statement.where(Run.status == status)
                if model:
                    statement = statement.where(Run.model_name == model)
                if year is not None:
                    statement = statement.where(Run.year == year)
                if iteration is not None:
                    statement = statement.where(Run.iteration == iteration)
                if stage is not None:
                    statement = statement.where(Run.stage == stage)
                if phase is not None:
                    statement = statement.where(Run.phase == phase)
                if parent_id:
                    statement = statement.where(Run.parent_run_id == parent_id)
                if name:
                    statement = statement.where(Run.description == name)

                if tags:
                    for tag in tags:
                        statement = statement.where(col(Run.tags).contains(tag))

                statement = _apply_facet_predicates(statement)

                results = session.exec(statement.limit(limit)).all()

                # Client-side filtering for JSON metadata
                if metadata:
                    filtered = []
                    for r in results:
                        match = True
                        for k, v in metadata.items():
                            if r.meta.get(k) != v:
                                match = False
                                break
                        if match:
                            filtered.append(r)
                    results = filtered

                # CRITICAL: Make objects independent of the session
                # This allows them to be used after the session closes
                for run in results:
                    # Force load all attributes while session is still open
                    _ = run.meta  # Access to load
                    _ = run.tags
                    # Then expunge so SQLAlchemy stops tracking them
                    session.expunge(run)

                return results

        try:
            return self.execute_with_retry(_query, operation_name="find_runs")
        except Exception as e:
            logging.warning(f"Failed to find runs: {e}")
            return []

    def get_artifact_by_uri(
        self,
        uri: str,
        run_id: Optional[str] = None,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
    ) -> Optional[Artifact]:
        """
        Get artifact by URI, optionally filtered by run_id.

        Parameters
        ----------
        uri : str
            The URI to retrieve.
        run_id : Optional[str]
            If provided, only return artifacts from this specific run.
        table_path : Optional[str]
            Optional table path to match.
        array_path : Optional[str]
            Optional array path to match.

        Returns
        -------
        Optional[Artifact]
            The most recent artifact at the URI (optionally filtered by run_id), or None if not found.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(Artifact).where(Artifact.container_uri == uri)
                # If run_id is specified, filter to only that run
                if run_id is not None:
                    statement = statement.where(Artifact.run_id == run_id)
                if table_path is not None:
                    statement = statement.where(Artifact.table_path == table_path)
                if array_path is not None:
                    statement = statement.where(Artifact.array_path == array_path)

                statement = statement.order_by(col(Artifact.created_at).desc()).limit(1)
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_config_facet(self, facet_id: str) -> Optional[ConfigFacet]:
        def _query():
            with self.session_scope() as session:
                facet = session.get(ConfigFacet, facet_id)
                if facet is not None:
                    _ = facet.facet_json
                    session.expunge(facet)
                return facet

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[ConfigFacet]:
        def _query():
            with self.session_scope() as session:
                statement = select(ConfigFacet).order_by(
                    col(ConfigFacet.created_at).desc()
                )
                if namespace:
                    statement = statement.where(ConfigFacet.namespace == namespace)
                if schema_name:
                    statement = statement.where(ConfigFacet.schema_name == schema_name)

                results = session.exec(statement.limit(limit)).all()
                for facet in results:
                    _ = facet.facet_json
                    session.expunge(facet)
                return results

        try:
            return self.execute_with_retry(_query)
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
        """
        Return flattened facet key/value rows for a run.

        Notes:
        - `key` values are the stored/escaped representation (e.g., dict keys with "." are
          written as "\\." in `RunConfigKV.key`).
        - `prefix` is matched against the stored key representation.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(RunConfigKV).where(RunConfigKV.run_id == run_id)
                if namespace:
                    statement = statement.where(RunConfigKV.namespace == namespace)
                if prefix:
                    statement = statement.where(col(RunConfigKV.key).like(f"{prefix}%"))
                results = session.exec(statement.limit(limit)).all()
                for row in results:
                    session.expunge(row)
                return results

        try:
            return self.execute_with_retry(_query)
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
        """
        Return flattened facet key/value rows for multiple runs.
        """
        if not run_ids:
            return []
        if keys is not None and len(keys) == 0:
            return []

        def _query():
            with self.session_scope() as session:
                statement = select(RunConfigKV).where(
                    col(RunConfigKV.run_id).in_(run_ids)
                )
                if namespace:
                    statement = statement.where(RunConfigKV.namespace == namespace)
                if keys is not None:
                    statement = statement.where(col(RunConfigKV.key).in_(keys))
                rows = session.exec(statement.limit(limit)).all()
                for row in rows:
                    session.expunge(row)
                return rows

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def get_artifact_kv(
        self,
        artifact_id: uuid.UUID,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ) -> List[ArtifactKV]:
        """
        Return flattened artifact facet key/value rows for an artifact.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(ArtifactKV).where(
                    ArtifactKV.artifact_id == artifact_id
                )
                if namespace is not None:
                    statement = statement.where(ArtifactKV.namespace == namespace)
                if prefix:
                    statement = statement.where(
                        col(ArtifactKV.key_path).like(f"{prefix}%")
                    )
                results = session.exec(statement.limit(limit)).all()
                for row in results:
                    session.expunge(row)
                return results

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def find_artifacts_by_facet_params(
        self,
        *,
        predicates: List[Dict[str, Any]],
        namespace: Optional[str] = None,
        key_prefix: Optional[str] = None,
        artifact_family_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """
        Find artifacts using one or more flattened facet predicates.
        """

        def _query():
            with self.session_scope() as session:
                statement = select(Artifact).distinct()

                if key_prefix:
                    statement = statement.where(
                        col(Artifact.key).like(f"{key_prefix}%")
                    )

                if artifact_family_prefix:
                    family_kv = aliased(ArtifactKV)
                    statement = statement.join(
                        family_kv,
                        col(Artifact.id) == col(family_kv.artifact_id),
                    )
                    statement = statement.where(family_kv.key_path == "artifact_family")
                    if namespace is not None:
                        statement = statement.where(family_kv.namespace == namespace)
                    statement = statement.where(family_kv.value_type == "str")
                    statement = statement.where(
                        col(family_kv.value_str).like(f"{artifact_family_prefix}%")
                    )

                statement = self._apply_artifact_kv_predicates(
                    statement=statement,
                    predicates=predicates,
                    namespace=namespace,
                )

                results = session.exec(
                    statement.order_by(col(Artifact.created_at).desc()).limit(limit)
                ).all()

                for artifact in results:
                    _ = artifact.meta
                    session.expunge(artifact)

                return results

        try:
            return self.execute_with_retry(
                _query, operation_name="find_artifacts_by_facet_params"
            )
        except Exception as e:
            logging.warning("Failed to find artifacts by facet params: %s", e)
            return []

    def _apply_artifact_kv_predicates(
        self,
        *,
        statement: Any,
        predicates: List[Dict[str, Any]],
        namespace: Optional[str],
    ) -> Any:
        """
        Apply parsed ArtifactKV predicates to a SQLAlchemy statement.
        """
        for predicate in predicates:
            kv = aliased(ArtifactKV)
            statement = statement.join(
                kv,
                col(Artifact.id) == col(kv.artifact_id),
            )
            statement = statement.where(kv.key_path == predicate["key_path"])

            predicate_namespace = predicate.get("namespace")
            effective_namespace = (
                predicate_namespace if predicate_namespace is not None else namespace
            )
            if effective_namespace is not None:
                statement = statement.where(kv.namespace == effective_namespace)

            operator = predicate["op"]
            value_kind = predicate["kind"]
            value = predicate["value"]

            if operator == "=":
                if value_kind == "str":
                    statement = statement.where(kv.value_type == "str")
                    statement = statement.where(kv.value_str == value)
                elif value_kind == "bool":
                    statement = statement.where(kv.value_type == "bool")
                    statement = statement.where(kv.value_bool == value)
                elif value_kind == "null":
                    statement = statement.where(kv.value_type == "null")
                else:
                    statement = statement.where(
                        col(kv.value_type).in_(["int", "float"])
                    )
                    statement = statement.where(col(kv.value_num) == float(value))
            else:
                statement = statement.where(col(kv.value_type).in_(["int", "float"]))
                if operator == ">=":
                    statement = statement.where(col(kv.value_num) >= float(value))
                else:
                    statement = statement.where(col(kv.value_num) <= float(value))

        return statement

    def _resolve_schema_compatible_ids(
        self,
        *,
        session: Session,
        schema_id: str,
        all_schemas: Optional[Sequence[ArtifactSchema]] = None,
        schema_fields_by_id: Optional[Dict[str, Set[str]]] = None,
    ) -> Set[str]:
        """
        Resolve schema IDs compatible with `schema_id` for union-by-name views.

        Compatibility is defined conservatively as:
        - same `summary_json.table_name` (when present on both schemas), and
        - field-name sets are subset/superset of each other.
        """
        schema_rows = (
            all_schemas
            if all_schemas is not None
            else session.exec(select(ArtifactSchema)).all()
        )
        schemas_by_id = {
            sid: row
            for row in schema_rows
            if isinstance((sid := getattr(row, "id", None)), str)
        }

        base_schema = schemas_by_id.get(schema_id)
        if base_schema is None:
            return {schema_id}

        field_map = (
            schema_fields_by_id
            if schema_fields_by_id is not None
            else self._load_schema_field_names(session=session)
        )
        base_fields = field_map.get(schema_id, set())
        if not base_fields:
            return {schema_id}

        base_summary = getattr(base_schema, "summary_json", {}) or {}
        base_table_name = (
            base_summary.get("table_name")
            if isinstance(base_summary.get("table_name"), str)
            else None
        )

        compatible: Set[str] = {schema_id}
        for candidate in schema_rows:
            candidate_id = getattr(candidate, "id", None)
            if not isinstance(candidate_id, str) or candidate_id == schema_id:
                continue

            candidate_summary = getattr(candidate, "summary_json", {}) or {}
            candidate_table_name = (
                candidate_summary.get("table_name")
                if isinstance(candidate_summary.get("table_name"), str)
                else None
            )
            if (
                base_table_name is not None
                and candidate_table_name is not None
                and candidate_table_name != base_table_name
            ):
                continue

            candidate_fields = field_map.get(candidate_id, set())
            if not candidate_fields:
                continue
            if base_fields.issubset(candidate_fields) or candidate_fields.issubset(
                base_fields
            ):
                compatible.add(candidate_id)
        return compatible

    def _load_schema_field_names(
        self,
        *,
        session: Session,
        schema_ids: Optional[Set[str]] = None,
    ) -> Dict[str, Set[str]]:
        """
        Load schema field-name sets keyed by schema_id in one query.
        """
        statement = select(ArtifactSchemaField)
        if schema_ids:
            statement = statement.where(
                col(ArtifactSchemaField.schema_id).in_(sorted(schema_ids))
            )

        rows = session.exec(statement).all()
        fields_by_schema: Dict[str, Set[str]] = {}
        for row in rows:
            schema_id = getattr(row, "schema_id", None)
            field_name = getattr(row, "name", None)
            if not isinstance(schema_id, str) or not isinstance(field_name, str):
                continue
            fields_by_schema.setdefault(schema_id, set()).add(field_name)
        return fields_by_schema

    @staticmethod
    def _schema_model_field_names(schema_model: Type[SQLModel]) -> Set[str]:
        field_names: Set[str] = set()
        for attr_name, field_info in schema_model.model_fields.items():
            sa_col = getattr(field_info, "sa_column", None)
            col_name = getattr(sa_col, "name", None) if sa_col is not None else None
            sql_name = (
                str(col_name) if isinstance(col_name, str) and col_name else attr_name
            )
            if str(sql_name).startswith("consist_"):
                continue
            field_names.add(str(sql_name))
        return field_names

    def find_schema_ids_for_model(
        self, *, schema_model: Type[SQLModel], compatible: bool = True
    ) -> List[str]:
        """
        Resolve stored schema ids that match a SQLModel class.

        Matching rules:
        - Candidate schema must have the same table name in summary_json when present.
        - If `compatible=True`, field-name subset/superset matches are included.
          If `compatible=False`, exact field-name equality is required.
        """

        def _query() -> List[str]:
            with self.session_scope() as session:
                model_table_name = getattr(
                    schema_model, "__tablename__", schema_model.__name__
                )
                model_fields = self._schema_model_field_names(schema_model)
                if not model_fields:
                    return []

                schema_rows = session.exec(select(ArtifactSchema)).all()
                schema_fields_by_id = self._load_schema_field_names(session=session)
                matched: List[str] = []
                for schema_row in schema_rows:
                    schema_id = getattr(schema_row, "id", None)
                    if not isinstance(schema_id, str):
                        continue

                    summary = getattr(schema_row, "summary_json", {}) or {}
                    candidate_table_name = (
                        summary.get("table_name")
                        if isinstance(summary.get("table_name"), str)
                        else None
                    )
                    if candidate_table_name is not None and str(
                        candidate_table_name
                    ) != str(model_table_name):
                        continue

                    candidate_fields = schema_fields_by_id.get(schema_id, set())
                    if not candidate_fields:
                        continue

                    if compatible:
                        if model_fields.issubset(
                            candidate_fields
                        ) or candidate_fields.issubset(model_fields):
                            matched.append(schema_id)
                    elif candidate_fields == model_fields:
                        matched.append(schema_id)
                return sorted(set(matched))

        try:
            return self.execute_with_retry(
                _query, operation_name="find_schema_ids_for_model"
            )
        except Exception as e:
            logging.warning("Failed to resolve schema ids for model: %s", e)
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
    ) -> List[Tuple[Artifact, Run]]:
        """
        Resolve artifacts for grouped-view materialization.
        """
        if schema_id is None and not schema_ids:
            raise ValueError("Provide schema_id or schema_ids for grouped view lookup.")

        resolved_predicates = predicates or []

        def _query() -> List[Tuple[Artifact, Run]]:
            with self.session_scope() as session:
                resolved_schema_ids: Set[str]
                base_ids = list(schema_ids or [])
                if schema_id is not None:
                    base_ids.append(schema_id)
                base_ids = sorted(set(base_ids))
                if schema_compatible:
                    schema_rows = session.exec(select(ArtifactSchema)).all()
                    schema_fields_by_id = self._load_schema_field_names(session=session)
                    resolved_schema_ids = set()
                    for base_schema_id in base_ids:
                        resolved_schema_ids.update(
                            self._resolve_schema_compatible_ids(
                                session=session,
                                schema_id=base_schema_id,
                                all_schemas=schema_rows,
                                schema_fields_by_id=schema_fields_by_id,
                            )
                        )
                else:
                    resolved_schema_ids = set(base_ids)

                obs = aliased(ArtifactSchemaObservation)
                statement = (
                    select(Artifact, Run)
                    .join(Run, col(Artifact.run_id) == col(Run.id))
                    .join(obs, col(Artifact.id) == col(obs.artifact_id))
                    .where(col(obs.schema_id).in_(sorted(resolved_schema_ids)))
                    .distinct()
                )

                if drivers:
                    statement = statement.where(col(Artifact.driver).in_(drivers))
                if run_id is not None:
                    statement = statement.where(Run.id == run_id)
                if parent_run_id is not None:
                    statement = statement.where(Run.parent_run_id == parent_run_id)
                if model is not None:
                    statement = statement.where(Run.model_name == model)
                if status is not None:
                    statement = statement.where(Run.status == status)
                if year is not None:
                    statement = statement.where(Run.year == year)
                if iteration is not None:
                    statement = statement.where(Run.iteration == iteration)

                statement = self._apply_artifact_kv_predicates(
                    statement=statement,
                    predicates=resolved_predicates,
                    namespace=namespace,
                )

                statement = statement.order_by(
                    col(Artifact.created_at).asc(), col(Artifact.id).asc()
                )
                rows = session.exec(statement.limit(limit)).all()

                # Hydrate required attributes while instances are still bound.
                seen_run_ids: Set[str] = set()
                for artifact, run in rows:
                    _ = artifact.meta
                    run_id_value = getattr(run, "id", None)
                    if isinstance(run_id_value, str) and run_id_value in seen_run_ids:
                        continue
                    _ = run.meta
                    _ = run.tags
                    if isinstance(run_id_value, str):
                        seen_run_ids.add(run_id_value)

                # Detach in a second pass to avoid expunging shared Run instances
                # mid-iteration when multiple artifacts belong to the same run.
                seen_artifact_instances: Set[int] = set()
                for artifact, _ in rows:
                    artifact_ref = id(artifact)
                    if artifact_ref in seen_artifact_instances:
                        continue
                    seen_artifact_instances.add(artifact_ref)
                    if artifact in session:
                        session.expunge(artifact)

                seen_run_instances: Set[int] = set()
                for _, run in rows:
                    run_ref = id(run)
                    if run_ref in seen_run_instances:
                        continue
                    seen_run_instances.add(run_ref)
                    if run in session:
                        session.expunge(run)
                return rows

        try:
            return self.execute_with_retry(
                _query, operation_name="find_artifacts_for_grouped_view"
            )
        except Exception as e:
            logging.warning("Failed to resolve grouped-view artifacts: %s", e)
            return []

    def get_artifact_kv_values_bulk(
        self,
        *,
        artifact_ids: List[uuid.UUID],
        key_paths: List[str],
        namespace: Optional[str] = None,
    ) -> Dict[uuid.UUID, Dict[str, Dict[str, Any]]]:
        """
        Bulk lookup for typed artifact facet values.

        Returns
        -------
        Dict[uuid.UUID, Dict[str, Dict[str, Any]]]
            Mapping: artifact_id -> key_path -> {"value": Any, "type": str}
        """
        if not artifact_ids or not key_paths:
            return {}

        def _query() -> Dict[uuid.UUID, Dict[str, Dict[str, Any]]]:
            with self.session_scope() as session:
                statement = select(ArtifactKV).where(
                    col(ArtifactKV.artifact_id).in_(artifact_ids)
                )
                statement = statement.where(col(ArtifactKV.key_path).in_(key_paths))
                if namespace is not None:
                    statement = statement.where(ArtifactKV.namespace == namespace)
                statement = statement.order_by(
                    col(ArtifactKV.created_at).desc(),
                    col(ArtifactKV.facet_id).desc(),
                )
                rows = session.exec(statement).all()

                out: Dict[uuid.UUID, Dict[str, Dict[str, Any]]] = {}
                for row in rows:
                    by_key = out.setdefault(row.artifact_id, {})
                    # Most-recent row wins if duplicates exist.
                    if row.key_path in by_key:
                        continue
                    value: Any = None
                    if row.value_type == "str":
                        value = row.value_str
                    elif row.value_type == "int":
                        value = (
                            int(row.value_num) if row.value_num is not None else None
                        )
                    elif row.value_type == "float":
                        value = row.value_num
                    elif row.value_type == "bool":
                        value = row.value_bool
                    elif row.value_type == "json":
                        value = row.value_json
                    elif row.value_type == "null":
                        value = None
                    by_key[row.key_path] = {"value": value, "type": row.value_type}
                return out

        try:
            return self.execute_with_retry(
                _query, operation_name="get_artifact_kv_values_bulk"
            )
        except Exception as e:
            logging.warning("Failed bulk artifact kv lookup: %s", e)
            return {}

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
        """
        Find completed runs by a single facet KV predicate.

        This is intended as an ergonomic wrapper around querying `run_config_kv` for
        real-world filtering, e.g.:
        - model="clean_data" and threshold==0.5
        - model="beam" and sample==0.1
        """

        def _query():
            with self.session_scope() as session:
                statement = (
                    select(Run)
                    .join(RunConfigKV, RunConfigKV.run_id == Run.id)  # ty: ignore[invalid-argument-type]
                    .where(Run.status == "completed")
                    .where(RunConfigKV.namespace == namespace)
                    .where(RunConfigKV.key == key)
                )

                if value_type is not None:
                    statement = statement.where(RunConfigKV.value_type == value_type)

                if value_str is not None:
                    statement = statement.where(RunConfigKV.value_str == value_str)
                elif value_num is not None:
                    statement = statement.where(
                        RunConfigKV.value_num == float(value_num)
                    )
                elif value_bool is not None:
                    statement = statement.where(RunConfigKV.value_bool == value_bool)

                results = session.exec(statement.limit(limit)).all()
                for run in results:
                    _ = run.meta
                    _ = run.tags
                    session.expunge(run)
                return results

        try:
            return self.execute_with_retry(
                _query, operation_name="find_runs_by_facet_kv"
            )
        except Exception as e:
            logging.warning("Failed to find runs by facet kv: %s", e)
            return []

    def get_facet_values_for_runs(
        self,
        run_ids: List[str],
        *,
        key: str,
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch facet KV values for a set of runs.

        Returns a map of run_id -> Python value (str/bool/int/float/json/None).
        """
        if not run_ids:
            return {}

        def _query():
            with self.session_scope() as session:
                statement = select(RunConfigKV).where(
                    col(RunConfigKV.run_id).in_(run_ids)
                )
                statement = statement.where(RunConfigKV.key == key)
                if namespace:
                    statement = statement.where(RunConfigKV.namespace == namespace)
                rows = session.exec(statement).all()
                for row in rows:
                    session.expunge(row)
                return rows

        try:
            rows = self.execute_with_retry(
                _query, operation_name="get_facet_values_for_runs"
            )
        except Exception as e:
            logging.warning("Failed to get facet values for runs: %s", e)
            return {}

        values: Dict[str, Any] = {}
        for row in rows:
            if row.value_type == "null":
                value: Any = None
            elif row.value_type == "bool":
                value = row.value_bool
            elif row.value_type == "int":
                value = int(row.value_num) if row.value_num is not None else None
            elif row.value_type == "float":
                value = row.value_num
            elif row.value_type == "str":
                value = row.value_str
            else:
                value = row.value_json
            values[row.run_id] = value

        return values

    def get_history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        query = select(Run).order_by(col(Run.created_at).desc()).limit(limit)
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty and tags:
                # Basic client-side tag filtering for simplicity
                def has_all_tags(run_tags):
                    # (Implementation of tag parsing logic from original code)
                    if not run_tags:
                        return False
                    if isinstance(run_tags, str):
                        try:
                            run_tags = load_json_safe(run_tags, max_depth=10)
                        except ValueError:
                            pass
                    return all(t in (run_tags or []) for t in tags)

                df = df[df["tags"].apply(has_all_tags)]

            if "started_at" in df.columns and "ended_at" in df.columns:
                start = pd.to_datetime(df["started_at"])
                end = pd.to_datetime(df["ended_at"])
                df["duration_seconds"] = (end - start).dt.total_seconds()
            return df
        except Exception as e:
            logging.warning(f"Failed to fetch history: {e}")
            return pd.DataFrame()


class ProvenanceWriter:
    """
    Lightweight persistence helper for JSON snapshots and DB synchronization.

    This keeps non-public persistence mechanics out of managers while allowing
    the tracker to remain the primary orchestrator.
    """

    def __init__(self, tracker: "Tracker"):
        self._tracker = tracker
        self._artifact_batch_depth = 0
        self._batch_pending_json_flush = False
        self._batch_pending_artifacts: list[tuple["Artifact", str]] = []

    @contextmanager
    def batch_artifact_writes(self) -> Iterator[None]:
        """
        Defer artifact JSON flushes and DB syncs until the end of a batch.

        This keeps `log_artifact(...)` semantics unchanged while allowing callers
        such as `log_artifacts(...)` to avoid redundant per-item persistence I/O.
        """
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
                self._flush_json_now()
            if pending_artifacts:
                self._sync_artifacts_now(pending_artifacts)

    def flush_json(self) -> None:
        """
        Flush the current in-memory run state to JSON snapshots on disk.

        Uses an atomic write strategy for both the per-run snapshot and the
        rolling latest snapshot.
        """
        if self._artifact_batch_depth > 0:
            self._batch_pending_json_flush = True
            return
        self._flush_json_now()

    def _flush_json_now(self) -> None:
        """Immediate implementation behind `flush_json`."""
        tracker = self._tracker
        if not tracker.current_consist:
            return
        self._write_record_json(tracker.current_consist)

    def flush_record_json(self, record: "ConsistRecord") -> None:
        """
        Flush a specific run snapshot record to JSON files on disk.

        This is used when metadata is updated after run execution has ended and
        there is no active ``current_consist`` context.
        """
        self._write_record_json(record)

    def _write_record_json(self, record: "ConsistRecord") -> None:
        """Write per-run and latest JSON snapshots for a record atomically."""
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
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
            raise

    def sync_run(self, run: "RunModel") -> None:
        """
        Sync a Run object to the database, if configured.

        Parameters
        ----------
        run : Run
            Run instance to upsert into the database.
        """
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
        """
        Sync a Run and link artifacts in one database transaction.

        Parameters
        ----------
        run : Run
            Run instance to upsert into the database.
        artifact_ids : list[uuid.UUID]
            Artifact ids to link to the run.
        direction : str, default "output"
            Direction for all linked artifacts.
        """
        tracker = self._tracker
        if tracker.db:
            try:
                tracker.db.sync_run_with_links(
                    run=run, artifact_ids=artifact_ids, direction=direction
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)

    def sync_artifact(self, artifact: "Artifact", direction: str) -> None:
        """
        Sync an Artifact and its run link to the database, if configured.

        Parameters
        ----------
        artifact : Artifact
            Artifact instance to upsert into the database.
        direction : str
            Direction of the artifact relative to the current run
            ("input" or "output").
        """
        if self._artifact_batch_depth > 0:
            self._batch_pending_artifacts.append((artifact, direction))
            return

        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                tracker.db.sync_artifact(
                    artifact, tracker.current_consist.run.id, direction
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
