import time
import random
import logging
import re
import uuid
import json
import contextvars
from contextlib import contextmanager
from typing import Optional, List, Callable, Any, Dict, Tuple, TYPE_CHECKING, Literal, Iterator

import pandas as pd
from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import ConcurrentModificationError
from sqlalchemy.orm import aliased
from sqlalchemy.pool import NullPool
from sqlmodel import create_engine, Session, select, SQLModel, col, delete

from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.models.run import Run as RunModel


MAX_JSON_DEPTH = 50


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

    def __init__(self, db_path: str):
        self.db_path = db_path
        # Using NullPool ensures the file lock is released when the session closes
        self.engine = create_engine(f"duckdb:///{db_path}", poolclass=NullPool)
        self._session_ctx: contextvars.ContextVar[Session | None] = contextvars.ContextVar(
            "consist_session", default=None
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
                    getattr(ArtifactSchema, "__table__"),
                    getattr(ArtifactSchemaField, "__table__"),
                    getattr(ArtifactSchemaObservation, "__table__"),
                    getattr(ArtifactSchemaRelation, "__table__"),
                ],
            )
            # DuckDB self-referential FK on run.parent_run_id blocks status updates.
            self._relax_run_parent_fk()
            # Lightweight migrations for additive schema changes.
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

    def execute_with_retry(
        self,
        func: Callable,
        operation_name: str = "db_op",
        retries: int = 20,
        **kwargs,  # <--- Add this to absorb extra arguments
    ) -> Any:
        """Executes a function with exponential backoff for DB locks."""
        for i in range(retries):
            try:
                return func()
            except (OperationalError, DatabaseError) as e:
                msg = str(e)
                if "lock" in msg or "IO Error" in msg or "database is locked" in msg:
                    if i == retries - 1:
                        raise e
                    sleep_time = min((0.1 * (1.5**i)) + random.uniform(0.05, 0.2), 2.0)
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
                # Use merge for a simple upsert. DuckDB's MERGE semantics via SQLModel
                # handle inserts and updates in one call and avoid stale state issues.
                logging.debug(
                    f"[DB sync_run] upserting run={run.id} status={run.status}"
                )
                session.merge(run)
                session.commit()
                # Read-back to verify persisted status for diagnostics
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
                    if isinstance(table_schema, str) and table_schema:
                        from_table = f"{table_schema}.{table_name}"
                    else:
                        from_table = table_name
                    safe_from = from_table.replace(".", "_")
                    constraint = f"fk_{safe_from}_{rel.from_field}"
                    stmt = (
                        "ALTER TABLE "
                        f"{from_table} "
                        "ADD CONSTRAINT "
                        f"{constraint} "
                        f"FOREIGN KEY ({rel.from_field}) "
                        f"REFERENCES {rel.to_table}({rel.to_field})"
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
                # Merge artifact (create or update)
                session.merge(artifact)
                session.commit()

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_artifact")
            # Now create the link
            self.link_artifact_to_run(artifact.id, run_id, direction)
        except Exception as e:
            logging.warning("Artifact sync failed: %s", e)
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

    def get_artifact(self, key_or_id: str | uuid.UUID) -> Optional[Artifact]:
        def _query():
            with self.session_scope() as session:
                try:
                    # Try UUID lookup first
                    uuid_obj = uuid.UUID(str(key_or_id))
                    return session.get(Artifact, uuid_obj)
                except ValueError:
                    # Fallback to Key lookup (most recent)
                    return session.exec(
                        select(Artifact)
                        .where(Artifact.key == key_or_id)
                        .order_by(col(Artifact.created_at).desc())
                        .limit(1)
                    ).first()

        try:
            return self.execute_with_retry(_query)
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
        prefer_source: Optional[Literal["file", "duckdb"]] = None,
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
        prefer_source : {"file", "duckdb"}, optional
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

        def _query_observations() -> Optional[str]:
            """
            Query all schema observations for this artifact and apply preference logic
            to select the best schema_id.
            """
            with self.session_scope() as session:
                # Fetch all observations for this artifact, most recent first.
                # We order by observed_at DESC so that if the same source has multiple
                # observations, we get the most recent one.
                observations = session.exec(
                    select(ArtifactSchemaObservation)
                    .where(ArtifactSchemaObservation.artifact_id == artifact_id)
                    .order_by(col(ArtifactSchemaObservation.observed_at).desc())
                ).all()

                if not observations:
                    return None

                # User-provided schemas are ALWAYS preferred if they exist, because they
                # represent manually-curated, production-ready definitions with FK constraints,
                # indexes, etc. This is unconditional: prefer_source does not override it.
                user_provided_obs = next(
                    (obs for obs in observations if obs.source == "user_provided"), None
                )
                if user_provided_obs:
                    return user_provided_obs.schema_id

                # If user explicitly requested a source and user_provided doesn't exist,
                # use their preference.
                if prefer_source:
                    matching = next(
                        (obs for obs in observations if obs.source == prefer_source),
                        None,
                    )
                    if matching:
                        return matching.schema_id

                # Fall back to default preference order. File profiles preserve more type
                # information than database profiles (e.g., pandas category vs VARCHAR),
                # so file > duckdb (when user_provided is not available).
                default_source_order = ["file", "duckdb"]
                for source in default_source_order:
                    matching = next(
                        (obs for obs in observations if obs.source == source), None
                    )
                    if matching:
                        return matching.schema_id

                return None

        schema_id = self.execute_with_retry(_query_observations)
        if schema_id is None:
            return None

        return self.get_artifact_schema(
            schema_id=schema_id, backfill_ordinals=backfill_ordinals
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
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        name: Optional[str] = None,
    ) -> List[Run]:
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
                if parent_id:
                    statement = statement.where(Run.parent_run_id == parent_id)
                if name:
                    statement = statement.where(Run.description == name)

                if tags:
                    for tag in tags:
                        statement = statement.where(col(Run.tags).contains(tag))

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

    def flush_json(self) -> None:
        """
        Flush the current in-memory run state to JSON snapshots on disk.

        Uses an atomic write strategy for both the per-run snapshot and the
        rolling latest snapshot.
        """
        tracker = self._tracker
        if not tracker.current_consist:
            return
        json_str = tracker.current_consist.model_dump_json(indent=2)

        run_id = tracker.current_consist.run.id
        safe_run_id = "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )

        per_run_dir = tracker.fs.run_dir / "consist_runs"
        per_run_dir.mkdir(parents=True, exist_ok=True)
        per_run_target = per_run_dir / f"{safe_run_id}.json"
        per_run_tmp = per_run_target.with_suffix(".tmp")
        with open(per_run_tmp, "w", encoding="utf-8") as f:
            f.write(json_str)
        per_run_tmp.replace(per_run_target)

        latest_target = tracker.fs.run_dir / "consist.json"
        latest_tmp = latest_target.with_suffix(".tmp")
        with open(latest_tmp, "w", encoding="utf-8") as f:
            f.write(json_str)
        latest_tmp.replace(latest_target)

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
        tracker = self._tracker
        if tracker.db and tracker.current_consist:
            try:
                tracker.db.sync_artifact(
                    artifact, tracker.current_consist.run.id, direction
                )
            except Exception as e:
                logging.warning("Database sync failed: %s", e)
