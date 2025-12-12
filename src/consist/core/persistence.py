import time
import random
import logging
import uuid
from typing import Optional, List, Callable, Any, Dict, Tuple

import pandas as pd
from sqlalchemy.exc import OperationalError, DatabaseError
from sqlalchemy.orm.exc import ConcurrentModificationError
from sqlalchemy.pool import NullPool
from sqlmodel import create_engine, Session, select, SQLModel, col

from consist.models.artifact import Artifact
from consist.models.config_facet import ConfigFacet
from consist.models.run import Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV


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
        self._init_schema()

    def _init_schema(self):
        """Creates tables if they don't exist, wrapped in retry logic."""

        def _create():
            SQLModel.metadata.create_all(
                self.engine,
                tables=[
                    Run.__table__,
                    Artifact.__table__,
                    RunArtifactLink.__table__,
                    ConfigFacet.__table__,
                    RunConfigKV.__table__,
                ],
            )
            # DuckDB self-referential FK on run.parent_run_id blocks status updates.
            self._relax_run_parent_fk()

        self.execute_with_retry(_create, operation_name="init_schema")

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

    # --- Write Operations ---

    def update_artifact_meta(self, artifact: Artifact, updates: Dict[str, Any]) -> None:
        """Updates artifact metadata safely with retries."""

        def _update():
            with Session(self.engine) as session:
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
            with Session(self.engine) as session:
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
            logging.warning(f"Database sync failed: {e}")

    def update_run_meta(self, run_id: str, meta_updates: Dict[str, Any]) -> None:
        """Updates a run's meta field with a partial dict."""

        def _update():
            with Session(self.engine) as session:
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
            with Session(self.engine) as session:
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
            with Session(self.engine) as session:
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
            with Session(self.engine) as session:
                session.add_all(rows)
                session.commit()

        try:
            self.execute_with_retry(_insert, operation_name="insert_run_config_kv_bulk")
        except Exception as e:
            logging.warning(f"Failed to insert run config kv rows: {e}")

    def link_artifact_to_run(
        self, artifact_id: uuid.UUID, run_id: str, direction: str
    ) -> None:
        """Creates a link between an artifact and a run without syncing the artifact itself."""

        def _do_link():
            with Session(self.engine) as session:
                link = RunArtifactLink(
                    run_id=run_id,
                    artifact_id=artifact_id,
                    direction=direction,
                )
                session.merge(link)
                session.commit()

        try:
            self.execute_with_retry(_do_link, operation_name="link_artifact")
        except Exception as e:
            logging.warning(
                f"Failed to link artifact {artifact_id} to run {run_id}: {e}"
            )

    def sync_artifact(self, artifact: Artifact, run_id: str, direction: str) -> None:
        """Upserts an Artifact and links it to the Run."""

        def _do_sync():
            with Session(self.engine) as session:
                # Merge artifact (create or update)
                db_artifact = session.merge(artifact)
                session.commit()

        try:
            self.execute_with_retry(_do_sync, operation_name="sync_artifact")
            # Now create the link
            self.link_artifact_to_run(artifact.id, run_id, direction)
        except Exception as e:
            logging.warning(f"Artifact sync failed for {artifact.key}: {e}")

    # --- Read Operations ---

    def find_latest_artifact_at_uri(self, uri: str) -> Optional[Artifact]:
        """Finds the most recent artifact created at this location by a run."""

        def _query():
            with Session(self.engine) as session:
                statement = (
                    select(Artifact)
                    .where(Artifact.uri == uri)
                    .where(Artifact.run_id.is_not(None))  # Must be produced by a run
                    .order_by(Artifact.created_at.desc())
                    .limit(1)
                )
                return session.exec(statement).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_run(self, run_id: str) -> Optional[Run]:
        def _query():
            with Session(self.engine) as session:
                return session.get(Run, run_id)

        return self.execute_with_retry(_query)

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        def _query():
            with Session(self.engine) as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.config_hash == config_hash)
                    .where(Run.input_hash == input_hash)
                    .where(Run.git_hash == git_hash)
                    .order_by(Run.created_at.desc())
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
            with Session(self.engine) as session:
                statement = (
                    select(Run)
                    .where(Run.status == "completed")
                    .where(Run.signature == signature)
                    .order_by(Run.created_at.desc())
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

    def get_artifact(self, key_or_id: str) -> Optional[Artifact]:
        def _query():
            with Session(self.engine) as session:
                try:
                    # Try UUID lookup first
                    uuid_obj = uuid.UUID(str(key_or_id))
                    return session.get(Artifact, uuid_obj)
                except ValueError:
                    # Fallback to Key lookup (most recent)
                    return session.exec(
                        select(Artifact)
                        .where(Artifact.key == key_or_id)
                        .order_by(Artifact.created_at.desc())
                        .limit(1)
                    ).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_artifacts_for_run(self, run_id: str) -> List[Tuple[Artifact, str]]:
        def _query():
            with Session(self.engine) as session:
                return session.exec(
                    select(Artifact, RunArtifactLink.direction)
                    .join(RunArtifactLink, Artifact.id == RunArtifactLink.artifact_id)
                    .where(RunArtifactLink.run_id == run_id)
                ).all()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return []

    def find_runs(
        self,
        tags: Optional[List[str]] = None,
        year: Optional[int] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        name: Optional[str] = None,
    ) -> List[Run]:
        def _query():
            with Session(self.engine) as session:
                statement = select(Run).order_by(Run.created_at.desc())

                if status:
                    statement = statement.where(Run.status == status)
                if model:
                    statement = statement.where(Run.model_name == model)
                if year is not None:
                    statement = statement.where(Run.year == year)
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

    def get_artifact_by_uri(self, uri: str) -> Optional[Artifact]:
        def _query():
            with Session(self.engine) as session:
                return session.exec(
                    select(Artifact)
                    .where(Artifact.uri == uri)
                    .order_by(Artifact.created_at.desc())
                    .limit(1)
                ).first()

        try:
            return self.execute_with_retry(_query)
        except Exception:
            return None

    def get_history(self, limit: int = 10, tags: List[str] = None) -> pd.DataFrame:
        query = f"SELECT * FROM run ORDER BY created_at DESC LIMIT {limit}"
        try:
            df = pd.read_sql(query, self.engine)
            if not df.empty and tags:
                # Basic client-side tag filtering for simplicity
                def has_all_tags(run_tags):
                    # (Implementation of tag parsing logic from original code)
                    if not run_tags:
                        return False
                    if isinstance(run_tags, str):
                        import json

                        try:
                            run_tags = json.loads(run_tags)
                        except:
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
