import os
import json
from pathlib import Path
from typing import Dict, Optional, List, Any, Type, Iterable
from datetime import datetime, UTC
from uuid import uuid4
from contextlib import contextmanager

from sqlmodel import create_engine, Session, select, SQLModel
from sqlmodel.main import SQLModelMetaclass

from consist.core.views import ViewFactory
# Models
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink, ConsistRecord


class Tracker:
    """
    The central orchestrator for Consist, managing the lifecycle of a Run and its associated Artifacts.

    The Tracker is responsible for:
    1. Initiating and managing the state of individual "Runs" (e.g., model executions, data processing steps).
    2. Logging "Artifacts" (input files, output data, etc.) and their relationships to runs.
    3. Implementing a dual-write mechanism, logging provenance to both human-readable JSON files
       and a DuckDB database for analytical querying.
    4. Providing path virtualization to make runs portable across different environments.
    """
    def __init__(
        self,
        run_dir: Path,
        db_path: Optional[str] = None,
        mounts: Dict[str, str] = None,
    ):
        """
        Initializes the Consist Tracker.

        Sets up the directory for run logs, configures path virtualization mounts,
        and optionally initializes the DuckDB database connection.

        Args:
            run_dir (Path): The root directory where run-specific logs (e.g., `consist.json`)
                            and potentially other run outputs will be stored. This directory
                            will be created if it does not exist.
            db_path (Optional[str]): The file path to the DuckDB database. If provided,
                                     the tracker will persist run and artifact metadata
                                     to this database. If None, database features are disabled.
            mounts (Optional[Dict[str, str]]): A dictionary mapping scheme names (e.g., "inputs", "outputs")
                                             to absolute file system paths. These mounts are used for
                                             virtualizing artifact paths, making runs portable.
                                             Defaults to an empty dictionary if None.
        """
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}
        self.db_path = db_path

        # Database Setup (Optional, tolerant to missing DB)
        self.engine = None
        if db_path:
            # Using duckdb-engine for SQLAlchemy support
            self.engine = create_engine(f"duckdb:///{db_path}")

            SQLModel.metadata.create_all(
                self.engine,
                tables=[
                    Run.__table__,
                    Artifact.__table__,
                    RunArtifactLink.__table__
                ]
            )

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

    @contextmanager
    def start_run(
        self, run_id: str, model: str, config: Dict[str, Any] = None, **kwargs
    ):
        """
        A context manager that defines and manages the lifecycle of a single Consist run.

        This method initializes a new run record, sets its status, and ensures proper
        state management and logging (to both JSON and the database) throughout the
        execution block. It handles both successful completion and exceptions.

        Args:
            run_id (str): A unique identifier for the current run.
            model (str): The name of the model or logical step associated with this run.
            config (Optional[Dict[str, Any]]): A dictionary representing the configuration
                                                used for this run. Defaults to an empty dict.
            **kwargs: Additional metadata to be stored in the `meta` field of the Run object.
                      Common examples include 'year', 'iteration', or custom tags.

        Yields:
            Tracker: The Tracker instance, allowing methods to be called within the run context.

        Raises:
            Exception: Any exception raised within the 'with' block will be caught,
                       the run status will be updated to "failed", and then the exception
                       will be re-raised.
        """

        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)

        run = Run(
            id=run_id,
            model_name=model,
            year=year,  # Goes to optimized SQL column
            iteration=iteration,  # Goes to optimized SQL column
            status="running",
            meta=kwargs,
            created_at=datetime.now(UTC),
        )

        self.current_consist = ConsistRecord(run=run, config=config or {})

        # Initial Flush
        self._flush_json()
        self._sync_run_to_db(run)

        try:
            yield self
            run.status = "completed"
        except Exception as e:
            run.status = "failed"
            run.meta["error"] = str(e)
            raise e
        finally:
            run.updated_at = datetime.now(UTC)
            self._flush_json()
            self._sync_run_to_db(run)
            self.current_consist = None

    def ingest(
            self,
            artifact: Artifact,
            data: Iterable[Dict[str, Any]],
            schema: Optional[Type[SQLModel]] = None
    ):
        """
        Ingests an iterable of dictionary data into the global DuckDB database.

        This method uses the `dlt` (Data Load Tool) integration to load data associated
        with a given artifact into the database. It handles temporary database connection
        disposal and re-establishment to allow `dlt` exclusive access.

        Args:
            artifact (Artifact): The artifact object representing the data being ingested.
                                 Its metadata might include schema information.
            data (Iterable[Dict[str, Any]]): An iterable (e.g., list of dicts, generator)
                                             where each item represents a row of data to be ingested.
            schema (Optional[Type[SQLModel]]): An optional SQLModel class that defines the
                                                expected schema for the ingested data. If provided,
                                                `dlt` will use this for strict validation.

        Raises:
            RuntimeError: If no database is configured (`db_path` was not provided during
                          Tracker initialization) or if `ingest` is called outside of
                          an active run context.
            Exception: Any exception raised by the underlying `dlt` ingestion process.
        """
        if not self.db_path:
            raise RuntimeError("Cannot ingest data: No database configured.")

        if not self.current_consist:
            raise RuntimeError("Cannot ingest data outside of a run context.")

        # 1. Release Lock
        # We temporarily close our connection so dlt can open the file exclusively.
        if self.engine:
            self.engine.dispose()

        # 2. Delegate to Loader
        # Local import to avoid top-level dependency weight
        from consist.integrations.dlt_loader import ingest_artifact

        try:
            return ingest_artifact(
                artifact=artifact,
                run_context=self.current_consist.run,
                db_path=self.db_path,
                data_iterable=data,
                schema_model=schema
            )
        except Exception as e:
            # Re-raise, but the engine remains disposed (safe)
            raise e
        # Note: We don't need to explicitly reconnect self.engine.
        # SQLAlchemy will automatically reconnect the next time it's used.

    def log_artifact(
        self,
        path: str,
        key: str,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        **meta,
    ) -> Artifact:
        """
        Logs an artifact (file or data reference) within the current run context.

        This method virtualizes the artifact's path, infers its driver, injects schema
        metadata if provided, creates an `Artifact` object, and then persists its
        metadata to both the JSON log and the database.

        Args:
            path (str): The absolute or relative file system path to the artifact.
                        This path will be converted to a portable URI.
            key (str): A semantic name for the artifact (e.g., "households", "cleaned_data").
            direction (str): Specifies whether the artifact is an "input" to the run
                             or an "output" generated by the run. Defaults to "output".
            schema (Optional[Type[SQLModel]]): An optional SQLModel class that describes the
                                                structure of the data contained in this artifact.
                                                If provided, its name and a flag will be stored
                                                in the artifact's metadata.
            **meta: Arbitrary keyword arguments that will be stored as additional metadata
                    within the artifact's `meta` dictionary.

        Returns:
            Artifact: The newly created and logged `Artifact` object.

        Raises:
            RuntimeError: If `log_artifact` is called outside of an active run context.
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        # 1. Path Virtualization
        uri = self._virtualize_path(path)

        # 2. Driver Inference
        driver = Path(path).suffix.lstrip(".").lower() or "unknown"

        # 3. Schema Metadata Injection
        if schema:
            # We store the class name. In the future, we could store a hash of model_json_schema()
            meta["schema_name"] = schema.__name__
            meta["has_strict_schema"] = True

        # 4. Create Object
        artifact = Artifact(
            key=key,
            uri=uri,
            driver=driver,
            run_id=self.current_consist.run.id if direction == "output" else None,
            meta=meta,
        )

        # 5. Update Memory
        if direction == "input":
            self.current_consist.inputs.append(artifact)
        else:
            self.current_consist.outputs.append(artifact)

        # 6. Write
        self._flush_json()
        self._sync_artifact_to_db(artifact, direction)

        return artifact

    def create_view(self, view_name: str, concept_key: str):
        """
        Creates a hybrid SQL view that consolidates data from both materialized tables
        in DuckDB and raw file-based artifacts (e.g., Parquet, CSV).

        This view allows transparent querying of data regardless of its underlying
        storage mechanism.

        Args:
            view_name (str): The desired name for the generated SQL view.
            concept_key (str): The semantic key (e.g., "households", "transactions")
                               that identifies the logical concept this view represents.
                               The view will union all artifacts with this key.
        """
        factory = ViewFactory(self)
        return factory.create_hybrid_view(view_name, concept_key)

    def resolve_uri(self, uri: str) -> str:
        """
        Converts a portable Consist URI back into an absolute file system path.

        This is the inverse operation of `_virtualize_path`, using the configured mounts
        and run directory to reconstruct the local path to an artifact.

        Args:
            uri (str): The portable URI (e.g., "inputs://file.csv", "./output/data.parquet")
                       to resolve.

        Returns:
            str: The absolute file system path corresponding to the given URI.
                 If the URI cannot be fully resolved (e.g., scheme not mounted),
                 it returns the most resolved path or the original URI.
        """
        # 1. Check schemes (mounts)
        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            if scheme in self.mounts:
                root = self.mounts[scheme]
                return str(Path(root) / rel_path)

            # Handle file:// protocol if present
            if scheme == "file":
                return rel_path

        # 2. Handle relative paths (./output/...)
        if uri.startswith("./"):
            return str(self.run_dir / uri[2:])

        # 3. Fallback: Return as is (assume absolute or unrecognized)
        return uri

    # --- Internals ---

    def _virtualize_path(self, path: str) -> str:
        """
        Converts an absolute file system path into a portable Consist URI.

        This method attempts to replace parts of the absolute path with scheme-based URIs
        (e.g., "inputs://") if a matching mount is configured, or makes it relative
        to the run directory if possible. This ensures artifact paths are portable.

        Args:
            path (str): The absolute file system path to virtualize.

        Returns:
            str: A portable URI representation of the path (e.g., "inputs://file.csv",
                 "./output/data.parquet", or the original absolute path if no virtualization
                 is possible).
        """
        abs_path = str(Path(path).resolve())

        # Check mounts longest-match first
        for name, root in sorted(
            self.mounts.items(), key=lambda x: len(x[1]), reverse=True
        ):
            root_abs = str(Path(root).resolve())
            if abs_path.startswith(root_abs):
                rel = os.path.relpath(abs_path, root_abs)
                return f"{name}://{rel}"

        # Fallback: Relative to run_dir if possible, else strict absolute
        try:
            rel = os.path.relpath(abs_path, self.run_dir)
            if not rel.startswith(".."):
                return f"./{rel}"
        except ValueError:
            pass

        return abs_path

    def _flush_json(self):
        """
        Atomically writes the current `ConsistRecord` (representing the run's state)
        to a human-readable JSON file (`consist.json`) within the run directory.

        This method ensures data integrity by writing to a temporary file first
        and then renaming it, preventing corruption if the process is interrupted.
        It only operates if there is an active run context (`current_consist`).
        """
        if not self.current_consist:
            return

        # Note: Pydantic V2 uses model_dump_json()
        # We rely on a standard encoder for things like numpy (not implemented here yet)
        json_str = self.current_consist.model_dump_json(indent=2)

        target = self.run_dir / "consist.json"
        # Write temp then rename to ensure no corrupted files
        tmp = target.with_suffix(".tmp")
        with open(tmp, "w") as f:
            f.write(json_str)
        tmp.rename(target)

    def _sync_run_to_db(self, run: Run):
        """
        Synchronizes the state of a `Run` object to the DuckDB database.

        This method either updates an existing run record or inserts a new one,
        ensuring that the database reflects the most current status and metadata
        of the run. It uses a "Clone and Push" strategy to avoid binding the
        live run object to the session and tolerates database failures.

        Args:
            run (Run): The `Run` object whose state needs to be synchronized with the database.
        """
        if not self.engine:
            return
        try:
            with Session(self.engine) as session:
                # FIX: Never bind the live 'run' object to this temporary session.
                # If we do session.add(run), it gets attached, then expired on commit.
                # Instead, we perform a "Clone and Push" operation.

                db_run = session.get(Run, run.id)
                if db_run:
                    # Update existing DB row explicitly
                    db_run.status = run.status
                    db_run.updated_at = run.updated_at
                    db_run.meta = run.meta
                    session.add(db_run)
                else:
                    # Insert new: Create a fresh copy for the DB
                    # This ensures the original 'run' variable stays pure/detached
                    run_data = run.model_dump()
                    new_run = Run(**run_data)
                    session.add(new_run)

                session.commit()
        except Exception as e:
            print(f"[Consist Warning] Database sync failed: {e}")

    def _sync_artifact_to_db(self, artifact: Artifact, direction: str):
        """
        Synchronizes an `Artifact` object and its `RunArtifactLink` to the DuckDB database.

        This method merges the artifact (either creating it or updating an existing one)
        and creates a link entry associating it with the current run and its direction
        (input/output). It tolerates database failures.

        Args:
            artifact (Artifact): The `Artifact` object to synchronize.
            direction (str): The direction of the artifact relative to the current run
                             ("input" or "output").
        """
        if not self.engine or not self.current_consist:
            return
        try:
            with Session(self.engine) as session:
                # Merge artifact (create or update)
                db_artifact = session.merge(artifact)

                # Create Link
                link = RunArtifactLink(
                    run_id=self.current_consist.run.id,
                    artifact_id=db_artifact.id,  # Use DB ID
                    direction=direction,
                )
                session.merge(link)
                session.commit()
        except Exception as e:
            print(f"[Consist Warning] Artifact sync failed: {e}")
