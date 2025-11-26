import os
from pathlib import Path
from typing import Dict, Optional, Any, Type, Iterable, Union
from datetime import datetime, UTC
from contextlib import contextmanager

from sqlmodel import create_engine, Session, SQLModel

from consist.core.views import ViewFactory
from consist.core.context import push_tracker, pop_tracker

# Models
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink, ConsistRecord

# Core
from consist.core.identity import IdentityManager


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
        project_root: str = ".",
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
        self.identity = IdentityManager(project_root=project_root)
        self.engine = None
        if db_path:
            # Using duckdb-engine for SQLAlchemy support
            self.engine = create_engine(f"duckdb:///{db_path}")

            SQLModel.metadata.create_all(
                self.engine,
                tables=[Run.__table__, Artifact.__table__, RunArtifactLink.__table__],
            )

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

    @contextmanager
    def start_run(
        self, run_id: str, model: str, config: Dict[str, Any] = None, **kwargs
    ):
        if config is None:
            config = {}

        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)

        config_hash = self.identity.compute_config_hash(config)
        git_hash = self.identity.get_code_version()

        run = Run(
            id=run_id,
            model_name=model,
            year=year,
            iteration=iteration,
            status="running",
            config_hash=config_hash,
            git_hash=git_hash,
            meta=kwargs,
            created_at=datetime.now(UTC),
        )

        push_tracker(self)

        self.current_consist = ConsistRecord(run=run, config=config)

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
            pop_tracker()

            run.updated_at = datetime.now(UTC)
            self._flush_json()
            self._sync_run_to_db(run)
            self.current_consist = None

    def log_artifact(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
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
            path (Union[str, Artifact]): Either a file path string OR an existing Artifact object
                                         (e.g., from a previous run).
            key (Optional[str]): Semantic name. Required if 'path' is a string.
                                 Defaults to artifact.key if 'path' is an Artifact object.
            direction (str): "input" or "output".
            schema (Optional[Type[SQLModel]]): Schema definition for the data.
            **meta: Additional metadata.

        Returns:
            Artifact: The newly created and logged `Artifact` object.

        Raises:
            RuntimeError: If `log_artifact` is called outside an active run context.
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        artifact_obj = None
        resolved_abs_path = None

        # --- Logic Branch A: Artifact Object Passed (Reuse/Link) ---
        if isinstance(path, Artifact):
            artifact_obj = path

            # 1. Resolve Physical Path (for hashing/validity)
            # Use runtime cache if available, else try resolving URI
            if artifact_obj.abs_path:
                resolved_abs_path = artifact_obj.abs_path
            else:
                # Warning: If URI is relative, this might resolve incorrectly in a new run dir
                # but we attempt it as best-effort.
                resolved_abs_path = self.resolve_uri(artifact_obj.uri)

            # 2. Inherit/Override Metadata
            # If key is not provided, keep original. If provided, we treat it as an alias?
            # For simplicity, we stick to the original key unless we want to rename.
            # Let's enforce the key matches or default to it.
            if key is None:
                key = artifact_obj.key

            # Update meta if provided
            if meta:
                # Shallow merge
                artifact_obj.meta.update(meta)

        # --- Logic Branch B: String Path Passed (New/Discovery) ---
        else:
            if key is None:
                raise ValueError("Argument 'key' is required when logging a new path.")

            # 1. Path Virtualization
            # Convert physical path -> Virtual URI
            resolved_abs_path = str(Path(path).resolve())
            uri = self._virtualize_path(resolved_abs_path)

            # 2. Driver Inference
            driver = Path(path).suffix.lstrip(".").lower() or "unknown"

            # 3. Create Object
            artifact_obj = Artifact(
                key=key,
                uri=uri,
                driver=driver,
                run_id=self.current_consist.run.id if direction == "output" else None,
                meta=meta,
            )

        # --- Common Logic ---

        # Schema Metadata Injection
        if schema:
            artifact_obj.meta["schema_name"] = schema.__name__
            artifact_obj.meta["has_strict_schema"] = True

        # Attach Runtime Path (Vital for chaining)
        artifact_obj.abs_path = resolved_abs_path

        # Update Memory (Current Run Record)
        if direction == "input":
            self.current_consist.inputs.append(artifact_obj)
        else:
            self.current_consist.outputs.append(artifact_obj)

        # Write to Persistence
        self._flush_json()
        self._sync_artifact_to_db(artifact_obj, direction)

        return artifact_obj

    def ingest(
        self,
        artifact: Artifact,
        data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
        schema: Optional[Type[SQLModel]] = None,
        run: Optional[Run] = None,
    ):
        """
        Ingests an iterable of dictionary data into the global DuckDB database.

        This method uses the `dlt` (Data Load Tool) integration to load data associated
        with a given artifact into the database. It handles temporary database connection
        disposal and re-establishment to allow `dlt` exclusive access.

        Args:
            artifact (Artifact): The artifact object representing the data being ingested.
                                 Its metadata might include schema information.
            data (Optional[Iterable[Dict[str, Any]]]): An iterable (e.g., list of dicts, generator)
                                             where each item represents a row of data to be ingested.
                                             If 'data' is omitted, Consist attempts to stream it
                                             directly from the artifact's file URI.
            schema (Optional[Type[SQLModel]]): An optional SQLModel class that defines the
                                                expected schema for the ingested data. If provided,
                                                `dlt` will use this for strict validation.
            run (Optional[Run]): If provided, tags data with this run's ID (Offline Mode).
                                 If None, uses the currently active run (Online Mode).

        Raises:
            RuntimeError: If no database is configured (`db_path` was not provided during
                          Tracker initialization) or if `ingest` is called outside of
                          an active run context.
            Exception: Any exception raised by the underlying `dlt` ingestion process.
        """
        # Determine Context
        target_run = None
        if run:
            # Offline / Post-Hoc Mode
            target_run = run
        elif self.current_consist:
            # Online / Active Mode
            target_run = self.current_consist.run
        else:
            raise RuntimeError(
                "Cannot ingest data: No active run context and no explicit 'run' argument provided."
            )

        # 1. Release Lock
        # We temporarily close our connection so dlt can open the file exclusively.
        if self.engine:
            self.engine.dispose()

        data_to_pass = data
        if data_to_pass is None:
            # If no data provided, we assume we should read from the artifact's file
            # We resolve the URI to an absolute path string
            data_to_pass = self.resolve_uri(artifact.uri)

        # 2. Delegate to Loader
        from consist.integrations.dlt_loader import ingest_artifact

        try:
            info = ingest_artifact(
                artifact=artifact,
                run_context=target_run,
                db_path=self.db_path,
                data_iterable=data_to_pass,
                schema_model=schema,
            )

            # Prevent hybrid views from double-counting this artifact
            artifact.meta["is_ingested"] = True

            # Persist metadata update to DB
            if self.engine:
                with Session(self.engine) as session:
                    session.merge(artifact)
                    session.commit()

            return info
        except Exception as e:
            raise e
        # Note: We don't need to explicitly reconnect self.engine.
        # SQLAlchemy will automatically reconnect the next time it's used.

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
                    db_run.config_hash = run.config_hash
                    db_run.git_hash = run.git_hash
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
                    artifact_id=db_artifact.id,
                    direction=direction,
                )
                session.merge(link)
                session.commit()
        except Exception as e:
            print(f"[Consist Warning] Artifact sync failed: {e}")
