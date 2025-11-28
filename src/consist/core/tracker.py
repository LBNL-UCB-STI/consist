import logging
import os
from pathlib import Path
from typing import Dict, Optional, List, Any, Type, Iterable, Union
from datetime import datetime, UTC
from contextlib import contextmanager

from sqlmodel import create_engine, Session, select, SQLModel

# Removed SQLModelMetaclass

from consist.core.views import ViewFactory
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink, ConsistRecord
from consist.core.identity import IdentityManager
from consist.core.context import push_tracker, pop_tracker


class Tracker:
    """
    The central orchestrator for Consist, managing the lifecycle of a Run and its associated Artifacts.

    The Tracker is responsible for:
    1.  Initiating and managing the state of individual "Runs" (e.g., model executions, data processing steps).
    2.  Logging "Artifacts" (input files, output data, etc.) and their relationships to runs.
    3.  Implementing a **dual-write mechanism**, logging provenance to both human-readable JSON files (`consist.json`)
        and an analytical DuckDB database (`provenance.duckdb`).
    4.  Providing **path virtualization** to make runs portable across different environments,
        as described in the "Path Resolution & Mounts" architectural section.
    5.  Facilitating **smart caching** based on a Merkle DAG strategy, enabling "run forking" and "hydration"
        of previously computed results.
    """

    def __init__(
        self,
        run_dir: Path,
        db_path: Optional[str] = None,
        mounts: Dict[str, str] = None,
        project_root: str = ".",
        hashing_strategy: str = "full",
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
                                     to this database as part of the **dual-write mechanism**.
                                     If None, database features are disabled.
            mounts (Optional[Dict[str, str]]): A dictionary mapping scheme names (e.g., "inputs", "outputs")
                                             to absolute file system paths. These mounts are used for
                                             **path virtualization**, allowing Consist to store portable URIs
                                             instead of absolute paths, making runs reproducible across environments.
                                             Defaults to an empty dictionary if None.
            project_root (str): The root directory of the project, used by the `IdentityManager`
                                to compute relative paths for code hashing and artifact virtualization.
                                Defaults to the current working directory ".".
        """
        # Force absolute resolve on run_dir to prevent /var vs /private/var mismatches
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}
        self.db_path = db_path
        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )

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

    @property
    def is_cached(self) -> bool:
        """
        Returns True if the current run is a valid Cache Hit.
        This means a `cached_run` (a previously completed `Run` with a matching signature)
        was found and its outputs were successfully validated (e.g., files exist or data
        is in the database, also known as "Ghost Mode").
        """
        return self.current_consist and self.current_consist.cached_run is not None

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        """
        Attempts to find a previously "completed" run that matches the given
        `config_hash`, `input_hash`, and `git_hash`. This method is central
        to Consist's **Smart Caching Strategy** and performs the **Signature Lookup**
        to identify potential cache hits.

        Args:
            config_hash (str): The hash of the run's configuration.
            input_hash (str): The hash derived from the run's input artifacts' provenance IDs.
            git_hash (str): The Git commit hash of the code version.

        Returns:
            Optional[Run]: The most recent matching `Run` object if a cache hit is found,
                           otherwise None.
        """
        if not self.engine:
            return None
        try:
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
        except Exception as e:
            logging.warning(f"[Consist Warning] Cache lookup failed: {e}")
            return None

    def _validate_run_outputs(self, run: Run) -> bool:
        """
        Verifies that all output artifacts of a cached run are still accessible,
        either on disk or marked as ingested in the database.

        This method is crucial for implementing **"Ghost Mode"** (where data in DB
        allows cache hits even if files are missing) and **"Self-Healing"** (forcing
        a re-run if data is missing from both disk and DB).

        Args:
            run (Run): The cached `Run` object whose outputs need validation.

        Returns:
            bool: True if all outputs are accessible, False otherwise.
        """
        if not self.engine:
            return True

        with Session(self.engine) as session:
            statement = (
                select(Artifact)
                .join(RunArtifactLink, Artifact.id == RunArtifactLink.artifact_id)
                .where(RunArtifactLink.run_id == run.id)
                .where(RunArtifactLink.direction == "output")
            )
            outputs = session.exec(statement).all()

            for art in outputs:
                resolved_path = self.resolve_uri(art.uri)
                on_disk = Path(resolved_path).exists()
                in_db = art.meta.get("is_ingested", False)

                if not on_disk and not in_db:
                    logging.warning(
                        f"⚠️ [Consist] Cache Validation Failed. Missing: {art.uri}"
                    )
                    return False
            return True

    @contextmanager
    def start_run(
        self,
        run_id: str,
        model: str,
        config: Dict[str, Any] = None,
        inputs: Optional[List[Union[str, Artifact]]] = None,
        cache_mode: str = "reuse",
        **kwargs,
    ):
        """
        Context manager to initiate and manage the lifecycle of a Consist run.

        This method orchestrates the core Consist features:
        1.  **Run Initialization**: Creates a new `Run` record with basic metadata.
        2.  **Configuration Hashing**: Computes a canonical hash of the provided `config`.
        3.  **Git Version Capture**: Records the current Git commit hash for code provenance.
        4.  **Input Processing**: Resolves input paths, logs them as `Artifacts`, and performs
            **"Auto-Forking"** by detecting lineage from previous runs.
        5.  **Parent Lineage**: Automatically identifies and links to a parent run if inputs
            are outputs of a previous run.
        6.  **Signature Calculation**: Computes the run's unique "signature" (Merkle DAG hash)
            based on code, config, and input hashes.
        7.  **Cache Lookup**: Based on `cache_mode`, attempts to find a matching previously
            completed run. If found and validated (considering **"Ghost Mode"**), it
            flags the current run as cached.
        8.  **Output Hydration**: If a cache hit occurs, outputs from the cached run are
            "hydrated" (loaded into the current context) to simulate re-execution without
            actual computation.
        9.  **Dual-Write Logging**: Persists run metadata to both `consist.json` (for human
            readability and source of truth) and DuckDB (for analytical querying) at
            the start and end of the run, adhering to **"Dual-Write Safety"**.

        Args:
            run_id (str): A unique identifier for this specific run.
            model (str): The name of the model or logical step being executed.
            config (Dict[str, Any], optional): The configuration dictionary for this run.
                                             Defaults to an empty dictionary.
            inputs (Optional[List[Union[str, Artifact]]], optional): A list of paths (str)
                                                                     or `Artifact` objects
                                                                     representing inputs to this run.
            cache_mode (str): Determines caching behavior:
                              - "reuse" (default): Attempts to find and reuse cached results.
                              - "overwrite": Executes the run regardless of cache, updating the cache.
                              - "readonly": Executes if not cached, but does not save new results.
            **kwargs: Additional metadata to store in the run's `meta` field.

        Yields:
            Tracker: The current Tracker instance, providing context for logging artifacts.

        Raises:
            RuntimeError: If `log_artifact` is called outside this context manager.
        """
        # 1. Run Initialization and Context Setup
        if config is None:
            config = {}
        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)

        # Compute core identity hashes early
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

        push_tracker(
            self
        )  # Make this tracker instance globally accessible for log_artifact
        self.current_consist = ConsistRecord(run=run, config=config)

        # 2. Process Inputs & Auto-Forking
        # This step involves resolving input paths and detecting lineage from previous runs.
        if inputs:
            for item in inputs:
                # We reuse the existing log_artifact logic.
                # This handles path resolution, linking, and DB syncing.
                if isinstance(item, Artifact):
                    # If it's an object, we use its key by default for logging
                    self.log_artifact(item, direction="input")
                else:
                    # Infer key from filename for convenience if a string path is given
                    key = Path(item).stem
                    self.log_artifact(item, key=key, direction="input")

        # 3. Detect Parent Lineage for run.parent_run_id
        # Automatically link to a parent run if any input artifact was an output of another run.
        parent_candidates = [
            a.run_id for a in self.current_consist.inputs if a.run_id is not None
        ]
        if parent_candidates:
            # We pick the run_id of the last processed input artifact that has lineage
            run.parent_run_id = parent_candidates[-1]

        # 4. Identity Completion: Compute Merkle DAG signature
        try:
            # Hash of all input artifacts' provenance (recursively)
            input_hash = self.identity.compute_input_hash(
                self.current_consist.inputs, path_resolver=self.resolve_uri
            )
            run.input_hash = input_hash
            # The composite signature for cache lookup
            run.signature = self.identity.calculate_run_signature(
                code_hash=git_hash, config_hash=config_hash, input_hash=input_hash
            )
        except Exception as e:
            logging.warning(
                f"[Consist Warning] Failed to compute inputs hash for run {run_id}: {e}"
            )
            run.input_hash = "error"  # Mark as error to prevent false cache hits
            run.signature = "error"

        # 5. Cache Lookup (Smart Caching)
        cached_run = None

        if cache_mode == "reuse":
            # Only perform cache lookup if mode is "reuse"
            cached_run = self.find_matching_run(
                config_hash=run.config_hash,
                input_hash=run.input_hash,
                git_hash=run.git_hash,
            )
            if cached_run:
                # Validate cached run outputs for "Ghost Mode" and "Self-Healing"
                if self._validate_run_outputs(cached_run):
                    self.current_consist.cached_run = cached_run
                    logging.info(
                        f"✅ [Consist] Cache HIT! Matching run: {cached_run.id}"
                    )

                    # HYDRATE OUTPUTS: Populate current run's outputs from the cached run
                    with Session(self.engine) as session:
                        statement = (
                            select(Artifact)
                            .join(
                                RunArtifactLink,
                                Artifact.id == RunArtifactLink.artifact_id,
                            )
                            .where(RunArtifactLink.run_id == cached_run.id)
                            .where(RunArtifactLink.direction == "output")
                        )
                        cached_outputs = session.exec(statement).all()
                        for art in cached_outputs:
                            # Expunge from session to prevent conflicts and allow use outside session
                            session.expunge(art)
                            # Ensure _abs_path is set for hydrated artifacts for consistency
                            art.abs_path = self.resolve_uri(art.uri)
                            self.current_consist.outputs.append(art)
                else:
                    logging.info(
                        "🔄 [Consist] Cache Miss (Data Missing or Invalidated). Re-running..."
                    )
        elif cache_mode == "overwrite":
            logging.warning(
                "⚠️ [Consist] Cache lookup skipped (Mode: Overwrite). Run will execute and update cache."
            )
        elif cache_mode == "readonly":
            logging.info(
                "👁️ [Consist] Cache lookup in read-only mode. New results will not be saved."
            )
            # In readonly, we still lookup but don't set cached_run so it executes, but also don't save new results.
            # This current logic implies "readonly" still re-runs if no cache hit.
            # If the intent for "readonly" is to *only* use cache and skip if not cached,
            # further modification would be needed here (e.g., raise an error or exit).

        # 6. Dual-Write Logging: Initial flush to JSON and DB
        self._flush_json()  # Always flush JSON first for "Dual-Write Safety"
        self._sync_run_to_db(run)

        try:
            yield self  # The user's code executes here
            # If we reached here, the run completed successfully
            if cache_mode != "readonly":  # Only update status if not in readonly mode
                run.status = "completed"
            else:
                run.status = "skipped_save"  # A new status for readonly completed runs
        except Exception as e:
            run.status = "failed"
            run.meta["error"] = str(e)
            raise e  # Re-raise the exception after logging
        finally:
            pop_tracker()  # Clean up global tracker context

            # 7. Final Dual-Write Logging
            run.updated_at = datetime.now(UTC)
            self._flush_json()
            if cache_mode != "readonly":  # Only sync to DB if not in readonly mode
                self._sync_run_to_db(run)
            self.current_consist = None  # Clear current run context

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

        This method supports:
        - **Automatic Input Discovery**: If an input `path` matches a previously
          logged output artifact, Consist automatically links them, building the
          provenance graph. This is a key part of **"Auto-Forking"**.
        - **Path Virtualization**: Converts absolute file system paths to portable URIs
          (e.g., `inputs://data.csv`) using configured mounts, adhering to
          **"Path Resolution & Mounts"**.
        - **Schema Metadata Injection**: Embeds schema information (if provided) into the
          artifact's metadata, useful for later "Strict Mode" validation or introspection.

        Args:
            path (Union[str, Artifact]): The file path (str) or an existing `Artifact` object
                                         to be logged.
            key (Optional[str]): A semantic, human-readable name for the artifact (e.g., "households").
                                 Required if `path` is a string.
            direction (str): Specifies whether the artifact is an "input" or "output" for the
                             current run. Defaults to "output".
            schema (Optional[Type[SQLModel]]): An optional SQLModel class that defines the
                                                expected schema for the artifact's data. Its
                                                name will be stored in artifact metadata.
            **meta: Additional key-value pairs to store in the artifact's flexible `meta` field.

        Returns:
            Artifact: The created or updated `Artifact` object.

        Raises:
            RuntimeError: If called outside an active run context.
            ValueError: If `key` is not provided when `path` is a string.
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        artifact_obj = None
        resolved_abs_path = None

        # --- Logic Branch A: Artifact Object Passed (Explicit Chaining/Re-logging) ---
        if isinstance(path, Artifact):
            artifact_obj = path
            # Resolve absolute path from existing artifact URI or _abs_path if already set
            resolved_abs_path = artifact_obj.abs_path or self.resolve_uri(
                artifact_obj.uri
            )
            if key is None:
                key = artifact_obj.key  # Use existing key if not overridden
            if meta:
                artifact_obj.meta.update(meta)  # Update existing meta with new values

        # --- Logic Branch B: String Path Passed (New or Discovered Artifact) ---
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a string.")

            # Resolve to absolute path and then virtualize for storage
            resolved_abs_path = str(Path(path).resolve())
            uri = self._virtualize_path(resolved_abs_path)

            # 1. Lineage Discovery (Automatic Input Discovery)
            # For input artifacts, check if an artifact with this URI was previously output by another run.
            if direction == "input" and self.engine:
                try:
                    with Session(self.engine) as session:
                        # Find the most recent artifact created at this location with a run_id
                        statement = (
                            select(Artifact)
                            .where(Artifact.uri == uri)
                            .where(
                                Artifact.run_id.is_not(None)
                            )  # Ensure it's an output of a run
                            .order_by(Artifact.created_at.desc())
                            .limit(1)
                        )
                        parent = session.exec(statement).first()

                        if parent:
                            # LINEAGE FOUND! Reuse the existing artifact object to link to its creator run.
                            # Detach from session so we can use it in our current flow without binding issues
                            session.expunge(parent)
                            artifact_obj = parent
                            if meta:
                                artifact_obj.meta.update(meta)  # Apply any new meta
                except Exception as e:
                    logging.warning(
                        f"[Consist Warning] Lineage discovery failed for {uri}: {e}"
                    )

            # 2. If no parent artifact found or it's an output, create a fresh Artifact object
            if artifact_obj is None:
                driver = Path(path).suffix.lstrip(".").lower() or "unknown"
                artifact_obj = Artifact(
                    key=key,
                    uri=uri,
                    driver=driver,
                    run_id=(
                        self.current_consist.run.id if direction == "output" else None
                    ),  # Only outputs have a creating run_id
                    meta=meta,
                )

        # --- Common Logic (Applies to both branches) ---

        # Schema Metadata Injection
        if schema:
            artifact_obj.meta["schema_name"] = schema.__name__
            artifact_obj.meta["has_strict_schema"] = True

        # Attach Runtime Absolute Path (Vital for chaining and external tools)
        # This is a PrivateAttr and not persisted to the DB, part of "Artifact Chaining (Runtime vs Persistence)"
        artifact_obj.abs_path = resolved_abs_path

        # Update In-Memory State (The ConsistRecord being built for the current run)
        if direction == "input":
            self.current_consist.inputs.append(artifact_obj)
        else:
            self.current_consist.outputs.append(artifact_obj)

        # Write to Persistence (Dual-Write)
        self._flush_json()  # Always flush JSON first
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
        Ingests data associated with an `Artifact` into the Consist DuckDB database.

        This method is central to Consist's **"Hot Data Strategy"**, where data is
        materialized into the database for faster query performance and easier sharing.
        It leverages the `dlt` (Data Load Tool) integration for efficient and robust
        data loading, including support for schema inference and evolution.

        Args:
            artifact (Artifact): The artifact object representing the data being ingested.
                                 Its metadata might include schema information.
            data (Optional[Iterable[Dict[str, Any]]]): An iterable (e.g., list of dicts, generator)
                                             where each item represents a row of data to be ingested.
                                             If 'data' is omitted, Consist attempts to stream it
                                             directly from the artifact's file URI, resolving the path.
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
        if not self.db_path:
            raise RuntimeError("Cannot ingest data: No database configured.")
        target_run = run or (self.current_consist.run if self.current_consist else None)
        if not target_run:
            raise RuntimeError("Cannot ingest data: No active run context.")

        if self.engine:
            self.engine.dispose()
        from consist.integrations.dlt_loader import ingest_artifact

        # Auto-Resolve Data if None
        data_to_pass = data
        if data_to_pass is None:
            # If no data provided, we assume we should read from the artifact's file
            # We resolve the URI to an absolute path string
            data_to_pass = self.resolve_uri(artifact.uri)

        try:
            info = ingest_artifact(
                artifact=artifact,
                run_context=target_run,
                db_path=self.db_path,
                data_iterable=data_to_pass,
                schema_model=schema,
            )

            # FORCE Metadata update
            new_meta = dict(artifact.meta)
            new_meta["is_ingested"] = True
            artifact.meta = new_meta

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

        This view is a core component of Consist's **"View Factory"** and **"Hybrid Views"**
        strategy, allowing transparent querying of data regardless of its underlying
        storage mechanism (hot/cold data).

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

        This is the inverse operation of `_virtualize_path`, crucial for **"Path Resolution & Mounts"**.
        It uses the configured `mounts` and the `run_dir` to reconstruct the local
        absolute path to an artifact, making runs portable.

        Args:
            uri (str): The portable URI (e.g., "inputs://file.csv", "./output/data.parquet")
                       to resolve.

        Returns:
            str: The absolute file system path corresponding to the given URI.
                 If the URI cannot be fully resolved (e.g., scheme not mounted),
                 it returns the most resolved path or the original URI.
        """
        path_str = uri
        # 1. Check schemes (mounts)
        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            if scheme in self.mounts:
                path_str = str(Path(self.mounts[scheme]) / rel_path)
            elif scheme == "file":
                path_str = rel_path
        elif uri.startswith("./"):
            path_str = str(self.run_dir / uri[2:])

        # Ensure we always return absolute, resolved paths
        return str(Path(path_str).resolve())

    def _virtualize_path(self, path: str) -> str:
        """
        Converts an absolute file system path into a portable Consist URI.

        This method is a key part of **"Path Resolution & Mounts"**, attempting to
        replace parts of the absolute path with scheme-based URIs (e.g., "inputs://")
        if a matching mount is configured, or makes it relative to the `run_dir`
        if possible. This ensures artifact paths stored in the provenance are portable
        across different execution environments.

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
        Writes the current `ConsistRecord` (in-memory state of the run) to a `consist.json` file.
        This operation is performed using an atomic write pattern (write to temp, then rename)
        to ensure data integrity.

        This is a critical part of the **"Dual-Write Safety"** strategy: the JSON file
        is always flushed first, ensuring that a human-readable record of the run exists
        even if the subsequent database synchronization fails.
        """
        if not self.current_consist:
            return
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
        of the run. It uses a **"Clone and Push"** strategy to avoid binding the
        live run object to the session.

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

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
                    db_run.input_hash = run.input_hash
                    db_run.signature = run.signature
                    db_run.parent_run_id = run.parent_run_id
                    session.add(db_run)
                else:
                    # Insert new: Create a fresh copy for the DB
                    # This ensures the original 'run' variable stays pure/detached
                    run_data = run.model_dump()
                    new_run = Run(**run_data)
                    session.add(new_run)

                session.commit()
        except Exception as e:
            logging.warning(f"[Consist Warning] Database sync failed: {e}")

    def _sync_artifact_to_db(self, artifact: Artifact, direction: str):
        """
        Synchronizes an `Artifact` object and its `RunArtifactLink` to the DuckDB database.

        This method merges the artifact (either creating it or updating an existing one)
        and creates a link entry associating it with the current run and its direction
        (input/output).

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

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
            logging.warning(f"[Consist Warning] Artifact sync failed: {e}")
