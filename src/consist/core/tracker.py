import logging
import uuid
from pathlib import Path
from typing import Dict, Optional, List, Any, Type, Iterable, Union, Callable, Tuple
from datetime import datetime, timezone

from sqlmodel import SQLModel

UTC = timezone.utc
from contextlib import contextmanager

import pandas as pd

from pydantic import BaseModel

from consist.core.views import ViewFactory
from consist.core.fs import FileSystemManager
from consist.core.persistence import DatabaseManager
from consist.core.decorators import create_task_decorator
from consist.core.events import EventManager
from consist.models.artifact import Artifact
from consist.models.run import Run, ConsistRecord
from consist.core.identity import IdentityManager
from consist.core.context import push_tracker, pop_tracker


class OutputCapture:
    """
    A helper object to temporarily hold artifacts captured during a `capture_outputs` block.

    This class provides a convenient way for users to access the artifacts that were
    automatically logged by the system immediately after a `capture_outputs` context manager
    finishes execution.

    Attributes:
        artifacts (List[Artifact]): A list of `Artifact` objects that were
                                    captured and logged during the `capture_outputs` block.
    """

    def __init__(self) -> None:
        """
        Initializes the OutputCapture object.

        This constructor sets up an empty list to store `Artifact` objects that
        will be captured during a `capture_outputs` context.
        """
        self.artifacts: List[Artifact] = []


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
        # 1. Initialize FileSystem Service
        # (This handles the mkdir and path resolution internally now)
        self.fs = FileSystemManager(run_dir, mounts)
        self.events = EventManager()

        self.mounts = self.fs.mounts
        self.run_dir = self.fs.run_dir

        self.db_path = db_path
        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )

        self.db = None
        if db_path:
            self.db = DatabaseManager(db_path)

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

        # Introspection State (Last completed run)
        self._last_consist: Optional[ConsistRecord] = None

        # Active run tracking (for imperative begin_run/end_run pattern)
        self._active_run_cache_mode: Optional[str] = None

    @property
    def engine(self):
        """Delegates to the DatabaseManager engine."""
        return self.db.engine if self.db else None

    def _resolve_run_signature(self, run_id: str) -> Optional[str]:
        """
        Internal helper to look up a run's signature (Merkle identity) via the database.
        Used by IdentityManager to stabilize input hashes against ephemeral Run IDs.
        """
        # 1. Check active run (unlikely for inputs, but good for completeness)
        if self.current_consist and self.current_consist.run.id == run_id:
            return self.current_consist.run.signature
        # 2. Check Database
        if self.db:
            run = self.db.get_run(run_id)
            if run:
                return run.signature
        return None

    # --- Run Management ---

    def begin_run(
        self,
        run_id: str,
        model: str,
        config: Union[Dict[str, Any], BaseModel, None] = None,
        inputs: Optional[List[Union[str, Artifact]]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        **kwargs: Any,
    ) -> Run:
        """
        Start a run imperatively (without context manager).

        Use this when run start and end are in separate methods, or when integrating
        with frameworks that have their own lifecycle management. Returns the Run object.
        Call end_run() when complete.

        This provides an alternative to the context manager pattern when you need more
        control over the run lifecycle, such as in PILATES-like integrations where
        start_model_run() and complete_model_run() are separate method calls.

        Parameters
        ----------
        run_id : str
            A unique identifier for the current run.
        model : str
            A descriptive name for the model or process being executed.
        config : Union[Dict[str, Any], BaseModel, None], optional
            Configuration parameters for this run.
        inputs : Optional[List[Union[str, Artifact]]], optional
            A list of input paths or Artifact objects.
        tags : Optional[List[str]], optional
            A list of string labels for categorization and filtering.
        description : Optional[str], optional
            A human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching: "reuse", "overwrite", or "readonly".
        **kwargs : Any
            Additional metadata. Special keywords `year` and `iteration` can be used.

        Returns
        -------
        Run
            The Run object representing the started run.

        Raises
        ------
        RuntimeError
            If there is already an active run.

        Example
        -------
        ```python
        run = tracker.begin_run("run_001", "urbansim", config={...})
        try:
            tracker.log_artifact(input_file, direction="input")
            # ... do work ...
            tracker.log_artifact(output_file, direction="output")
            tracker.end_run("completed")
        except Exception as e:
            tracker.end_run("failed", error=e)
            raise
        ```
        """
        if self.current_consist is not None:
            raise RuntimeError(
                f"Cannot begin_run: A run is already active (id={self.current_consist.run.id}). "
                "Call end_run() first."
            )

        if config is None:
            config = {}
        elif isinstance(config, BaseModel):
            config = config.model_dump()

        # Extract explicit Run fields
        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)
        parent_run_id = kwargs.pop("parent_run_id", None)

        # Compute core identity hashes early
        config_hash = self.identity.compute_config_hash(config)
        git_hash = self.identity.get_code_version()

        kwargs["_physical_run_dir"] = str(self.run_dir)

        now = datetime.now(UTC)
        run = Run(
            id=run_id,
            model_name=model,
            description=description,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            tags=tags or [],
            status="running",
            config_hash=config_hash,
            git_hash=git_hash,
            meta=kwargs,
            started_at=now,
            created_at=now,
        )

        push_tracker(self)
        self.current_consist = ConsistRecord(run=run, config=config)
        self._active_run_cache_mode = cache_mode

        # Process Inputs & Auto-Forking
        if inputs:
            for item in inputs:
                if isinstance(item, Artifact):
                    self.log_artifact(item, direction="input")
                else:
                    key = Path(item).stem
                    self.log_artifact(item, key=key, direction="input")

        # Auto-Lineage
        if not run.parent_run_id:
            parent_candidates = [
                a.run_id for a in self.current_consist.inputs if a.run_id is not None
            ]
            if parent_candidates:
                run.parent_run_id = parent_candidates[-1]

        try:
            input_hash = self.identity.compute_input_hash(
                self.current_consist.inputs,
                path_resolver=self.resolve_uri,
                signature_lookup=self._resolve_run_signature,
            )
            run.input_hash = input_hash
            run.signature = self.identity.calculate_run_signature(
                code_hash=git_hash, config_hash=config_hash, input_hash=input_hash
            )
        except Exception as e:
            logging.warning(
                f"[Consist Warning] Failed to compute inputs hash for run {run_id}: {e}"
            )
            run.input_hash = "error"
            run.signature = "error"

        # --- Cache Logic (Refactored) ---
        if cache_mode == "reuse":
            cached_run = self.find_matching_run(
                config_hash=run.config_hash,
                input_hash=run.input_hash,
                git_hash=run.git_hash,
            )
            if cached_run and self._validate_run_outputs(cached_run):
                self.current_consist.cached_run = cached_run
                logging.info(f"✅ [Consist] Cache HIT! Matching run: {cached_run.id}")

                # Hydrate outputs using service (No Session!)
                cached_items = self.get_artifacts_for_run(cached_run.id)
                for art, direction in cached_items:
                    if direction == "output":
                        art.abs_path = self.resolve_uri(art.uri)
                        self.current_consist.outputs.append(art)
            else:
                logging.info("🔄 [Consist] Cache Miss. Running...")
        elif cache_mode == "overwrite":
            logging.warning("⚠️ [Consist] Cache lookup skipped (Mode: Overwrite).")
        elif cache_mode == "readonly":
            logging.info("👁️ [Consist] Read-only mode.")

        self._flush_json()
        self._sync_run_to_db(run)
        self._emit_run_start(run)

        return run

    @contextmanager
    def start_run(
        self,
        run_id: str,
        model: str,
        **kwargs: Any,
    ) -> "Tracker":
        """
        Context manager to initiate and manage the lifecycle of a Consist run.

        This is the primary entry point for defining a reproducible and observable unit
        of work. It wraps the imperative `begin_run()`/`end_run()` methods to provide
        automatic cleanup and exception handling.

        Parameters
        ----------
        run_id : str
            A unique identifier for the current run.
        model : str
            A descriptive name for the model or process being executed.
        config : Union[Dict[str, Any], BaseModel, None], optional
            Configuration parameters for this run, hashed to form part of run identity.
        inputs : Optional[List[Union[str, Artifact]]], optional
            Input paths or Artifact objects that this run depends on.
        tags : Optional[List[str]], optional
            String labels for categorization and filtering.
        description : Optional[str], optional
            Human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching:
            - "reuse": Attempts to find a matching run in the cache. If found and valid,
                       the current run becomes a cache hit and its outputs are hydrated.
                       Otherwise, the run executes.
            - "overwrite": The run will always execute, and its results will overwrite
                           any existing cache entry for its signature.
            - "readonly": The run will perform a cache lookup. If a hit, outputs are hydrated.
                          If a miss, the run executes but its results are NOT saved to the cache.
        **kwargs : Any
            Additional metadata. Special keywords `year` and `iteration` can be used.

        Yields
        ------
        Tracker
            The current `Tracker` instance for use within the `with` block.

        Raises
        ------
        Exception
            Any exception raised within the `with` block will be caught, the run
            marked as "failed", and then re-raised after cleanup.

        See Also
        --------
        begin_run : Imperative alternative for starting runs.
        end_run : Imperative alternative for ending runs.
        """
        self.begin_run(run_id=run_id, model=model, **kwargs)
        try:
            yield self
            self.end_run(status="completed")
        except Exception as e:
            self.end_run(status="failed", error=e)
            raise

    def end_run(
        self,
        status: str = "completed",
        error: Optional[Exception] = None,
    ) -> Run:
        """
        End the current run started with begin_run().

        This method finalizes the run, persists the final state to JSON and database,
        and emits lifecycle hooks. It is idempotent - calling it multiple times
        on an already-ended run will log a warning but not raise an error.

        Parameters
        ----------
        status : str, default "completed"
            The final status of the run. Typically "completed" or "failed".
        error : Optional[Exception], optional
            The exception that caused the failure, if status is "failed".
            The error message will be stored in the run's metadata.

        Returns
        -------
        Run
            The completed Run object.

        Raises
        ------
        RuntimeError
            If there is no active run to end.

        Example
        -------
        ```python
        run = tracker.begin_run("run_001", "urbansim")
        try:
            # ... do work ...
            tracker.end_run("completed")
        except Exception as e:
            tracker.end_run("failed", error=e)
            raise
        ```
        """
        if not self.current_consist:
            raise RuntimeError("No active run to end. Call begin_run() first.")

        run = self.current_consist.run
        cache_mode = self._active_run_cache_mode or "reuse"

        # Update run status
        if cache_mode != "readonly":
            run.status = status
        else:
            run.status = "skipped_save" if status == "completed" else status

        if error:
            run.meta["error"] = str(error)

        # Clean up global tracker context
        pop_tracker()

        # Snapshot the result for introspection
        self._last_consist = self.current_consist

        # Set timing fields
        end_time = datetime.now(UTC)
        run.ended_at = end_time
        run.updated_at = end_time

        # Persist final state
        self._flush_json()
        if cache_mode != "readonly":
            self._sync_run_to_db(run)

        # Emit lifecycle hooks
        if error is not None:
            self._emit_run_failed(run, error)
        elif run.status == "completed":
            outputs = self.current_consist.outputs if self.current_consist else []
            self._emit_run_complete(run, outputs)

        # Clear current run context
        self.current_consist = None
        self._active_run_cache_mode = None

        return run

    # --- Query Helpers ---

    def find_runs(
        self,
        tags: Optional[List[str]] = None,
        year: Optional[int] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 100,
    ) -> List[Run]:
        """
        Retrieves a list of Runs matching the specified criteria.
        """
        if self.db:
            return self.db.find_runs(
                tags, year, model, status, parent_id, metadata, limit
            )
        return []

    # --- Artifact Logging & Ingestion ---

    def log_artifact(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        direction: str = "output",
        schema: Optional[Type[SQLModel]] = None,
        driver: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Logs an artifact (file or data reference) within the current run context.

        This method supports:
        -   **Automatic Input Discovery**: If an input `path` matches a previously
            logged output artifact, Consist automatically links them, building the
            provenance graph. This is a key part of **"Auto-Forking"**.
        -   **Path Virtualization**: Converts absolute file system paths to portable URIs
            (e.g., `inputs://data.csv`) using configured mounts, adhering to
            **"Path Resolution & Mounts"**.
        -   **Schema Metadata Injection**: Embeds schema information (if provided) into the
            artifact's metadata, useful for later "Strict Mode" validation or introspection.

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact (e.g., "households").
            Required if `path` is a string.
        direction : str, default "output"
            Specifies whether the artifact is an "input" or "output" for the
            current run. Defaults to "output".
        schema : Optional[Type[SQLModel]], optional
            An optional SQLModel class that defines the expected schema for the artifact's data.
            Its name will be stored in artifact metadata.
        driver : Optional[str], optional
            Explicitly specify the driver (e.g., 'h5_table').
            If None, the driver is inferred from the file extension.
        **meta : Any
            Additional key-value pairs to store in the artifact's flexible `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ValueError
            If `key` is not provided when `path` is a string.
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
            if driver:
                artifact_obj.driver = driver
            if meta:
                artifact_obj.meta.update(meta)

        # --- Logic Branch B: String Path Passed (New or Discovered Artifact) ---
        else:
            if key is None:
                raise ValueError("Argument 'key' required when 'path' is a string.")

            # Resolve to absolute path and then virtualize for storage
            resolved_abs_path = str(Path(path).resolve())
            uri = self._virtualize_path(resolved_abs_path)

            # 1. Lineage Discovery (Automatic Input Discovery)
            # For input artifacts, check if an artifact with this URI was previously output by another run.
            if direction == "input" and self.db:
                parent = self.db.find_latest_artifact_at_uri(uri)
                if parent:
                    # LINEAGE FOUND!
                    artifact_obj = parent
                    if driver:
                        artifact_obj.driver = driver
                    if meta:
                        artifact_obj.meta.update(meta)

            # 2. If no parent artifact found or it's an output, create a fresh Artifact object
            if artifact_obj is None:
                # Infer driver if not provided
                if driver is None:
                    driver = Path(path).suffix.lstrip(".").lower() or "unknown"

                # Compute content hash for the artifact
                content_hash = None
                try:
                    content_hash = self.identity._compute_file_checksum(
                        resolved_abs_path
                    )
                except Exception as e:
                    logging.warning(
                        f"[Consist Warning] Failed to compute hash for {path}: {e}"
                    )

                artifact_obj = Artifact(
                    key=key,
                    uri=uri,
                    driver=driver,
                    hash=content_hash,
                    run_id=(
                        self.current_consist.run.id if direction == "output" else None
                    ),
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

        self._flush_json()
        self._sync_artifact_to_db(artifact_obj, direction)

        return artifact_obj

    def log_artifacts(
        self,
        paths: List[Union[str, Path]],
        direction: str = "output",
        driver: Optional[str] = None,
        **shared_meta: Any,
    ) -> List[Artifact]:
        """
        Log multiple artifacts in a single call for efficiency.

        This is a convenience method for bulk artifact logging, particularly useful
        when a model produces many output files or when registering multiple inputs.
        Each path is logged as a separate artifact, with the filename stem used as the key.

        Parameters
        ----------
        paths : List[Union[str, Path]]
            A list of file paths to log as artifacts.
        direction : str, default "output"
            Specifies whether the artifacts are "input" or "output" for the current run.
        driver : Optional[str], optional
            Explicitly specify the driver for all artifacts. If None, driver is inferred
            from each file's extension individually.
        **shared_meta : Any
            Metadata key-value pairs to apply to ALL logged artifacts.
            Useful for tagging a batch of related files.

        Returns
        -------
        List[Artifact]
            A list of the created `Artifact` objects, in the same order as the input paths.

        Raises
        ------
        RuntimeError
            If called outside an active run context.

        Example
        -------
        ```python
        # Log all CSV files from a directory
        csv_files = list(Path("./outputs").glob("*.csv"))
        artifacts = tracker.log_artifacts(csv_files, direction="output", batch="run_001")

        # Log specific input files
        inputs = tracker.log_artifacts(
            ["data/train.parquet", "data/test.parquet"],
            direction="input",
            dataset_version="v2"
        )
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifacts outside of a run context.")

        artifacts = []
        for path in paths:
            path_obj = Path(path)
            key = path_obj.stem
            art = self.log_artifact(
                str(path_obj),
                key=key,
                direction=direction,
                driver=driver,
                **shared_meta,
            )
            artifacts.append(art)

        return artifacts

    def log_input(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an input artifact. Convenience wrapper for log_artifact(direction='input').

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(path, key=key, direction="input", **meta)

    def log_output(
        self,
        path: Union[str, Artifact],
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an output artifact. Convenience wrapper for log_artifact(direction='output').

        Parameters
        ----------
        path : Union[str, Artifact]
            The file path (str) or an existing `Artifact` object to be logged.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(path, key=key, direction="output", **meta)

    def log_h5_container(
        self,
        path: Union[str, Path],
        key: Optional[str] = None,
        direction: str = "output",
        discover_tables: bool = True,
        table_filter: Optional[Union[Callable[[str], bool], List[str]]] = None,
        **meta: Any,
    ) -> Tuple[Artifact, List[Artifact]]:
        """
        Log an HDF5 file and optionally discover its internal tables.

        This method provides first-class HDF5 container support, automatically
        discovering and logging internal tables as child artifacts. This is
        particularly useful for PILATES-like workflows that extensively use
        HDF5 files containing multiple tables.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the HDF5 file.
        key : Optional[str], optional
            Semantic name for the container. If not provided, uses the file stem.
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        discover_tables : bool, default True
            If True, scan the file and create child artifacts for each table/dataset.
        table_filter : Optional[Union[Callable[[str], bool], List[str]]], optional
            Filter which tables to log. Can be:
            - A callable that takes a table name and returns True to include
            - A list of table names to include (exact match)
            If None, all tables are included.
        **meta : Any
            Additional metadata for the container artifact.

        Returns
        -------
        Tuple[Artifact, List[Artifact]]
            A tuple of (container_artifact, list_of_table_artifacts).

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ImportError
            If h5py is not installed and discover_tables is True.

        Example
        -------
        ```python
        # Log HDF5 file with auto-discovery of all tables
        container, tables = tracker.log_h5_container("data.h5", key="urbansim_data")
        print(f"Logged {len(tables)} tables from container")

        # Filter tables by callable
        container, tables = tracker.log_h5_container(
            "data.h5",
            key="urbansim_data",
            table_filter=lambda name: name.startswith("/2025/")
        )

        # Filter tables by list of names
        container, tables = tracker.log_h5_container(
            "data.h5",
            key="urbansim_data",
            table_filter=["households", "persons", "buildings"]
        )
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem

        # Log the container artifact
        container = self.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="h5",
            is_container=True,
            **meta,
        )

        table_artifacts: List[Artifact] = []

        if discover_tables:
            try:
                import h5py
            except ImportError:
                logging.warning(
                    "[Consist] h5py not installed. Cannot discover HDF5 tables."
                )
                return container, table_artifacts

            # Build filter function
            if table_filter is None:
                filter_fn = lambda name: True
            elif isinstance(table_filter, list):
                # Convert list to set for O(1) lookup
                filter_set = set(table_filter)
                filter_fn = (
                    lambda name: name in filter_set or name.lstrip("/") in filter_set
                )
            else:
                filter_fn = table_filter

            try:
                with h5py.File(str(path_obj), "r") as f:

                    def visit_datasets(name: str, obj: Any) -> None:
                        if isinstance(obj, h5py.Dataset):
                            if filter_fn(name):
                                table_key = f"{key}_{name.replace('/', '_')}"
                                table_art = self.log_artifact(
                                    str(path_obj),
                                    key=table_key,
                                    direction=direction,
                                    driver="h5_table",
                                    parent_id=str(container.id),
                                    table_path=name,
                                    shape=list(obj.shape),
                                    dtype=str(obj.dtype),
                                )
                                table_artifacts.append(table_art)

                    f.visititems(visit_datasets)
            except Exception as e:
                logging.warning(f"[Consist] Failed to discover HDF5 tables: {e}")

        # Update container metadata with table info
        container.meta["table_count"] = len(table_artifacts)
        container.meta["table_ids"] = [str(t.id) for t in table_artifacts]

        # Re-sync the container with updated metadata
        self._flush_json()
        self._sync_artifact_to_db(container, direction)

        return container, table_artifacts

    def ingest(
        self,
        artifact: Artifact,
        data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
        schema: Optional[Type[SQLModel]] = None,
        run: Optional[Run] = None,
    ) -> Any:
        """
        Ingests data associated with an `Artifact` into the Consist DuckDB database.

        This method is central to Consist's **"Hot Data Strategy"**, where data is
        materialized into the database for faster query performance and easier sharing.
        It leverages the `dlt` (Data Load Tool) integration for efficient and robust
        data loading, including support for schema inference and evolution.

        Parameters
        ----------
        artifact : Artifact
            The artifact object representing the data being ingested. Its metadata
            might include schema information.
        data : Optional[Union[Iterable[Dict[str, Any]], Any]], optional
            An iterable (e.g., list of dicts, generator) where each item represents a
            row of data to be ingested. If `data` is omitted, Consist attempts to
            stream it directly from the artifact's file URI, resolving the path.
            Can also be other data types that `dlt` can handle directly (e.g., Pandas DataFrame).
        schema : Optional[Type[SQLModel]], optional
            An optional SQLModel class that defines the expected schema for the ingested data.
            If provided, `dlt` will use this for strict validation.
        run : Optional[Run], optional
            If provided, tags data with this run's ID (Offline Mode).
            If None, uses the currently active run (Online Mode).

        Returns
        -------
        Any
            The result information from the `dlt` ingestion process.

        Raises
        ------
        RuntimeError
            If no database is configured (`db_path` was not provided during
            Tracker initialization) or if `ingest` is called outside of
            an active run context.
        Exception
            Any exception raised by the underlying `dlt` ingestion process.
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
            info, resource_name = ingest_artifact(
                artifact=artifact,
                run_context=target_run,
                db_path=self.db_path,
                data_iterable=data_to_pass,
                schema_model=schema,
            )

            # FORCE Metadata update via Service
            if self.db:
                self.db.update_artifact_meta(
                    artifact, {"is_ingested": True, "dlt_table_name": resource_name}
                )
            return info

        except Exception as e:
            raise e

    # --- View Factory ---

    def create_view(self, view_name: str, concept_key: str) -> Any:
        factory = ViewFactory(self)
        return factory.create_hybrid_view(view_name, concept_key)

    # --- Retrieval Helpers ---

    def get_artifact(self, key_or_id: Union[str, uuid.UUID]) -> Optional[Artifact]:
        """
        Retrieves an Artifact by its semantic key or UUID.

        This method provides a flexible way to locate artifacts, first checking
        the in-memory context of the current run, and then querying the database
        for persistent records.

        Parameters
        ----------
        key_or_id : Union[str, uuid.UUID]
            The artifact's 'key' (e.g., "households") or its unique UUID.
            When a string is provided, the most recently created artifact matching
            that key is returned.

        Returns
        -------
        Optional[Artifact]
            The found `Artifact` object, or `None` if no matching artifact is found.
        """
        if self.db:
            return self.db.get_artifact(key_or_id)
        return None

    def get_artifact_by_uri(self, uri: str) -> Optional[Artifact]:
        """
        Find an artifact by its URI.

        Useful for checking if a specific file has been logged,
        or for retrieving artifact metadata by path.

        Parameters
        ----------
        uri : str
            The portable URI to search for (e.g., "inputs://households.csv").

        Returns
        -------
        Optional[Artifact]
            The found `Artifact` object, or `None` if no matching artifact is found.
        """
        # 1. Check In-Memory Context (Current Run)
        if self.current_consist:
            for art in self.current_consist.inputs + self.current_consist.outputs:
                if art.uri == uri:
                    return art

        # 2. Check Database
        if self.db:
            return self.db.get_artifact_by_uri(uri)

        return None

    def resolve_historical_path(self, artifact: Artifact, run: Run) -> Path:
        """
        Helper to find the physical location of an artifact from a past run.
        """
        if not run:
            return Path(self.resolve_uri(artifact.uri))

        old_dir = run.meta.get("_physical_run_dir")

        # Delegate the path math to the FS service
        path_str = self.fs.resolve_historical_path(artifact.uri, old_dir)
        return Path(path_str)

    def get_run(self, run_id: str) -> Optional[Run]:
        """
        Retrieves a single Run by its ID from the database.

        Args:
            run_id (str): The unique identifier of the run to retrieve.

        Returns:
            Optional[Run]: The found Run object, or None if not found.
        """
        if self.db:
            return self.db.get_run(run_id)
        return None

    def get_artifacts_for_run(self, run_id: str) -> List[Tuple[Artifact, str]]:
        if self.db:
            return self.db.get_artifacts_for_run(run_id)
        return []

    def find_matching_run(
        self, config_hash: str, input_hash: str, git_hash: str
    ) -> Optional[Run]:
        """
        Attempts to find a previously "completed" run that matches the given
        identity hashes.
        """
        if self.db:
            return self.db.find_matching_run(config_hash, input_hash, git_hash)
        return None

    def get_artifact_lineage(
        self, artifact_key_or_id: Union[str, uuid.UUID]
    ) -> Optional[Dict[str, Any]]:
        """
        Recursively builds a lineage tree for a given artifact.
        """
        if not self.engine:
            return None

        start_artifact = self.get_artifact(key_or_id=artifact_key_or_id)
        if not start_artifact:
            return None

        def _trace(artifact: Artifact, visited_runs: set) -> Dict[str, Any]:
            lineage_node: Dict[str, Any] = {"artifact": artifact, "producing_run": None}

            producing_run_id = artifact.run_id
            if not producing_run_id or producing_run_id in visited_runs:
                return lineage_node

            visited_runs.add(producing_run_id)
            producing_run = self.get_run(producing_run_id)
            if not producing_run:
                return lineage_node

            # Recursively find inputs
            input_artifacts_with_direction = self.get_artifacts_for_run(
                producing_run.id
            )

            run_node: Dict[str, Any] = {"run": producing_run, "inputs": []}
            for input_artifact, direction in input_artifacts_with_direction:
                if direction == "input":
                    run_node["inputs"].append(
                        _trace(input_artifact, visited_runs.copy())
                    )

            lineage_node["producing_run"] = run_node
            return lineage_node

        return _trace(start_artifact, set())

    # --- Path Resolution & Utils ---

    def resolve_uri(self, uri: str) -> str:
        """
        ** Delegates to FileSystemManager. **

        Converts a portable Consist URI back into an absolute file system path.

        This is the inverse operation of `_virtualize_path`, crucial for **"Path Resolution & Mounts"**.
        It uses the configured `mounts` and the `run_dir` to reconstruct the local
        absolute path to an artifact, making runs portable across different environments.

        Parameters
        ----------
        uri : str
            The portable URI (e.g., "inputs://file.csv", "./output/data.parquet")
            to resolve.

        Returns
        -------
        str
            The absolute file system path corresponding to the given URI.
            If the URI cannot be fully resolved (e.g., scheme not mounted),
            it returns the most resolved path or the original URI after
            attempting to make it absolute.
        """
        return self.fs.resolve_uri(uri)

    def _virtualize_path(self, path: str) -> str:
        """
        ** Delegates to FileSystemManager. **

        Converts an absolute file system path into a portable Consist URI.

        This method is a key part of **"Path Resolution & Mounts"**, attempting to
        replace parts of the absolute path with scheme-based URIs (e.g., "inputs://")
        if a matching mount is configured, or makes it relative to the `run_dir`
        if possible. This ensures artifact paths stored in the provenance are portable
        across different execution environments, making Consist runs reproducible.

        Parameters
        ----------
        path : str
            The absolute file system path to virtualize.

        Returns
        -------
        str
            A portable URI representation of the path (e.g., "inputs://file.csv",
            "./output/data.parquet"). If no virtualization is possible, the original
            absolute path is returned.
        """
        return self.fs.virtualize_path(path)

    # --- Hooks ---

    def on_run_start(self, callback: Callable[[Run], None]):
        """
        Register a callback to be invoked when a run starts.

        The callback receives the `Run` object after it has been initialized
        but before any user code executes. This is useful for external integrations
        like OpenLineage event emission, logging, or notifications.

        Parameters
        ----------
        callback : Callable[[Run], None]
            A function that takes a `Run` object as its only argument.

        Returns
        -------
        Callable[[Run], None]
            The same callback, allowing use as a decorator.

        Example
        -------
        ```python
        @tracker.on_run_start
        def log_start(run):
            print(f"Starting run: {run.id}")

        # Or without decorator:
        tracker.on_run_start(my_callback_function)
        ```
        """
        return self.events.on_run_start(callback)

    def on_run_complete(self, callback: Callable[[Run, List[Artifact]], None]):
        return self.events.on_run_complete(callback)

    def on_run_failed(self, callback: Callable[[Run, Exception], None]):
        return self.events.on_run_failed(callback)

    def _emit_run_start(self, run: Run):
        self.events.emit_start(run)

    def _emit_run_complete(self, run: Run, outputs: List[Artifact]):
        self.events.emit_complete(run, outputs)

    def _emit_run_failed(self, run: Run, error: Exception):
        self.events.emit_failed(run, error)

    # --- Internal Persistence ---

    def _flush_json(self) -> None:
        """
        Writes the current `ConsistRecord` (in-memory state of the run) to a `consist.json` file.

        This operation is performed using an atomic write pattern (write to a temporary file,
        then rename) to ensure data integrity and prevent corruption, even if the process
        is interrupted.

        This is a critical part of the **"Dual-Write Safety"** strategy: the JSON file
        is always flushed first, ensuring that a human-readable record of the run exists
        even if the subsequent database synchronization fails.
        """
        if not self.current_consist:
            return
        json_str = self.current_consist.model_dump_json(indent=2)
        target = self.fs.run_dir / "consist.json"
        tmp = target.with_suffix(".tmp")
        with open(tmp, "w") as f:
            f.write(json_str)
        tmp.rename(target)

    def _sync_run_to_db(self, run: Run) -> None:
        """
        Synchronizes the state of a `Run` object to the DuckDB database.

        This method either updates an existing run record or inserts a new one,
        ensuring that the database reflects the most current status and metadata
        of the run. It uses a **"Clone and Push"** strategy to avoid binding the
        live run object to the session, which helps prevent potential ORM issues.

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

        Parameters
        ----------
        run : Run
            The `Run` object whose state needs to be synchronized with the database.
        """
        if self.db:
            self.db.sync_run(run)

    def _sync_artifact_to_db(self, artifact: Artifact, direction: str) -> None:
        """
        Synchronizes an `Artifact` object and its `RunArtifactLink` to the DuckDB database.

        This method merges the artifact (either creating it or updating an existing one)
        into the database and creates a `RunArtifactLink` entry. This link explicitly
        associates the artifact with the current run and its role (input or output).

        As part of the **"Dual-Write Safety"** mechanism, this method
        tolerates database failures (logs a warning instead of crashing),
        prioritizing the completion of the user's run.

        Parameters
        ----------
        artifact : Artifact
            The `Artifact` object to synchronize.
        direction : str
            The direction of the artifact relative to the current run
            ("input" or "output").
        """
        if self.db and self.current_consist:
            self.db.sync_artifact(artifact, self.current_consist.run.id, direction)

    def _validate_run_outputs(self, run: Run) -> bool:
        if not self.db:
            return True

            # Use service instead of raw SQL
        artifacts_links = self.db.get_artifacts_for_run(run.id)

        for art, direction in artifacts_links:
            if direction == "output":
                resolved_path = self.resolve_uri(art.uri)
                if not Path(resolved_path).exists() and not art.meta.get(
                    "is_ingested", False
                ):
                    logging.warning(f"⚠️ Cache Validation Failed. Missing: {art.uri}")
                    return False
        return True

    def history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if self.db:
            return self.db.get_history(limit, tags)
        return pd.DataFrame()

    @contextmanager
    def capture_outputs(
        self, directory: Union[str, Path], pattern: str = "*", recursive: bool = False
    ) -> OutputCapture:
        """
        A context manager to automatically capture and log new or modified files in a directory.

        This context manager is used within a `@task` function or `start_run` block
        to monitor a specified directory. Any files created or modified within this
        directory during the execution of the `with` block will be automatically
        logged as output artifacts of the current run.

        Parameters
        ----------
        directory : Union[str, Path]
            The path to the directory to monitor for new or modified files.
        pattern : str, default "*"
            A glob pattern (e.g., "*.csv", "data_*.parquet") to filter which files
            are captured within the specified directory. Defaults to all files.
        recursive : bool, default False
            If True, the capture will recursively scan subdirectories within `directory`.

        Yields
        ------
        OutputCapture
            An `OutputCapture` object containing a list of `Artifact` objects that were
            captured and logged after the `with` block finishes.

        Raises
        ------
        RuntimeError
            If `capture_outputs` is used outside of an active `start_run` context.
        """
        if not self.current_consist:
            raise RuntimeError(
                "capture_outputs must be used within a start_run context."
            )

        # Use FS service to scan
        before_state = self.fs.scan_directory(directory, pattern, recursive)
        capture_result = OutputCapture()

        try:
            yield capture_result
        finally:
            after_state = self.fs.scan_directory(directory, pattern, recursive)

            for f_path, mtime in after_state.items():
                # Check if file is new or modified
                if f_path not in before_state or mtime > before_state[f_path]:
                    try:
                        key = f_path.stem
                        # Log it
                        art = self.log_artifact(
                            str(f_path),
                            key=key,
                            direction="output",
                            captured_automatically=True,
                        )
                        capture_result.artifacts.append(art)
                    except Exception as e:
                        logging.error(f"[Consist] Failed to auto-capture {f_path}: {e}")

    def log_meta(self, **kwargs: Any) -> None:
        """
        Updates the metadata for the current run.

        This method allows logging additional key-value pairs to the `meta` field
        of the currently active `Run` object. This is particularly useful for
        recording runtime metrics (e.g., accuracy, loss, F1-score), tags, or
        any other arbitrary information generated during the run's execution.
        The metadata is immediately flushed to both the JSON log and the database.

        Parameters
        ----------
        **kwargs : Any
            Arbitrary key-value pairs to merge into the `meta` dictionary of
            the current run. Existing keys will be updated, and new keys will be added.
        """
        if not self.current_consist:
            logging.warning("[Consist] Cannot log_meta: No active run.")
            return

        # 1. Update In-Memory
        # Ensure 'meta' is a dict (SQLModel sometimes initializes defaults oddly depending on version)
        if self.current_consist.run.meta is None:
            self.current_consist.run.meta = {}

        self.current_consist.run.meta.update(kwargs)

        # 2. Persist
        self._flush_json()
        # We also sync to DB immediately so external monitors can see progress/tags
        self._sync_run_to_db(self.current_consist.run)

    def task(self, **kwargs) -> Callable:
        return create_task_decorator(self, **kwargs)

    @property
    def last_run(self) -> Optional[ConsistRecord]:
        return self._last_consist

    @property
    def is_cached(self) -> bool:
        return self.current_consist and self.current_consist.cached_run is not None
