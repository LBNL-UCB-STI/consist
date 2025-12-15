import logging
import uuid
import weakref
from pathlib import Path
from typing import (
    Dict,
    Optional,
    List,
    Any,
    Type,
    Iterable,
    Union,
    Callable,
    Tuple,
    Literal,
)
from datetime import datetime, timezone

from sqlmodel import SQLModel

from consist.core.artifacts import ArtifactManager
from consist.core.artifact_schemas import ArtifactSchemaManager
from consist.core.workflow import ScenarioContext, OutputCapture

UTC = timezone.utc
from contextlib import contextmanager

import pandas as pd

from pydantic import BaseModel

from consist.types import ArtifactRef, FacetLike, HasFacetSchemaVersion, HashInputs

from consist.core.views import ViewFactory, ViewRegistry
from consist.core.fs import FileSystemManager
from consist.core.persistence import DatabaseManager
from consist.core.config_facets import ConfigFacetManager
from consist.core.indexing import FacetIndex, IndexBySpec, RunFieldIndex
from consist.core.decorators import create_task_decorator
from consist.core.events import EventManager
from consist.models.artifact import Artifact
from consist.models.run import Run, ConsistRecord, RunArtifacts
from consist.core.identity import IdentityManager
from consist.core.context import push_tracker, pop_tracker

AccessMode = Literal["standard", "analysis", "read_only"]


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
        schemas: Optional[List[Type[SQLModel]]] = None,
        access_mode: AccessMode = "standard",
    ):
        """
        Initialize a Consist Tracker.

        Sets up the directory for run logs, configures path virtualization mounts,
        and optionally initializes the DuckDB database connection.

        Parameters
        ----------
        run_dir : Path
            Root directory where run logs (e.g., `consist.json`) and run outputs are written.
        db_path : Optional[str], default None
            Path to the DuckDB database. If provided, Consist dual-writes provenance to DB
            in addition to JSON. If None, DB-backed features are disabled.
        mounts : Optional[Dict[str, str]], default None
            Mapping of URI schemes to absolute filesystem roots for path virtualization,
            e.g. `{"inputs": "/abs/inputs", "outputs": "/abs/outputs"}`.
        project_root : str, default "."
            Project root for identity hashing (e.g., Git discovery and relative file hashing).
        hashing_strategy : str, default "full"
            Hashing strategy for file inputs (`"full"` content hash, or `"fast"` metadata-based).
        schemas : Optional[List[Type[SQLModel]]], default None
            Optional SQLModel table definitions to register as hybrid views.
        access_mode : AccessMode, default "standard"
            Database access policy:
            - `"standard"`: Full read/write (runs, artifacts, ingestion).
            - `"analysis"`: Read-oriented; blocks creating new provenance nodes but allows
              ingest/backfill and view/query helpers.
            - `"read_only"`: No permanent DB writes.
        """
        # 1. Initialize FileSystem Service
        # (This handles the mkdir and path resolution internally now)
        self.fs = FileSystemManager(run_dir, mounts)
        self.events = EventManager()

        self.mounts = self.fs.mounts
        self.run_dir = self.fs.run_dir

        self.access_mode = access_mode

        self.db_path = db_path
        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )

        self.db = None
        if db_path:
            self.db = DatabaseManager(db_path)

        self.artifacts = ArtifactManager(self)
        self.config_facets = ConfigFacetManager(db=self.db, identity=self.identity)
        self.artifact_schemas = ArtifactSchemaManager(self)

        self.views = ViewRegistry(self)
        if schemas:
            if not self.db:
                logging.warning(
                    "[Consist] Schemas provided but no database configured. Views will not be created."
                )
            else:
                for schema in schemas:
                    self.view(schema)

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None

        # Introspection State (Last completed run)
        self._last_consist: Optional[ConsistRecord] = None

        # Active run tracking (for imperative begin_run/end_run pattern)
        self._active_run_cache_mode: Optional[str] = None

        # In-process cache index to avoid DB timing/lock flakiness for immediate re-runs.
        # Keyed by (config_hash, input_hash, git_hash) to match cache lookup semantics.
        self._local_cache_index: Dict[Tuple[str, str, str], Run] = {}
        self._local_cache_max_entries: int = 1024

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
        inputs: Optional[list[ArtifactRef]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        *,
        facet: Optional[FacetLike] = None,
        hash_inputs: HashInputs = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = True,
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
        inputs : Optional[list[ArtifactRef]], optional
            A list of input paths (str/Path) or Artifact references.
        tags : Optional[List[str]], optional
            A list of string labels for categorization and filtering.
        description : Optional[str], optional
            A human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching: "reuse", "overwrite", or "readonly".
        facet : Optional[FacetLike], optional
            Optional small, queryable configuration facet to persist alongside the run.
            This is distinct from `config` (which is hashed and stored in the JSON snapshot).
        hash_inputs : HashInputs, optional
            Extra inputs to include in the run identity hash without logging them as run
            inputs/outputs. Useful for config bundles or auxiliary files. Each entry is
            either a path (str/Path) or a named tuple `(name, path)`.
        facet_schema_version : Optional[Union[str, int]], optional
            Optional schema version tag for the persisted facet.
        facet_index : bool, default True
            Whether to flatten and index facet keys/values for DB querying.
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
        self._ensure_write_provenance()
        if self.current_consist is not None:
            raise RuntimeError(
                f"Cannot begin_run: A run is already active (id={self.current_consist.run.id}). "
                "Call end_run() first."
            )

        raw_config_model: Optional[BaseModel] = (
            config if isinstance(config, BaseModel) else None
        )

        if config is None:
            config_dict: Dict[str, Any] = {}
        elif isinstance(config, BaseModel):
            config_dict = config.model_dump()
        else:
            config_dict = config

        if hash_inputs:
            config_dict = dict(config_dict)
            digest_map = self.identity.compute_hash_inputs_digests(hash_inputs)
            # Fold into config identity so it contributes to config_hash/signature.
            if "__consist_hash_inputs__" in config_dict:
                logging.warning(
                    "[Consist] Overwriting user-provided '__consist_hash_inputs__' in config for run %s.",
                    run_id,
                )
            config_dict["__consist_hash_inputs__"] = digest_map
            kwargs["consist_hash_inputs"] = digest_map

        # Extract explicit Run fields
        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)
        parent_run_id = kwargs.pop("parent_run_id", None)

        # Compute core identity hashes early
        config_hash = self.identity.compute_config_hash(config_dict)
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

        if run.meta is None:
            run.meta = {}
        run.meta.setdefault("mounts", dict(self.mounts))

        push_tracker(self)
        self.current_consist = ConsistRecord(run=run, config=config_dict)
        self._active_run_cache_mode = cache_mode

        # Persist a queryable facet (optional)
        facet_dict = self.config_facets.resolve_facet_dict(
            model=model, raw_config_model=raw_config_model, facet=facet, run_id=run.id
        )
        if facet_dict is not None:
            self.current_consist.facet = facet_dict
            schema_version = facet_schema_version
            if (
                schema_version is None
                and raw_config_model is not None
                and isinstance(raw_config_model, HasFacetSchemaVersion)
            ):
                schema_version = raw_config_model.facet_schema_version
            self.config_facets.persist_facet(
                run=run,
                model=model,
                facet_dict=facet_dict,
                schema_name=self.config_facets.infer_schema_name(
                    raw_config_model, facet
                ),
                schema_version=schema_version,
                index_kv=facet_index,
            )

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
            cache_key = (run.config_hash, run.input_hash, run.git_hash)

            cached_run = self._local_cache_index.get(cache_key)
            if cached_run is None:
                cached_run = self.find_matching_run(
                    config_hash=run.config_hash,
                    input_hash=run.input_hash,
                    git_hash=run.git_hash,
                )
            if cached_run and self._validate_run_outputs(cached_run):
                self.current_consist.cached_run = cached_run
                # Hydrate outputs using service (No Session!)
                cached_items = self.get_artifacts_for_run(cached_run.id)

                scenario_hint = (
                    f", scenario='{cached_run.parent_run_id}'"
                    if getattr(cached_run, "parent_run_id", None)
                    else ""
                )
                logging.info(
                    "✅ [Consist] Cache HIT for step '%s': matched cached run '%s'%s "
                    "(signature=config+inputs+code, inputs=%d, outputs=%d).",
                    run_id,
                    cached_run.id,
                    scenario_hint,
                    len(cached_items.inputs),
                    len(cached_items.outputs),
                )

                # We only need to hydrate outputs into current_consist
                for art in cached_items.outputs.values():
                    art.abs_path = self.resolve_uri(art.uri)
                    self.current_consist.outputs.append(art)

                # Mirror cache metadata on the active run for downstream checks (e.g., tests)
                run.meta["cache_hit"] = True
                run.meta["cache_source"] = cached_run.id
                run.meta["declared_outputs"] = list(cached_items.outputs.keys())

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
        **kwargs : Any
            Additional arguments forwarded to `begin_run()`, including commonly used keys:

            - `config`: Union[Dict[str, Any], BaseModel, None]
            - `inputs`: Optional[list[ArtifactRef]]
            - `tags`: Optional[List[str]]
            - `description`: Optional[str]
            - `cache_mode`: str ("reuse", "overwrite", "readonly")
            - `facet`, `hash_inputs`, `facet_schema_version`, `facet_index`
            - `year`, `iteration`

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

    def scenario(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        model: str = "scenario",
        **kwargs: Any,
    ) -> ScenarioContext:
        """
        Create a ScenarioContext to manage a grouped workflow of steps.

        This method initializes a scenario context manager that acts as a "header"
        run. It allows defining multiple steps (runs) that are automatically
        linked to this header run via `parent_run_id`, without manual threading.

        The scenario run is started, then immediately suspended (allowing steps
        to run), and finally restored and completed when the context exits.

        Parameters
        ----------
        name : str
            The name of the scenario. This will become the Run ID.
        config : Optional[Dict[str, Any]], optional
            Scenario-level configuration. Stored on the header run but NOT
            automatically inherited by steps.
        tags : Optional[List[str]], optional
            Tags for the scenario. "scenario_header" is automatically appended.
        model : str, default "scenario"
            The model name for the header run.
        **kwargs : Any
            Additional metadata or arguments for the header run.

        Returns
        -------
        ScenarioContext
            A context manager object that provides `.step()` and `.add_input()` methods.

        Example
        -------
        ```python
        with tracker.scenario("baseline", config={"mode": "test"}) as sc:
            sc.add_input("data.csv", key="data")
            with sc.step("init"):
                ...
        ```
        """
        return ScenarioContext(self, name, config, tags, model, **kwargs)

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

        # Update in-process cache index (best-effort) so immediate re-runs can cache-hit
        # even if the DB is briefly locked or slow to reflect status updates.
        if cache_mode != "readonly" and run.status == "completed":
            cache_key = (
                run.config_hash or "",
                run.input_hash or "",
                run.git_hash or "",
            )
            self._local_cache_index[cache_key] = run
            if len(self._local_cache_index) > self._local_cache_max_entries:
                # FIFO eviction (dict preserves insertion order).
                self._local_cache_index.pop(next(iter(self._local_cache_index)))

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
        index_by: Optional[Union[str, IndexBySpec]] = None,
        name: Optional[str] = None,
    ) -> Union[List[Run], Dict[Any, Run]]:
        """
        Retrieve runs matching the specified criteria.

        Parameters
        ----------
        tags : Optional[List[str]], optional
            Filter runs that contain all provided tags.
        year : Optional[int], optional
            Filter by run year.
        model : Optional[str], optional
            Filter by run model name.
        status : Optional[str], optional
            Filter by run status (e.g., "completed", "failed").
        parent_id : Optional[str], optional
            Filter by scenario/header parent id.
        metadata : Optional[Dict[str, Any]], optional
            Filter by exact matches in `Run.meta` (client-side filter).
        limit : int, default 100
            Maximum number of runs to return.
        index_by : Optional[Union[str, IndexBySpec]], optional
            If provided, returns a dict keyed by a run attribute or facet value.
            Supported forms:
            - `"year"` / `"iteration"` / any Run attribute name
            - `"facet.<key>"` or `"facet:<key>"` to key by a persisted facet value
            - `IndexBySpec` helpers like `index_by_field(...)` / `index_by_facet(...)`

            Note: if multiple runs share the same key, the last one wins.
        name : Optional[str], optional
            Filter by `Run.model_name`/name alias used by DatabaseManager.

        Returns
        -------
        Union[List[Run], Dict[Any, Run]]
            List of runs, or a dict keyed by `index_by` when requested.

        Raises
        ------
        TypeError
            If `index_by` is an unsupported type.
        """
        runs = []
        if self.db:
            runs = self.db.find_runs(
                tags, year, model, status, parent_id, metadata, limit, name
            )

        if index_by:
            if isinstance(index_by, FacetIndex):
                facet_key = index_by.key
                if not self.db:
                    return {}
                values_by_run = self.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            if isinstance(index_by, RunFieldIndex):
                return {getattr(r, index_by.field): r for r in runs}

            if isinstance(index_by, str) and (
                index_by.startswith("facet.") or index_by.startswith("facet:")
            ):
                if not self.db:
                    return {}
                facet_key = (
                    index_by.split(".", 1)[1]
                    if index_by.startswith("facet.")
                    else index_by.split(":", 1)[1]
                )
                values_by_run = self.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                # Only include runs that have this facet key present.
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            # Create dictionary keyed by the requested attribute
            if not isinstance(index_by, str):
                raise TypeError(f"Unsupported index_by type: {type(index_by)}")
            return {getattr(r, index_by): r for r in runs}

        return runs

    def find_run(self, **kwargs) -> Run:
        """
        Find exactly one run matching the criteria.

        This is a convenience wrapper around `find_runs(...)` that enforces uniqueness.

        Parameters
        ----------
        **kwargs : Any
            Filters forwarded to `find_runs(...)`.
            Special cases:
            - `id` or `run_id`: if provided, performs a direct primary-key lookup.

        Returns
        -------
        Run
            The matching run.

        Raises
        ------
        ValueError
            If no runs match, or more than one run matches.
        """
        # 1. Primary Key Lookup Optimization
        # If the user asks for a specific ID, we skip the search query and use get_run.
        # We accept 'id' or 'run_id' for ergonomics.
        run_id = kwargs.pop("id", None) or kwargs.pop("run_id", None)

        if run_id:
            run = self.get_run(run_id)
            if not run:
                raise ValueError(f"No run found with ID: {run_id}")
            # If other filters were passed (e.g. year=2020), strictly we should check them,
            # but ID lookup usually implies absolute intent.
            return run

        # 2. Standard Search
        # Enforce limit=2 to optimize checking for multiple results
        kwargs["limit"] = 2

        # Note: We popped 'id'/'run_id' above, so kwargs now only contains
        # arguments valid for find_runs (assuming user didn't pass bad args).
        results = self.find_runs(**kwargs)

        if not results:
            raise ValueError(f"No run found matching criteria: {kwargs}")

        if len(results) > 1:
            raise ValueError(
                f"Multiple runs ({len(results)}+) found matching criteria: {kwargs}. Narrow your search."
            )

        return results[0]

    # --- Artifact Logging & Ingestion ---

    def log_artifact(
        self,
        path: ArtifactRef,
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
        path : ArtifactRef
            A file path (str/Path) or an existing `Artifact` reference to be logged.
            Passing an `Artifact` is useful for explicitly linking an already-logged artifact
            as an input or output in the current run.
        key : Optional[str], optional
            A semantic, human-readable name for the artifact (e.g., "households").
            Required if `path` is a path-like (str/Path).
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
            If `key` is not provided when `path` is a path-like (str/Path).
        """
        self._ensure_write_provenance()

        if not self.current_consist:
            raise RuntimeError("Cannot log artifact: no active run.")

        run_id = self.current_consist.run.id if direction == "output" else None

        # DELEGATE CREATION LOGIC
        artifact_obj = self.artifacts.create_artifact(
            path, run_id, key, direction, schema, driver, **meta
        )

        # Artifact contract clarification:
        # - If the caller passes an existing Artifact reference, `artifact.run_id` is treated as the
        #   producing run id (and is not overwritten), but we warn when the caller attempts to log it
        #   as an output of a different run.
        # - If an Artifact has no producing run_id yet and is logged as an output, we attribute it to
        #   the current run.
        if isinstance(path, Artifact) and direction == "output":
            producing_run_id = artifact_obj.run_id
            if producing_run_id is None:
                artifact_obj.run_id = run_id
            elif producing_run_id != run_id:
                logging.warning(
                    "[Consist] log_artifact received an Artifact with run_id=%s but is logging it as output of run_id=%s (artifact key=%s id=%s).",
                    producing_run_id,
                    run_id,
                    getattr(artifact_obj, "key", None),
                    getattr(artifact_obj, "id", None),
                )

        # Inherit selected run metadata onto the artifact unless already present
        run_ctx = self.current_consist.run
        inherited_fields = {
            "year": run_ctx.year,
            "iteration": run_ctx.iteration,
            "tags": run_ctx.tags or [],
        }
        if artifact_obj.meta is None:
            artifact_obj.meta = {}
        for k, v in inherited_fields.items():
            if v is not None and k not in artifact_obj.meta:
                artifact_obj.meta[k] = v

        # TRACKER HANDLES STATE & PERSISTENCE
        artifact_obj._tracker = weakref.ref(self)
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
        path: ArtifactRef,
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an input artifact. Convenience wrapper for log_artifact(direction='input').

        Parameters
        ----------
        path : ArtifactRef
            A file path (str/Path) or an existing `Artifact` reference to be logged.
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
        path: ArtifactRef,
        key: Optional[str] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log an output artifact. Convenience wrapper for log_artifact(direction='output').

        Parameters
        ----------
        path : ArtifactRef
            A file path (str/Path) or an existing `Artifact` reference to be logged.
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

    def load(self, artifact: Artifact, **kwargs: Any) -> Any:
        """
        Convenience method to load an artifact using the public API while
        automatically passing this tracker for context.
        """
        from consist.api import load as api_load

        return api_load(artifact, tracker=self, **kwargs)

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

            path_obj = Path(path)  # Ensure this matches what you passed to log_artifact
            table_artifacts = self.artifacts.scan_h5_container(
                container, path_obj, key, direction, filter_fn
            )

            # Persist the children found by the manager
            for t in table_artifacts:
                # Note: create_artifact returns them un-persisted, so we must add them here
                # Or better yet, have scan_h5_container return objects that just need syncing
                self.current_consist.outputs.append(t)
                self._sync_artifact_to_db(t, direction)

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
        profile_schema: bool = True,
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
        profile_schema : bool, default True
            If True, profile and persist a deduped schema record for the ingested table,
            writing `schema_id`/`schema_summary` (and optionally `schema_profile`) into
            `Artifact.meta`.

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
        self._ensure_write_data()

        # 1. Determine Context
        target_run = run

        if not target_run and not self.current_consist:
            # AUTO-DISCOVERY: Use the run that created the artifact
            if artifact.run_id:
                target_run = self.get_run(artifact.run_id)
                logging.info(
                    f"[Consist] Ingesting in Analysis Mode. Attributing to Run: {target_run.id}"
                )

        # Fallback to active run if available
        if not target_run and self.current_consist:
            target_run = self.current_consist.run

        if not target_run:
            raise RuntimeError(
                "Cannot ingest: Could not determine associated Run context."
            )

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

                if profile_schema:
                    self.artifact_schemas.profile_ingested_table(
                        artifact=artifact,
                        run=target_run,
                        table_schema="global_tables",
                        table_name=resource_name,
                    )
            return info

        except Exception as e:
            raise e

    # --- View Factory ---

    def view(self, model: Type[SQLModel], key: Optional[str] = None) -> Type[SQLModel]:
        """
        Create/register a hybrid view for a given SQLModel schema.

        Parameters
        ----------
        model : Type[SQLModel]
            SQLModel schema defining the logical columns for the concept.
        key : Optional[str], optional
            Override the concept key (defaults to `model.__tablename__`).

        Returns
        -------
        Type[SQLModel]
            The dynamic SQLModel view class exposed via `tracker.views`.

        Raises
        ------
        RuntimeError
            If the tracker has no database configured.
        """
        if not self.db:
            raise RuntimeError("Database required to create views.")

        # 1. Register metadata (so __getattr__ works later)
        self.views.register(model, key)

        # 2. Trigger immediate creation/refresh
        # This returns the dynamic class
        return getattr(self.views, model.__name__)

    def create_view(self, view_name: str, concept_key: str) -> Any:
        """
        Create a named hybrid view over a registered concept.

        This is a lower-level helper than `Tracker.view(...)`. It is useful when you
        want to create multiple named views over the same concept key, or when you
        want explicit control over the view name.

        Parameters
        ----------
        view_name : str
            The SQL view name to create in the database (e.g., `"v_persons"`).
        concept_key : str
            The registered concept key to materialize (typically a table/artifact key).

        Returns
        -------
        Any
            Backend-specific result from `ViewFactory.create_hybrid_view`.
        """
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

    # --- Config Facet Query Helpers ---

    def get_config_facet(self, facet_id: str):
        """
        Retrieve a single persisted config facet by ID.

        Parameters
        ----------
        facet_id : str
            The facet identifier.

        Returns
        -------
        Any
            The facet record if present, otherwise `None`.
        """
        if not self.db:
            return None
        return self.db.get_config_facet(facet_id)

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ):
        """
        List persisted config facets, optionally filtered.

        Parameters
        ----------
        namespace : Optional[str], optional
            Filter facets by namespace.
        schema_name : Optional[str], optional
            Filter facets by schema name.
        limit : int, default 100
            Maximum number of facet records to return.

        Returns
        -------
        list
            A list of facet records (empty if DB is not configured).
        """
        if not self.db:
            return []
        return self.db.get_config_facets(
            namespace=namespace, schema_name=schema_name, limit=limit
        )

    def get_run_config_kv(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ):
        """
        Retrieve flattened key/value config entries for a run.

        This is primarily used for querying and debugging indexed config facets.

        Parameters
        ----------
        run_id : str
            Run identifier.
        namespace : Optional[str], optional
            Filter by namespace.
        prefix : Optional[str], optional
            Filter keys by prefix (e.g. `"inputs."`).
        limit : int, default 10_000
            Maximum number of entries to return.

        Returns
        -------
        list
            A list of key/value rows (empty if DB is not configured).
        """
        if not self.db:
            return []
        return self.db.get_run_config_kv(
            run_id, namespace=namespace, prefix=prefix, limit=limit
        )

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
    ):
        """
        Find runs by a flattened config facet key/value.

        Parameters
        ----------
        namespace : str
            Facet namespace.
        key : str
            Flattened facet key.
        value_type : Optional[str], optional
            Optional discriminator for the value column (implementation dependent).
        value_str : Optional[str], optional
            String value to match.
        value_num : Optional[float], optional
            Numeric value to match.
        value_bool : Optional[bool], optional
            Boolean value to match.
        limit : int, default 100
            Maximum number of runs to return.

        Returns
        -------
        list
            Matching run records (empty if DB is not configured).
        """
        if not self.db:
            return []
        return self.db.find_runs_by_facet_kv(
            namespace=namespace,
            key=key,
            value_type=value_type,
            value_str=value_str,
            value_num=value_num,
            value_bool=value_bool,
            limit=limit,
        )

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

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts:
        """
        Retrieves inputs and outputs for a specific run, organized by key.
        """
        if not self.db:
            return RunArtifacts()

        # Get raw list [(Artifact, "input"), (Artifact, "output")]
        raw_list = self.db.get_artifacts_for_run(run_id)

        inputs = {}
        outputs = {}

        for artifact, direction in raw_list:
            if direction == "input":
                inputs[artifact.key] = artifact
            elif direction == "output":
                outputs[artifact.key] = artifact

        return RunArtifacts(inputs=inputs, outputs=outputs)

    def get_run_artifact(
        self,
        run_id: str,
        key: Optional[str] = None,
        key_contains: Optional[str] = None,
        direction: str = "output",
    ) -> Optional[Artifact]:
        """
        Convenience helper to fetch a single artifact for a specific run.

        Args:
            run_id: Run identifier.
            key: Exact key to match (if present in logged artifacts).
            key_contains: Optional substring to match when the exact key is unknown.
            direction: \"output\" (default) or \"input\".
        """
        record = self.get_artifacts_for_run(run_id)
        collection = record.outputs if direction == "output" else record.inputs
        if key and key in collection:
            return collection[key]
        if key_contains:
            for k, art in collection.items():
                if key_contains in k:
                    return art
        return next(iter(collection.values()), None)

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
            # NEW: Returns RunArtifacts object
            run_artifacts = self.get_artifacts_for_run(producing_run.id)

            run_node: Dict[str, Any] = {"run": producing_run, "inputs": []}

            # NEW: Iterate over inputs dict
            for input_artifact in run_artifacts.inputs.values():
                run_node["inputs"].append(_trace(input_artifact, visited_runs.copy()))

            lineage_node["producing_run"] = run_node
            return lineage_node

        return _trace(start_artifact, set())

        # --- Permission Helpers ---

    def _ensure_write_provenance(self):
        """Guard for start_run, log_artifact"""
        if self.access_mode != "standard":
            raise RuntimeError(
                f"Operation forbidden in '{self.access_mode}' mode. "
                "Switch to access_mode='standard' to create new runs or artifacts."
            )

    def _ensure_write_data(self):
        """Guard for ingest"""
        if self.access_mode == "read_only":
            raise RuntimeError(
                "Ingestion forbidden in 'read_only' mode. "
                "Switch to access_mode='analysis' or 'standard'."
            )

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
        """
        Register a callback to be invoked when a run completes successfully.

        Parameters
        ----------
        callback : Callable[[Run, List[Artifact]], None]
            Called with the completed `Run` and its output artifacts.

        Returns
        -------
        Callable[[Run, List[Artifact]], None]
            The same callback, allowing use as a decorator.
        """
        return self.events.on_run_complete(callback)

    def on_run_failed(self, callback: Callable[[Run, Exception], None]):
        """
        Register a callback to be invoked when a run fails.

        Parameters
        ----------
        callback : Callable[[Run, Exception], None]
            Called with the failed `Run` and the raised exception.

        Returns
        -------
        Callable[[Run, Exception], None]
            The same callback, allowing use as a decorator.
        """
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

        # NOTE:
        # `Tracker.run_dir` is often a *scenario* directory that contains many runs (steps).
        # Writing a single `run_dir/consist.json` would overwrite previous steps and can look
        # "broken" to users expecting multiple steps. To preserve backward compatibility,
        # we still write `run_dir/consist.json` as the "latest snapshot", but we also write
        # a stable per-run snapshot in `run_dir/consist_runs/<run_id>.json`.
        run_id = self.current_consist.run.id
        safe_run_id = "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )

        per_run_dir = self.fs.run_dir / "consist_runs"
        per_run_dir.mkdir(parents=True, exist_ok=True)
        per_run_target = per_run_dir / f"{safe_run_id}.json"
        per_run_tmp = per_run_target.with_suffix(".tmp")
        with open(per_run_tmp, "w") as f:
            f.write(json_str)
        per_run_tmp.rename(per_run_target)

        latest_target = self.fs.run_dir / "consist.json"
        latest_tmp = latest_target.with_suffix(".tmp")
        with open(latest_tmp, "w") as f:
            f.write(json_str)
        latest_tmp.rename(latest_target)

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
                    from consist.tools.mount_diagnostics import (
                        build_mount_resolution_hint,
                        format_missing_artifact_mount_help,
                    )

                    hint = build_mount_resolution_hint(
                        art.uri, artifact_meta=art.meta, mounts=self.mounts
                    )
                    help_text = (
                        "\n"
                        + format_missing_artifact_mount_help(
                            hint, resolved_path=resolved_path
                        )
                        if hint
                        else f"\nResolved path: {resolved_path}"
                    )
                    logging.warning(
                        "⚠️ Cache Validation Failed. Missing: %s%s", art.uri, help_text
                    )
                    return False
        return True

    def history(
        self, limit: int = 10, tags: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Return recent runs as a Pandas DataFrame.

        Parameters
        ----------
        limit : int, default 10
            Maximum number of runs to include.
        tags : Optional[List[str]], optional
            If provided, filter runs to those containing any of the given tags.

        Returns
        -------
        pd.DataFrame
            A DataFrame of recent runs (empty if DB is not configured).
        """
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
        """
        Create a task decorator bound to this tracker.

        Tasks wrap functions with Consist's caching + provenance behavior.
        The decorator accepts options like `cache_mode`, `depends_on`,
        and output-capture settings; see the user guide for details.

        Parameters
        ----------
        **kwargs : Any
            Options forwarded to `create_task_decorator(...)`.

        Returns
        -------
        Callable
            A decorator that can be applied to a function to create a cached task.
        """
        return create_task_decorator(self, **kwargs)

    @property
    def last_run(self) -> Optional[ConsistRecord]:
        """
        Return the most recent run record observed by this tracker.

        Returns
        -------
        Optional[ConsistRecord]
            The last completed/failed run record, or `None` if no run has executed yet.
        """
        return self._last_consist

    @property
    def is_cached(self) -> bool:
        """
        Whether the currently active run is a cache hit.

        Returns
        -------
        bool
            True if the current `start_run`/task execution is reusing a cached run.
        """
        return self.current_consist and self.current_consist.cached_run is not None

    def cached_artifacts(self, direction: str = "output"):
        """
        Returns hydrated artifacts for the active run when it is a cache hit.

        Parameters
        ----------
        direction : str, default "output"
            "output" or "input" to filter hydrated artifacts.

        Returns
        -------
        Dict[str, Artifact]
            Mapping of artifact key to Artifact for the specified direction.
            Returns an empty dict if no cache hit or no artifacts.
        """
        if not self.current_consist or not self.current_consist.cached_run:
            return {}
        if direction == "output":
            return {a.key: a for a in self.current_consist.outputs}
        if direction == "input":
            return {a.key: a for a in self.current_consist.inputs}
        return {}

    def cached_output(self, key: Optional[str] = None):
        """
        Convenience to fetch a hydrated cached output artifact for the current run.

        Parameters
        ----------
        key : Optional[str]
            If provided, returns the artifact with this key; otherwise returns the
            first available cached output.

        Returns
        -------
        Optional[Artifact]
            The cached output artifact, or None if not cached / not found.
        """
        outputs = self.cached_artifacts(direction="output")
        if not outputs:
            return None
        artifact = outputs.get(key) if key else next(iter(outputs.values()))
        if artifact:
            if not artifact.abs_path:
                try:
                    artifact.abs_path = self.resolve_uri(artifact.uri)
                except Exception:
                    pass
            artifact._tracker = weakref.ref(self)
        return artifact
