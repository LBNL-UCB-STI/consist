import inspect
import itertools
from collections.abc import Mapping as MappingABC
from contextlib import contextmanager
from datetime import datetime, timezone
import logging
import os
import time
import uuid
import weakref
import warnings
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
)

import pandas as pd
from pydantic import BaseModel
from sqlmodel import SQLModel

from consist.core.artifact_schemas import ArtifactSchemaManager
from consist.core.artifacts import ArtifactManager
from consist.core.cache import (
    ActiveRunCacheOptions,
    hydrate_cache_hit_outputs,
    materialize_missing_inputs,
    parse_materialize_cached_outputs_kwargs,
    validate_cached_run_outputs,
)
from consist.core.cache_output_logging import (
    maybe_return_cached_output_or_demote_cache_hit,
)
from consist.core.config_facets import ConfigFacetManager
from consist.core.context import pop_tracker, push_tracker
from consist.core.decorators import define_step as define_step_decorator
from consist.core.events import EventManager
from consist.core.fs import FileSystemManager
from consist.core.identity import IdentityManager
from consist.core.indexing import IndexBySpec
from consist.core.persistence import DatabaseManager, ProvenanceWriter
from consist.core.views import ViewFactory, ViewRegistry
from consist.core.ingestion import ingest_artifact
from consist.core.lineage import LineageService
from consist.core.materialize import materialize_artifacts
from consist.core.matrix import MatrixViewFactory
from consist.core.queries import RunQueryService
from consist.core.workflow import OutputCapture, RunContext, ScenarioContext
from consist.core.input_utils import coerce_input_map
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord, Run, RunArtifacts, RunResult
from consist.types import ArtifactRef, FacetLike, HasFacetSchemaVersion, HashInputs

UTC = timezone.utc

AccessMode = Literal["standard", "analysis", "read_only"]


def _resolve_input_ref(
    tracker: "Tracker", ref: ArtifactRef, key: Optional[str]
) -> ArtifactRef:
    if isinstance(ref, Artifact):
        return ref
    if not isinstance(ref, (str, Path)):
        raise TypeError(f"inputs must be Artifact, Path, or str (got {type(ref)}).")
    ref_str = str(ref)
    resolved = (
        Path(tracker.resolve_uri(ref_str))
        if isinstance(ref, str) and "://" in ref_str
        else Path(ref_str)
    )
    if not resolved.exists():
        raise ValueError(f"Input path does not exist: {resolved!s}")
    if key is None:
        return resolved
    return tracker.artifacts.create_artifact(
        resolved, run_id=None, key=key, direction="input"
    )


def _is_xarray_dataset(value: Any) -> bool:
    try:
        import xarray as xr  # type: ignore[import-not-found]
    except ImportError:
        return False
    return isinstance(value, xr.Dataset)


def _write_xarray_dataset(dataset: Any, path: Path) -> None:
    try:
        import xarray as xr  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ImportError("xarray is required to log xarray.Dataset outputs.") from exc
    if not isinstance(dataset, xr.Dataset):
        raise TypeError(f"Expected xarray.Dataset, got {type(dataset)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_zarr(path, mode="w")


def _resolve_input_refs(
    tracker: "Tracker",
    inputs: Optional[Union[Mapping[str, ArtifactRef], Iterable[ArtifactRef]]],
    depends_on: Optional[List[ArtifactRef]],
    *,
    include_keyed_artifacts: bool,
) -> tuple[List[ArtifactRef], Dict[str, Artifact]]:
    resolved_inputs: List[ArtifactRef] = []
    input_artifacts_by_key: Dict[str, Artifact] = {}
    if inputs is not None:
        if isinstance(inputs, MappingABC):
            inputs_map = coerce_input_map(inputs)
            for key, ref in inputs_map.items():
                resolved = _resolve_input_ref(tracker, ref, str(key))
                resolved_inputs.append(resolved)
                if include_keyed_artifacts and isinstance(resolved, Artifact):
                    input_artifacts_by_key[str(key)] = resolved
        else:
            for ref in list(inputs):
                resolved_inputs.append(_resolve_input_ref(tracker, ref, None))

    if depends_on:
        for dep in depends_on:
            resolved_inputs.append(_resolve_input_ref(tracker, dep, None))

    return resolved_inputs, input_artifacts_by_key


def _preview_run_artifact_dir(
    tracker: "Tracker",
    *,
    run_id: str,
    model: str,
    description: Optional[str],
    year: Optional[int],
    iteration: Optional[int],
    parent_run_id: Optional[str],
    tags: Optional[List[str]],
) -> Path:
    preview = Run(
        id=run_id,
        model_name=model,
        description=description,
        year=year,
        iteration=iteration,
        parent_run_id=parent_run_id,
        tags=tags or [],
        status="running",
        config_hash=None,
        git_hash=None,
        meta={},
        started_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    return tracker.run_artifact_dir(preview)


def _resolve_output_path(tracker: "Tracker", ref: ArtifactRef, base_dir: Path) -> Path:
    if isinstance(ref, Artifact):
        raw = ref.path or tracker.resolve_uri(ref.uri)
        return Path(raw)
    ref_str = str(ref)
    if isinstance(ref, str) and "://" in ref_str:
        return Path(tracker.resolve_uri(ref_str))
    ref_path = Path(ref_str)
    if not ref_path.is_absolute():
        return base_dir / ref_path
    return ref_path


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
        db_path: Optional[str | os.PathLike[str]] = None,
        mounts: Optional[Dict[str, str]] = None,
        project_root: str = ".",
        hashing_strategy: str = "full",
        schemas: Optional[List[Type[SQLModel]]] = None,
        access_mode: AccessMode = "standard",
        run_subdir_fn: Optional[Callable[[Run], str]] = None,
    ):
        """
        Initialize a Consist Tracker.

        Sets up the directory for run logs, configures path virtualization mounts,
        and optionally initializes the DuckDB database connection.

        Parameters
        ----------
        run_dir : Path
            Root directory where run logs (e.g., `consist.json`) and run outputs are written.
        db_path : Optional[str | os.PathLike[str]], default None
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
        run_subdir_fn : Optional[Callable[[Run], str]], default None
            Optional callable that returns a relative subdirectory name for a run.
            When provided, this takes precedence over the default pattern.
        """
        # 1. Initialize FileSystem Service
        # (This handles the mkdir and path resolution internally now)
        self.fs = FileSystemManager(run_dir, mounts)
        self.events = EventManager()

        self.mounts = self.fs.mounts
        self.run_dir = self.fs.run_dir

        self.access_mode = access_mode
        self._run_subdir_fn = run_subdir_fn

        self.db_path = os.fspath(db_path) if db_path is not None else None
        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )

        self.db = None
        if self.db_path:
            self.db = DatabaseManager(self.db_path)
        self.persistence = ProvenanceWriter(self)

        self.artifacts = ArtifactManager(self)
        self.config_facets = ConfigFacetManager(db=self.db, identity=self.identity)
        self.artifact_schemas = ArtifactSchemaManager(self)
        self.queries = RunQueryService(self)
        self.lineage = LineageService(self)

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
        self._active_run_cache_options: ActiveRunCacheOptions = ActiveRunCacheOptions()

        # In-process cache index to avoid DB timing/lock flakiness for immediate re-runs.
        # Keyed by (config_hash, input_hash, git_hash) to match cache lookup semantics.
        self._local_cache_index: Dict[Tuple[str, str, str], Run] = {}
        self._local_cache_max_entries: int = 1024
        self._run_signature_cache: Dict[str, str] = {}
        self._run_signature_cache_max_entries: int = 4096
        self._run_artifacts_cache: Dict[str, RunArtifacts] = {}
        self._run_artifacts_cache_max_entries: int = 1024

    @property
    def engine(self):
        """Delegates to the DatabaseManager engine."""
        return self.db.engine if self.db else None

    @staticmethod
    def _default_run_subdir(run: Run) -> str:
        """
        Default run subdirectory pattern.

        Uses `<parent_run_id>/<model>/iteration_<iteration>` when available, otherwise
        `<model>/<run_id>`.
        """
        if run.parent_run_id and run.iteration is not None:
            return f"{run.parent_run_id}/{run.model_name}/iteration_{run.iteration}"
        return f"{run.model_name}/{run.id}"

    def set_run_subdir_fn(self, fn: Optional[Callable[[Run], str]]) -> None:
        """
        Set a callable that returns the per-run artifact subdirectory name.

        Parameters
        ----------
        fn : Optional[Callable[[Run], str]]
            Callable that accepts a ``Run`` and returns a relative directory name.
            Set to ``None`` to disable the custom resolver.
        """
        self._run_subdir_fn = fn

    def run_artifact_dir(self, run: Optional[Run] = None) -> Path:
        """
        Resolve the run-specific artifact directory for the active run.

        Parameters
        ----------
        run : Optional[Run], optional
            Run to resolve the directory for. Defaults to the current run if active.

        Returns
        -------
        Path
            Directory under ``run_dir`` where run artifacts should be written.
        """
        target_run = run
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            return self.run_dir / "outputs"

        base_dir = self.run_dir / "outputs"
        artifact_dir = (
            target_run.meta.get("artifact_dir")
            if isinstance(target_run.meta, dict)
            else None
        )
        if isinstance(artifact_dir, Path):
            artifact_dir = str(artifact_dir)
        if isinstance(artifact_dir, str) and artifact_dir:
            artifact_path = Path(artifact_dir)
            if artifact_path.is_absolute():
                return artifact_path
            return base_dir / artifact_path

        subdir = (
            self._run_subdir_fn(target_run)
            if self._run_subdir_fn is not None
            else self._default_run_subdir(target_run)
        )

        if subdir:
            subdir = subdir.strip().lstrip("/\\")
        if not subdir:
            subdir = target_run.id

        subdir_path = Path(subdir)
        if subdir_path.is_absolute():
            raise ValueError(
                "run_subdir must be a relative path; got an absolute path."
            )

        return base_dir / subdir_path

    def export_schema_sqlmodel(
        self,
        *,
        schema_id: Optional[str] = None,
        artifact_id: Optional[Union[str, uuid.UUID]] = None,
        out_path: Optional[Path] = None,
        table_name: Optional[str] = None,
        class_name: Optional[str] = None,
        abstract: bool = True,
        include_system_cols: bool = False,
        include_stats_comments: bool = True,
    ) -> str:
        """
        Export a captured artifact schema as a SQLModel stub string for manual editing.

        Exactly one of `schema_id` or `artifact_id` must be provided.
        """
        if not self.db:
            raise ValueError("Schema export requires a configured database (db_path).")
        if (schema_id is None) == (artifact_id is None):
            raise ValueError("Provide exactly one of schema_id or artifact_id.")

        backfill_ordinals = self.access_mode != "read_only"

        if artifact_id is not None:
            artifact_uuid = (
                uuid.UUID(artifact_id) if isinstance(artifact_id, str) else artifact_id
            )
            fetched = self.db.get_artifact_schema_for_artifact(
                artifact_id=artifact_uuid, backfill_ordinals=backfill_ordinals
            )
        else:
            assert schema_id is not None
            fetched = self.db.get_artifact_schema(
                schema_id=schema_id, backfill_ordinals=backfill_ordinals
            )

        if fetched is None:
            raise KeyError("Schema not found for the provided selector.")
        schema, fields = fetched

        from consist.core.schema_export import render_sqlmodel_stub

        code = render_sqlmodel_stub(
            schema=schema,
            fields=fields,
            table_name=table_name,
            class_name=class_name,
            abstract=abstract,
            include_system_cols=include_system_cols,
            include_stats_comments=include_stats_comments,
        )

        if out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(code, encoding="utf-8")

        return code

    def _resolve_run_signature(self, run_id: str) -> Optional[str]:
        """
        Internal helper to look up a run's signature (Merkle identity) via the database.
        Used by IdentityManager to stabilize input hashes against ephemeral Run IDs.
        """
        cached = self._run_signature_cache.get(run_id)
        if cached is not None:
            return cached

        # 1. Check active run (unlikely for inputs, but good for completeness)
        if self.current_consist and self.current_consist.run.id == run_id:
            signature = self.current_consist.run.signature
            if signature:
                self._run_signature_cache[run_id] = signature
            return signature
        # 2. Check Database
        if self.db:
            run = self.db.get_run(run_id)
            if run:
                signature = run.signature
                if signature:
                    self._run_signature_cache[run_id] = signature
                    if (
                        len(self._run_signature_cache)
                        > self._run_signature_cache_max_entries
                    ):
                        self._run_signature_cache.pop(
                            next(iter(self._run_signature_cache))
                        )
                return signature
        return None

    def _prefetch_run_signatures(self, inputs: Iterable[Artifact]) -> None:
        """
        Warm the run-signature cache for input artifacts in bulk to reduce DB chatter.
        """
        if not self.db:
            return
        run_ids = {
            str(a.run_id)
            for a in inputs
            if a.run_id and a.run_id not in self._run_signature_cache
        }
        if not run_ids:
            return
        signatures = self.db.get_run_signatures(list(run_ids))
        if not signatures:
            return
        for run_id, signature in signatures.items():
            self._run_signature_cache[run_id] = signature
            if len(self._run_signature_cache) > self._run_signature_cache_max_entries:
                self._run_signature_cache.pop(next(iter(self._run_signature_cache)))

    def _coerce_facet_mapping(self, obj: Any, label: str) -> Dict[str, Any]:
        if obj is None:
            raise ValueError(f"facet_from requires a {label} to extract from.")
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        if hasattr(obj, "dict") and hasattr(obj, "json"):
            return obj.dict()
        if isinstance(obj, Mapping):
            return dict(obj)
        raise ValueError(f"Tracker {label} must be a mapping or Pydantic model.")

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
        artifact_dir: Optional[Union[str, Path]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
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
        artifact_dir : Optional[Union[str, Path]], optional
            Override the per-run artifact directory. Relative paths are resolved
            under ``<run_dir>/outputs``. Absolute paths are used as-is.
        facet : Optional[FacetLike], optional
            Optional small, queryable configuration facet to persist alongside the run.
            This is distinct from `config` (which is hashed and stored in the JSON snapshot).
        facet_from : Optional[List[str]], optional
            List of config keys to extract into the facet. Extracted values are merged
            with any explicit `facet`, with explicit keys taking precedence.
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
        timing_enabled = os.getenv("CONSIST_CACHE_TIMING", "").lower() in {
            "1",
            "true",
            "yes",
        }
        debug_cache = os.getenv("CONSIST_CACHE_DEBUG", "").lower() in {
            "1",
            "true",
            "yes",
        }

        def _log_timing(label: str, start: float) -> None:
            elapsed = time.perf_counter() - start
            if timing_enabled:
                logging.info("[Consist][timing] %s %.3fs", label, elapsed)
            if debug_cache:
                logging.info("[Consist][cache] %s %.3fs", label, elapsed)

        if self.current_consist is not None:
            raise RuntimeError(
                f"Cannot begin_run: A run is already active (id={self.current_consist.run.id}). "
                "Call end_run() first."
            )

        (
            cache_hydration,
            materialize_cached_output_paths,
            materialize_cached_outputs_dir,
            validate_cached_outputs,
        ) = parse_materialize_cached_outputs_kwargs(kwargs)

        raw_config_model: Optional[BaseModel] = (
            config if isinstance(config, BaseModel) else None
        )
        if facet_from:
            if isinstance(facet_from, str):
                raise ValueError("facet_from must be a list of config keys.")
            config_dict_for_facet = self._coerce_facet_mapping(config, "config")
            missing = [key for key in facet_from if key not in config_dict_for_facet]
            if missing:
                raise KeyError(f"facet_from keys not found in config: {missing}")
            derived = {key: config_dict_for_facet[key] for key in facet_from}
            if facet is not None:
                facet_dict = self._coerce_facet_mapping(facet, "facet")
                merged = dict(derived)
                merged.update(facet_dict)
                facet = self.identity.normalize_json(merged)
            else:
                facet = self.identity.normalize_json(derived)

        # Extract explicit Run fields early so they can contribute to identity hashing.
        # These are identity-relevant parameters for most workflows (e.g., a different year
        # should not silently cache-hit a different year).
        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)
        parent_run_id = kwargs.pop("parent_run_id", None)

        if artifact_dir is not None:
            kwargs["artifact_dir"] = str(artifact_dir)

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

        config_dict = self.identity.normalize_json(config_dict)

        # Compute core identity hashes early
        # Important: keep the stored config snapshot as user-provided config, but include
        # selected Run fields in the identity hash to avoid accidental cache hits.
        config_hash = self.identity.compute_run_config_hash(
            config=config_dict, model=model, year=year, iteration=iteration
        )
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
        self._active_run_cache_options = ActiveRunCacheOptions(
            cache_mode=cache_mode,
            cache_hydration=cache_hydration,
            materialize_cached_output_paths=materialize_cached_output_paths,
            materialize_cached_outputs_dir=materialize_cached_outputs_dir,
            validate_cached_outputs=validate_cached_outputs,
        )

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

        current_consist = self.current_consist
        if current_consist is None:
            raise RuntimeError("Cannot start run: no active consist record.")

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
                a.run_id for a in current_consist.inputs if a.run_id is not None
            ]
            if parent_candidates:
                run.parent_run_id = parent_candidates[-1]

        try:
            t0 = time.perf_counter()
            self._prefetch_run_signatures(current_consist.inputs)
            _log_timing("prefetch_run_signatures", t0)
            t0 = time.perf_counter()
            input_hash = self.identity.compute_input_hash(
                current_consist.inputs,
                path_resolver=self.resolve_uri,
                signature_lookup=self._resolve_run_signature,
            )
            run.input_hash = input_hash
            run.signature = self.identity.calculate_run_signature(
                code_hash=git_hash, config_hash=config_hash, input_hash=input_hash
            )
            _log_timing("compute_input_hash", t0)
        except Exception as e:
            logging.warning(
                f"[Consist Warning] Failed to compute inputs hash for run {run_id}: {e}"
            )
            run.input_hash = "error"
            run.signature = "error"

        cached_output_ids: Optional[List[uuid.UUID]] = None

        # --- Cache Logic (Refactored) ---
        if cache_mode == "reuse":
            cache_key = (
                (run.config_hash, run.input_hash, run.git_hash)
                if run.config_hash and run.input_hash and run.git_hash
                else None
            )
            if debug_cache:
                logging.info(
                    "[Consist][cache] lookup key config=%s input=%s code=%s",
                    run.config_hash,
                    run.input_hash,
                    run.git_hash,
                )

            cached_run = self._local_cache_index.get(cache_key) if cache_key else None
            if cached_run is None:
                t0 = time.perf_counter()
                if cache_key:
                    config_hash, input_hash, git_hash = cache_key
                    cached_run = self.find_matching_run(
                        config_hash=config_hash,
                        input_hash=input_hash,
                        git_hash=git_hash,
                    )
                _log_timing("find_matching_run", t0)
            cache_valid = False
            if cached_run:
                t0 = time.perf_counter()
                cache_valid = validate_cached_run_outputs(
                    tracker=self,
                    run=cached_run,
                    options=self._active_run_cache_options,
                )
                _log_timing("validate_cached_outputs", t0)
            elif debug_cache:
                logging.info("[Consist][cache] miss: no matching completed run.")

            if cached_run and cache_valid:
                t0 = time.perf_counter()
                cached_items = hydrate_cache_hit_outputs(
                    tracker=self,
                    run=run,
                    cached_run=cached_run,
                    options=self._active_run_cache_options,
                    link_outputs=False,
                )
                cached_output_ids = [art.id for art in cached_items.outputs.values()]
                _log_timing("hydrate_cache_hit_outputs", t0)
                if debug_cache:
                    logging.info(
                        "[Consist][cache] hit: cached_run=%s outputs=%d hydration=%s",
                        cached_run.id,
                        len(cached_items.outputs),
                        self._active_run_cache_options.cache_hydration,
                    )
            else:
                if cached_run and not cache_valid and debug_cache:
                    logging.info(
                        "[Consist][cache] miss: cached run %s failed validation.",
                        cached_run.id,
                    )
                logging.info("🔄 [Consist] Cache Miss. Running...")
        elif cache_mode == "overwrite":
            logging.warning("⚠️ [Consist] Cache lookup skipped (Mode: Overwrite).")
        elif cache_mode == "readonly":
            logging.info("👁️ [Consist] Read-only mode.")

        if not self.current_consist.cached_run:
            materialize_missing_inputs(
                tracker=self, options=self._active_run_cache_options
            )

        self._flush_json()
        if cached_output_ids and self.db:
            self.persistence.sync_run_with_links(
                run, artifact_ids=cached_output_ids, direction="output"
            )
        else:
            self._sync_run_to_db(run)
        self._emit_run_start(run)

        return run

    @contextmanager
    def start_run(
        self,
        run_id: str,
        model: str,
        **kwargs: Any,
    ) -> Iterator["Tracker"]:
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
            - `facet`, `facet_from`, `hash_inputs`, `facet_schema_version`, `facet_index`
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

    def run(
        self,
        fn: Optional[Callable[..., Any]] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        model: Optional[str] = None,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        inputs: Optional[
            Union[Mapping[str, ArtifactRef], Iterable[ArtifactRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[ArtifactRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        hash_inputs: HashInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_mode: str = "reuse",
        cache_hydration: Optional[str] = None,
        validate_cached_outputs: str = "lazy",
        load_inputs: Optional[bool] = None,
        executor: str = "python",
        container: Optional[Mapping[str, Any]] = None,
        fn_args: Optional[Dict[str, Any]] = None,
        inject_context: bool | str = False,
        output_mismatch: str = "warn",
        output_missing: str = "warn",
    ) -> RunResult:
        """
        Execute a function-shaped run with caching and output handling.
        """
        if executor not in {"python", "container"}:
            raise ValueError("Tracker.run supports executor='python' or 'container'.")

        if executor == "container":
            if container is None:
                raise ValueError("executor='container' requires a container spec.")
            if output_paths is None:
                raise ValueError("executor='container' requires output_paths.")
            if outputs is not None:
                raise ValueError(
                    "executor='container' does not accept outputs; use output_paths."
                )
            if fn is None and name is None:
                raise ValueError("executor='container' requires name when fn is None.")
        else:
            if fn is None:
                raise ValueError("Tracker.run requires a callable fn.")

        if executor == "container":
            resolved_name = name or (fn.__name__ if fn else None)
            if resolved_name is None:
                raise ValueError("executor='container' requires a run name.")
        else:
            resolved_name = name or fn.__name__
        resolved_model = model or resolved_name

        if executor == "python":
            step_def = getattr(fn, "__consist_step__", None)
            if step_def is not None:
                if outputs is None and step_def.outputs is not None:
                    outputs = list(step_def.outputs)
                if tags is None and step_def.tags is not None:
                    tags = list(step_def.tags)
                if description is None and step_def.description is not None:
                    description = step_def.description

            if load_inputs is None:
                load_inputs = isinstance(inputs, Mapping)
            if load_inputs and inputs is not None and not isinstance(inputs, Mapping):
                raise ValueError("load_inputs=True requires inputs to be a dict.")

            if cache_hydration is None and load_inputs:
                cache_hydration = "inputs-missing"

        if input_keys is not None or optional_input_keys is not None:
            warnings.warn(
                "Tracker.run ignores input_keys/optional_input_keys; use inputs mapping instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        if output_mismatch not in {"warn", "error", "ignore"}:
            raise ValueError(
                "output_mismatch must be one of: 'warn', 'error', 'ignore'"
            )
        if output_missing not in {"warn", "error", "ignore"}:
            raise ValueError("output_missing must be one of: 'warn', 'error', 'ignore'")

        resolved_inputs, input_artifacts_by_key = _resolve_input_refs(
            self,
            inputs,
            depends_on,
            include_keyed_artifacts=executor == "python",
        )

        if run_id is None:
            run_id = f"{resolved_name}_{uuid.uuid4().hex[:8]}"

        materialize_cached_output_paths: Optional[Dict[str, Path]] = None
        materialize_cached_outputs_dir: Optional[Path] = None
        if cache_hydration == "outputs-requested":
            if output_paths is None:
                raise ValueError(
                    "cache_hydration='outputs-requested' requires output_paths."
                )
            output_base_dir = _preview_run_artifact_dir(
                self,
                run_id=run_id,
                model=resolved_model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )
            materialize_cached_output_paths = {
                str(key): _resolve_output_path(self, ref, output_base_dir)
                for key, ref in output_paths.items()
            }
        elif cache_hydration == "outputs-all":
            materialize_cached_outputs_dir = _preview_run_artifact_dir(
                self,
                run_id=run_id,
                model=resolved_model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )

        if executor == "container" and cache_mode != "overwrite":
            logging.warning(
                "[Consist] executor='container' uses container-level caching; forcing cache_mode='overwrite'."
            )
            cache_mode = "overwrite"

        start_kwargs: Dict[str, Any] = {
            "run_id": run_id,
            "model": resolved_model,
            "config": config,
            "inputs": resolved_inputs or None,
            "tags": tags,
            "description": description,
            "cache_mode": cache_mode,
            "facet": facet,
            "facet_from": facet_from,
            "hash_inputs": hash_inputs,
            "year": year,
            "iteration": iteration,
            "parent_run_id": parent_run_id,
            "validate_cached_outputs": validate_cached_outputs,
        }
        if facet_schema_version is not None:
            start_kwargs["facet_schema_version"] = facet_schema_version
        if facet_index is not None:
            start_kwargs["facet_index"] = facet_index
        if cache_hydration is not None:
            start_kwargs["cache_hydration"] = cache_hydration
        if materialize_cached_output_paths is not None:
            start_kwargs["materialize_cached_output_paths"] = (
                materialize_cached_output_paths
            )
        if materialize_cached_outputs_dir is not None:
            start_kwargs["materialize_cached_outputs_dir"] = (
                materialize_cached_outputs_dir
            )

        def _handle_missing_outputs(label: str, missing: List[str]) -> None:
            if not missing:
                return
            msg = f"{label} missing outputs: {missing}"
            if output_missing == "error":
                raise RuntimeError(msg)
            if output_missing == "warn":
                logging.warning("[Consist] %s", msg)

        def _handle_output_mismatch(msg: str) -> bool:
            if output_mismatch == "error":
                raise RuntimeError(msg)
            if output_mismatch == "warn":
                logging.warning("[Consist] %s", msg)
            return False

        with self.start_run(**start_kwargs) as t:
            current_consist = t.current_consist
            if current_consist is None:
                raise RuntimeError("No active run context is available.")

            if t.is_cached:
                cached_outputs = {a.key: a for a in current_consist.outputs}
                expected_keys = set(outputs or [])
                if output_paths:
                    expected_keys.update(output_paths.keys())
                missing = [k for k in expected_keys if k not in cached_outputs]
                _handle_missing_outputs(f"Cache hit for run {resolved_name!r}", missing)
                if expected_keys:
                    outputs_map = {
                        k: cached_outputs[k]
                        for k in expected_keys
                        if k in cached_outputs
                    }
                else:
                    outputs_map = cached_outputs
                return RunResult(
                    run=current_consist.run,
                    outputs=outputs_map,
                    cache_hit=True,
                )

            if executor == "container":
                if container is None or output_paths is None:
                    raise RuntimeError(
                        "Container execution requires container and output_paths."
                    )
                if load_inputs:
                    raise ValueError(
                        "executor='container' does not support load_inputs."
                    )
                if not isinstance(container, MappingABC):
                    raise TypeError(
                        "container must be a mapping of run_container arguments."
                    )

                from consist.integrations.containers import run_container

                container_args = dict(container)
                image = container_args.pop("image", None)
                command = container_args.pop("command", None)
                backend_type = container_args.pop(
                    "backend", None
                ) or container_args.pop("backend_type", None)
                backend_type = backend_type or "docker"
                environment = container_args.pop("environment", None) or {}
                working_dir = container_args.pop("working_dir", None)
                volumes = container_args.pop("volumes", None) or {}
                pull_latest = bool(container_args.pop("pull_latest", False))
                lineage_mode = container_args.pop("lineage_mode", "full")

                if container_args:
                    raise ValueError(
                        f"Unknown container options: {sorted(container_args.keys())}"
                    )
                if image is None or command is None:
                    raise ValueError("container spec must include image and command.")

                output_base_dir = self.run_artifact_dir()
                resolved_output_paths = {
                    str(key): _resolve_output_path(self, ref, output_base_dir)
                    for key, ref in output_paths.items()
                }

                result = run_container(
                    tracker=self,
                    run_id=run_id,
                    image=image,
                    command=command,
                    volumes=volumes,
                    inputs=resolved_inputs,
                    outputs=resolved_output_paths,
                    environment=environment,
                    working_dir=working_dir,
                    backend_type=backend_type,
                    pull_latest=pull_latest,
                    lineage_mode=lineage_mode,
                )

                outputs_map = dict(result.artifacts)
                missing = [key for key in output_paths.keys() if key not in outputs_map]
                _handle_missing_outputs(f"Run {resolved_name!r}", missing)

                return RunResult(
                    run=current_consist.run,
                    outputs=outputs_map,
                    cache_hit=result.cache_hit,
                )

            fn_args = dict(fn_args or {})
            config_dict: Dict[str, Any] = {}
            if config is None:
                config_dict = {}
            elif isinstance(config, BaseModel):
                config_dict = config.model_dump()
            else:
                config_dict = dict(config)

            if fn is None:
                raise ValueError("run() requires a callable `fn` to execute.")
            fn_callable = fn
            sig = inspect.signature(fn_callable)
            params = sig.parameters
            has_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
            )
            call_kwargs: Dict[str, Any] = {}

            if "config" in params and "config" not in fn_args and config is not None:
                call_kwargs["config"] = (
                    config if isinstance(config, BaseModel) else config_dict
                )

            for param_name, param in params.items():
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                if param_name in call_kwargs:
                    continue
                if param_name in fn_args:
                    call_kwargs[param_name] = fn_args[param_name]
                    continue
                if load_inputs and isinstance(inputs, Mapping):
                    if param_name in input_artifacts_by_key:
                        if param_name in config_dict:
                            logging.warning(
                                "[Consist] Ambiguous param %r present in inputs and config; preferring inputs.",
                                param_name,
                            )
                        artifact = input_artifacts_by_key[param_name]
                        if getattr(artifact, "_tracker", None) is None:
                            artifact._tracker = weakref.ref(self)
                        call_kwargs[param_name] = self.load(artifact)
                        continue
                if param_name in config_dict:
                    call_kwargs[param_name] = config_dict[param_name]

            for key, value in fn_args.items():
                if key not in call_kwargs:
                    call_kwargs[key] = value

            if inject_context:
                ctx_name = (
                    inject_context
                    if isinstance(inject_context, str)
                    else "_consist_ctx"
                )
                if ctx_name not in call_kwargs:
                    if ctx_name in params or has_var_kw:
                        call_kwargs[ctx_name] = RunContext(self)
                    else:
                        raise ValueError(
                            f"inject_context requested '{ctx_name}', but fn does not accept it."
                        )

            try:
                sig.bind_partial(**call_kwargs)
            except TypeError as exc:
                raise TypeError(
                    f"Tracker.run could not bind arguments for {resolved_name!r}: {exc}"
                ) from exc

            captured_outputs: Dict[str, Artifact] = {}
            if capture_dir is not None:
                with t.capture_outputs(capture_dir, pattern=capture_pattern) as cap:
                    result = fn_callable(**call_kwargs)
                if result is not None:
                    raise ValueError(
                        "capture_dir requires the run function to return None. "
                        "Use inject_context to log outputs manually if you need a return value."
                    )
                captured_outputs = {a.key: a for a in cap.artifacts}
            else:
                result = fn_callable(**call_kwargs)

            outputs_map: Dict[str, Artifact] = {}
            output_base_dir = self.run_artifact_dir()

            def _log_output_value(key: str, value: ArtifactRef) -> Optional[Artifact]:
                if value is None:
                    return None
                return self.log_artifact(value, key=key, direction="output")

            if output_paths is not None:
                for key, ref in output_paths.items():
                    if isinstance(ref, Artifact):
                        ref_path = _resolve_output_path(self, ref, output_base_dir)
                        if not ref_path.exists():
                            _handle_missing_outputs(
                                f"Run {resolved_name!r}", [str(key)]
                            )
                            continue
                        outputs_map[key] = self.log_artifact(
                            ref, key=key, direction="output"
                        )
                    else:
                        ref_path = _resolve_output_path(self, ref, output_base_dir)
                        if not ref_path.exists():
                            _handle_missing_outputs(
                                f"Run {resolved_name!r}", [str(key)]
                            )
                            continue
                        outputs_map[key] = self.log_artifact(
                            ref_path, key=key, direction="output"
                        )

            if outputs:
                if result is None:
                    pass
                elif isinstance(result, dict):
                    for key, value in result.items():
                        logged = _log_output_value(str(key), value)
                        if logged is not None:
                            outputs_map[str(key)] = logged
                elif isinstance(result, (list, tuple)):
                    if len(result) != len(outputs):
                        _handle_output_mismatch(
                            "Output list length does not match declared outputs."
                        )
                    else:
                        for key, value in zip(outputs, result):
                            logged = _log_output_value(key, value)
                            if logged is not None:
                                outputs_map[key] = logged
                elif isinstance(result, pd.DataFrame):
                    if len(outputs) != 1:
                        _handle_output_mismatch(
                            "Single return value does not match declared outputs."
                        )
                    else:
                        outputs_map[outputs[0]] = self.log_dataframe(
                            result, key=outputs[0]
                        )
                elif isinstance(result, pd.Series):
                    if len(outputs) != 1:
                        _handle_output_mismatch(
                            "Single return value does not match declared outputs."
                        )
                    else:
                        outputs_map[outputs[0]] = self.log_dataframe(
                            result.to_frame(name=outputs[0]), key=outputs[0]
                        )
                elif _is_xarray_dataset(result):
                    if len(outputs) != 1:
                        _handle_output_mismatch(
                            "Single return value does not match declared outputs."
                        )
                    else:
                        key = outputs[0]
                        output_path = output_base_dir / f"{key}.zarr"
                        _write_xarray_dataset(result, output_path)
                        outputs_map[key] = self.log_artifact(
                            output_path, key=key, direction="output", driver="zarr"
                        )
                elif isinstance(result, (Artifact, str, Path)):
                    if len(outputs) != 1:
                        _handle_output_mismatch(
                            "Single return value does not match declared outputs."
                        )
                    else:
                        logged = _log_output_value(outputs[0], result)
                        if logged is not None:
                            outputs_map[outputs[0]] = logged
                else:
                    raise TypeError(f"Run returned unsupported type {type(result)}")
            elif result is not None:
                logging.warning(
                    "[Consist] Run %r returned a value but no outputs were declared; ignoring return value.",
                    resolved_name,
                )

            if captured_outputs:
                for key, artifact in captured_outputs.items():
                    outputs_map.setdefault(key, artifact)

            logged_outputs = {a.key: a for a in current_consist.outputs}
            if outputs:
                missing_keys = [
                    key
                    for key in outputs
                    if key not in outputs_map and key not in logged_outputs
                ]
                _handle_missing_outputs(f"Run {resolved_name!r}", missing_keys)
                for key in outputs:
                    if key not in outputs_map and key in logged_outputs:
                        outputs_map[key] = logged_outputs[key]
            elif not outputs_map and logged_outputs:
                outputs_map = logged_outputs

            return RunResult(
                run=current_consist.run,
                outputs=outputs_map,
                cache_hit=False,
            )

    @contextmanager
    def trace(
        self,
        name: str,
        *,
        run_id: Optional[str] = None,
        model: Optional[str] = None,
        description: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        inputs: Optional[
            Union[Mapping[str, ArtifactRef], Iterable[ArtifactRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[ArtifactRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        hash_inputs: HashInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_mode: str = "reuse",
        cache_hydration: Optional[str] = None,
        validate_cached_outputs: str = "lazy",
        output_mismatch: str = "warn",
        output_missing: str = "warn",
    ) -> Iterator["Tracker"]:
        """
        Manual tracing context manager for a run.
        """
        resolved_model = model or name

        if input_keys is not None or optional_input_keys is not None:
            warnings.warn(
                "Tracker.trace ignores input_keys/optional_input_keys; use inputs mapping instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        if output_mismatch not in {"warn", "error", "ignore"}:
            raise ValueError(
                "output_mismatch must be one of: 'warn', 'error', 'ignore'"
            )
        if output_missing not in {"warn", "error", "ignore"}:
            raise ValueError("output_missing must be one of: 'warn', 'error', 'ignore'")

        resolved_inputs, _ = _resolve_input_refs(
            self,
            inputs,
            depends_on,
            include_keyed_artifacts=False,
        )

        if run_id is None:
            run_id = f"{name}_{uuid.uuid4().hex[:8]}"

        materialize_cached_output_paths: Optional[Dict[str, Path]] = None
        materialize_cached_outputs_dir: Optional[Path] = None
        if cache_hydration == "outputs-requested":
            if output_paths is None:
                raise ValueError(
                    "cache_hydration='outputs-requested' requires output_paths."
                )
            output_base_dir = _preview_run_artifact_dir(
                self,
                run_id=run_id,
                model=resolved_model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )
            materialize_cached_output_paths = {
                str(key): _resolve_output_path(self, ref, output_base_dir)
                for key, ref in output_paths.items()
            }
        elif cache_hydration == "outputs-all":
            materialize_cached_outputs_dir = _preview_run_artifact_dir(
                self,
                run_id=run_id,
                model=resolved_model,
                description=description,
                year=year,
                iteration=iteration,
                parent_run_id=parent_run_id,
                tags=tags,
            )

        start_kwargs: Dict[str, Any] = {
            "run_id": run_id,
            "model": resolved_model,
            "config": config,
            "inputs": resolved_inputs or None,
            "tags": tags,
            "description": description,
            "cache_mode": cache_mode,
            "facet": facet,
            "facet_from": facet_from,
            "hash_inputs": hash_inputs,
            "year": year,
            "iteration": iteration,
            "parent_run_id": parent_run_id,
            "validate_cached_outputs": validate_cached_outputs,
        }
        if facet_schema_version is not None:
            start_kwargs["facet_schema_version"] = facet_schema_version
        if facet_index is not None:
            start_kwargs["facet_index"] = facet_index
        if cache_hydration is not None:
            start_kwargs["cache_hydration"] = cache_hydration
        if materialize_cached_output_paths is not None:
            start_kwargs["materialize_cached_output_paths"] = (
                materialize_cached_output_paths
            )
        if materialize_cached_outputs_dir is not None:
            start_kwargs["materialize_cached_outputs_dir"] = (
                materialize_cached_outputs_dir
            )

        def _handle_missing_outputs(label: str, missing: List[str]) -> None:
            if not missing:
                return
            msg = f"{label} missing outputs: {missing}"
            if output_missing == "error":
                raise RuntimeError(msg)
            if output_missing == "warn":
                logging.warning("[Consist] %s", msg)

        with self.start_run(**start_kwargs) as t:
            current_consist = t.current_consist
            if current_consist is None:
                raise RuntimeError("No active run context is available.")

            output_base_dir = self.run_artifact_dir()
            try:
                if capture_dir is not None:
                    with t.capture_outputs(capture_dir, pattern=capture_pattern):
                        yield t
                else:
                    yield t
            finally:
                if output_paths is not None:
                    for key, ref in output_paths.items():
                        if isinstance(ref, Artifact):
                            ref_path = _resolve_output_path(self, ref, output_base_dir)
                            if not ref_path.exists():
                                _handle_missing_outputs(f"Run {name!r}", [str(key)])
                                continue
                            self.log_artifact(ref, key=key, direction="output")
                        else:
                            ref_path = _resolve_output_path(self, ref, output_base_dir)
                            if not ref_path.exists():
                                _handle_missing_outputs(f"Run {name!r}", [str(key)])
                                continue
                            self.log_artifact(ref_path, key=key, direction="output")

                if outputs:
                    logged_outputs = {a.key: a for a in current_consist.outputs}
                    missing_keys = [key for key in outputs if key not in logged_outputs]
                    _handle_missing_outputs(f"Run {name!r}", missing_keys)

    def scenario(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        model: str = "scenario",
        step_cache_hydration: Optional[str] = None,
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
        step_cache_hydration : Optional[str], optional
            Default cache hydration policy for all scenario steps unless overridden
            in a specific `scenario.trace(...)` or `scenario.run(...)`.
        **kwargs : Any
            Additional metadata or arguments for the header run (including `facet_from`).

        Returns
        -------
        ScenarioContext
            A context manager object that provides `.trace()` and `.add_input()` methods.

        Example
        -------
        ```python
        with tracker.scenario("baseline", config={"mode": "test"}) as sc:
            sc.add_input("data.csv", key="data")
            with sc.step("init"):
                ...
        ```
        """
        return ScenarioContext(
            self,
            name,
            config,
            tags,
            model,
            step_cache_hydration=step_cache_hydration,
            **kwargs,
        )

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
        cache_mode = self._active_run_cache_options.cache_mode or "reuse"

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
        self._active_run_cache_options = ActiveRunCacheOptions()

        return run

    # --- Query Helpers ---

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
        index_by: Optional[Union[str, IndexBySpec]] = None,
        name: Optional[str] = None,
    ) -> Union[List[Run], Dict[Hashable, Run]]:
        """
        Retrieve runs matching the specified criteria.

        Parameters
        ----------
        tags : Optional[List[str]], optional
            Filter runs that contain all provided tags.
        year : Optional[int], optional
            Filter by run year.
        iteration : Optional[int], optional
            Filter by run iteration.
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
        Union[List[Run], Dict[Hashable, Run]]
            List of runs, or a dict keyed by `index_by` when requested.

        Raises
        ------
        TypeError
            If `index_by` is an unsupported type.
        """
        return self.queries.find_runs(
            tags=tags,
            year=year,
            iteration=iteration,
            model=model,
            status=status,
            parent_id=parent_id,
            metadata=metadata,
            limit=limit,
            index_by=index_by,
            name=name,
        )

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
        return self.queries.find_run(**kwargs)

    def find_latest_run(
        self,
        *,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 10_000,
    ) -> Run:
        """
        Return the most recent run matching the filters.

        Selection priority:
        1) Highest `iteration` (when present)
        2) Newest `created_at` (fallback when no iteration is set)
        """
        return self.queries.find_latest_run(
            parent_id=parent_id,
            model=model,
            status=status,
            year=year,
            tags=tags,
            metadata=metadata,
            limit=limit,
        )

    def get_latest_run_id(self, **kwargs) -> str:
        """
        Convenience wrapper to return the latest run ID for the given filters.
        """
        return self.queries.get_latest_run_id(**kwargs)

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

        # Cache-hit ergonomics:
        # - Scenario/trace bodies still execute on cache hits (we hydrate cached outputs,
        #   but we do not skip user code).
        # - Many workflows write cache-agnostic code that calls `log_artifact(...)` in
        #   both cache-hit and cache-miss cases.
        # - On cache hits, re-logging outputs should typically *return the hydrated cached
        #   output* (by key) rather than creating a new artifact node.
        #
        # If the caller attempts to produce outputs that do not match the cached outputs,
        # we *demote* the cache hit to an executing run (so provenance remains truthful).
        if direction == "output" and self.is_cached:
            cached = maybe_return_cached_output_or_demote_cache_hit(
                tracker=self,
                path=path,
                key=key,
            )
            if cached is not None:
                return cached

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

    def log_dataframe(
        self,
        df: pd.DataFrame,
        key: str,
        schema: Optional[Type[SQLModel]] = None,
        direction: str = "output",
        path: Optional[Union[str, Path]] = None,
        driver: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        **to_file_kwargs: Any,
    ) -> Artifact:
        """
        Serialize a DataFrame, log it as an artifact, and trigger optional ingestion.

        Parameters
        ----------
        df : pd.DataFrame
            Data to persist.
        key : str
            Logical artifact key.
        schema : Optional[Type[SQLModel]], optional
            Schema used for ingestion, if provided.
        direction : str, default "output"
            Artifact direction relative to the run.
        path : Optional[Union[str, Path]], optional
            Output path; defaults to `<run_dir>/outputs/<run_subdir>/<key>.<driver>` where
            ``run_subdir`` is derived from ``run_subdir_fn`` (or the default pattern).
        driver : Optional[str], optional
            File format driver (e.g., "parquet" or "csv").
        meta : Optional[Dict[str, Any]], optional
            Additional metadata for the artifact.
        **to_file_kwargs : Any
            Keyword arguments forwarded to ``pd.DataFrame.to_parquet`` or ``to_csv``.

        Returns
        -------
        Artifact
            The artifact logged for the written dataset.

        Raises
        ------
        ValueError
            If the requested driver is unsupported.
        """
        if path is None:
            base_dir = self.run_artifact_dir()
            resolved_path = base_dir / f"{key}.{driver or 'parquet'}"
        else:
            resolved_path = Path(path)

        inferred_driver = driver
        if inferred_driver is None:
            suffix = resolved_path.suffix.lower().lstrip(".")
            inferred_driver = suffix or "parquet"

        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        if inferred_driver == "parquet":
            df.to_parquet(resolved_path, **to_file_kwargs)
        elif inferred_driver == "csv":
            df.to_csv(resolved_path, index=False, **to_file_kwargs)
        else:
            raise ValueError(f"Unsupported driver for log_dataframe: {inferred_driver}")

        meta_payload = meta or {}
        art = self.log_artifact(
            resolved_path, key=key, direction=direction, schema=schema, **meta_payload
        )
        if schema is not None:
            self.ingest(art, df, schema=schema)
        return art

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
        return self.artifacts.log_h5_container(
            path,
            key=key,
            direction=direction,
            discover_tables=discover_tables,
            table_filter=table_filter,
            **meta,
        )

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

        return ingest_artifact(
            tracker=self,
            artifact=artifact,
            data=data,
            schema=schema,
            run=run,
            profile_schema=profile_schema,
        )

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

    def load_matrix(
        self,
        concept_key: str,
        variables: Optional[List[str]] = None,
        *,
        run_ids: Optional[List[str]] = None,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Any:
        """
        Convenience wrapper for loading a matrix view from tracked artifacts.
        """
        factory = MatrixViewFactory(self)
        return factory.load_matrix_view(
            concept_key,
            variables,
            run_ids=run_ids,
            parent_id=parent_id,
            model=model,
            status=status,
        )

    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        """
        Materialize a cached artifact onto the filesystem.
        """
        result = materialize_artifacts(
            tracker=self,
            items=[(artifact, Path(destination_path))],
            on_missing=on_missing,
        )
        return result.get(artifact.key)

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
            artifact = self.db.get_artifact(key_or_id)
            if artifact is not None:
                artifact._tracker = weakref.ref(self)
            return artifact
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
            artifact = self.db.get_artifact_by_uri(uri)
            if artifact is not None:
                artifact._tracker = weakref.ref(self)
            return artifact

        return None

    def find_artifacts(
        self,
        *,
        creator: Optional[Union[str, Run]] = None,
        consumer: Optional[Union[str, Run]] = None,
        key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """
        Find artifacts by producing/consuming runs and key.

        Parameters
        ----------
        creator : Optional[Union[str, Run]]
            Run ID (or Run) that logged the artifact as an output.
        consumer : Optional[Union[str, Run]]
            Run ID (or Run) that logged the artifact as an input.
        key : Optional[str]
            Exact artifact key to match.
        limit : int, default 100
            Maximum number of artifacts to return.

        Returns
        -------
        list
            Matching artifact records (empty if DB is not configured).
        """
        if not self.db:
            return []

        creator_id = creator.id if isinstance(creator, Run) else creator
        consumer_id = consumer.id if isinstance(consumer, Run) else consumer

        artifacts = self.db.find_artifacts(
            creator=creator_id, consumer=consumer_id, key=key, limit=limit
        )
        for artifact in artifacts:
            artifact._tracker = weakref.ref(self)
        return artifacts

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
        return self.queries.get_config_facet(facet_id)

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
        return self.queries.get_config_facets(
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
        return self.queries.get_run_config_kv(
            run_id, namespace=namespace, prefix=prefix, limit=limit
        )

    def get_config_values(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
    ) -> Dict[str, Any]:
        """
        Return a flattened config facet as a dict of key/value pairs.

        Parameters
        ----------
        run_id : str
            Run identifier.
        namespace : Optional[str], optional
            Namespace for the facet. Defaults to the run's model name when available.
        prefix : Optional[str], optional
            Filter keys by prefix (e.g. ``"inputs."``).
        keys : Optional[Iterable[str]], optional
            Only include specific keys when provided.
        limit : int, default 10_000
            Maximum number of entries to return.

        Returns
        -------
        dict
            Mapping of flattened keys to typed values.

        Notes
        -----
        Keys are stored as flattened dotted paths. If an original key contains a
        literal dot, it is escaped as ``"\\."`` in the stored key.
        """
        return self.queries.get_config_values(
            run_id,
            namespace=namespace,
            prefix=prefix,
            keys=keys,
            limit=limit,
        )

    def diff_runs(
        self,
        run_id_a: str,
        run_id_b: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
        include_equal: bool = False,
    ) -> Dict[str, Any]:
        """
        Compare flattened config facets between two runs.

        Parameters
        ----------
        run_id_a : str
            Baseline run identifier.
        run_id_b : str
            Comparison run identifier.
        namespace : Optional[str], optional
            Namespace for facets. Defaults to each run's model name.
        prefix : Optional[str], optional
            Filter keys by prefix (e.g. ``"inputs."``).
        keys : Optional[Iterable[str]], optional
            Only include specific keys when provided.
        limit : int, default 10_000
            Maximum number of entries to inspect per run.
        include_equal : bool, default False
            If True, include keys whose values are unchanged.

        Returns
        -------
        dict
            A dict with `namespace` metadata and `changes` mapping keys to values.
        """
        return self.queries.diff_runs(
            run_id_a,
            run_id_b,
            namespace=namespace,
            prefix=prefix,
            keys=keys,
            limit=limit,
            include_equal=include_equal,
        )

    def get_config_value(
        self,
        run_id: str,
        key: str,
        *,
        namespace: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        """
        Retrieve a single config value from a flattened config facet.

        Parameters
        ----------
        run_id : str
            Run identifier.
        key : str
            Flattened key to fetch.
        namespace : Optional[str], optional
            Namespace for the facet. Defaults to the run's model name when available.
        default : Any, optional
            Value to return when the key is missing.

        Returns
        -------
        Any
            The typed value for the key, or ``default`` if missing.
        """
        return self.queries.get_config_value(
            run_id, key, namespace=namespace, default=default
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
        return self.queries.find_runs_by_facet_kv(
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

        current_run_id = self.current_consist.run.id if self.current_consist else None
        if run_id != current_run_id:
            cached = self._run_artifacts_cache.get(run_id)
            if cached is not None:
                return cached

        # Get raw list [(Artifact, "input"), (Artifact, "output")]
        raw_list = self.db.get_artifacts_for_run(run_id)

        inputs = {}
        outputs = {}

        for artifact, direction in raw_list:
            if direction == "input":
                inputs[artifact.key] = artifact
            elif direction == "output":
                outputs[artifact.key] = artifact

        artifacts = RunArtifacts(inputs=inputs, outputs=outputs)
        for artifact in itertools.chain(inputs.values(), outputs.values()):
            artifact._tracker = weakref.ref(self)
        if run_id != current_run_id:
            self._run_artifacts_cache[run_id] = artifacts
            if len(self._run_artifacts_cache) > self._run_artifacts_cache_max_entries:
                self._run_artifacts_cache.pop(next(iter(self._run_artifacts_cache)))
        return artifacts

    def get_run_outputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return output artifacts for a run, keyed by artifact key.
        """
        return self.get_artifacts_for_run(run_id).outputs

    def get_run_inputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return input artifacts for a run, keyed by artifact key.
        """
        return self.get_artifacts_for_run(run_id).inputs

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

    def load_run_output(self, run_id: str, key: str, **kwargs: Any) -> Any:
        """
        Load a specific output artifact from a run by key.

        Parameters
        ----------
        run_id : str
            Run identifier.
        key : str
            Output artifact key to load.
        **kwargs : Any
            Forwarded to `Tracker.load(...)`.
        """
        artifact = self.get_run_artifact(run_id, key=key, direction="output")
        if artifact is None:
            raise ValueError(
                f"No output artifact found for run_id={run_id!r} key={key!r}."
            )
        return self.load(artifact, **kwargs)

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
        self,
        artifact_key_or_id: Union[str, uuid.UUID],
        *,
        max_depth: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Recursively builds a lineage tree for a given artifact.

        Parameters
        ----------
        artifact_key_or_id : Union[str, uuid.UUID]
            Artifact key or UUID.
        max_depth : Optional[int], optional
            Maximum depth to traverse (0 returns only the artifact). Useful for
            large graphs or iterative workflows.
        """
        return self.lineage.get_lineage(
            artifact_key_or_id=artifact_key_or_id,
            max_depth=max_depth,
        )

    def print_lineage(
        self,
        artifact_key_or_id: Union[str, uuid.UUID],
        *,
        max_depth: Optional[int] = None,
    ) -> str:
        """
        Return and print a formatted lineage tree for an artifact.
        """
        return self.lineage.print_lineage(
            artifact_key_or_id=artifact_key_or_id,
            max_depth=max_depth,
        )

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
        self.persistence.flush_json()

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
        self.persistence.sync_run(run)

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
        self.persistence.sync_artifact(artifact, direction)

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

    def load_input_bundle(self, run_id: str) -> dict[str, Artifact]:
        """
        Load a set of input artifacts from a prior "bundle" run by run_id.

        This is a convenience helper for shared DuckDB bundles where a dedicated
        run logs all required inputs as outputs. The returned dict can be passed
        directly to `inputs=[...]` on a new run.

        Parameters
        ----------
        run_id : str
            The run id that logged the bundle outputs.

        Returns
        -------
        dict[str, Artifact]
            Mapping of artifact key -> Artifact from the bundle run.

        Raises
        ------
        ValueError
            If the run does not exist or has no output artifacts.
        """
        run = self.get_run(run_id)
        if not run:
            raise ValueError(f"Input bundle run_id={run_id!r} not found.")

        outputs = self.get_artifacts_for_run(run_id).outputs
        if not outputs:
            raise ValueError(f"Input bundle run_id={run_id!r} has no output artifacts.")
        return outputs

    @contextmanager
    def capture_outputs(
        self, directory: Union[str, Path], pattern: str = "*", recursive: bool = False
    ) -> Iterator[OutputCapture]:
        """
        A context manager to automatically capture and log new or modified files in a directory.

        This context manager is used within a `tracker.run`/`tracker.trace` call or `start_run` block
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

        normalized = self.identity.normalize_json(kwargs)
        self.current_consist.run.meta.update(normalized)

        # 2. Persist
        self._flush_json()
        # We also sync to DB immediately so external monitors can see progress/tags
        self._sync_run_to_db(self.current_consist.run)

    def define_step(self, **kwargs) -> Callable:
        """
        Attach metadata to a function without changing execution behavior.

        This is used by Tracker.run/ScenarioContext.run to infer defaults.
        """
        return define_step_decorator(**kwargs)

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
            True if the current `start_run`/`run`/`trace` execution is reusing a cached run.
        """
        return bool(
            self.current_consist and self.current_consist.cached_run is not None
        )

    def suspend_cache_options(self) -> ActiveRunCacheOptions:
        """
        Suspend active-run cache options and reset them to defaults.

        Returns
        -------
        ActiveRunCacheOptions
            The previously active cache options, for later restoration.
        """
        suspended = self._active_run_cache_options
        self._active_run_cache_options = ActiveRunCacheOptions()
        return suspended

    def restore_cache_options(self, options: ActiveRunCacheOptions) -> None:
        """
        Restore previously suspended active-run cache options.

        Parameters
        ----------
        options : ActiveRunCacheOptions
            Cache options to restore.
        """
        self._active_run_cache_options = options

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
