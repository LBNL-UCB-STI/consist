import inspect
import itertools
from dataclasses import replace
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
    TYPE_CHECKING,
    Tuple,
    Type,
    Union,
    cast,
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
from consist.core.config_canonicalization import (
    ConfigAdapter,
    ConfigAdapterOptions,
    CanonicalConfig,
    CanonicalizationResult,
    ConfigContribution,
    ConfigPlan,
    validate_config_plan,
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
from consist.core.netcdf_views import NetCdfMetadataView
from consist.core.openmatrix_views import OpenMatrixMetadataView
from consist.core.queries import RunQueryService
from consist.core.validation import (
    validate_config_structure,
    validate_run_meta,
    validate_run_strings,
)
from consist.core.workflow import OutputCapture, RunContext, ScenarioContext
from consist.core.input_utils import coerce_input_map
from consist.models.artifact import Artifact
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaField
from consist.models.run import ConsistRecord, Run, RunArtifacts, RunResult
from consist.types import ArtifactRef, FacetLike, HasFacetSchemaVersion, HashInputs

if TYPE_CHECKING:
    from consist.core.coupler import Coupler

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _is_xarray_dataset(value: Any) -> bool:
    try:
        import xarray as xr
    except ImportError:
        return False
    return isinstance(value, xr.Dataset)


def _write_xarray_dataset(dataset: Any, path: Path) -> None:
    try:
        import xarray as xr
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
        allow_external_paths: Optional[bool] = None,
        openlineage_enabled: bool = False,
        openlineage_namespace: Optional[str] = None,
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
        allow_external_paths : Optional[bool], default None
            Allow artifact directories and cached-output materialization to write
            outside `run_dir`. Defaults to CONSIST_ALLOW_EXTERNAL_PATHS when unset.
        openlineage_enabled : bool, default False
            Emit OpenLineage events to JSONL alongside `consist.json`.
        openlineage_namespace : Optional[str], default None
            Namespace for OpenLineage jobs and datasets. Defaults to the project
            root directory name.
        """
        # 1. Initialize FileSystem Service
        # (This handles the mkdir and path resolution internally now)
        self.fs = FileSystemManager(run_dir, mounts)

        self.mounts = self.fs.mounts
        self.run_dir = self.fs.run_dir

        self.access_mode = access_mode
        self._run_subdir_fn = run_subdir_fn
        if allow_external_paths is None:
            allow_external_paths = _env_bool("CONSIST_ALLOW_EXTERNAL_PATHS", False)
        self.allow_external_paths = bool(allow_external_paths)

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
        # Store registered schemas by class name for cross-session lookup.
        # When ingest() is called in a different Python session, it can look up
        # a schema by the artifact's schema_name (e.g., "MyDataSchema") if the
        # tracker was initialized with schemas=[MyDataSchema, ...].
        self._registered_schemas: Dict[str, Type[SQLModel]] = {}
        if schemas:
            if not self.db:
                logging.warning(
                    "[Consist] Schemas provided but no database configured. Views will not be created."
                )
            else:
                for schema in schemas:
                    # Register by class name so we can look it up later
                    self._registered_schemas[schema.__name__] = schema
                    self.view(schema)

        # In-Memory State (The Source of Truth)
        self.current_consist: Optional[ConsistRecord] = None
        self._active_coupler: Optional["Coupler"] = None

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

        self._runs_by_id: Dict[str, Run] = {}
        openlineage_emitter = None
        if openlineage_enabled:
            from consist.core.openlineage import OpenLineageEmitter, OpenLineageOptions

            project_root_path = Path(project_root) if project_root else Path.cwd()
            namespace = openlineage_namespace or project_root_path.resolve().name
            schema_resolver = None
            db = self.db
            if db is not None:

                def schema_resolver(
                    artifact: Artifact,
                    *,
                    _db: DatabaseManager = db,
                ) -> Optional[tuple[ArtifactSchema, List[ArtifactSchemaField]]]:
                    return _db.get_artifact_schema_for_artifact(artifact_id=artifact.id)

            openlineage_emitter = OpenLineageEmitter(
                OpenLineageOptions(
                    enabled=True,
                    namespace=namespace,
                    path=self.fs.run_dir / "openlineage.jsonl",
                ),
                schema_resolver=schema_resolver,
                run_lookup=self._run_lookup,
                run_facet_resolver=self._openlineage_run_facet,
            )

        self.events = EventManager()
        if openlineage_emitter:
            from consist.core.lifecycle import LifecycleEmitter

            self._lifecycle = LifecycleEmitter(
                openlineage=openlineage_emitter,
                input_resolver=self._openlineage_inputs,
            )
            self.on_run_start(self._lifecycle.emit_start)
            self.on_run_complete(self._lifecycle.emit_complete)
            self.on_run_failed(self._lifecycle.emit_failed)
        else:
            self._lifecycle = None

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
            Directory under ``run_dir`` where run artifacts should be written by default.
            Absolute artifact_dir values outside ``run_dir`` are only allowed when
            allow_external_paths is enabled.
        """
        target_run = run
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            return self.run_dir / "outputs"

        workspace_dir = self.run_dir.resolve()
        base_dir = (self.run_dir / "outputs").resolve()
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
                resolved = artifact_path.resolve()
                if not self._allow_external_paths_for_run(target_run):
                    try:
                        resolved.relative_to(workspace_dir)
                    except ValueError as exc:
                        raise ValueError(
                            f"artifact_dir must remain within {workspace_dir}; got {resolved}. "
                            "Set allow_external_paths=True or CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
                        ) from exc
                return resolved
            resolved = (base_dir / artifact_path).resolve()
            try:
                resolved.relative_to(base_dir)
            except ValueError as exc:
                raise ValueError(
                    f"artifact_dir {artifact_dir!r} escapes base directory {base_dir}. "
                    "Set allow_external_paths=True or CONSIST_ALLOW_EXTERNAL_PATHS=1 to override."
                ) from exc
            return resolved

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

        resolved = (base_dir / subdir_path).resolve()
        try:
            resolved.relative_to(base_dir)
        except ValueError as exc:
            raise ValueError(
                f"run_subdir must resolve under {base_dir}; got {resolved}"
            ) from exc
        return resolved

    @staticmethod
    def _safe_run_id(run_id: str) -> str:
        return "".join(
            c if (c.isalnum() or c in ("-", "_", ".")) else "_" for c in run_id
        )

    def _resolve_run_snapshot_path(self, run_id: str, run: Optional[Run]) -> Path:
        run_dir = self.fs.run_dir
        if run and run.meta:
            physical_run_dir = run.meta.get("_physical_run_dir")
            if physical_run_dir:
                run_dir = Path(physical_run_dir)
        safe_run_id = self._safe_run_id(run_id)
        return run_dir / "consist_runs" / f"{safe_run_id}.json"

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
        prefer_source: Optional[Literal["file", "duckdb"]] = None,
    ) -> str:
        """
        Export a captured artifact schema as a SQLModel stub for manual editing.

        Exactly one of ``schema_id`` or ``artifact_id`` must be provided. The
        generated Python source is returned and can optionally be written to
        ``out_path``.

        Parameters
        ----------
        schema_id : Optional[str], optional
            Schema identifier to export (from the schema registry). If provided,
            prefer_source is ignored and this specific schema is used.
        artifact_id : Optional[Union[str, uuid.UUID]], optional
            Artifact ID to export the associated schema. When used, the schema
            selection respects the prefer_source parameter.
        out_path : Optional[Path], optional
            If provided, write the stub to this path and return its contents.
        table_name : Optional[str], optional
            Override the SQL table name in the generated class.
        class_name : Optional[str], optional
            Override the Python class name in the generated class.
        abstract : bool, default True
            Whether to mark the generated class as abstract.
        include_system_cols : bool, default False
            Whether to include Consist system columns in the stub.
        include_stats_comments : bool, default True
            Whether to include column-level stats as comments.
        prefer_source : {"file", "duckdb"}, optional
            Preference hint for when user_provided schema does not exist. This is
            useful when an artifact has both a file profile (pandas dtypes) and a
            duckdb profile (post-ingestion types). Ignored if schema_id is provided
            directly.

            IMPORTANT: User-provided schemas (manually curated with FK constraints,
            indexes, etc.) are ALWAYS preferred if they exist. This parameter does
            not override user_provided schemas.

            - "file": Prefer the original file schema (CSV/Parquet with pandas dtypes)
            - "duckdb": Prefer the post-ingestion schema from the DuckDB table
            - None (default): Prefer file, as it preserves richer type information
              (e.g., pandas category)

        Returns
        -------
        str
            The rendered SQLModel stub source.

        Raises
        ------
        ValueError
            If the tracker has no database configured or if the selector is invalid.
        KeyError
            If no schema is found for the provided selector.

        Examples
        --------
        Export file schema (original raw file dtypes):
        >>> tracker.export_schema_sqlmodel(artifact_id=art.id)

        Export ingested table schema (after dlt normalization):
        >>> tracker.export_schema_sqlmodel(artifact_id=art.id, prefer_source="duckdb")

        Export a specific schema directly by ID:
        >>> tracker.export_schema_sqlmodel(schema_id="abc123xyz")
        """
        if not self.db:
            raise ValueError("Schema export requires a configured database (db_path).")
        if (schema_id is None) == (artifact_id is None):
            raise ValueError("Provide exactly one of schema_id or artifact_id.")

        backfill_ordinals = self.access_mode != "read_only"

        if artifact_id is not None:
            # When fetching by artifact, pass through the source preference.
            # This allows users to choose between file and duckdb profiles.
            artifact_uuid = (
                uuid.UUID(artifact_id) if isinstance(artifact_id, str) else artifact_id
            )
            fetched = self.db.get_artifact_schema_for_artifact(
                artifact_id=artifact_uuid,
                backfill_ordinals=backfill_ordinals,
                prefer_source=prefer_source,
            )
        else:
            # When fetching by schema_id directly, we ignore prefer_source
            # (the user has already specified which schema they want).
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
        # 1. Check active run (unlikely for inputs, but good for completeness)
        if self.current_consist and self.current_consist.run.id == run_id:
            return self.current_consist.run.signature
        # 2. Check Database
        if self.db:
            run = self.db.get_run(run_id)
            if run:
                return run.signature
        return None

    def _prefetch_run_signatures(self, inputs: Iterable[Artifact]) -> None:
        """
        Warm the run-signature cache for input artifacts in bulk to reduce DB chatter.
        """
        return

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

    def _allow_external_paths_for_run(self, run: Optional[Run]) -> bool:
        if run and isinstance(run.meta, dict) and "allow_external_paths" in run.meta:
            return bool(run.meta["allow_external_paths"])
        return self.allow_external_paths

    def _run_lookup(self, run_id: str) -> Optional[Run]:
        return self._runs_by_id.get(run_id)

    def _openlineage_inputs(self) -> List[Artifact]:
        if self.current_consist is None:
            return []
        return list(self.current_consist.inputs)

    def _openlineage_run_facet(self, run: Run) -> Dict[str, Any]:
        if self.current_consist is None:
            return {}
        if self.current_consist.run.id != run.id:
            return {}
        facet: Dict[str, Any] = {}
        if self.current_consist.facet:
            facet["config_facet"] = dict(self.current_consist.facet)
        if self.current_consist.config:
            facet["config_keys"] = sorted(self.current_consist.config.keys())
        return facet

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
        allow_external_paths: Optional[bool] = None,
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
            A descriptive name for the model or process being executed (non-empty,
            length-limited).
        config : Union[Dict[str, Any], BaseModel, None], optional
            Configuration parameters for this run.
            Keys must be strings; extremely large string values are rejected.
        inputs : Optional[list[ArtifactRef]], optional
            A list of input paths (str/Path) or Artifact references.
        tags : Optional[List[str]], optional
            A list of string labels for categorization and filtering (non-empty, length-limited).
        description : Optional[str], optional
            A human-readable description of the run's purpose.
        cache_mode : str, default "reuse"
            Strategy for caching: "reuse", "overwrite", or "readonly".
        artifact_dir : Optional[Union[str, Path]], optional
            Override the per-run artifact directory. Relative paths are resolved
            under ``<run_dir>/outputs``. Absolute paths must remain within ``run_dir``
            unless allow_external_paths is enabled.
        allow_external_paths : Optional[bool], optional
            Allow artifact_dir and cached-output materialization outside ``run_dir``.
            Defaults to the Tracker setting when unset.
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
            Metadata keys/values are validated and size-limited; use
            CONSIST_MAX_METADATA_ITEMS/KEY_LENGTH/VALUE_LENGTH to override.

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

        validate_run_strings(model_name=model, description=description, tags=tags)

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
        if allow_external_paths is not None:
            kwargs["allow_external_paths"] = bool(allow_external_paths)

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
        if not isinstance(config_dict, MappingABC):
            raise TypeError("config must be a mapping after normalization.")
        validate_config_structure(config_dict)

        # Compute core identity hashes early
        # Important: keep the stored config snapshot as user-provided config, but include
        # selected Run fields in the identity hash to avoid accidental cache hits.
        config_hash = self.identity.compute_run_config_hash(
            config=config_dict, model=model, year=year, iteration=iteration
        )
        git_hash = self.identity.get_code_version()

        kwargs["_physical_run_dir"] = str(self.run_dir)
        if kwargs:
            validate_run_meta(kwargs)

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
        self._runs_by_id[run.id] = run

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
        config_plan: Optional[ConfigPlan] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
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
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str = False,
        output_mismatch: str = "warn",
        output_missing: str = "warn",
    ) -> RunResult:
        """
        Execute a function-shaped run with caching and output handling.

        This method executes a callable (or container) with automatic provenance tracking,
        intelligent caching based on code+config+inputs, and artifact logging.

        Parameters
        ----------
        fn : Optional[Callable]
            The function to execute. Required for executor='python'. Can be None for executor='container'.
        name : Optional[str]
            Human-readable name for the run. Defaults to function name if not provided.
        run_id : Optional[str], optional
            Unique identifier for this run. Auto-generated if not provided.
        model : Optional[str], optional
            Model/component name for categorizing runs. Defaults to the run name.
        description : Optional[str], optional
            Human-readable description of the run.
        config : Optional[Dict[str, Any]], optional
            Configuration parameters. Becomes part of the cache signature. Can be a dict or Pydantic model.
        config_plan : Optional[ConfigPlan], optional
            Precomputed config plan (e.g., from ActivitySim adapter). The plan's identity hash
            is folded into the run config hash and its artifacts/ingestables are applied on cache miss.
        config_plan_ingest : bool, default True
            Whether to ingest tables from the config plan.
        config_plan_profile_schema : bool, default False
            Whether to profile ingested schemas for the config plan.
        inputs : Optional[Mapping[str, ArtifactRef] | Iterable[ArtifactRef]], optional
            Input files or artifacts.
            - Dict: Maps names to paths/Artifacts. Auto-loads into function parameters (default load_inputs=True).
            - List/Iterable: Hashed for cache key but not auto-loaded (use load_inputs=False).
        input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        optional_input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        depends_on : Optional[List[ArtifactRef]], optional
            Additional file paths or artifacts to hash for the cache signature (e.g., config files).

        tags : Optional[List[str]], optional
            Labels for filtering and organizing runs (e.g., ["production", "baseline"]).
        facet : Optional[FacetLike], optional
            Queryable metadata facets (small config values) logged to the run.
        facet_from : Optional[List[str]], optional
            List of config keys to extract and log as facets.
        facet_schema_version : Optional[Union[str, int]], optional
            Schema version for facet compatibility tracking.
        facet_index : Optional[bool], optional
            Whether to index facets for faster queries.

        hash_inputs : Optional[HashInputs], optional
            Strategy for hashing inputs: "fast" (mtime), "full" (content), or None (auto-detect).

        year : Optional[int], optional
            Year metadata (for multi-year simulations). Included in provenance.
        iteration : Optional[int], optional
            Iteration count (for iterative workflows). Included in provenance.
        parent_run_id : Optional[str], optional
            Parent run ID (for nested runs in scenarios).

        outputs : Optional[List[str]], optional
            Names of output artifacts to log (for executor='python' with auto-loaded DataFrames).
            Maps artifact key to ingested table name.
        output_paths : Optional[Mapping[str, ArtifactRef]], optional
            Output file paths to log. Dict maps artifact keys to host paths or Artifact refs.
        capture_dir : Optional[Path], optional
            Directory to scan for outputs (legacy tools that write to specific dirs).
        capture_pattern : str, default "*"
            Glob pattern for capturing outputs (used with capture_dir).

        cache_mode : str, default "reuse"
            Cache behavior: "reuse" (return cache hit), "overwrite" (always re-execute), or "skip_check".
        cache_hydration : Optional[str], optional
            Materialization strategy for cache hits:
            - "outputs-requested": Copy only output_paths to disk
            - "outputs-all": Copy all cached outputs to run_artifact_dir
            - "inputs-missing": Backfill missing inputs from prior runs before executing
        validate_cached_outputs : str, default "lazy"
            Validation for cached outputs: "lazy" (check if files exist), "strict", or "none".

        load_inputs : Optional[bool], optional
            Whether to auto-load input artifacts into function parameters.
            Defaults to True if inputs is a dict, False if a list.
        executor : str, default "python"
            Execution backend: "python" (call fn directly) or "container" (use Docker/Singularity).
        container : Optional[Mapping[str, Any]], optional
            Container spec (required if executor='container'). Must contain 'image' and 'command'.
        runtime_kwargs : Optional[Dict[str, Any]], optional
            Additional kwargs to pass to fn at runtime (merged with auto-loaded inputs).
            These values are not part of the cache signature; use them for handles
            or runtime-only dependencies. Consider `consist.require_runtime_kwargs`
            to enforce required keys.
        inject_context : bool | str, optional
            If True or a parameter name, inject a RunContext as that parameter (for file I/O, output logging).

        output_mismatch : str, default "warn"
            Behavior when output count doesn't match: "warn", "error", or "ignore".
        output_missing : str, default "warn"
            Behavior when expected outputs are missing: "warn", "error", or "ignore".

        Returns
        -------
        RunResult
            Contains:
            - `outputs`: Dict[str, Artifact] of logged output artifacts
            - `cache_hit`: bool indicating if this was a cache hit
            - `run_id`: The run's unique identifier

        Raises
        ------
        ValueError
            If fn is None (for executor='python'), or if container/output_paths not provided for executor='container'.
        RuntimeError
            If the function execution fails or container execution returns non-zero code.

        Examples
        --------
        Simple data processing:

        >>> def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
        ...     return raw[raw['value'] > 0.5]
        >>>
        >>> result = tracker.run(
        ...     fn=clean_data,
        ...     inputs={"raw": Path("raw.csv")},
        ...     outputs=["cleaned"],
        ... )

        With config for cache distinction:

        >>> result = tracker.run(
        ...     fn=clean_data,
        ...     inputs={"raw": Path("raw.csv")},
        ...     config={"threshold": 0.5},
        ...     outputs=["cleaned"],
        ... )

        See Also
        --------
        start_run : Manual run context management (more control)
        trace : Context manager alternative (always executes, even on cache hit)
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
            resolved_name = name or getattr(fn, "__name__", None)
            if resolved_name is None:
                raise ValueError("executor='container' requires a run name.")
        else:
            resolved_name = name or getattr(fn, "__name__", None)
            if resolved_name is None:
                raise ValueError("Tracker.run requires a run name.")
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

        config_for_run = config
        if config_plan is not None:
            if config is None:
                config_for_run = {}
            elif isinstance(config, BaseModel):
                config_for_run = config.model_dump()
            else:
                config_for_run = dict(config)
            if "__consist_config_plan__" in config_for_run:
                logging.warning(
                    "[Consist] Overwriting user-provided '__consist_config_plan__' in config for run %s.",
                    run_id,
                )
            config_for_run["__consist_config_plan__"] = {
                "adapter": config_plan.adapter_name,
                "hash": config_plan.identity_hash,
                "adapter_version": config_plan.adapter_version,
            }

        start_kwargs: Dict[str, Any] = {
            "run_id": run_id,
            "model": resolved_model,
            "config": config_for_run,
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

            if config_plan is not None:
                self.apply_config_plan(
                    config_plan,
                    run=current_consist.run,
                    ingest=config_plan_ingest,
                    profile_schema=config_plan_profile_schema,
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

            runtime_kwargs = dict(runtime_kwargs or {})
            required_runtime = getattr(fn, "__consist_runtime_required__", ())
            if required_runtime:
                missing = [
                    name for name in required_runtime if name not in runtime_kwargs
                ]
                if missing:
                    missing_list = ", ".join(sorted(missing))
                    raise ValueError(
                        f"Missing runtime_kwargs for {resolved_name!r}: {missing_list}. "
                        "Provide them via runtime_kwargs={...} or remove "
                        "@consist.require_runtime_kwargs."
                    )
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

            if (
                "config" in params
                and "config" not in runtime_kwargs
                and config is not None
            ):
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
                if param_name in runtime_kwargs:
                    call_kwargs[param_name] = runtime_kwargs[param_name]
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

            for key, value in runtime_kwargs.items():
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
        config_plan: Optional[ConfigPlan] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
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
        Context manager for inline tracing of a run with inline execution.

        This context manager allows you to define a run directly within a `with` block,
        with the Python code inside executing every time (even on cache hits). This differs
        from `tracker.run()`, which skips execution on cache hits.

        Use `trace()` when you need inline control: for data loading, file I/O, or
        integrations that require code execution regardless of cache state.

        Parameters
        ----------
        name : str
            Human-readable name for the run. Also defaults the model name if not provided.
        run_id : Optional[str], optional
            Unique identifier for this run. Auto-generated if not provided.
        model : Optional[str], optional
            Model/component name for categorizing runs. Defaults to the run name.
        description : Optional[str], optional
            Human-readable description of the run.
        config : Optional[Dict[str, Any]], optional
            Configuration parameters. Becomes part of the cache signature. Can be a dict or Pydantic model.
        config_plan : Optional[ConfigPlan], optional
            Precomputed config plan (e.g., from ActivitySim adapter). The plan's identity hash
            is folded into the run config hash and its artifacts/ingestables are applied on cache miss.
        config_plan_ingest : bool, default True
            Whether to ingest tables from the config plan.
        config_plan_profile_schema : bool, default False
            Whether to profile ingested schemas for the config plan.

        inputs : Optional[Mapping[str, ArtifactRef] | Iterable[ArtifactRef]], optional
            Input files or artifacts.
            - Dict: Maps names to paths/Artifacts. Logged as inputs but not auto-loaded.
            - List/Iterable: Hashed for cache key but not auto-loaded.
        input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        optional_input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        depends_on : Optional[List[ArtifactRef]], optional
            Additional file paths or artifacts to hash for the cache signature (e.g., config files).

        tags : Optional[List[str]], optional
            Labels for filtering and organizing runs (e.g., ["production", "baseline"]).
        facet : Optional[FacetLike], optional
            Queryable metadata facets (small config values) logged to the run.
        facet_from : Optional[List[str]], optional
            List of config keys to extract and log as facets.
        facet_schema_version : Optional[Union[str, int]], optional
            Schema version for facet compatibility tracking.
        facet_index : Optional[bool], optional
            Whether to index facets for faster queries.

        hash_inputs : Optional[HashInputs], optional
            Strategy for hashing inputs: "fast" (mtime), "full" (content), or None (auto-detect).

        year : Optional[int], optional
            Year metadata (for multi-year simulations). Included in provenance.
        iteration : Optional[int], optional
            Iteration count (for iterative workflows). Included in provenance.
        parent_run_id : Optional[str], optional
            Parent run ID (for nested runs in scenarios).

        outputs : Optional[List[str]], optional
            Names of output artifacts to log. Each item is a key name for logged outputs.
        output_paths : Optional[Mapping[str, ArtifactRef]], optional
            Output file paths to log. Dict maps artifact keys to host paths or Artifact refs.
        capture_dir : Optional[Path], optional
            Directory to scan for outputs. New/modified files are auto-logged.
        capture_pattern : str, default "*"
            Glob pattern for capturing outputs (used with capture_dir).

        cache_mode : str, default "reuse"
            Cache behavior: "reuse" (return cache hit), "overwrite" (always re-execute), or "skip_check".
        cache_hydration : Optional[str], optional
            Materialization strategy for cache hits:
            - "outputs-requested": Copy only output_paths to disk
            - "outputs-all": Copy all cached outputs to run_artifact_dir
            - "inputs-missing": Backfill missing inputs from prior runs before executing
        validate_cached_outputs : str, default "lazy"
            Validation for cached outputs: "lazy" (check if files exist), "strict", or "none".

        output_mismatch : str, default "warn"
            Behavior when output count doesn't match: "warn", "error", or "ignore".
        output_missing : str, default "warn"
            Behavior when expected outputs are missing: "warn", "error", or "ignore".

        Yields
        ------
        Tracker
            The current `Tracker` instance for use within the `with` block.

        Raises
        ------
        ValueError
            If output_mismatch or output_missing are invalid values.
        RuntimeError
            If output validation fails based on validation settings.

        Notes
        -----
        Unlike `tracker.run()`, the Python code inside a `trace()` block ALWAYS executes,
        even on cache hits. This is useful for side effects, data loading, or code that
        should run regardless of cache state.

        If you want to skip execution on cache hits (like `tracker.run()`), consider using
        `tracker.run()` with a callable instead.

        Examples
        --------
        Simple inline tracing with file capture:

        >>> with tracker.trace(
        ...     "my_analysis",
        ...     output_paths={"results": "./results.csv"}
        ... ):
        ...     df = pd.read_csv("raw.csv")
        ...     df["value"] = df["value"] * 2
        ...     df.to_csv("./results.csv", index=False)

        Multi-year simulation:

        >>> with tracker.scenario("baseline") as sc:
        ...     for year in [2020, 2030, 2040]:
        ...         with sc.trace(name="simulate", year=year):
        ...             results = run_model(year)
        ...             tracker.log_artifact(results, key="output")

        See Also
        --------
        run : Function-shaped alternative (skips on cache hit)
        scenario : Multi-step workflow grouping
        start_run : Imperative alternative for run lifecycle management
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

        config_for_run = config
        if config_plan is not None:
            if config is None:
                config_for_run = {}
            elif isinstance(config, BaseModel):
                config_for_run = config.model_dump()
            else:
                config_for_run = dict(config)
            if "__consist_config_plan__" in config_for_run:
                logging.warning(
                    "[Consist] Overwriting user-provided '__consist_config_plan__' in config for run %s.",
                    run_id,
                )
            config_for_run["__consist_config_plan__"] = {
                "adapter": config_plan.adapter_name,
                "hash": config_plan.identity_hash,
                "adapter_version": config_plan.adapter_version,
            }

        start_kwargs: Dict[str, Any] = {
            "run_id": run_id,
            "model": resolved_model,
            "config": config_for_run,
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
                if config_plan is not None and not t.is_cached:
                    self.apply_config_plan(
                        config_plan,
                        run=current_consist.run,
                        ingest=config_plan_ingest,
                        profile_schema=config_plan_profile_schema,
                    )
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
        coupler: Optional["Coupler"] = None,
        require_outputs: Optional[Iterable[str]] = None,
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
        coupler : Optional[Coupler], optional
            Optional Coupler instance to use for the scenario.
        require_outputs : Optional[Iterable[str]], optional
            Declare required outputs at scenario creation time.
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
            coupler=coupler,
            require_outputs=require_outputs,
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
            if cache_key in self._local_cache_index and cache_mode != "overwrite":
                logging.warning(
                    "Cache key collision detected (extremely rare): %s. "
                    "Keeping first cached run (created %s).",
                    cache_key,
                    self._local_cache_index[cache_key].created_at,
                )
            else:
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

        Parameters
        ----------
        parent_id : Optional[str], optional
            Filter by scenario/parent run ID.
        model : Optional[str], optional
            Filter by model name.
        status : Optional[str], optional
            Filter by run status.
        year : Optional[int], optional
            Filter by run year.
        tags : Optional[List[str]], optional
            Filter runs that contain all provided tags.
        metadata : Optional[Dict[str, Any]], optional
            Filter by exact matches in ``Run.meta`` (client-side filter).
        limit : int, default 10_000
            Maximum number of runs to consider.
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

        Parameters
        ----------
        **kwargs : Any
            Filters forwarded to ``find_latest_run``.
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
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        profile_file_schema: bool = False,
        file_schema_sample_rows: Optional[int] = 1000,
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
        content_hash : Optional[str], optional
            Precomputed content hash to use for the artifact instead of hashing
            the path on disk.
        force_hash_override : bool, default False
            If True, overwrite an existing artifact hash when it differs from
            `content_hash`. By default, mismatched overrides are ignored with a warning.
        validate_content_hash : bool, default False
            If True, verify `content_hash` against the on-disk data and raise on mismatch.
        profile_file_schema : bool, default False
            If True, profile a lightweight schema for file-based tabular artifacts.
        file_schema_sample_rows : Optional[int], default 1000
            Maximum rows to sample when profiling file-based schemas.
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
                if self._active_coupler is not None and cached.key:
                    self._active_coupler.set(cached.key, cached)
                return cached

        run_id = self.current_consist.run.id if direction == "output" else None

        # DELEGATE CREATION LOGIC
        artifact_obj = self.artifacts.create_artifact(
            path,
            run_id,
            key,
            direction,
            schema,
            driver,
            content_hash=content_hash,
            force_hash_override=force_hash_override,
            validate_content_hash=validate_content_hash,
            **meta,
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
            if self._active_coupler is not None and artifact_obj.key:
                self._active_coupler.set(artifact_obj.key, artifact_obj)

        self._flush_json()
        self._sync_artifact_to_db(artifact_obj, direction)

        if (
            profile_file_schema
            and artifact_obj.is_tabular
            and self.current_consist is not None
        ):
            try:
                if isinstance(artifact_obj.meta, dict) and artifact_obj.meta.get(
                    "schema_id"
                ):
                    return artifact_obj
                resolved_path = artifact_obj.abs_path or self.resolve_uri(
                    artifact_obj.uri
                )
                if resolved_path:
                    driver = artifact_obj.driver
                    if driver not in ("csv", "parquet"):
                        return artifact_obj
                    self.artifact_schemas.profile_file_artifact(
                        artifact=artifact_obj,
                        run=self.current_consist.run,
                        resolved_path=str(resolved_path),
                        driver=cast(Literal["csv", "parquet"], driver),
                        sample_rows=file_schema_sample_rows,
                        source="file",
                    )
            except FileNotFoundError:
                logging.warning(
                    "[Consist] File schema capture skipped; file not found: %s",
                    artifact_obj.uri,
                )

        # Store user-provided schema if one was given.
        # User-provided schemas (manually curated with FK constraints, indexes, etc.)
        # are stored with source="user_provided" and will be preferred over
        # auto-profiled schemas (file or duckdb) during export.
        # NOTE: Schema persistence requires a configured database. Without a DB,
        # schema_name will not be set in artifact metadata, so registry-based
        # auto-detection in ingest() won't work (but explicit schema parameter will).
        if (
            schema is not None
            and artifact_obj.is_tabular
            and self.current_consist is not None
        ):
            try:
                self.artifact_schemas.profile_user_provided_schema(
                    artifact=artifact_obj,
                    run=self.current_consist.run,
                    schema_model=schema,
                    source="user_provided",
                )
            except Exception as e:
                logging.warning(
                    "[Consist] Failed to store user-provided schema for artifact=%s: %s",
                    getattr(artifact_obj, "key", None),
                    e,
                )

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
        profile_file_schema: bool = False,
        file_schema_sample_rows: Optional[int] = 1000,
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
        profile_file_schema : bool, default False
            If True, profile a lightweight schema for file-based tabular artifacts.
        file_schema_sample_rows : Optional[int], default 1000
            Maximum rows to sample when profiling file-based schemas.
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
            resolved_path,
            key=key,
            direction=direction,
            schema=schema,
            profile_file_schema=profile_file_schema,
            file_schema_sample_rows=file_schema_sample_rows,
            **meta_payload,
        )
        if schema is not None:
            self.ingest(art, df, schema=schema)
        return art

    def log_artifacts(
        self,
        outputs: Mapping[str, ArtifactRef],
        direction: str = "output",
        driver: Optional[str] = None,
        metadata_by_key: Optional[Mapping[str, Dict[str, Any]]] = None,
        **shared_meta: Any,
    ) -> Dict[str, Artifact]:
        """
        Log multiple artifacts in a single call for efficiency.

        This is a convenience method for bulk artifact logging, particularly useful
        when a model produces many output files or when registering multiple inputs.
        This requires an explicit mapping so artifact keys are always deliberate.

        Parameters
        ----------
        outputs : mapping
            Mapping of key -> path/Artifact to log.
        direction : str, default "output"
            Specifies whether the artifacts are "input" or "output" for the current run.
        driver : Optional[str], optional
            Explicitly specify the driver for all artifacts. If None, driver is inferred
            from each file's extension individually.
        metadata_by_key : Optional[Mapping[str, Dict[str, Any]]], optional
            Per-key metadata overrides applied on top of shared metadata.
        **shared_meta : Any
            Metadata key-value pairs to apply to ALL logged artifacts.
            Useful for tagging a batch of related files.

        Returns
        -------
        Dict[str, Artifact]
            Mapping of key -> logged Artifact.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ValueError
            If metadata_by_key contains keys not present in outputs.
        TypeError
            If mapping keys are not strings.

        Example
        -------
        ```python
        # Log explicit outputs
        outputs = tracker.log_artifacts(
            {"persons": "output/persons.parquet", "households": "output/households.parquet"},
            metadata_by_key={"households": {"role": "primary"}},
            year=2030,
        )
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifacts outside of a run context.")

        if not isinstance(outputs, MappingABC):
            raise TypeError("log_artifacts requires a mapping of key -> artifact.")

        if metadata_by_key:
            extra_keys = set(metadata_by_key).difference(outputs)
            if extra_keys:
                extras = ", ".join(sorted(str(key) for key in extra_keys))
                raise ValueError(
                    f"metadata_by_key contains keys not present in outputs: {extras}"
                )

        keys = list(outputs.keys())
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("log_artifacts keys must be strings.")

        base_meta = dict(shared_meta)
        logged: Dict[str, Artifact] = {}
        for key in sorted(keys):
            value = outputs[key]
            if value is None:
                raise ValueError(f"log_artifacts received None for key {key!r}.")
            meta = dict(base_meta)
            if metadata_by_key and key in metadata_by_key:
                meta.update(metadata_by_key[key])
            logged[key] = self.log_artifact(
                value, key=key, direction=direction, driver=driver, **meta
            )
        return logged

    def log_input(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
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
        content_hash : Optional[str], optional
            Precomputed content hash to use for the artifact instead of hashing
            the path on disk.
        force_hash_override : bool, default False
            If True, overwrite an existing artifact hash when it differs from
            `content_hash`. By default, mismatched overrides are ignored with a warning.
        validate_content_hash : bool, default False
            If True, verify `content_hash` against the on-disk data and raise on mismatch.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(
            path,
            key=key,
            direction="input",
            content_hash=content_hash,
            force_hash_override=force_hash_override,
            validate_content_hash=validate_content_hash,
            **meta,
        )

    def log_output(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
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
        content_hash : Optional[str], optional
            Precomputed content hash to use for the artifact instead of hashing
            the path on disk.
        force_hash_override : bool, default False
            If True, overwrite an existing artifact hash when it differs from
            `content_hash`. By default, mismatched overrides are ignored with a warning.
        validate_content_hash : bool, default False
            If True, verify `content_hash` against the on-disk data and raise on mismatch.
        **meta : Any
            Additional key-value pairs to store in the artifact's `meta` field.

        Returns
        -------
        Artifact
            The created or updated `Artifact` object.
        """
        return self.log_artifact(
            path,
            key=key,
            direction="output",
            content_hash=content_hash,
            force_hash_override=force_hash_override,
            validate_content_hash=validate_content_hash,
            **meta,
        )

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

    def log_netcdf_file(
        self,
        path: Union[str, Path],
        key: Optional[str] = None,
        direction: str = "output",
        **meta: Any,
    ) -> Artifact:
        """
        Log a NetCDF file as an artifact with metadata extraction.

        This method provides convenient logging for NetCDF files, automatically
        detecting the driver and storing structural metadata about variables,
        dimensions, and coordinates.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the NetCDF file.
        key : Optional[str], optional
            Semantic name for the artifact. If not provided, uses the file stem.
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        **meta : Any
            Additional metadata for the artifact.

        Returns
        -------
        Artifact
            The logged artifact with metadata extracted from the NetCDF structure.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ImportError
            If xarray is not installed.

        Example
        -------
        ```python
        # Log NetCDF file
        art = tracker.log_netcdf_file("climate_data.nc", key="temperature")
        # Optionally ingest metadata
        tracker.ingest(art)
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem

        artifact = self.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="netcdf",
            **meta,
        )
        return artifact

    def log_openmatrix_file(
        self,
        path: Union[str, Path],
        key: Optional[str] = None,
        direction: str = "output",
        **meta: Any,
    ) -> Artifact:
        """
        Log an OpenMatrix (OMX) file as an artifact with metadata extraction.

        This method provides convenient logging for OpenMatrix files, automatically
        detecting the driver and storing structural metadata about matrices,
        dimensions, and attributes.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the OpenMatrix file.
        key : Optional[str], optional
            Semantic name for the artifact. If not provided, uses the file stem.
        direction : str, default "output"
            Whether this is an "input" or "output" artifact.
        **meta : Any
            Additional metadata for the artifact.

        Returns
        -------
        Artifact
            The logged artifact with metadata extracted from the OpenMatrix structure.

        Raises
        ------
        RuntimeError
            If called outside an active run context.
        ImportError
            If neither h5py nor openmatrix is installed.

        Example
        -------
        ```python
        # Log OpenMatrix file (e.g., ActivitySim travel demand)
        art = tracker.log_openmatrix_file("demand.omx", key="travel_demand")
        # Optionally ingest metadata
        tracker.ingest(art)
        ```
        """
        if not self.current_consist:
            raise RuntimeError("Cannot log artifact outside of a run context.")

        path_obj = Path(path)
        if key is None:
            key = path_obj.stem

        artifact = self.log_artifact(
            str(path_obj),
            key=key,
            direction=direction,
            driver="openmatrix",
            **meta,
        )
        return artifact

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
            The artifact object representing the data being ingested. If the artifact
            was logged with a schema (e.g., ``log_artifact(path, schema=MySchema)``)
            and that schema was registered with the Tracker at initialization
            (e.g., ``Tracker(..., schemas=[MySchema])``), it will be automatically
            looked up and used for ingestion.
        data : Optional[Union[Iterable[Dict[str, Any]], Any]], optional
            An iterable (e.g., list of dicts, generator) where each item represents a
            row of data to be ingested. If `data` is omitted, Consist attempts to
            stream it directly from the artifact's file URI, resolving the path.
            Can also be other data types that `dlt` can handle directly (e.g., Pandas DataFrame).
        schema : Optional[Type[SQLModel]], optional
            An optional SQLModel class that defines the expected schema for the ingested data.
            If provided, `dlt` will use this for strict validation and this parameter
            takes precedence over any auto-detected schema. If not provided, Consist will
            automatically look up the schema by name from schemas registered in Tracker.__init__
            (using artifact.meta["schema_name"]).
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

        Examples
        --------
        Auto-detected schema workflow (register schemas at tracker init):

        >>> tracker = Tracker(..., schemas=[MyDataSchema])
        >>> art = tracker.log_artifact(file.csv, schema=MyDataSchema)
        >>> tracker.ingest(art, data=df)  # Automatically looks up and uses MyDataSchema

        Cross-session workflow (schemas persist in metadata):

        >>> # Session 1:
        >>> tracker = Tracker(..., schemas=[MyDataSchema])
        >>> art = tracker.log_artifact(file.csv, schema=MyDataSchema)
        >>> # Session 2:
        >>> tracker2 = Tracker(..., schemas=[MyDataSchema])
        >>> art2 = tracker2.get_artifact("mydata")
        >>> tracker2.ingest(art2, data=df)  # Looks up MyDataSchema by artifact's schema_name

        Override auto-detection (explicit schema always wins):

        >>> tracker.ingest(art, data=df, schema=DifferentSchema)  # Uses DifferentSchema
        """
        self._ensure_write_data()

        # Auto-detect schema from artifact if not explicitly provided.
        # Look up the schema by name in the tracker's registered schemas
        # (schemas passed to Tracker.__init__).
        resolved_schema = schema
        if resolved_schema is None:
            schema_name = (
                artifact.meta.get("schema_name")
                if hasattr(artifact, "meta") and isinstance(artifact.meta, dict)
                else None
            )
            if schema_name and schema_name in self._registered_schemas:
                resolved_schema = self._registered_schemas[schema_name]
                logging.debug(
                    "[Consist] Resolved schema '%s' for artifact=%s from registered schemas",
                    schema_name,
                    getattr(artifact, "key", None),
                )

        return ingest_artifact(
            tracker=self,
            artifact=artifact,
            data=data,
            schema=resolved_schema,
            run=run,
            profile_schema=profile_schema,
        )

    def _adapter_accepts_options(
        self, adapter: ConfigAdapter, method_name: str
    ) -> bool:
        method = getattr(adapter, method_name, None)
        if method is None:
            return False
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return False
        if "options" in signature.parameters:
            return True
        return any(
            param.kind == param.VAR_KEYWORD for param in signature.parameters.values()
        )

    def _discover_config(
        self,
        adapter: ConfigAdapter,
        config_dir_paths: list[Path],
        strict: bool,
        options: Optional[ConfigAdapterOptions],
    ) -> CanonicalConfig:
        kwargs: dict[str, Any] = {}
        if options is not None and self._adapter_accepts_options(adapter, "discover"):
            kwargs["options"] = options
        return adapter.discover(
            config_dir_paths, identity=self.identity, strict=strict, **kwargs
        )

    def _canonicalize_config(
        self,
        adapter: ConfigAdapter,
        canonical: CanonicalConfig,
        *,
        run: Optional[Run],
        tracker: Optional["Tracker"],
        strict: bool,
        plan_only: bool,
        options: Optional[ConfigAdapterOptions],
    ) -> CanonicalizationResult:
        kwargs: dict[str, Any] = {}
        if options is not None and self._adapter_accepts_options(
            adapter, "canonicalize"
        ):
            kwargs["options"] = options
        return adapter.canonicalize(
            canonical,
            run=run,
            tracker=tracker,
            strict=strict,
            plan_only=plan_only,
            **kwargs,
        )

    def canonicalize_config(
        self,
        adapter: ConfigAdapter,
        config_dirs: Iterable[Union[str, Path]],
        *,
        run: Optional[Run] = None,
        run_id: Optional[str] = None,
        strict: bool = False,
        ingest: bool = True,
        profile_schema: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> ConfigContribution:
        """
        Canonicalize a model-specific config directory and ingest queryable slices.

        Parameters
        ----------
        adapter : ConfigAdapter
            Adapter implementation for the model (e.g., ActivitySim).
        config_dirs : Iterable[Union[str, Path]]
            Ordered config directories to canonicalize.
        run : Optional[Run], optional
            Run context to attach to; defaults to the active run.
        run_id : Optional[str], optional
            Run identifier; must match the active run when provided.
        strict : bool, default False
            If True, adapter should error on missing references.
        ingest : bool, default True
            Whether to ingest any queryable tables produced by the adapter.
        profile_schema : bool, default False
            Whether to profile ingested schemas.
        options : Optional[ConfigAdapterOptions], optional
            Shared adapter options that override strict/ingest defaults.

        Returns
        -------
        ConfigContribution
            Structured summary of logged artifacts and ingestables.
        """
        if run is not None and run_id is not None:
            raise ValueError("Provide either run= or run_id=, not both.")
        if options is not None and (strict is not False or ingest is not True):
            raise ValueError(
                "When options= is provided, do not pass strict= or ingest=."
            )
        if options is not None:
            strict = options.strict
            ingest = options.ingest

        target_run = run
        if target_run is None and run_id is not None:
            if self.current_consist and self.current_consist.run.id == run_id:
                target_run = self.current_consist.run
            else:
                raise RuntimeError(
                    "canonicalize_config requires an active run matching run_id=."
                )
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            raise RuntimeError("canonicalize_config requires an active run or run=.")

        config_dir_paths = [Path(p).resolve() for p in config_dirs]
        canonical = self._discover_config(adapter, config_dir_paths, strict, options)
        result = self._canonicalize_config(
            adapter,
            canonical,
            run=target_run,
            tracker=self,
            strict=strict,
            plan_only=False,
            options=options,
        )

        contribution = ConfigContribution(
            identity_hash=canonical.content_hash,
            adapter_version=getattr(adapter, "adapter_version", None),
            artifacts=result.artifacts,
            ingestables=result.ingestables,
            meta={
                "adapter": adapter.model_name,
                "config_dirs": [str(p) for p in config_dir_paths],
            },
        )

        self._apply_config_contribution(
            contribution,
            run=target_run,
            ingest=ingest,
            profile_schema=profile_schema,
        )

        if target_run.meta is None:
            target_run.meta = {}
        target_run.meta["config_bundle_hash"] = canonical.content_hash
        target_run.meta["config_adapter"] = adapter.model_name
        if contribution.adapter_version is not None:
            target_run.meta["config_adapter_version"] = contribution.adapter_version

        return contribution

    def prepare_config(
        self,
        adapter: ConfigAdapter,
        config_dirs: Iterable[Union[str, Path]],
        *,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
        validate_only: bool = False,
        facet_spec: Optional[Dict[str, Any]] = None,
        facet_schema_name: Optional[str] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
    ) -> ConfigPlan:
        """
        Prepare a config plan without logging artifacts or ingesting data.

        Parameters
        ----------
        adapter : ConfigAdapter
            Adapter implementation for the model (e.g., ActivitySim).
        config_dirs : Iterable[Union[str, Path]]
            Ordered config directories to canonicalize.
        strict : bool, default False
            If True, adapter should error on missing references.
        options : Optional[ConfigAdapterOptions], optional
            Shared adapter options that override strict defaults.
        validate_only : bool, default False
            If True, validate ingestables without logging or ingesting.
        facet_spec : Optional[Dict[str, Any]], optional
            Adapter-specific facet extraction spec.
        facet_schema_name : Optional[str], optional
            Optional facet schema name for persistence.
        facet_schema_version : Optional[Union[str, int]], optional
            Optional facet schema version for persistence.
        facet_index : Optional[bool], optional
            Optional flag controlling KV facet indexing.

        Returns
        -------
        ConfigPlan
            Pre-run config plan containing artifacts and ingestables.
        """
        if options is not None and strict is not False:
            raise ValueError("When options= is provided, do not pass strict=.")
        if options is not None:
            strict = options.strict
        config_dir_paths = [Path(p).resolve() for p in config_dirs]
        canonical = self._discover_config(adapter, config_dir_paths, strict, options)
        result = self._canonicalize_config(
            adapter,
            canonical,
            run=None,
            tracker=None,
            strict=strict,
            plan_only=True,
            options=options,
        )
        facet_data = None
        if facet_spec is not None:
            if hasattr(adapter, "build_facet"):
                facet_data = adapter.build_facet(canonical, facet_spec=facet_spec)
                if facet_data is not None:
                    facet_data = self.identity.normalize_json(facet_data)
            else:
                raise ValueError(
                    "facet_spec provided but adapter does not support build_facet()."
                )

        plan = ConfigPlan(
            adapter_name=adapter.model_name,
            adapter_version=getattr(adapter, "adapter_version", None),
            canonical=canonical,
            artifacts=result.artifacts,
            ingestables=result.ingestables,
            facet=facet_data,
            facet_schema_name=facet_schema_name,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            meta={
                "adapter": adapter.model_name,
                "config_dirs": [str(p) for p in config_dir_paths],
            },
            adapter=adapter,
        )
        if validate_only:
            diagnostics = validate_config_plan(plan)
            return replace(plan, diagnostics=diagnostics)
        return plan

    def apply_config_plan(
        self,
        plan: ConfigPlan,
        *,
        run: Optional[Run] = None,
        ingest: bool = True,
        profile_schema: bool = False,
        adapter: Optional[ConfigAdapter] = None,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> ConfigContribution:
        """
        Apply a pre-run config plan to the active run.

        Parameters
        ----------
        plan : ConfigPlan
            Plan produced by `prepare_config`.
        run : Optional[Run], optional
            Run context to attach to; defaults to the active run.
        ingest : bool, default True
            Whether to ingest any queryable tables produced by the adapter.
        profile_schema : bool, default False
            Whether to profile ingested schemas.
        adapter : Optional[ConfigAdapter], optional
            Adapter instance used to create run-scoped artifacts, if needed.
        options : Optional[ConfigAdapterOptions], optional
            Shared adapter options that override ingest defaults.

        Returns
        -------
        ConfigContribution
            Structured summary of logged artifacts and ingestables.
        """
        if options is not None and ingest is not True:
            raise ValueError("When options= is provided, do not pass ingest=.")
        if options is not None:
            ingest = options.ingest

        target_run = run
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            raise RuntimeError("apply_config_plan requires an active run or run=.")

        artifacts = list(plan.artifacts)
        adapter_ref = adapter or plan.adapter
        if adapter_ref is not None and (options is None or options.bundle):
            bundle_artifact = getattr(adapter_ref, "bundle_artifact", None)
            if callable(bundle_artifact):
                bundle_spec = bundle_artifact(
                    plan.canonical, run=target_run, tracker=self
                )
                if bundle_spec is not None:
                    artifacts.append(bundle_spec)

        contribution = ConfigContribution(
            identity_hash=plan.identity_hash,
            adapter_version=plan.adapter_version,
            artifacts=artifacts,
            ingestables=plan.ingestables,
            facet=plan.facet,
            facet_schema_name=plan.facet_schema_name,
            facet_schema_version=plan.facet_schema_version,
            meta=dict(plan.meta or {}),
        )

        self._apply_config_contribution(
            contribution,
            run=target_run,
            ingest=ingest,
            profile_schema=profile_schema,
        )

        if target_run.meta is None:
            target_run.meta = {}
        target_run.meta["config_bundle_hash"] = plan.identity_hash
        target_run.meta["config_adapter"] = plan.adapter_name
        if plan.adapter_version is not None:
            target_run.meta["config_adapter_version"] = plan.adapter_version

        if plan.facet is not None:
            facet_dict = plan.facet
            if self.current_consist is not None:
                self.current_consist.facet = facet_dict
            schema_name = (
                plan.facet_schema_name
                or self.config_facets.infer_schema_name(None, facet_dict)
            )
            self.config_facets.persist_facet(
                run=target_run,
                model=target_run.model_name,
                facet_dict=facet_dict,
                schema_name=schema_name,
                schema_version=plan.facet_schema_version,
                index_kv=plan.facet_index if plan.facet_index is not None else True,
            )

        return contribution

    def identity_from_config_plan(self, plan: ConfigPlan) -> str:
        """
        Return the identity hash derived from a config plan.

        Parameters
        ----------
        plan : ConfigPlan
            Config plan produced by `prepare_config`.

        Returns
        -------
        str
            Stable hash representing the canonical config content.
        """
        return plan.identity_hash

    def _apply_config_contribution(
        self,
        contribution: ConfigContribution,
        *,
        run: Run,
        ingest: bool,
        profile_schema: bool,
    ) -> Dict[str, Artifact]:
        artifacts_by_key: Dict[str, Artifact] = {}
        for spec in contribution.artifacts:
            art = self.log_artifact(
                spec.path,
                key=spec.key,
                direction=spec.direction,
                **spec.meta,
            )
            artifacts_by_key[spec.key] = art

        if ingest:
            for spec in contribution.ingestables:
                source_key = spec.source
                artifact = (
                    artifacts_by_key.get(source_key)
                    if source_key
                    else next(iter(artifacts_by_key.values()), None)
                )
                if artifact is None:
                    logging.warning(
                        "[Consist] Skipping ingest for %s; no source artifact found.",
                        spec.table_name,
                    )
                    continue
                if spec.rows is None:
                    logging.warning(
                        "[Consist] Skipping ingest for %s; no rows provided.",
                        spec.table_name,
                    )
                    continue
                if spec.dedupe_on_hash and spec.content_hash:
                    if self._ingest_cache_hit(spec.table_name, spec.content_hash):
                        logging.info(
                            "[Consist] Skipping ingest for %s; cache hit for %s.",
                            spec.table_name,
                            spec.content_hash,
                        )
                        continue
                if source_key is None:
                    logging.warning(
                        "[Consist] Ingest spec for %s missing source; using %s.",
                        spec.table_name,
                        artifact.key,
                    )
                rows = spec.rows(run.id) if callable(spec.rows) else spec.rows
                self.ingest(
                    artifact,
                    data=rows,
                    schema=spec.schema,
                    run=run,
                    profile_schema=profile_schema,
                )

        return artifacts_by_key

    def _ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        if self.engine is None:
            return False
        try:
            with self.engine.begin() as connection:
                result = connection.exec_driver_sql(
                    f"SELECT 1 FROM global_tables.{table_name} "
                    "WHERE content_hash = ? LIMIT 1",
                    (content_hash,),
                ).fetchone()
            return result is not None
        except Exception:
            return False

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

        Parameters
        ----------
        concept_key : str
            Semantic key for the matrix artifacts.
        variables : Optional[List[str]], optional
            Variables to load from each Zarr store; defaults to all variables.
        run_ids : Optional[List[str]], optional
            Restrict to specific run IDs.
        parent_id : Optional[str], optional
            Filter by scenario/parent run ID.
        model : Optional[str], optional
            Filter by model name.
        status : Optional[str], optional
            Filter by run status.

        Returns
        -------
        Any
            An ``xarray.Dataset`` containing the combined matrix data.
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

    def netcdf_metadata(self, concept_key: str) -> NetCdfMetadataView:
        """
        Access NetCDF metadata views for a given artifact key.

        This provides convenient access to query and explore NetCDF file structures
        stored in Consist's metadata catalog.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the NetCDF artifact.

        Returns
        -------
        NetCdfMetadataView
            A view object with methods to explore variables, dimensions, and attributes.

        Example
        -------
        ```python
        view = tracker.netcdf_metadata("climate")
        variables = view.get_variables(year=2024)
        print(view.summary("climate"))
        ```
        """
        return NetCdfMetadataView(self)

    def openmatrix_metadata(self, concept_key: str) -> OpenMatrixMetadataView:
        """
        Access OpenMatrix metadata views for a given artifact key.

        This provides convenient access to query and explore OpenMatrix file structures
        stored in Consist's metadata catalog.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the OpenMatrix artifact.

        Returns
        -------
        OpenMatrixMetadataView
            A view object with methods to explore matrices, zones, and attributes.

        Example
        -------
        ```python
        view = tracker.openmatrix_metadata("demand")
        matrices = view.get_matrices(year=2024)
        zones = view.get_zone_counts()
        print(view.summary("demand"))
        ```
        """
        return OpenMatrixMetadataView(self)

    def materialize(
        self,
        artifact: Artifact,
        destination_path: Union[str, Path],
        *,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[str]:
        """
        Materialize a cached artifact onto the filesystem.

        This copies bytes from the resolved artifact URI to ``destination_path``.
        It does not perform database-backed reconstruction.

        Returns
        -------
        Optional[str]
            The destination path for the materialized artifact, or ``None`` if
            missing and ``on_missing="warn"``.
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

    def get_run_record(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[ConsistRecord]:
        """
        Load the full run record snapshot from disk.
        """
        run = self.get_run(run_id) if self.db else None
        snapshot_path = self._resolve_run_snapshot_path(run_id, run)
        if not snapshot_path.exists():
            if allow_missing:
                return None
            raise FileNotFoundError(
                f"Run snapshot not found at {snapshot_path!s} for run_id={run_id}."
            )
        try:
            return ConsistRecord.model_validate_json(
                snapshot_path.read_text(encoding="utf-8")
            )
        except Exception as exc:
            if allow_missing:
                return None
            raise ValueError(
                f"Failed to parse run snapshot at {snapshot_path!s} for run_id={run_id}."
            ) from exc

    def get_run_config(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Load the full config snapshot for a historical run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        allow_missing : bool, default False
            Return ``None`` if the snapshot is missing instead of raising.

        Returns
        -------
        Optional[Dict[str, Any]]
            The stored config payload, or ``None`` if missing and ``allow_missing``.
        """
        record = self.get_run_record(run_id, allow_missing=allow_missing)
        if record is None:
            return None
        return record.config

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

        Returns an empty dict if the database is not configured or the run is
        unknown to the tracker.
        """
        return self.get_artifacts_for_run(run_id).outputs

    def get_run_inputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return input artifacts for a run, keyed by artifact key.

        Returns an empty dict if the database is not configured or the run is
        unknown to the tracker.
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
        show_run_ids: bool = False,
    ) -> None:
        """
        Print a formatted lineage tree for an artifact.

        Parameters
        ----------
        artifact_key_or_id : Union[str, uuid.UUID]
            Artifact key or UUID to print.
        max_depth : Optional[int], optional
            Maximum depth to traverse (0 prints only the artifact).
        show_run_ids : bool, default False
            Include run IDs alongside artifact entries.
        """
        self.lineage.print_lineage(
            artifact_key_or_id=artifact_key_or_id,
            max_depth=max_depth,
            show_run_ids=show_run_ids,
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
            Mounted URIs are validated to prevent path traversal outside the mount root.
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

        This decorator lets you attach defaults such as ``outputs``, ``tags``, or
        ``cache_mode`` to a function. ``Tracker.run`` and ``ScenarioContext.run``
        read this metadata when executing the function.
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
