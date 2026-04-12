import inspect
import itertools
import re
import shutil
from dataclasses import replace
from collections.abc import Mapping as MappingABC
from contextlib import contextmanager
import logging
import os
from types import MappingProxyType
import uuid
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
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    Union,
    cast,
)
from sqlalchemy.sql import Executable

import pandas as pd
from pydantic import BaseModel
from sqlmodel import SQLModel, Session

from consist.core.artifact_schemas import ArtifactSchemaManager
from consist.core.artifact_facets import ArtifactFacetManager
from consist.core.artifacts import ArtifactManager
from consist.core.cache import (
    ActiveRunCacheOptions,
)
from consist.core.run_resolution import (
    is_xarray_dataset as _is_xarray_dataset,
    preview_run_artifact_dir as _preview_run_artifact_dir,
    resolve_input_reference_configured as _resolve_input_reference_configured,
    resolve_input_refs as _resolve_input_refs,
    resolve_output_path as _resolve_output_path,
    write_xarray_dataset as _write_xarray_dataset,
)
from consist.core.tracker_artifact_logging import ArtifactLoggingCoordinator
from consist.core.tracker_artifact_queries import TrackerArtifactQueryService
from consist.core.tracker_lifecycle import RunLifecycleCoordinator
from consist.core.tracker_history import TrackerHistoryService
from consist.core.tracker_orchestration import RunTraceCoordinator, RunTraceHelpers
from consist.core.tracker_recovery import TrackerRecoveryService
from consist.core.tracker_config import TrackerConfig
from consist.core.tracker_config_plans import TrackerConfigPlanService
from consist.core.config_canonicalization import (
    ConfigAdapter,
    ConfigAdapterOptions,
    CanonicalConfig,
    CanonicalizationResult,
    ConfigContribution,
    ConfigPlan,
    SupportsRunWithConfigOverrides,
    validate_config_plan,
)
from consist.core.config_facets import ConfigFacetManager
from consist.core.decorators import define_step as define_step_decorator
from consist.core.events import EventManager
from consist.core.fs import FileSystemManager
from consist.core.identity import IdentityManager
from consist.core.indexing import IndexBySpec
from consist.core.persistence import (
    ArtifactSchemaSelection,
    DatabaseManager,
    ProvenanceWriter,
    SchemaProfileSource,
)
from consist.core.views import ViewFactory, ViewRegistry
from consist.core.ingestion import ingest_artifact
from consist.core.lineage import LineageService
from consist.core.materialize import (
    hydrate_run_outputs as hydrate_run_outputs_core,  # noqa: F401
)
from consist.core.materialize_options import normalize_materialize_output_keys
from consist.core.matrix import MatrixViewFactory
from consist.core.netcdf_views import NetCdfMetadataView
from consist.core.openmatrix_views import OpenMatrixMetadataView
from consist.core.error_messages import format_problem_cause_fix
from consist.core.spatial_views import SpatialMetadataView
from consist.core.queries import RunQueryService
from consist.core.settings import ConsistSettings
from consist.core.stores import HotDataStore, MetadataStore
from consist.core.workflow import OutputCapture, ScenarioContext
from consist.models.artifact import Artifact, set_tracker_ref
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaField
from consist.models.run import (
    ConsistRecord,
    Run,
    RunArtifacts,
    RunResult,
)
from consist.types import (
    ArtifactRef,
    CacheOptions,
    CodeIdentityMode,
    ExecutionOptions,
    FacetLike,
    HashInputs,
    IdentityInputs,
    OutputPolicyOptions,
    RunInputRef,
)

if TYPE_CHECKING:
    from consist.core.coupler import Coupler
    from consist.core.materialize import (
        HydratedRunOutputsResult,
        MaterializationResult,
        StagedInput,
        StagedInputsResult,
    )
    from consist.core.step_context import StepContext
    from consist.runset import RunSet

AccessMode = Literal["standard", "analysis", "read_only"]
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(identifier: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(identifier))


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Tracker:
    """
    The central orchestrator for Consist, managing the lifecycle of a Run and its associated Artifacts.

    The Tracker is responsible for:

    1.  Initiating and managing the state of individual "Runs" (e.g., model executions, data processing steps).

    2.  Logging "Artifacts" (input files, output data, etc.) and their relationships to runs.

    3.  Implementing a **dual-write mechanism**, logging provenance to both
        human-readable JSON files (`consist.json`) and a DuckDB-backed store.
        In this refactor phase, one configured ``db_path`` still points to a
        single local DuckDB file used by both internal stores:
        ``metadata_store`` (runs/artifacts/lineage metadata) and
        ``hot_data_store`` (`global_tables.*` ingest/load surfaces).

    4.  Providing **path virtualization** to make runs portable across different environments,
        as described in the "Path Resolution & Mounts" architectural section.

    5.  Facilitating **smart caching** based on a Merkle DAG strategy, enabling "run forking" and "hydration"
        of previously computed results.
    """

    @classmethod
    def from_config(cls, config: TrackerConfig) -> "Tracker":
        """
        Construct a tracker from a ``TrackerConfig`` object.
        """
        if not isinstance(config, TrackerConfig):
            raise TypeError("config must be a TrackerConfig instance.")
        return cls(**config.to_init_kwargs())

    def __init__(
        self,
        run_dir: Path,
        db_path: Optional[str | os.PathLike[str]] = None,
        mounts: Optional[Dict[str, str]] = None,
        project_root: str = ".",
        hashing_strategy: str = "full",
        cache_epoch: int = 1,
        schemas: Optional[List[Type[SQLModel]]] = None,
        access_mode: AccessMode = "standard",
        run_subdir_fn: Optional[Callable[[Run], str]] = None,
        allow_external_paths: Optional[bool] = None,
        openlineage_enabled: bool = False,
        openlineage_namespace: Optional[str] = None,
    ):
        """
        Orchestrate provenance tracking and intelligent caching for simulation workflows.

        The Tracker serves as the primary entry point for managing the lifecycle of
        scientific runs. It implements a dual-write persistence strategy, recording
        fine-grained lineage to both a portable JSON snapshot and an analytical
        DuckDB database.

        Through path virtualization and Merkle-based identity hashing, the Tracker
        enables computational reproducibility and ensures that redundant simulation
        steps can be safely bypassed via cache hydration.

        Parameters
        ----------
        run_dir : Path
            The root directory for all workflow outputs. Consist will manage
            run-specific subdirectories and JSON provenance logs under this path.
        db_path : Optional[str | os.PathLike[str]], default None
            Filesystem path to the backing DuckDB file. In this refactor phase,
            this single path configures both metadata persistence and hot-data
            ingestion/query storage in single-store mode.
        mounts : Optional[Dict[str, str]], default None
            A mapping of URI schemes (e.g., 'inputs://') to absolute filesystem roots.
            This facilitates environment-independent path resolution and portability.
        project_root : str, default "."
            The root directory used for Git-based code versioning and relative
            path resolution during identity hashing.
        hashing_strategy : str, default "full"
            The method used to compute artifact identity. 'full' performs a
            complete SHA256 content hash, while 'fast' leverages filesystem metadata.
        cache_epoch : int, default 1
            Global cache version for this tracker. Increment to invalidate all
            previously cached runs without modifying code or config.
        schemas : Optional[List[Type[SQLModel]]], default None
            SQLModel definitions to be automatically registered as hybrid views
            within the DuckDB instance for immediate querying. These schemas are
            also registered by class name for runtime lookup via
            ``get_registered_schema(...)``.
        access_mode : AccessMode, default "standard"
            Policy for database interactions. 'standard' allows full writes;
            'analysis' permits ingestion but prevents new run recording;
            'read_only' prohibits all modifications.
        run_subdir_fn : Optional[Callable[[Run], str]], default None
            Custom logic to determine the relative subdirectory for run artifacts.
            Accepts a Run instance and returns a string path.
        allow_external_paths : Optional[bool], default None
            If True, permits artifacts to be logged or materialized outside the
            configured `run_dir`.
        openlineage_enabled : bool, default False
            If True, emits OpenLineage-compliant events to a local JSONL log
            for integration with external metadata catalogs.
        openlineage_namespace : Optional[str], default None
            The namespace identifier for OpenLineage datasets and jobs.
        """
        # 1. Initialize FileSystem Service
        # (This handles the mkdir and path resolution internally now)
        self.fs = FileSystemManager(run_dir, mounts)

        self.mounts = self.fs.mounts
        self.run_dir = self.fs.run_dir

        self.access_mode = access_mode
        self._run_subdir_fn = run_subdir_fn
        self._cache_epoch = cache_epoch
        if allow_external_paths is None:
            allow_external_paths = _env_bool("CONSIST_ALLOW_EXTERNAL_PATHS", False)
        self.allow_external_paths = bool(allow_external_paths)

        configured_db_path = os.fspath(db_path) if db_path is not None else None
        self.metadata_store: MetadataStore | None = None
        self.hot_data_store: HotDataStore | None = None
        self._compat_db: DatabaseManager | None = None
        self._compat_db_path: str | None = configured_db_path

        self.identity = IdentityManager(
            project_root=project_root, hashing_strategy=hashing_strategy
        )
        self.settings = ConsistSettings.from_env()
        self._dlt_lock_retries = self.settings.dlt_lock_retries
        self._dlt_lock_base_sleep_seconds = self.settings.dlt_lock_base_sleep_seconds
        self._dlt_lock_max_sleep_seconds = self.settings.dlt_lock_max_sleep_seconds
        self._db_lock_retries = self.settings.db_lock_retries
        self._db_lock_base_sleep_seconds = self.settings.db_lock_base_sleep_seconds
        self._db_lock_max_sleep_seconds = self.settings.db_lock_max_sleep_seconds

        if configured_db_path:
            metadata_db = DatabaseManager(
                configured_db_path,
                lock_retries=self._db_lock_retries,
                lock_base_sleep_seconds=self._db_lock_base_sleep_seconds,
                lock_max_sleep_seconds=self._db_lock_max_sleep_seconds,
            )
            self.metadata_store = MetadataStore(db=metadata_db)
            self.hot_data_store = HotDataStore(
                db_path=configured_db_path,
                metadata_store=self.metadata_store,
            )

        self.persistence = ProvenanceWriter(self)

        metadata_db = self.metadata_store.db if self.metadata_store else None
        self.artifacts = ArtifactManager(self)
        self.config_facets = ConfigFacetManager(db=metadata_db, identity=self.identity)
        self.artifact_facets = ArtifactFacetManager(
            db=metadata_db, identity=self.identity
        )
        self.artifact_schemas = ArtifactSchemaManager(self)
        self.queries = RunQueryService(self)
        self.lineage = LineageService(self)
        self._run_lifecycle = RunLifecycleCoordinator(self)
        self._artifact_logging = ArtifactLoggingCoordinator(self)
        self._run_trace = RunTraceCoordinator(
            self,
            helpers=RunTraceHelpers(
                resolve_input_refs=_resolve_input_refs,
                preview_run_artifact_dir=_preview_run_artifact_dir,
                resolve_output_path=_resolve_output_path,
                is_xarray_dataset=_is_xarray_dataset,
                write_xarray_dataset=_write_xarray_dataset,
            ),
        )
        self._artifact_queries = TrackerArtifactQueryService(self)
        self._history_service = TrackerHistoryService(self)
        self._recovery_service = TrackerRecoveryService(self)
        self._config_plan_service = TrackerConfigPlanService(self)

        self.views = ViewRegistry(self)
        # Store registered schemas by class name for cross-session lookup.
        # When ingest() is called in a different Python session, it can look up
        # a schema by the artifact's schema_name (e.g., "MyDataSchema") if the
        # tracker was initialized with schemas=[MyDataSchema, ...].
        self._registered_schemas: Dict[str, Type[SQLModel]] = {}
        if schemas:
            if not self.metadata_store:
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
        self._artifact_facet_parsers: list[
            tuple[str, Callable[[str], Optional[FacetLike]]]
        ] = []
        openlineage_emitter = None
        if openlineage_enabled:
            from consist.core.openlineage import OpenLineageEmitter, OpenLineageOptions

            project_root_path = Path(project_root) if project_root else Path.cwd()
            namespace = openlineage_namespace or project_root_path.resolve().name
            schema_resolver = None
            db = metadata_db
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

        # Bind extracted service methods on the instance while keeping the
        # public Tracker API and import path unchanged.
        self.get_artifact = self._artifact_queries.get_artifact
        self.find_artifacts_with_same_content = (
            self._artifact_queries.find_artifacts_with_same_content
        )
        self.find_runs_producing_same_content = (
            self._artifact_queries.find_runs_producing_same_content
        )
        self.select_artifact_schema_for_artifact = (
            self._artifact_queries.select_artifact_schema_for_artifact
        )
        self.get_artifact_by_uri = self._artifact_queries.get_artifact_by_uri
        self.find_artifacts = self._artifact_queries.find_artifacts
        self._parse_artifact_param_value = (
            self._artifact_queries._parse_artifact_param_value
        )
        self._parse_artifact_param_expression = (
            self._artifact_queries._parse_artifact_param_expression
        )
        self.find_artifacts_by_params = self._artifact_queries.find_artifacts_by_params
        self.get_artifact_kv = self._artifact_queries.get_artifact_kv

        self.resolve_historical_path = self._history_service.resolve_historical_path
        self.get_run = self._history_service.get_run
        self.snapshot_db = self._history_service.snapshot_db
        self.get_run_record = self._history_service.get_run_record
        self.get_run_config = self._history_service.get_run_config
        self.get_config_bundle = self._history_service.get_config_bundle
        self.get_artifacts_for_run = self._history_service.get_artifacts_for_run
        self.get_run_outputs = self._history_service.get_run_outputs
        self.get_run_result = self._history_service.get_run_result
        self.get_run_inputs = self._history_service.get_run_inputs
        self.get_run_artifact = self._history_service.get_run_artifact
        self.load_run_output = self._history_service.load_run_output
        self.find_matching_run = self._history_service.find_matching_run
        self.find_recent_completed_runs_for_model = (
            self._history_service.find_recent_completed_runs_for_model
        )
        self.history = self._history_service.history
        self.load_input_bundle = self._history_service.load_input_bundle

        self._adapter_accepts_options = (
            self._config_plan_service._adapter_accepts_options
        )
        self._discover_config = self._config_plan_service._discover_config
        self._canonicalize_config = self._config_plan_service._canonicalize_config
        self.canonicalize_config = self._config_plan_service.canonicalize_config
        self.prepare_config = self._config_plan_service.prepare_config
        self.prepare_config_resolver = self._config_plan_service.prepare_config_resolver
        self.apply_config_plan = self._config_plan_service.apply_config_plan
        self.identity_from_config_plan = (
            self._config_plan_service.identity_from_config_plan
        )
        self._apply_config_contribution = (
            self._config_plan_service._apply_config_contribution
        )
        self._ingest_cache_hit = self._config_plan_service._ingest_cache_hit

    @property
    def db(self) -> DatabaseManager | None:
        """
        Compatibility accessor for the metadata ``DatabaseManager``.

        New code should prefer ``tracker.metadata_store``.
        """
        metadata_store = getattr(self, "metadata_store", None)
        if metadata_store is not None:
            return metadata_store.db
        return getattr(self, "_compat_db", None)

    @db.setter
    def db(self, value: DatabaseManager | None) -> None:
        metadata_store = getattr(self, "metadata_store", None)
        if metadata_store is not None:
            if value is metadata_store.db:
                return
            raise AttributeError(
                "tracker.db is a compatibility accessor in single-store mode and "
                "cannot be reassigned independently of metadata_store."
            )
        self._compat_db = value

    @property
    def db_path(self) -> str | None:
        """
        Compatibility accessor for the configured backing DuckDB path.

        New code should prefer ``tracker.hot_data_store.db_path`` or
        ``tracker.metadata_store.db_path``.
        """
        hot_store = getattr(self, "hot_data_store", None)
        if hot_store is not None:
            return hot_store.db_path
        metadata_store = getattr(self, "metadata_store", None)
        if metadata_store is not None:
            return metadata_store.db_path
        return getattr(self, "_compat_db_path", None)

    @db_path.setter
    def db_path(self, value: str | os.PathLike[str] | None) -> None:
        normalized = os.fspath(value) if value is not None else None
        hot_store = getattr(self, "hot_data_store", None)
        metadata_store = getattr(self, "metadata_store", None)

        active_path = None
        if hot_store is not None:
            active_path = hot_store.db_path
        elif metadata_store is not None:
            active_path = metadata_store.db_path

        if active_path is not None:
            if normalized == active_path:
                return
            raise AttributeError(
                "tracker.db_path is a compatibility accessor in single-store mode "
                "and cannot be reassigned independently of the stores."
            )
        self._compat_db_path = normalized

    @property
    def engine(self):
        """
        Return the SQLAlchemy engine used by this tracker.

        This is a single-store compatibility alias. New code should prefer
        explicit ``metadata_store`` / ``hot_data_store`` ownership boundaries.

        Returns
        -------
        Optional[Engine]
            The SQLAlchemy engine if a database is configured, otherwise ``None``.
        """
        hot_store = getattr(self, "hot_data_store", None)
        if hot_store is not None:
            return hot_store.engine
        metadata_store = getattr(self, "metadata_store", None)
        if metadata_store is not None:
            return metadata_store.engine
        db = getattr(self, "db", None)
        if db is None:
            return None
        return db.engine

    @property
    def registered_schemas(self) -> Mapping[str, Type[SQLModel]]:
        """
        Return the SQLModel schemas registered on this tracker.

        Registered schemas are the SQLModel classes passed via
        ``Tracker(..., schemas=[...])`` during initialization. They are stored by
        class name (for example, ``"LinkstatsRow"``) and used by lookup-based
        workflows such as schema-aware ingestion.

        Returns
        -------
        Mapping[str, Type[SQLModel]]
            Read-only mapping from schema class name to the corresponding SQLModel
            class object.

        Notes
        -----
        The returned mapping is immutable from the caller perspective.

        Examples
        --------
        ```python
        tracker = Tracker(..., schemas=[MySchema])
        assert "MySchema" in tracker.registered_schemas
        ```
        """
        return cast(
            Mapping[str, Type[SQLModel]], MappingProxyType(self._registered_schemas)
        )

    def get_registered_schema(
        self,
        schema_name: str,
        default: Optional[Type[SQLModel]] = None,
    ) -> Optional[Type[SQLModel]]:
        """
        Resolve a registered SQLModel schema by its class name.

        This is an ergonomic lookup helper for workflows that persist or exchange
        schema names (for example ``artifact.meta["schema_name"]``) and then need
        the corresponding SQLModel class at runtime.

        Parameters
        ----------
        schema_name : str
            Registered schema class name to resolve. Matching is exact and
            case-sensitive.
        default : Optional[Type[SQLModel]], optional
            Value returned when ``schema_name`` is not found in the registry.
            Defaults to ``None``.

        Returns
        -------
        Optional[Type[SQLModel]]
            The registered SQLModel class when found, otherwise ``default``.

        Raises
        ------
        TypeError
            If ``schema_name`` is not a string.
        ValueError
            If ``schema_name`` is an empty or whitespace-only string.

        Examples
        --------
        ```python
        tracker = Tracker(..., schemas=[MySchema])
        schema_cls = tracker.get_registered_schema("MySchema")
        missing = tracker.get_registered_schema("UnknownSchema")
        ```
        """
        if not isinstance(schema_name, str):
            raise TypeError("schema_name must be a string.")
        normalized_schema_name = schema_name.strip()
        if not normalized_schema_name:
            raise ValueError("schema_name must be a non-empty string.")
        return self._registered_schemas.get(normalized_schema_name, default)

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
        prefer_source: Optional[SchemaProfileSource] = None,
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
        prefer_source : {"file", "duckdb", "user_provided"}, optional
            Preference hint for when user_provided schema does not exist. This is
            useful when an artifact has both a file profile (pandas dtypes) and a
            duckdb profile (post-ingestion types). Ignored if schema_id is provided
            directly.

            IMPORTANT: User-provided schemas (manually curated with FK constraints,
            indexes, etc.) are ALWAYS preferred if they exist. This parameter does
            not override user_provided schemas.

            - "file": Prefer the original file schema (CSV/Parquet with pandas dtypes)
            - "duckdb": Prefer the post-ingestion schema from the DuckDB table
            - "user_provided": Prefer manually curated schema observations explicitly
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

        ```python
        tracker.export_schema_sqlmodel(artifact_id=art.id)
        ```

        Export ingested table schema (after dlt normalization):

        ```python
        tracker.export_schema_sqlmodel(artifact_id=art.id, prefer_source="duckdb")
        ```

        Export a specific schema directly by ID:

        ```python
        tracker.export_schema_sqlmodel(schema_id="abc123xyz")
        ```
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
            db=self.db,
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

    def _resolve_input_reference(
        self,
        ref: RunInputRef,
        key: Optional[str] = None,
        *,
        type_label: str = "inputs",
        missing_path_error: str = (
            "Problem: Input path does not exist: {path!s}\n"
            "Cause: The provided input path is missing or not accessible.\n"
            "Fix: Pass an existing file/directory path or a valid Artifact/RunResult "
            "reference."
        ),
        missing_string_error: Optional[str] = None,
        string_ref_resolver: Optional[Callable[[str], Optional[ArtifactRef]]] = None,
    ) -> ArtifactRef:
        return _resolve_input_reference_configured(
            self,
            ref,
            key,
            type_label=type_label,
            missing_path_error=missing_path_error,
            missing_string_error=missing_string_error,
            string_ref_resolver=string_ref_resolver,
        )

    def register_artifact_facet_parser(
        self, prefix: str, parser_fn: Callable[[str], Optional[FacetLike]]
    ) -> None:
        """
        Register a key-prefix parser for deriving artifact facets.

        Parsers are evaluated in descending prefix-length order when
        ``log_artifact(..., facet=None)`` is used.
        """
        if not isinstance(prefix, str) or not prefix:
            raise ValueError("prefix must be a non-empty string.")
        self._artifact_facet_parsers = [
            (p, fn) for p, fn in self._artifact_facet_parsers if p != prefix
        ]
        self._artifact_facet_parsers.append((prefix, parser_fn))
        self._artifact_facet_parsers.sort(key=lambda row: len(row[0]), reverse=True)

    def set_artifact_recovery_roots(
        self,
        artifact: Artifact,
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]],
        *,
        append: bool = False,
    ) -> Artifact:
        """
        Persist advisory filesystem recovery roots for an artifact.

        Recovery roots are ordered fallback locations used during historical
        rematerialization and cache-hit output hydration when the canonical
        cold bytes are no longer available at their original location.

        The artifact's ``container_uri`` remains the canonical logical
        location. Recovery roots are only alternate byte sources.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if self.db is None:
            raise RuntimeError(
                "Cannot update artifact recovery roots: tracker has no database configured."
            )

        incoming = self.fs.normalize_recovery_roots(roots)
        existing = self.fs.normalize_recovery_roots(
            (artifact.meta or {}).get("recovery_roots")
        )
        normalized = incoming
        if append:
            normalized = self.fs.normalize_recovery_roots([*existing, *incoming])

        updates: dict[str, Any]
        if normalized:
            updates = {"recovery_roots": normalized}
        else:
            current_meta = dict(artifact.meta or {})
            current_meta.pop("recovery_roots", None)
            self.db.update_artifact_meta(
                artifact,
                {"recovery_roots": None},
                raise_on_error=True,
            )
            artifact.meta = current_meta
            self._run_artifacts_cache.clear()
            return artifact

        self.db.update_artifact_meta(artifact, updates, raise_on_error=True)
        artifact.meta = dict(artifact.meta or {})
        artifact.meta["recovery_roots"] = normalized
        self._run_artifacts_cache.clear()
        return artifact

    def archive_artifact(
        self,
        artifact: Artifact,
        archive_root: str | os.PathLike[str],
        *,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> Path:
        """
        Archive a rematerializable artifact into a stable recovery root.

        The archived copy preserves the artifact's URI-relative layout under
        ``archive_root`` and records that root in
        ``artifact.meta["recovery_roots"]``.

        This helper is intended for workflows that promote bytes into archival
        storage while keeping the original artifact identity and
        ``container_uri`` unchanged.
        """
        if not isinstance(artifact, Artifact):
            raise TypeError("artifact must be an Artifact instance.")
        if mode not in {"copy", "move"}:
            raise ValueError("mode must be 'copy' or 'move'.")
        if self.db is None:
            raise RuntimeError(
                "Cannot archive artifact: tracker has no database configured."
            )

        relative_path = self.fs.get_remappable_relative_path(artifact.container_uri)
        if relative_path is None:
            raise ValueError(
                f"Artifact {artifact.key!r} does not have a rematerializable URI "
                "layout. Use managed output paths or preserve a stable relative "
                "layout before archiving. Absolute-path and file:// artifacts "
                "cannot be recovered from root-only recovery metadata."
            )

        archive_root_path = Path(archive_root).resolve()
        destination = (archive_root_path / relative_path).resolve()
        source_path: Path | None = None

        if artifact.run_id:
            from consist.core.materialize import find_existing_recovery_source_path

            producing_run = self.get_run(str(artifact.run_id))
            if producing_run is not None:
                _, recovered, _ = find_existing_recovery_source_path(
                    self,
                    artifact=artifact,
                    run=producing_run,
                    source_root=None,
                )
                source_path = recovered

        if source_path is None and artifact.run_id is None and artifact.abs_path:
            candidate = Path(artifact.abs_path).resolve()
            if candidate.exists():
                source_path = candidate

        if source_path is None and artifact.run_id is None:
            candidate = Path(self.resolve_uri(artifact.container_uri)).resolve()
            if candidate.exists():
                source_path = candidate

        if source_path is None or not source_path.exists():
            raise FileNotFoundError(
                f"Cannot archive artifact {artifact.key!r}: source bytes are unavailable."
            )

        destination_preexisted = destination.exists()
        moved_from: Path | None = None
        if destination.exists():
            if destination.is_symlink():
                raise ValueError(
                    f"Symlink detected in archive destination: {destination}"
                )
            if destination.resolve() != source_path.resolve():
                if source_path.is_file() and destination.is_file():
                    same_size = source_path.stat().st_size == destination.stat().st_size
                    same_hash = False
                    if same_size:
                        same_hash = self.identity.compute_file_checksum(
                            str(source_path)
                        ) == self.identity.compute_file_checksum(str(destination))
                    if not same_hash:
                        raise FileExistsError(
                            f"Archive destination already exists: {destination}"
                        )
                else:
                    raise FileExistsError(
                        f"Archive destination already exists: {destination}"
                    )
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source_path.resolve() != destination.resolve():
                if mode == "copy":
                    if source_path.is_dir():
                        shutil.copytree(source_path, destination)
                    else:
                        shutil.copy2(source_path, destination)
                else:
                    moved_from = source_path
                    shutil.move(str(source_path), str(destination))

        try:
            self.set_artifact_recovery_roots(
                artifact, [archive_root_path], append=append
            )
        except Exception:
            if moved_from is not None and destination.exists():
                moved_from.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(destination), str(moved_from))
            elif not destination_preexisted and destination.exists():
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
            raise

        if mode == "move":
            artifact.abs_path = str(destination.resolve())
        return destination

    def archive_run_outputs(
        self,
        run_id: str,
        archive_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> dict[str, Path]:
        """
        Archive one or more historical run outputs into a stable recovery root.

        Each archived output retains its canonical artifact identity and gains
        ``archive_root`` as an advisory recovery root.
        """
        normalized_keys = normalize_materialize_output_keys(
            keys,
            caller="archive_run_outputs",
        )
        outputs = self.get_run_outputs(run_id)
        if normalized_keys is not None:
            missing = [key for key in normalized_keys if key not in outputs]
            if missing:
                raise KeyError(
                    "Requested output keys were not found for run "
                    f"{run_id!r}: {', '.join(repr(key) for key in missing)}"
                )
            selected = {key: outputs[key] for key in normalized_keys}
        else:
            selected = outputs

        archived: dict[str, Path] = {}
        for key, artifact in selected.items():
            archived[key] = self.archive_artifact(
                artifact,
                archive_root,
                mode=mode,
                append=append,
            )
        return archived

    def archive_current_run_outputs(
        self,
        archive_root: str | os.PathLike[str],
        *,
        keys: Sequence[str] | None = None,
        mode: Literal["copy", "move"] = "copy",
        append: bool = True,
    ) -> dict[str, Path]:
        """
        Archive outputs for the currently active run into a stable recovery root.

        This is a convenience wrapper around ``archive_run_outputs(...)`` for
        the common workflow of archiving outputs immediately after they are
        logged, without manually extracting the active run ID first.
        """
        if not self.current_consist or self.current_consist.run is None:
            raise RuntimeError(
                "archive_current_run_outputs(...) requires an active run context."
            )
        return self.archive_run_outputs(
            self.current_consist.run.id,
            archive_root,
            keys=keys,
            mode=mode,
            append=append,
        )

    def _parse_artifact_facet_from_registered_parsers(
        self, key: str
    ) -> Optional[FacetLike]:
        for prefix, parser_fn in self._artifact_facet_parsers:
            if not key.startswith(prefix):
                continue
            try:
                parsed = parser_fn(key)
            except Exception as exc:
                logging.warning(
                    "[Consist] Artifact facet parser failed for key=%s prefix=%s: %s",
                    key,
                    prefix,
                    exc,
                )
                continue
            if parsed is not None:
                return parsed
        return None

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
        code_identity: Optional[CodeIdentityMode] = None,
        code_identity_extra_deps: Optional[List[str]] = None,
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
        control over the run lifecycle, such as in external model integrations where
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
        code_identity : Optional[CodeIdentityMode], optional
            Strategy for hashing code identity in cache keys. ``"repo_git"`` (default)
            uses repository git state. ``"callable_module"`` and ``"callable_source"``
            scope identity to the callable executed by ``tracker.run``.
        code_identity_extra_deps : Optional[List[str]], optional
            Extra dependency file paths to fold into callable-scoped code identity.
        facet_schema_version : Optional[Union[str, int]], optional
            Optional schema version tag for the persisted facet.
        facet_index : bool, default True
            Whether to flatten and index facet keys/values for DB querying.
        **kwargs : Any
            Additional metadata. Special keywords `year`, `iteration`, `stage`,
            and `phase` are recognized, with `stage` and `phase` persisted on
            the run as workflow metadata.
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
        return self._run_lifecycle.begin_run(
            run_id=run_id,
            model=model,
            config=config,
            inputs=inputs,
            tags=tags,
            description=description,
            cache_mode=cache_mode,
            artifact_dir=artifact_dir,
            allow_external_paths=allow_external_paths,
            facet=facet,
            facet_from=facet_from,
            hash_inputs=hash_inputs,
            code_identity=code_identity,
            code_identity_extra_deps=code_identity_extra_deps,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            **kwargs,
        )

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
            - `year`, `iteration`, `stage`, `phase`

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

        Example
        -------
        ```python
         with tracker.start_run("run_1", "my_model", config={"p": 1}):
             tracker.log_artifact("data.csv", "input")
             # ... execution ...
             tracker.log_artifact("results.parquet", "output")
        ```
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
        adapter: Optional[ConfigAdapter] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
        inputs: Optional[
            Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[RunInputRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        identity_inputs: IdentityInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        phase: Optional[str] = None,
        stage: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_options: Optional[CacheOptions] = None,
        output_policy: Optional[OutputPolicyOptions] = None,
        execution_options: Optional[ExecutionOptions] = None,
        runtime_kwargs: Optional[Mapping[str, Any]] = None,
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
        adapter : Optional[ConfigAdapter], optional
            Config adapter used to derive a config plan before execution.
        config_plan_ingest : bool, default True
            Whether to ingest tables from the config plan.
        config_plan_profile_schema : bool, default False
            Whether to profile ingested schemas for the config plan.
        inputs : Optional[Mapping[str, RunInputRef] | Iterable[RunInputRef]], optional
            Input files or artifacts.
            - Dict: Maps names to paths/Artifacts. Named inputs can bind into function
              parameters according to `execution_options.input_binding` (or legacy
              `load_inputs`).
            - List/Iterable: Hashed for cache key but not automatically bound.
        input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        optional_input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        depends_on : Optional[List[RunInputRef]], optional
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

        identity_inputs : Optional[IdentityInputs], optional
            Additional hash-only identity inputs (for example config files or
            directories) that should affect cache keys without being logged as
            run inputs.

        year : Optional[int], optional
            Year metadata (for multi-year simulations). Included in provenance.
        iteration : Optional[int], optional
            Iteration count (for iterative workflows). Included in provenance.
        phase : Optional[str], optional
            Optional lifecycle phase label persisted in run metadata.
        stage : Optional[str], optional
            Optional workflow stage label persisted in run metadata.
        parent_run_id : Optional[str], optional
            Parent run ID (for nested runs in scenarios).

        outputs : Optional[List[str]], optional
            Output artifact keys for return-value logging with executor='python'.
            Supports DataFrame/Series/xarray returns and path-like returns. If omitted,
            Consist auto-logs artifact-like returns (Path/str/Artifact or dict[str, ...])
            when ``output_paths`` is not provided.
        output_paths : Optional[Mapping[str, ArtifactRef]], optional
            Output file paths to log. Dict maps artifact keys to host paths or Artifact refs.
        capture_dir : Optional[Path], optional
            Directory to scan for outputs (legacy tools that write to specific dirs).
        capture_pattern : str, default "*"
            Glob pattern for capturing outputs (used with capture_dir).
        cache_options : Optional[CacheOptions], optional
            Grouped cache controls (`cache_mode`, `cache_hydration`, `cache_version`,
            `cache_epoch`, `validate_cached_outputs`, `code_identity`,
            `code_identity_extra_deps`).
        output_policy : Optional[OutputPolicyOptions], optional
            Grouped output policies (`output_mismatch`, `output_missing`).
        execution_options : Optional[ExecutionOptions], optional
            Grouped execution controls (`input_binding`, legacy `load_inputs`,
            `input_materialization`, `input_paths`,
            `input_materialization_mode`, `executor`, `container`,
            `runtime_kwargs`, `inject_context`). Use requested input
            materialization with path-bound runs when a callable or external
            tool expects inputs at specific local paths on both cache misses
            and cache hits.
        runtime_kwargs : Optional[Mapping[str, Any]], optional
            Top-level alias for `execution_options.runtime_kwargs`. This is
            mutually exclusive with
            `execution_options=ExecutionOptions(runtime_kwargs=...)`.

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
        Execute a basic data processing step:

        ```python
        def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
            return raw[raw['value'] > 0.5]

        result = tracker.run(
            fn=clean_data,
            inputs={"raw": Path("raw.csv")},
            outputs=["cleaned"],
        )
        ```

        Configure identity hashing for granular cache control:

        ```python
        result = tracker.run(
            fn=clean_data,
            inputs={"raw": Path("raw.csv")},
            config={"threshold": 0.5},
            outputs=["cleaned"],
        )
        ```

        See Also
        --------
        start_run : Manual run context management (more control)
        trace : Context manager alternative (always executes, even on cache hit)
        """
        return self._run_trace.run(
            fn=fn,
            name=name,
            run_id=run_id,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            config_plan_ingest=config_plan_ingest,
            config_plan_profile_schema=config_plan_profile_schema,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            depends_on=depends_on,
            tags=tags,
            facet=facet,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            identity_inputs=identity_inputs,
            year=year,
            iteration=iteration,
            phase=phase,
            stage=stage,
            parent_run_id=parent_run_id,
            outputs=outputs,
            output_paths=output_paths,
            capture_dir=capture_dir,
            capture_pattern=capture_pattern,
            cache_options=cache_options,
            output_policy=output_policy,
            execution_options=execution_options,
            runtime_kwargs=runtime_kwargs,
        )

    def run_with_config_overrides(
        self,
        *,
        adapter: SupportsRunWithConfigOverrides,
        base_run_id: Optional[str] = None,
        base_config_dirs: Optional[Sequence[Path]] = None,
        base_primary_config: Optional[Path] = None,
        overrides: Any,
        output_dir: Path,
        fn: Callable[..., Any],
        name: str,
        model: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        outputs: Optional[List[str]] = None,
        execution_options: Optional[ExecutionOptions] = None,
        strict: bool = True,
        identity_inputs: IdentityInputs = None,
        resolved_config_identity: Literal["auto", "off"] = "auto",
        identity_label: str = "activitysim_config",
        override_runtime_kwargs: Optional[Mapping[str, Any]] = None,
        **run_kwargs: Any,
    ) -> RunResult:
        """
        Delegate config-override execution to an adapter-specific implementation.

        The tracker remains adapter-agnostic by forwarding to
        ``adapter.run_with_config_overrides(...)`` when available.

        Exactly one base selector is required:
        ``base_run_id`` or ``base_config_dirs``.
        ``base_primary_config`` is optional and only applies to
        ``base_config_dirs`` flows.
        """
        if not isinstance(adapter, SupportsRunWithConfigOverrides):
            raise TypeError(
                format_problem_cause_fix(
                    problem=(
                        "Adapter does not support run_with_config_overrides delegation."
                    ),
                    cause=(
                        "The provided adapter does not implement "
                        "run_with_config_overrides(...)."
                    ),
                    fix=(
                        "Use an adapter that implements override execution, or "
                        "materialize configs manually then call tracker.run(...)."
                    ),
                )
            )
        if base_run_id is not None and base_config_dirs is not None:
            raise ValueError(
                "run_with_config_overrides requires exactly one base selector. "
                "Provide either base_run_id or base_config_dirs, not both."
            )
        if base_run_id is None and base_config_dirs is None:
            raise ValueError(
                "run_with_config_overrides requires a base selector. "
                "Provide either base_run_id or base_config_dirs."
            )
        if base_run_id is not None and not str(base_run_id).strip():
            raise ValueError("base_run_id must be a non-empty string when provided.")
        if base_config_dirs is not None and len(base_config_dirs) == 0:
            raise ValueError(
                "base_config_dirs must contain at least one directory when provided."
            )
        if resolved_config_identity not in {"auto", "off"}:
            raise ValueError(
                "resolved_config_identity must be either 'auto' or 'off'. "
                f"Got {resolved_config_identity!r}."
            )
        return adapter.run_with_config_overrides(
            tracker=self,
            base_run_id=base_run_id,
            base_config_dirs=base_config_dirs,
            base_primary_config=base_primary_config,
            overrides=overrides,
            output_dir=output_dir,
            fn=fn,
            name=name,
            model=model,
            config=config,
            outputs=outputs,
            execution_options=execution_options,
            strict=strict,
            identity_inputs=identity_inputs,
            resolved_config_identity=resolved_config_identity,
            identity_label=identity_label,
            override_runtime_kwargs=override_runtime_kwargs,
            **run_kwargs,
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
        adapter: Optional[ConfigAdapter] = None,
        config_plan_ingest: bool = True,
        config_plan_profile_schema: bool = False,
        inputs: Optional[
            Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]
        ] = None,
        input_keys: Optional[Iterable[str] | str] = None,
        optional_input_keys: Optional[Iterable[str] | str] = None,
        depends_on: Optional[List[RunInputRef]] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
        identity_inputs: IdentityInputs = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        parent_run_id: Optional[str] = None,
        outputs: Optional[List[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
        capture_dir: Optional[Path] = None,
        capture_pattern: str = "*",
        cache_mode: str = "reuse",
        cache_hydration: Optional[str] = None,
        cache_version: Optional[int] = None,
        cache_epoch: Optional[int] = None,
        validate_cached_outputs: str = "lazy",
        code_identity: Optional[CodeIdentityMode] = None,
        code_identity_extra_deps: Optional[List[str]] = None,
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
        adapter : Optional[ConfigAdapter], optional
            Config adapter used to derive a config plan before execution.
        config_plan_ingest : bool, default True
            Whether to ingest tables from the config plan.
        config_plan_profile_schema : bool, default False
            Whether to profile ingested schemas for the config plan.

        inputs : Optional[Mapping[str, RunInputRef] | Iterable[RunInputRef]], optional
            Input files or artifacts.
            - Dict: Maps names to paths/Artifacts. Logged as inputs but not auto-loaded.
            - List/Iterable: Hashed for cache key but not auto-loaded.
        input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        optional_input_keys : Optional[Iterable[str] | str], optional
            Deprecated. Use `inputs` mapping instead.
        depends_on : Optional[List[RunInputRef]], optional
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

        identity_inputs : Optional[IdentityInputs], optional
            Additional hash-only identity inputs (for example config files or
            directories) that should affect cache keys without being logged as
            run inputs.

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
        cache_version : Optional[int], optional
            Optional cache-version discriminator folded into run identity.
        cache_epoch : Optional[int], optional
            Optional cache-epoch discriminator folded into run identity.
        validate_cached_outputs : str, default "lazy"
            Validation for cached outputs: "lazy" (check if files exist), "strict", or "none".
        code_identity : Optional[CodeIdentityMode], optional
            Strategy for hashing code identity in cache keys.
        code_identity_extra_deps : Optional[List[str]], optional
            Extra dependency file paths folded into code identity hashing.

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

        ```python
        with tracker.trace(
            "my_analysis",
            output_paths={"results": "./results.csv"}
        ):
            df = pd.read_csv("raw.csv")
            df["value"] = df["value"] * 2
            df.to_csv("./results.csv", index=False)
        ```

        Multi-year simulation loop:

        ```python
        with tracker.scenario("baseline") as sc:
            for year in [2020, 2030, 2040]:
                with sc.trace(name="simulate", year=year):
                    results = run_model(year)
                    tracker.log_artifact(results, key="output")
        ```

        See Also
        --------
        run : Function-shaped alternative (skips on cache hit)
        scenario : Multi-step workflow grouping
        start_run : Imperative alternative for run lifecycle management
        """
        with self._run_trace.trace(
            name=name,
            run_id=run_id,
            model=model,
            description=description,
            config=config,
            adapter=adapter,
            config_plan_ingest=config_plan_ingest,
            config_plan_profile_schema=config_plan_profile_schema,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            depends_on=depends_on,
            tags=tags,
            facet=facet,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            identity_inputs=identity_inputs,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            outputs=outputs,
            output_paths=output_paths,
            capture_dir=capture_dir,
            capture_pattern=capture_pattern,
            cache_mode=cache_mode,
            cache_hydration=cache_hydration,
            cache_version=cache_version,
            cache_epoch=cache_epoch,
            validate_cached_outputs=validate_cached_outputs,
            code_identity=code_identity,
            code_identity_extra_deps=code_identity_extra_deps,
            output_mismatch=output_mismatch,
            output_missing=output_missing,
        ) as active_tracker:
            yield active_tracker

    def scenario(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        model: str = "scenario",
        step_tags: Optional[List[str]] = None,
        step_facet: Optional[Dict[str, Any]] = None,
        step_cache_hydration: Optional[str] = None,
        name_template: Optional[str] = None,
        cache_epoch: Optional[int] = None,
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
        step_tags : Optional[List[str]], optional
            Default tags applied to every child ``scenario.run(...)`` and
            ``scenario.trace(...)`` call. These do not apply to the scenario
            header run itself. Per-step ``tags=...`` values are preserved and
            take precedence when duplicates exist.
        step_facet : Optional[Dict[str, Any]], optional
            Default facet mapping merged into every child ``scenario.run(...)``
            and ``scenario.trace(...)`` call. These defaults do not apply to
            the scenario header run itself. Per-step ``facet=...`` values win
            on key collisions.
        step_cache_hydration : Optional[str], optional
            Default cache hydration policy for all scenario steps unless overridden
            in a specific `scenario.trace(...)` or `scenario.run(...)`.
        name_template : Optional[str], optional
            Optional step name template applied when scenario.run() is called without
            an explicit name and no step-level template is provided.
        cache_epoch : Optional[int], optional
            Scenario-level cache epoch override for all steps in this scenario.
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
            step_tags=step_tags,
            step_facet=step_facet,
            step_cache_hydration=step_cache_hydration,
            name_template=name_template,
            cache_epoch=cache_epoch,
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
        return self._run_lifecycle.end_run(status=status, error=error)

    # --- Query Helpers ---

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
        stage : Optional[str], optional
            Filter by run stage.
        phase : Optional[str], optional
            Filter by run phase.
        model : Optional[str], optional
            Filter by run model name.
        status : Optional[str], optional
            Filter by run status (e.g., "completed", "failed").
        parent_id : Optional[str], optional
            Filter by scenario/header parent id.
        facet : Optional[Dict[str, Any]], optional
            Filter by exact matches against persisted run facet values. Nested
            mappings are matched by their flattened key paths.
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
            stage=stage,
            phase=phase,
            model=model,
            status=status,
            parent_id=parent_id,
            facet=facet,
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

    def run_set(self, label: Optional[str] = None, **filters: Any) -> "RunSet":
        """
        Build a RunSet from ``find_runs`` filters.

        Parameters
        ----------
        label : Optional[str], optional
            Optional label attached to the returned RunSet.
        **filters : Any
            Filters forwarded to ``find_runs``.

        Returns
        -------
        RunSet
            A tracker-backed RunSet for fluent grouping/alignment analysis.

        Notes
        -----
        This is equivalent to ``RunSet.from_query(self, label=label, **filters)``.
        """
        from consist.runset import RunSet

        return RunSet.from_query(self, label=label, **filters)

    def run_query(self, query: Executable) -> list:
        """
        Execute a SQLModel/SQLAlchemy query via the metadata store.

        Parameters
        ----------
        query : Executable
            Query object (``select``, ``text``, etc.).

        Returns
        -------
        list
            Results of the executed query.

        Raises
        ------
        RuntimeError
            If no database is configured for this tracker.
        """
        metadata_store = self.metadata_store
        if metadata_store is None:
            raise RuntimeError("Database connection required.")
        with Session(metadata_store.engine) as session:
            return session.exec(cast(Any, query)).all()

    def find_latest_run(
        self,
        *,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 10_000,
        name: Optional[str] = None,
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
        iteration : Optional[int], optional
            Filter by run iteration.
        stage : Optional[str], optional
            Filter by run stage.
        phase : Optional[str], optional
            Filter by run phase.
        tags : Optional[List[str]], optional
            Filter runs that contain all provided tags.
        facet : Optional[Dict[str, Any]], optional
            Filter by exact matches against persisted run facet values.
        metadata : Optional[Dict[str, Any]], optional
            Filter by exact matches in ``Run.meta`` (client-side filter).
        limit : int, default 10_000
            Maximum number of runs to consider.
        name : Optional[str], optional
            Filter by run description/name alias.
        """
        return self.queries.find_latest_run(
            parent_id=parent_id,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
            stage=stage,
            phase=phase,
            tags=tags,
            facet=facet,
            metadata=metadata,
            limit=limit,
            name=name,
        )

    def get_latest_run_id(self, **kwargs) -> str:
        """
        Convenience wrapper to return the latest run ID for the given filters.

        Parameters
        ----------
        **kwargs : Any
            Filters forwarded to ``find_latest_run``.

        Returns
        -------
        str
            The run ID of the latest matching run.

        Raises
        ------
        ValueError
            If no runs match the provided filters.
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
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        profile_file_schema: bool | Literal["if_changed"] | None = None,
        file_schema_sample_rows: Optional[int] = None,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
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

        -   **Immediate Persistence**: This single-artifact method flushes JSON state
            and syncs artifact links to the database immediately for this call.

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
        table_path : Optional[str], optional
            Optional table path inside a container (e.g., HDF5).
        array_path : Optional[str], optional
            Optional array path inside a container (e.g., Zarr group).
        content_hash : Optional[str], optional
            Precomputed content hash to use for the artifact instead of hashing
            the path on disk.
        force_hash_override : bool, default False
            If True, overwrite an existing artifact hash when it differs from
            `content_hash`. By default, mismatched overrides are ignored with a warning.
        validate_content_hash : bool, default False
            If True, verify `content_hash` against the on-disk data and raise on mismatch.
        reuse_if_unchanged : bool, default False
            Deprecated for outputs. Consist now always creates a fresh output artifact row;
            identical bytes are deduplicated via `artifact.content_id`. Setting this on outputs
            emits a warning and does not reuse prior rows. Input-side behavior is unaffected.
        reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
            Deprecated for outputs. `any_uri` is ignored for outputs; deduplication is governed
            by `content_id`. Input-side behavior is unaffected.
        profile_file_schema : bool, default False
            If True, profile a lightweight schema for file-based tabular artifacts.
            Use "if_changed" to skip profiling when matching content identity already
            has a stored schema (prefers content_id; falls back to hash for legacy rows).
        file_schema_sample_rows : Optional[int], default None
            Maximum rows to sample when profiling file-based schemas.
        facet : Optional[FacetLike], optional
            Optional artifact-level facet payload (dict or Pydantic model).
        facet_schema_version : Optional[Union[str, int]], optional
            Optional schema version for artifact facet compatibility.
        facet_index : bool, default False
            If True, flatten scalar facet fields into ``artifact_kv`` for fast queries.
        **meta : Any
            Additional key-value pairs to store in the artifact's flexible `meta` field.
            ``recovery_roots`` is normalized to an ordered list of absolute
            filesystem roots when provided.

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
        return self._artifact_logging.log_artifact(
            path=path,
            key=key,
            direction=direction,
            schema=schema,
            driver=driver,
            table_path=table_path,
            array_path=array_path,
            content_hash=content_hash,
            force_hash_override=force_hash_override,
            validate_content_hash=validate_content_hash,
            reuse_if_unchanged=reuse_if_unchanged,
            reuse_scope=reuse_scope,
            profile_file_schema=profile_file_schema,
            file_schema_sample_rows=file_schema_sample_rows,
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            **meta,
        )

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
        facets_by_key: Optional[Mapping[str, FacetLike]] = None,
        facet_schema_versions_by_key: Optional[Mapping[str, Union[str, int]]] = None,
        facet_index: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        **shared_meta: Any,
    ) -> Dict[str, Artifact]:
        """
        Log multiple artifacts in a single call for efficiency.

        This is a convenience method for bulk artifact logging, particularly useful
        when a model produces many output files or when registering multiple inputs.
        This requires an explicit mapping so artifact keys are always deliberate.
        For efficiency, persistence is batched: JSON flush and DB artifact sync occur
        once at the end of the call (not once per artifact).

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
        facets_by_key : Optional[Mapping[str, FacetLike]], optional
            Per-key artifact facet payloads.
        facet_schema_versions_by_key : Optional[Mapping[str, Union[str, int]]], optional
            Optional per-key schema versions for artifact facet payloads.
        facet_index : bool, default False
            Whether to index scalar artifact facet values in ``artifact_kv``.
        reuse_if_unchanged : bool, default False
            Deprecated for outputs. Batch output logging still creates a fresh artifact
            row per call; identical bytes are deduplicated via ``artifact.content_id``.
            Setting this on outputs emits a warning and does not reuse prior rows.
            Input-side behavior is unaffected.
        reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
            Deprecated for outputs. ``any_uri`` is ignored for outputs; deduplication
            is governed by ``content_id`` instead. Input-side behavior is unaffected.
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
        if facets_by_key:
            extra_keys = set(facets_by_key).difference(outputs)
            if extra_keys:
                extras = ", ".join(sorted(str(key) for key in extra_keys))
                raise ValueError(
                    f"facets_by_key contains keys not present in outputs: {extras}"
                )
        if facet_schema_versions_by_key:
            extra_keys = set(facet_schema_versions_by_key).difference(outputs)
            if extra_keys:
                extras = ", ".join(sorted(str(key) for key in extra_keys))
                raise ValueError(
                    "facet_schema_versions_by_key contains keys not present in "
                    f"outputs: {extras}"
                )

        keys = list(outputs.keys())
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("log_artifacts keys must be strings.")

        base_meta = dict(shared_meta)
        logged: Dict[str, Artifact] = {}
        with self.persistence.batch_artifact_writes():
            for key in sorted(keys):
                value = outputs[key]
                if value is None:
                    raise ValueError(f"log_artifacts received None for key {key!r}.")
                meta = dict(base_meta)
                if metadata_by_key and key in metadata_by_key:
                    meta.update(metadata_by_key[key])
                facet = facets_by_key.get(key) if facets_by_key else None
                facet_schema_version = (
                    facet_schema_versions_by_key.get(key)
                    if facet_schema_versions_by_key
                    else None
                )
                logged[key] = self.log_artifact(
                    value,
                    key=key,
                    direction=direction,
                    driver=driver,
                    facet=facet,
                    facet_schema_version=facet_schema_version,
                    facet_index=facet_index,
                    reuse_if_unchanged=reuse_if_unchanged,
                    reuse_scope=reuse_scope,
                    **meta,
                )
        return logged

    def log_input(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
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
        facet : Optional[FacetLike], optional
            Optional artifact-level facet payload for this input artifact.
        facet_schema_version : Optional[Union[str, int]], optional
            Optional facet schema version.
        facet_index : bool, default False
            Whether to index scalar facet fields for querying.
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
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            **meta,
        )

    def log_output(
        self,
        path: ArtifactRef,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        reuse_if_unchanged: bool = False,
        reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
        facet: Optional[FacetLike] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = False,
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
        reuse_if_unchanged : bool, default False
            Deprecated for outputs. A fresh output artifact row is always created; identical
            bytes share `content_id`. Setting this emits a warning and does not reuse prior rows.
        reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
            Deprecated for outputs. `any_uri` is ignored; deduplication is by `content_id`.
        facet : Optional[FacetLike], optional
            Optional artifact-level facet payload for this output artifact.
        facet_schema_version : Optional[Union[str, int]], optional
            Optional facet schema version.
        facet_index : bool, default False
            Whether to index scalar facet fields for querying.
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
            reuse_if_unchanged=reuse_if_unchanged,
            reuse_scope=reuse_scope,
            facet=facet,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            **meta,
        )

    def load(self, artifact: Artifact, **kwargs: Any) -> Any:
        """
        Load an artifact using the public API while binding this tracker context.

        This is equivalent to ``consist.load(artifact, tracker=self, ...)`` and
        uses the artifact driver to select the appropriate loader.

        Parameters
        ----------
        artifact : Artifact
            The artifact to load.
        **kwargs : Any
            Loader-specific options forwarded to ``consist.load``.

        Returns
        -------
        Any
            The loaded data object (e.g., DuckDB Relation, xarray.Dataset, etc.).
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
        hash_tables: Literal["always", "if_unchanged", "never"] = "if_unchanged",
        table_hash_chunk_rows: Optional[int] = None,
        **meta: Any,
    ) -> Tuple[Artifact, List[Artifact]]:
        """
        Log an HDF5 file and optionally discover its internal tables.

        This method provides first-class HDF5 container support, automatically
        discovering and logging internal tables as child artifacts. This is
        particularly useful for model pipelines that use HDF5 files containing
        multiple datasets or tables.

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
        hash_tables : Literal["always", "if_unchanged", "never"], default "if_unchanged"
            Whether to compute content hashes for discovered tables. "if_unchanged"
            skips hashing when a table appears unchanged based on lightweight checks.
        table_hash_chunk_rows : Optional[int], optional
            Row chunk size to use when hashing large tables.
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
            hash_tables=hash_tables,
            table_hash_chunk_rows=table_hash_chunk_rows,
            **meta,
        )

    def log_h5_table(
        self,
        path: Union[str, Path],
        *,
        table_path: str,
        key: Optional[str] = None,
        direction: str = "output",
        parent: Optional[Artifact] = None,
        hash_table: bool = True,
        table_hash_chunk_rows: Optional[int] = None,
        profile_file_schema: bool | Literal["if_changed"] = False,
        file_schema_sample_rows: Optional[int] = None,
        **meta: Any,
    ) -> Artifact:
        """
        Log a single HDF5 table as an artifact without scanning the container.

        Parameters
        ----------
        path : Union[str, Path]
            Path to the HDF5 file on disk.
        table_path : str
            Internal table/dataset path inside the HDF5 container.
        key : Optional[str], optional
            Semantic key for the table artifact. Defaults to the dataset name.
        direction : str, default "output"
            Whether the table is an "input" or "output".
        parent : Optional[Artifact], optional
            Optional parent container artifact to link this table to.
        hash_table : bool, default True
            Whether to compute a content hash for the table.
        table_hash_chunk_rows : Optional[int], optional
            Chunk size for hashing large tables.
        profile_file_schema : bool | Literal["if_changed"], default False
            Whether to profile table schema and store it as metadata. Use
            ``"if_changed"`` to skip profiling when matching content identity
            already has a schema (prefers ``content_id`` and falls back to hash
            for legacy rows).
        file_schema_sample_rows : Optional[int], optional
            Number of rows to sample when profiling schema.
        **meta : Any
            Additional metadata to store on the artifact.

        Returns
        -------
        Artifact
            The created table artifact.
        """
        return self.artifacts.log_h5_table(
            path,
            table_path=table_path,
            key=key,
            direction=direction,
            parent=parent,
            hash_table=hash_table,
            table_hash_chunk_rows=table_hash_chunk_rows,
            profile_file_schema=profile_file_schema,
            file_schema_sample_rows=file_schema_sample_rows,
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
        Register a schema and associate it with a logged artifact:

        ```python
        tracker = Tracker(..., schemas=[MyDataSchema])
        art = tracker.log_artifact(file.csv, schema=MyDataSchema)

        # Automatically looks up and uses MyDataSchema for ingestion
        tracker.ingest(art, data=df)
        ```

        Schemas are persisted by name, allowing lookup across different Python sessions:

        ```python
        # Session 1:
        tracker = Tracker(..., schemas=[MyDataSchema])
        art = tracker.log_artifact(file.csv, schema=MyDataSchema)

        # Session 2:
        tracker2 = Tracker(..., schemas=[MyDataSchema])
        art2 = tracker2.get_artifact("mydata")
        # Looks up MyDataSchema by artifact's schema_name ("MyDataSchema")
        tracker2.ingest(art2, data=df)
        ```

        Explicitly override the default schema during ingestion:

        ```python
        tracker.ingest(art, data=df, schema=DifferentSchema)
        ```
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
            if isinstance(schema_name, str):
                normalized_schema_name = schema_name.strip()
                if normalized_schema_name:
                    resolved_schema = self.get_registered_schema(normalized_schema_name)
            if resolved_schema is not None:
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

    def prepare_config_resolver(
        self,
        adapter: ConfigAdapter,
        *,
        config_dirs: Optional[Iterable[Union[str, Path]]] = None,
        config_dirs_from: Optional[
            Union[
                str,
                Callable[["StepContext"], Iterable[Union[str, Path]]],
            ]
        ] = None,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
        validate_only: bool = False,
        facet_spec: Optional[Dict[str, Any]] = None,
        facet_schema_name: Optional[str] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
    ) -> Callable[["StepContext"], ConfigPlan]:
        """
        Build a StepContext resolver for use with `@define_step(adapter=...)`.

        Exactly one config-directory source must be provided:
        - `config_dirs`: static iterable of directories.
        - `config_dirs_from`: runtime source (dot-path string or callable).

        The returned callable is metadata-safe (pre-run) and delegates to
        `prepare_config(...)`.
        """
        if (config_dirs is None) == (config_dirs_from is None):
            raise ValueError(
                "prepare_config_resolver requires exactly one of "
                "config_dirs= or config_dirs_from=."
            )

        static_dirs = tuple(config_dirs) if config_dirs is not None else None

        def _resolve_runtime_path(ctx: "StepContext", path: str) -> object:
            parts = [part for part in path.split(".") if part]
            if not parts:
                raise ValueError(
                    "config_dirs_from path must be non-empty (e.g., "
                    "'settings.config_dirs')."
                )
            value: object = ctx.require_runtime(parts[0])
            for part in parts[1:]:
                if isinstance(value, MappingABC):
                    mapping_value = cast(Mapping[str, object], value)
                    if part not in mapping_value:
                        raise ValueError(
                            f"Missing runtime mapping key {part!r} while resolving "
                            f"config_dirs_from={path!r}."
                        )
                    value = mapping_value[part]
                    continue
                if not hasattr(value, part):
                    raise ValueError(
                        f"Missing runtime attribute {part!r} while resolving "
                        f"config_dirs_from={path!r}."
                    )
                value = getattr(value, part)
            return value

        def _resolve_dirs(ctx: "StepContext") -> Iterable[Union[str, Path]]:
            if static_dirs is not None:
                return static_dirs
            source = config_dirs_from
            if isinstance(source, str):
                candidate = _resolve_runtime_path(ctx, source)
            else:
                source_from_ctx = cast(
                    Callable[["StepContext"], Iterable[Union[str, Path]]], source
                )
                candidate = source_from_ctx(ctx)
            if isinstance(candidate, (str, Path)):
                raise ValueError(
                    "Resolved config_dirs must be an iterable of paths, not a single "
                    f"value: {candidate!r}."
                )
            return cast(Iterable[Union[str, Path]], candidate)

        def _resolver(ctx: "StepContext") -> ConfigPlan:
            return self.prepare_config(
                adapter=adapter,
                config_dirs=_resolve_dirs(ctx),
                strict=strict,
                options=options,
                validate_only=validate_only,
                facet_spec=facet_spec,
                facet_schema_name=facet_schema_name,
                facet_schema_version=facet_schema_version,
                facet_index=facet_index,
            )

        return _resolver

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
        with self.persistence.batch_artifact_writes():
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
                rows = spec.materialize_rows(run.id)
                self.ingest(
                    artifact,
                    data=rows,
                    schema=spec.schema,
                    run=run,
                    profile_schema=profile_schema,
                )

        return artifacts_by_key

    def _ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        store = getattr(self, "hot_data_store", None)
        if store is not None:
            return store.ingest_cache_hit(table_name, content_hash)

        # Compatibility fallback for legacy/partial tracker stubs.
        if self.engine is None:
            return False
        if not _is_safe_identifier(table_name):
            return False
        table_ref = f"{_quote_ident('global_tables')}.{_quote_ident(table_name)}"
        try:
            with self.engine.begin() as connection:
                result = connection.exec_driver_sql(
                    f"SELECT 1 FROM {table_ref} WHERE content_hash = ? LIMIT 1",
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

    def create_grouped_view(
        self,
        view_name: str,
        *,
        schema_id: Optional[str] = None,
        schema: Optional[Type[SQLModel]] = None,
        namespace: Optional[str] = None,
        params: Optional[Iterable[str]] = None,
        drivers: Optional[List[str]] = None,
        attach_facets: Optional[List[str]] = None,
        include_system_columns: bool = True,
        mode: Literal["hybrid", "hot_only", "cold_only"] = "hybrid",
        if_exists: Literal["replace", "error"] = "replace",
        missing_files: Literal["warn", "error", "skip_silent"] = "warn",
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        schema_compatible: bool = False,
    ) -> Any:
        """
        Create one analysis view across many artifacts selected by schema/facets.

        Unlike ``create_view(view_name, concept_key)``, which targets one key,
        this method selects artifacts by ``schema_id`` plus optional facet/run
        filters and materializes a single view over hot and/or cold data.

        Parameters
        ----------
        view_name : str
            Name of the SQL view to create.
        schema_id : Optional[str], optional
            Schema identity used as the primary artifact selector.
        schema : Optional[Type[SQLModel]], optional
            SQLModel class selector convenience. When provided, Consist resolves
            matching stored schema ids from this model definition, first by exact
            field names and then by compatible subset/superset field-name matching.
        namespace : Optional[str], optional
            Default ArtifactKV namespace applied to facet predicates that do not
            include an explicit namespace.
        params : Optional[Iterable[str]], optional
            Facet predicate expressions, each in one of:
            ``<key>=<value>``, ``<key>>=<value>``, ``<key><=<value>``.
            A leading namespace is supported, for example
            ``beam.phys_sim_iteration=2``.
        drivers : Optional[List[str]], optional
            Optional artifact-driver filter, e.g. ``["parquet"]``.
        attach_facets : Optional[List[str]], optional
            Facet key paths to project into the view as typed ``facet_<key>``
            columns.
        include_system_columns : bool, default True
            Whether to include Consist system columns in the view.
        mode : {"hybrid", "hot_only", "cold_only"}, default "hybrid"
            Which storage tier(s) to include in the view.
        if_exists : {"replace", "error"}, default "replace"
            Behavior when ``view_name`` already exists.
        missing_files : {"warn", "error", "skip_silent"}, default "warn"
            Behavior when a selected cold file is missing.
        run_id : Optional[str], optional
            Optional exact run-id filter.
        parent_run_id : Optional[str], optional
            Optional parent/scenario run-id filter.
        model : Optional[str], optional
            Optional run model-name filter.
        status : Optional[str], optional
            Optional run status filter.
        year : Optional[int], optional
            Optional run year filter.
        iteration : Optional[int], optional
            Optional run iteration filter.
        schema_compatible : bool, default False
            If True, allow schema-compatible subset/superset variants by field
            names in addition to exact ``schema_id`` matches.

        Returns
        -------
        Any
            Backend-specific result from ``ViewFactory.create_grouped_hybrid_view``.

        Raises
        ------
        RuntimeError
            If no database is configured.
        ValueError
            If selector or facet predicates are invalid, or view policies are invalid.

        Examples
        --------
        ```python
        tracker.create_grouped_view(
            "v_linkstats_all",
            schema_id="abc123...",
            namespace="beam",
            params=["artifact_family=linkstats", "year=2018"],
            attach_facets=["artifact_family", "phys_sim_iteration"],
            drivers=["parquet"],
            mode="hybrid",
        )
        ```
        """
        if not self.db:
            raise RuntimeError("Database required to create grouped views.")
        if (schema_id is None) == (schema is None):
            raise ValueError("Provide exactly one of schema_id or schema.")

        resolved_schema_ids: Optional[List[str]] = None
        if schema is not None:
            if not isinstance(schema, type) or not issubclass(schema, SQLModel):
                raise ValueError("schema must be a SQLModel class.")
            resolved_schema_ids = self.db.find_schema_ids_for_model(
                schema_model=schema, compatible=False
            )
            if not resolved_schema_ids:
                resolved_schema_ids = self.db.find_schema_ids_for_model(
                    schema_model=schema, compatible=True
                )
            if not resolved_schema_ids:
                raise ValueError(
                    "No stored schema ids matched the provided SQLModel class."
                )

        predicates = (
            [self._parse_artifact_param_expression(param) for param in params]
            if params
            else []
        )

        factory = ViewFactory(self)
        return factory.create_grouped_hybrid_view(
            view_name=view_name,
            schema_id=schema_id,
            schema_ids=resolved_schema_ids,
            schema_compatible=schema_compatible,
            predicates=predicates,
            namespace=namespace,
            drivers=drivers,
            attach_facets=attach_facets,
            include_system_columns=include_system_columns,
            mode=mode,
            if_exists=if_exists,
            missing_files=missing_files,
            run_id=run_id,
            parent_run_id=parent_run_id,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
        )

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

    def spatial_metadata(self, concept_key: str) -> SpatialMetadataView:
        """
        Access spatial metadata views for a given artifact key.

        Parameters
        ----------
        concept_key : str
            The semantic key identifying the spatial artifact.

        Returns
        -------
        SpatialMetadataView
            A view object with methods to explore spatial metadata.

        Example
        -------
        ```python
        view = tracker.spatial_metadata("parcels")
        bounds = view.get_bounds("parcels")
        ```
        """
        return SpatialMetadataView(self)

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
        return self._recovery_service.materialize(
            artifact,
            destination_path,
            on_missing=on_missing,
        )

    def stage_artifact(
        self,
        artifact: Artifact,
        *,
        destination: str | Path,
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInput":
        """
        Stage one canonical input artifact to an explicit local destination.

        This is the low-level input-side equivalent of output hydration. It
        does not create a new tracked artifact identity; it returns a detached
        staged artifact view whose runtime path points at the staged location
        when successful.

        Prefer ``execution_options=ExecutionOptions(
        input_materialization="requested", input_paths={...})`` on
        ``Tracker.run(...)`` when you want the staging side effect to happen as
        part of a normal run lifecycle.
        """
        return self._recovery_service.stage_artifact(
            artifact,
            destination=destination,
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    def stage_inputs(
        self,
        inputs_by_key: Mapping[str, Artifact],
        *,
        destinations_by_key: Mapping[str, str | Path],
        mode: Literal["copy", "hardlink", "symlink"] = "copy",
        overwrite: bool = False,
        validate_content_hash: Literal["never", "if-present", "always"] = "if-present",
        allow_external_paths: Optional[bool] = None,
    ) -> "StagedInputsResult":
        """
        Stage multiple canonical input artifacts to explicit local destinations.

        This low-level helper is most useful for custom orchestration or
        preflight setup. Standard workflow code should usually request the same
        behavior through ``ExecutionOptions`` on ``run(...)`` or
        ``ScenarioContext.run(...)``.
        """
        return self._recovery_service.stage_inputs(
            inputs_by_key,
            destinations_by_key=destinations_by_key,
            mode=mode,
            overwrite=overwrite,
            validate_content_hash=validate_content_hash,
            allow_external_paths=allow_external_paths,
        )

    # --- Retrieval Helpers ---

    def get_artifact(
        self,
        key_or_id: Union[str, uuid.UUID],
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Artifact]:
        """
        Retrieves an Artifact by semantic key or UUID, optionally scoped to run_id.

        Parameters
        ----------
        key_or_id : Union[str, uuid.UUID]
            The artifact key (e.g., "households") or artifact UUID.
        run_id : Optional[str], optional
            If provided, limits results to artifacts linked to this run (as either
            input or output) via ``run_artifact_link``.

        Returns
        -------
        Optional[Artifact]
            The found artifact, or ``None`` if not found.
        """
        if self.db:
            artifact = self.db.get_artifact(key_or_id, run_id=run_id)
            if artifact is not None:
                set_tracker_ref(artifact, self)
            return artifact
        return None

    def find_artifacts_with_same_content(
        self, artifact: Union[Artifact, str, uuid.UUID]
    ) -> List[Artifact]:
        """
        Return artifact occurrences that share the same content identity.

        Parameters
        ----------
        artifact : Artifact | str | uuid.UUID
            Artifact instance or artifact id to resolve.

        Returns
        -------
        List[Artifact]
            Artifacts sharing the same ``content_id``. Returns an empty list when
            the artifact is unknown, has no content identity, or the database is not
            configured.
        """
        if not self.db:
            return []
        target = (
            artifact if isinstance(artifact, Artifact) else self.get_artifact(artifact)
        )
        if target is None or target.content_id is None:
            return []
        artifacts = self.db.find_artifacts_by_content_id(target.content_id)
        for item in artifacts:
            set_tracker_ref(item, self)
        return artifacts

    def find_runs_producing_same_content(
        self, artifact: Union[Artifact, str, uuid.UUID]
    ) -> List[str]:
        """
        Return run ids that produced artifacts with the same content identity.

        Parameters
        ----------
        artifact : Artifact | str | uuid.UUID
            Artifact instance or artifact id to resolve.

        Returns
        -------
        List[str]
            Run ids linked to output artifacts sharing the resolved ``content_id``.
        """
        if not self.db:
            return []
        target = (
            artifact if isinstance(artifact, Artifact) else self.get_artifact(artifact)
        )
        if target is None or target.content_id is None:
            return []
        return self.db.find_runs_producing_content(target.content_id)

    def select_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: Union[str, uuid.UUID],
        source: Optional[SchemaProfileSource] = None,
        strict_source: bool = False,
    ) -> Optional[ArtifactSchemaSelection]:
        """
        Resolve schema selection metadata for an artifact.

        Returns selection details (schema_id/source/candidate_count/rule) used for
        explainability and deterministic source selection in shell UX.
        """
        if not self.db:
            raise ValueError(
                "Schema selection requires a configured database (db_path)."
            )
        artifact_uuid = (
            uuid.UUID(artifact_id) if isinstance(artifact_id, str) else artifact_id
        )
        return self.db.select_artifact_schema_for_artifact(
            artifact_id=artifact_uuid,
            prefer_source=source,
            strict_source=strict_source,
        )

    def get_artifact_by_uri(
        self,
        uri: str,
        *,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
    ) -> Optional[Artifact]:
        """
        Find an artifact by its URI.

        Useful for checking if a specific file has been logged,
        or for retrieving artifact metadata by path.

        Parameters
        ----------
        uri : str
            The portable URI to search for (e.g., "inputs://households.csv").
        table_path : Optional[str]
            Optional table path to match.
        array_path : Optional[str]
            Optional array path to match.

        Returns
        -------
        Optional[Artifact]
            The found `Artifact` object, or `None` if no matching artifact is found.
        """
        # 1. Check In-Memory Context (Current Run)
        if self.current_consist:
            for art in self.current_consist.inputs + self.current_consist.outputs:
                if art.container_uri != uri:
                    continue
                if table_path is not None and art.table_path != table_path:
                    continue
                if array_path is not None and art.array_path != array_path:
                    continue
                return art

        # 2. Check Database
        if self.db:
            artifact = self.db.get_artifact_by_uri(
                uri, table_path=table_path, array_path=array_path
            )
            if artifact is not None:
                set_tracker_ref(artifact, self)
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
            set_tracker_ref(artifact, self)
        return artifacts

    @staticmethod
    def _parse_artifact_param_value(raw: str) -> tuple[str, Any]:
        value = raw.strip()
        lowered = value.lower()
        if lowered == "true":
            return "bool", True
        if lowered == "false":
            return "bool", False
        if lowered == "null":
            return "null", None
        try:
            if "." not in value and "e" not in lowered:
                return "num", int(value)
            return "num", float(value)
        except ValueError:
            return "str", value

    def _parse_artifact_param_expression(self, expression: str) -> Dict[str, Any]:
        raw = expression.strip()
        operator = None
        for candidate in (">=", "<=", "="):
            idx = raw.find(candidate)
            if idx > 0:
                operator = candidate
                lhs = raw[:idx].strip()
                rhs = raw[idx + len(candidate) :].strip()
                break

        if operator is None:
            raise ValueError(
                f"Invalid artifact facet predicate {expression!r}. "
                "Expected <key>=<value>, <key>>=<value>, or <key><=<value>."
            )
        if not lhs:
            raise ValueError(
                f"Artifact facet predicate is missing a key: {expression!r}"
            )
        if rhs == "":
            raise ValueError(
                f"Artifact facet predicate is missing a value: {expression!r}"
            )

        namespace = None
        key_path = lhs
        if "." in lhs:
            maybe_namespace, remainder = lhs.split(".", 1)
            if maybe_namespace and remainder:
                namespace = maybe_namespace
                key_path = remainder

        value_kind, value = self._parse_artifact_param_value(rhs)
        if operator in {">=", "<="} and value_kind != "num":
            raise ValueError(
                f"Artifact facet predicate {expression!r} uses {operator} with a "
                "non-numeric value."
            )

        return {
            "namespace": namespace,
            "key_path": key_path,
            "op": operator,
            "kind": value_kind,
            "value": value,
        }

    def find_artifacts_by_params(
        self,
        *,
        params: Optional[Iterable[str]] = None,
        namespace: Optional[str] = None,
        key_prefix: Optional[str] = None,
        artifact_family_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """
        Find artifacts by indexed facet predicates and optional prefix filters.
        """
        if not self.db:
            return []

        predicates = (
            [self._parse_artifact_param_expression(param) for param in params]
            if params
            else []
        )

        artifacts = self.db.find_artifacts_by_facet_params(
            predicates=predicates,
            namespace=namespace,
            key_prefix=key_prefix,
            artifact_family_prefix=artifact_family_prefix,
            limit=limit,
        )
        for artifact in artifacts:
            set_tracker_ref(artifact, self)
        return artifacts

    def get_artifact_kv(
        self,
        artifact: Union[Artifact, uuid.UUID],
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ):
        """
        Retrieve flattened artifact facet KV rows for an artifact.
        """
        if not self.db:
            return []
        artifact_id = artifact.id if isinstance(artifact, Artifact) else artifact
        return self.db.get_artifact_kv(
            artifact_id=artifact_id,
            namespace=namespace,
            prefix=prefix,
            limit=limit,
        )

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
        Resolve the on-disk path for an artifact from a prior run.

        Parameters
        ----------
        artifact : Artifact
            The artifact whose historical location should be resolved.
        run : Run
            The run that originally produced/consumed the artifact.

        Returns
        -------
        Path
            The resolved filesystem path for the artifact in its original run
            workspace.
        """
        if not run:
            return Path(self.resolve_uri(artifact.container_uri))

        old_dir = run.meta.get("_physical_run_dir")

        # Delegate the path math to the FS service
        path_str = self.fs.resolve_historical_path(artifact.container_uri, old_dir)
        return Path(path_str)

    def get_run(self, run_id: str) -> Optional[Run]:
        """
        Retrieve a single Run by its ID from the database.

        Parameters
        ----------
        run_id : str
            The unique identifier of the run to retrieve.

        Returns
        -------
        Optional[Run]
            The Run object if found, or ``None`` if missing or no database is
            configured.
        """
        if self.db:
            return self.db.get_run(run_id)
        return None

    def snapshot_db(
        self, dest_path: str | os.PathLike[str], checkpoint: bool = True
    ) -> Path:
        """
        Snapshot the configured provenance database to a destination path.

        Parameters
        ----------
        dest_path : str | os.PathLike[str]
            Destination path for the snapshot database file.
        checkpoint : bool, default True
            If True, checkpoint the source DB before copying.

        Returns
        -------
        Path
            Snapshot database path.
        """
        if self.db is None:
            raise RuntimeError("Database snapshot requires a configured database.")

        active_run_id = self.current_consist.run.id if self.current_consist else None
        last_completed_run_id: Optional[str] = None
        if (
            self._last_consist is not None
            and self._last_consist.run.status == "completed"
        ):
            last_completed_run_id = self._last_consist.run.id
        else:
            completed_runs = self.db.find_runs(status="completed", limit=1)
            if completed_runs:
                last_completed_run_id = completed_runs[0].id

        return self.db.snapshot_to(
            dest_path=dest_path,
            checkpoint=checkpoint,
            metadata={
                "run_id": active_run_id,
                "last_completed_run_id": last_completed_run_id,
                "cache_epoch": self._cache_epoch,
            },
        )

    def get_run_record(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[ConsistRecord]:
        """
        Load the full run record snapshot from disk.

        This reads the JSON snapshot produced at run time (``consist_runs/<id>.json``)
        and returns the parsed ``ConsistRecord``.

        Parameters
        ----------
        run_id : str
            Run identifier.
        allow_missing : bool, default False
            Return ``None`` if the snapshot file is missing or unreadable instead
            of raising.

        Returns
        -------
        Optional[ConsistRecord]
            The parsed run record, or ``None`` if missing and ``allow_missing``.
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

    def materialize_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "MaterializationResult":
        """
        Restore historical run outputs and return the legacy summary result.

        This method performs the same copy/export work as
        ``hydrate_run_outputs(...)`` but folds the keyed outcomes into the older
        ``MaterializationResult`` buckets. Use it when you only need to know
        which keys were restored, skipped, or failed.

        For new restart or cross-workspace recovery flows, prefer
        ``hydrate_run_outputs(...)`` because it returns detached artifacts and
        directly usable per-key paths.

        Parameters
        ----------
        run_id : str
            Identifier of the historical run whose outputs should be restored.
        target_root : str | Path
            Destination root under which historical relative layout is recreated.
        source_root : str | Path | None, optional
            Optional alternate root to probe before the original historical
            filesystem location. This is useful for archive mirrors.
        keys : Sequence[str] | None, optional
            Optional subset of output keys to restore. ``None`` means all
            outputs linked to the run.
        preserve_existing : bool, default True
            If ``True``, existing destinations are treated as reusable and
            reported in ``skipped_existing``.
        on_missing : {"warn", "raise"}, default "warn"
            Error handling policy for missing source bytes and copy/export
            failures.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether ingested CSV/Parquet artifacts may be exported from DuckDB
            when cold filesystem bytes are unavailable.

        Returns
        -------
        MaterializationResult
            Aggregate summary of the selected outputs.
        """
        return self._recovery_service.materialize_run_outputs(
            run_id,
            target_root=target_root,
            source_root=source_root,
            keys=keys,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )

    def hydrate_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> "HydratedRunOutputsResult":
        """
        Hydrate the output artifacts linked to a historical run.

        This is the first-class historical recovery API. It rematerializes
        selected run outputs into ``target_root`` and returns one keyed result
        per output, including:

        - a detached artifact view for that key
        - the resolved destination path under the new workspace root
        - a status describing how recovery happened
        - whether the detached artifact is immediately resolvable

        Compared with ``materialize_run_outputs(...)``, this method removes the
        need for separate output lookup and post-hoc "is this usable now?"
        checks. The returned detached artifacts preserve provenance metadata but
        are no longer attached to the original tracker state.

        Parameters
        ----------
        run_id : str
            Identifier of the run whose linked outputs should be restored.
        target_root : str | Path
            Destination root under which historical relative layout is recreated.
        source_root : str | Path | None, optional
            Optional alternate root to probe before the original historical
            filesystem location. This is useful for archive mirrors or copied
            cold-storage trees.
        keys : Sequence[str] | None, optional
            Optional subset of output keys to restore. ``None`` means all
            outputs linked to the run. Unknown keys raise ``KeyError`` before
            any copy/export work starts.
        preserve_existing : bool, default True
            If ``True``, existing destinations are treated as reusable and
            returned with ``status="preserved_existing"``.
        on_missing : {"warn", "raise"}, default "warn"
            Error handling policy for missing filesystem bytes or copy/export
            failures. ``"warn"`` returns per-key ``missing_source`` / ``failed``
            statuses. ``"raise"`` aborts on execution-time failures.
        db_fallback : {"never", "if_ingested"}, default "if_ingested"
            Whether ingested csv/parquet artifacts may be exported from DuckDB
            when cold filesystem bytes are unavailable.

        Returns
        -------
        HydratedRunOutputsResult
            Keyed hydration outcomes for the selected historical outputs.

        Notes
        -----
        Status values have the following meanings:

        - ``materialized_from_filesystem``: copied from historical cold bytes
        - ``materialized_from_db``: exported from DuckDB for an ingested output
        - ``preserved_existing``: destination already existed and was reused
        - ``skipped_unmapped``: no safe historical relative-path mapping exists
        - ``missing_source``: historical bytes were unavailable and not
          recoverable
        - ``failed``: recovery was attempted but failed

        Examples
        --------
        Restore two outputs into a new workspace root and inspect statuses:

        >>> hydrated = tracker.hydrate_run_outputs(
        ...     "prior_run_id",
        ...     keys=["persons", "households"],
        ...     target_root="restored",
        ... )
        >>> hydrated["persons"].status
        'materialized_from_filesystem'
        >>> hydrated.paths["persons"]
        PosixPath('.../restored/.../persons.parquet')

        Use the detached hydrated artifact directly:

        >>> persons = hydrated["persons"]
        >>> persons.resolvable
        True
        >>> persons.artifact.as_path()
        PosixPath('.../restored/.../persons.parquet')
        """
        return self._recovery_service.hydrate_run_outputs(
            run_id,
            target_root=target_root,
            source_root=source_root,
            keys=keys,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )

    def get_config_bundle(
        self,
        run_id: str,
        *,
        adapter: str | None = None,
        role: str = "bundle",
        allow_missing: bool = False,
    ) -> Path | None:
        """
        Resolve a config artifact path for a run by role.

        This helper scans run-linked artifacts and selects those with
        ``artifact.meta["config_role"] == role``. When ``adapter`` is provided,
        matching uses existing adapter identity conventions:
        ``run.meta["config_adapter"]`` and/or artifact metadata
        (``artifact.meta["config_adapter"]`` or ``artifact.meta["adapter"]``).

        If multiple artifacts match, selection is deterministic: sort by
        ``(artifact.key, artifact.created_at, artifact.id)`` and return the first.
        """
        artifacts = self.get_artifacts_for_run(run_id)
        input_artifacts = list(artifacts.inputs.values())

        matching: list[Artifact] = []
        for artifact in input_artifacts:
            meta = artifact.meta if isinstance(artifact.meta, dict) else {}
            if meta.get("config_role") == role:
                matching.append(artifact)

        run = self.get_run(run_id)
        run_adapter: str | None = None
        if run is not None and isinstance(run.meta, dict):
            candidate = run.meta.get("config_adapter")
            if isinstance(candidate, str) and candidate:
                run_adapter = candidate

        if adapter is not None:
            if run_adapter is not None and run_adapter != adapter:
                matching = []
            else:
                filtered: list[Artifact] = []
                for artifact in matching:
                    meta = artifact.meta if isinstance(artifact.meta, dict) else {}
                    artifact_adapters: list[str] = []
                    for key in ("config_adapter", "adapter"):
                        value = meta.get(key)
                        if isinstance(value, str) and value:
                            artifact_adapters.append(value)
                    if artifact_adapters:
                        if adapter in artifact_adapters:
                            filtered.append(artifact)
                    elif run_adapter == adapter:
                        filtered.append(artifact)
                matching = filtered

        if matching:
            selected = sorted(
                matching,
                key=lambda artifact: (
                    artifact.key,
                    artifact.created_at.isoformat() if artifact.created_at else "",
                    str(artifact.id),
                ),
            )[0]
            resolved = Path(self.resolve_uri(selected.container_uri))
            if resolved.exists():
                return resolved

            if allow_missing:
                return None
            raise FileNotFoundError(
                "Config artifact was found but the resolved file is missing for "
                f"run_id={run_id!r}, role={role!r}, key={selected.key!r}: {resolved!s}. "
                "Check path mounts or regenerate config artifacts for this run."
            )

        if allow_missing:
            return None
        adapter_hint = f", adapter={adapter!r}" if adapter is not None else ""
        raise FileNotFoundError(
            "No config artifact found for "
            f"run_id={run_id!r}, role={role!r}{adapter_hint}. "
            "Ensure config artifacts were logged with meta['config_role'] and, when "
            "adapter filtering is requested, run.meta['config_adapter'] and/or "
            "artifact.meta['config_adapter'|'adapter'] match."
        )

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts:
        """
        Retrieve inputs and outputs for a specific run, organized by key.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        RunArtifacts
            Container with ``inputs`` and ``outputs`` dicts. Returns empty
            collections if the database is not configured.
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
            set_tracker_ref(artifact, self)
        if run_id != current_run_id:
            self._run_artifacts_cache[run_id] = artifacts
            if len(self._run_artifacts_cache) > self._run_artifacts_cache_max_entries:
                self._run_artifacts_cache.pop(next(iter(self._run_artifacts_cache)))
        return artifacts

    def get_run_outputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return output artifacts for a run, keyed by artifact key.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Dict[str, Artifact]
            Output artifacts keyed by artifact key. Returns an empty dict if the
            database is not configured or the run is unknown.
        """
        return self.get_artifacts_for_run(run_id).outputs

    def get_run_result(
        self,
        run_id: str,
        *,
        keys: Optional[Iterable[str]] = None,
        validate: Literal["lazy", "strict", "none"] = "lazy",
    ) -> RunResult:
        """
        Build a ``RunResult`` view for a historical run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        keys : Optional[Iterable[str]], optional
            Optional subset of output keys to include. When provided, every key
            must exist in the historical run outputs.
        validate : {"lazy", "strict", "none"}, default "lazy"
            Output validation policy.
            - ``lazy`` / ``none``: no filesystem existence checks
            - ``strict``: require non-ingested output files to exist on disk

        Returns
        -------
        RunResult
            Historical run metadata plus selected outputs.

        Raises
        ------
        KeyError
            If ``run_id`` is unknown or requested ``keys`` are missing.
        ValueError
            If ``validate`` is invalid or ``keys`` contain non-string values.
        FileNotFoundError
            If ``validate='strict'`` and a selected non-ingested output is missing.
        """
        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"Run {run_id!r} was not found.")

        outputs = self.get_run_outputs(run_id)

        selected_outputs: Dict[str, Artifact]
        if keys is None:
            selected_outputs = dict(outputs)
        else:
            key_list = list(keys)
            if any(not isinstance(key, str) for key in key_list):
                raise ValueError("keys must contain only strings.")
            missing = sorted(key for key in key_list if key not in outputs)
            if missing:
                missing_str = ", ".join(repr(key) for key in missing)
                available = ", ".join(repr(key) for key in sorted(outputs)) or "<none>"
                raise KeyError(
                    f"Run {run_id!r} missing requested output keys: {missing_str}. "
                    f"Available keys: {available}."
                )
            selected_outputs = {key: outputs[key] for key in key_list}

        validation_policy = str(validate).lower()
        if validation_policy not in {"lazy", "strict", "none"}:
            raise ValueError("validate must be one of: 'lazy', 'strict', 'none'.")

        if validation_policy == "strict":
            missing_paths: list[str] = []
            for key, artifact in selected_outputs.items():
                if artifact.meta.get("is_ingested", False):
                    continue
                resolved = Path(self.resolve_uri(artifact.container_uri))
                if not resolved.exists():
                    missing_paths.append(f"{key!r} -> {resolved!s}")
            if missing_paths:
                details = "; ".join(missing_paths)
                raise FileNotFoundError(
                    f"Run {run_id!r} has missing output files: {details}"
                )

        cache_hit = (
            bool(run.meta.get("cache_hit")) if isinstance(run.meta, dict) else False
        )
        return RunResult(run=run, outputs=selected_outputs, cache_hit=cache_hit)

    def get_run_inputs(self, run_id: str) -> Dict[str, Artifact]:
        """
        Return input artifacts for a run, keyed by artifact key.

        Parameters
        ----------
        run_id : str
            Run identifier.

        Returns
        -------
        Dict[str, Artifact]
            Input artifacts keyed by artifact key. Returns an empty dict if the
            database is not configured or the run is unknown.
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

        Returns
        -------
        Any
            Loaded artifact data.
        """
        artifact = self.get_run_artifact(run_id, key=key, direction="output")
        if artifact is None:
            raise ValueError(
                f"No output artifact found for run_id={run_id!r} key={key!r}."
            )
        return self.load(artifact, **kwargs)

    def find_matching_run(
        self,
        config_hash: str,
        input_hash: str,
        git_hash: str,
        *,
        signature: Optional[str] = None,
    ) -> Optional[Run]:
        """
        Find a previously completed run that matches the identity hashes.

        Parameters
        ----------
        config_hash : str
            Hash of the canonicalized config for the run.
        input_hash : str
            Hash of the run inputs.
        git_hash : str
            Git commit hash captured with the run.
        signature : str | None, optional
            Composite run signature. When provided, Consist attempts a direct
            signature lookup first and falls back to the legacy component-hash
            lookup for compatibility with older rows.

        Returns
        -------
        Optional[Run]
            The matching run, or ``None`` if not found or if no database is configured.
        """
        if self.db:
            if signature:
                matched = self.db.find_run_by_signature(signature)
                if matched is not None:
                    return matched
            return self.db.find_matching_run(config_hash, input_hash, git_hash)
        return None

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]:
        """
        Return recent completed runs for a model, newest first.
        """
        if self.db:
            return self.db.find_recent_completed_runs_for_model(model_name, limit=limit)
        return []

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

    def _flush_run_snapshot(self, run: Run) -> None:
        """
        Flush JSON snapshots for a specific run record.

        This supports post-run metadata updates (for example adapter-managed
        identity metadata) when no active run context exists.
        """
        if self.current_consist is not None and self.current_consist.run.id == run.id:
            self._flush_json()
            return
        if self._last_consist is not None and self._last_consist.run.id == run.id:
            self.persistence.flush_record_json(self._last_consist)

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

    def _sync_artifact_to_db(
        self,
        artifact: Artifact,
        direction: str,
        *,
        profile_label: Optional[str] = None,
    ) -> None:
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
        self.persistence.sync_artifact(
            artifact,
            direction,
            profile_label=profile_label,
        )

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
        for field in ("stage", "phase"):
            value = normalized.get(field)
            if isinstance(value, str) or value is None:
                setattr(self.current_consist.run, field, value)
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

        Parameters
        ----------
        **kwargs : Any
            Step metadata (e.g., ``outputs``, ``tags``, ``cache_mode``,
            ``inject_context``) to attach to the function.

        Returns
        -------
        Callable
            A decorator that returns the original function with attached metadata.
        """
        return define_step_decorator(**kwargs)

    @property
    def last_run(self) -> Optional[ConsistRecord]:
        """
        Return the most recent run record observed by this tracker.

        Returns
        -------
        Optional[ConsistRecord]
            The last completed/failed run record for this tracker instance,
            or `None` if no run has executed yet.
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
            Returns ``False`` if no run is active.
        """
        return bool(
            self.current_consist and self.current_consist.cached_run is not None
        )

    def suspend_cache_options(self) -> ActiveRunCacheOptions:
        """
        Suspend active-run cache options and reset them to defaults.

        This is useful for helper functions that want default cache behavior
        without mutating the caller's options.

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

        This should typically be paired with a prior ``suspend_cache_options``
        call to restore the caller's cache behavior.

        Parameters
        ----------
        options : ActiveRunCacheOptions
            Cache options to restore (usually returned by
            ``suspend_cache_options``).
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
                    artifact.abs_path = self.resolve_uri(artifact.container_uri)
                except Exception:
                    pass
            set_tracker_ref(artifact, self)
        return artifact
