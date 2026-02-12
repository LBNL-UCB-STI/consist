from collections.abc import Mapping as MappingABC
from contextlib import contextmanager
import importlib
import logging
import os
import warnings
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Type,
    TypeVar,
    TypeGuard,
    Union,
    cast,
    overload,
    runtime_checkable,
)
import weakref

import pandas as pd
import duckdb
from sqlalchemy import and_, case, func, select as sa_select, Subquery
from sqlalchemy.sql import Executable
from sqlmodel import SQLModel, Session, col, select

# Internal imports
from consist.core.context import (
    get_active_tracker,
    get_default_tracker,
    set_default_tracker,
    use_tracker as _use_tracker,
)
from consist.core.decorators import (
    define_step as define_step_decorator,
    require_runtime_kwargs as require_runtime_kwargs_decorator,
)
from consist.core.drivers import ARRAY_DRIVERS, TABLE_DRIVERS, ArrayInfo, TableInfo
from consist.core.noop import NoopRunContext, NoopScenarioContext
from consist.core.run_options import raise_legacy_policy_kwargs_error
from consist.core.views import _quote_ident, create_view_model
from consist.core.workflow import OutputCapture, RunContext
from consist.models.artifact import Artifact, get_tracker_ref
from consist.models.run import ConsistRecord, Run, RunResult, resolve_run_result_output
from consist.models.run_config_kv import RunConfigKV
from consist.core.tracker import Tracker
from consist.types import (
    ArtifactRef,
    CacheOptions,
    DriverType,
    ExecutionOptions,
    OutputPolicyOptions,
)

if TYPE_CHECKING:
    import geopandas
    import xarray
    from consist.core.config_canonicalization import ConfigPlan
    from consist.core.step_context import StepContext

# Import loaders for specific formats
xr: Optional[ModuleType]
try:
    xr = importlib.import_module("xarray")
except ImportError:
    xr = None

tables: Optional[ModuleType]
try:
    tables = importlib.import_module("tables")
except ImportError:
    tables = None

gpd: Optional[ModuleType]
try:
    gpd = importlib.import_module("geopandas")
except ImportError:
    gpd = None

T = TypeVar("T", bound=SQLModel)


class OpenMatrixFileLike(Protocol):
    """Minimal file-like surface for OpenMatrix/HDF5 access."""

    def __contains__(self, key: object) -> bool: ...

    def __getitem__(self, key: str) -> Any: ...

    def close(self) -> None: ...


LoadResult = Union[
    duckdb.DuckDBPyRelation,
    pd.DataFrame,
    pd.Series,
    "geopandas.GeoDataFrame",
    "xarray.Dataset",
    pd.HDFStore,
]


@runtime_checkable
class ArtifactLike(Protocol):
    """
    Structural typing for artifact-like objects.

    Any object with `driver`, `container_uri`, `meta` attributes and a `path` property
    can be used where ArtifactLike is expected.
    """

    id: Any
    key: str
    container_uri: str
    table_path: Optional[str]
    array_path: Optional[str]
    meta: Dict[str, Any]

    @property
    def driver(self) -> str: ...

    @property
    def path(self) -> Path: ...


class DataFrameArtifact(ArtifactLike, Protocol):
    """Artifact that loads as DuckDB Relation."""

    @property
    def driver(self) -> Literal["parquet", "csv", "h5_table"]: ...


class TabularArtifact(ArtifactLike, Protocol):
    """Artifact that loads as tabular data (DuckDB Relation)."""

    @property
    def driver(self) -> Literal["parquet", "csv", "h5_table", "json"]: ...


class JsonArtifact(ArtifactLike, Protocol):
    """Artifact that loads as DuckDB Relation from JSON."""

    @property
    def driver(self) -> Literal["json"]: ...


class ZarrArtifact(ArtifactLike, Protocol):
    """Artifact that loads as xarray.Dataset."""

    @property
    def driver(self) -> Literal["zarr"]: ...


class HdfStoreArtifact(ArtifactLike, Protocol):
    """Artifact that loads as pandas HDFStore."""

    @property
    def driver(self) -> Literal["h5", "hdf5"]: ...


class NetCdfArtifact(ArtifactLike, Protocol):
    """Artifact that loads as xarray.Dataset (NetCDF format)."""

    @property
    def driver(self) -> Literal["netcdf"]: ...


class OpenMatrixArtifact(ArtifactLike, Protocol):
    """Artifact that loads as xarray.Dataset (OpenMatrix format)."""

    @property
    def driver(self) -> Literal["openmatrix"]: ...


class SpatialArtifact(ArtifactLike, Protocol):
    """Artifact that loads as GeoDataFrame (spatial formats)."""

    @property
    def driver(self) -> Literal["geojson", "shapefile", "geopackage"]: ...


def view(model: Type[T], name: Optional[str] = None) -> Type[T]:
    """
    Create a SQLModel class backed by a Consist hybrid view.

    This is a convenience wrapper around the view factory that lets you define
    a SQLModel schema for a concept (e.g., a canonicalized config table, an
    ingested dataset, or a computed artifact view) and then query it as a normal
    SQLModel table. The returned class is a dynamic subclass with `table=True`
    that points at a database view, so you can use it in `sqlmodel.Session`
    queries without creating a physical table.

    If you need explicit control over view naming or want to create multiple
    named views for the same concept, use `Tracker.create_view(...)` or
    `Tracker.view(...)` directly.

    Parameters
    ----------
    model : Type[T]
        Base SQLModel describing the schema (columns and types).
    name : Optional[str], optional
        Optional override for the generated view name (defaults to the model's
        table name).

    Returns
    -------
    Type[T]
        SQLModel subclass with ``table=True`` pointing at the hybrid view.

    Examples
    --------
    ```python
    from sqlmodel import Session, select
    from consist import view
    from consist.models.activitysim import ActivitySimConstantsCache

    # Create a dynamic view class for querying constants
    ConstantsView = view(ActivitySimConstantsCache)

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(ConstantsView)
            .where(ConstantsView.key == "sample_rate")
        ).all()
    ```
    """
    return create_view_model(model, name)


# --- Core Access ---


def _resolve_tracker(tracker: Optional["Tracker"]) -> "Tracker":
    if tracker is not None:
        return tracker
    default = get_default_tracker()
    if default is None:
        raise RuntimeError(
            "No default tracker configured for consist.run(). Provide one of:\n"
            "  1. Pass tracker= argument: consist.run(fn=..., tracker=my_tracker)\n"
            "  2. Set default context: with consist.use_tracker(my_tracker): consist.run(...)\n"
            "  3. Use Tracker directly: my_tracker.run(fn=...)"
        )
    return default


@contextmanager
def use_tracker(tracker: "Tracker") -> Iterator["Tracker"]:
    """
    Set a fallback (default) tracker for Consist API entrypoints.

    This configures which tracker is used by consist.run(), consist.start_run(), etc.
    when called outside an active run context (i.e., when the tracker stack is empty).
    Once inside a run, the tracker becomes "active" via push_tracker() and is accessed
    by logging functions like consist.log_artifact().
    """
    with _use_tracker(tracker) as tr:
        yield tr


def set_current_tracker(tracker: Optional["Tracker"]) -> Optional["Tracker"]:
    """
    Set the default (fallback) tracker used by Consist entrypoints.

    Entrypoints like `consist.run()`, `consist.start_run()`, and `consist.scenario()`
    use this tracker if they are called outside of an active run context and no
    explicit `tracker=` argument is provided.

    Parameters
    ----------
    tracker : Optional[Tracker]
        The tracker instance to set as the default, or `None` to clear.

    Returns
    -------
    Optional[Tracker]
        The previously configured default tracker, if any.
    """
    return set_default_tracker(tracker)


def define_step(
    *,
    model: Optional[str] = None,
    name_template: Optional[str] = None,
    outputs: Optional[list[str]] = None,
    schema_outputs: Optional[list[str]] = None,
    output_paths: Optional[Mapping[str, Any]] = None,
    inputs: Optional[Union[Mapping[str, Any], Iterable[Any]]] = None,
    input_keys: Optional[Union[Iterable[str], str]] = None,
    optional_input_keys: Optional[Union[Iterable[str], str]] = None,
    config: Optional[Dict[str, Any]] = None,
    config_plan: Optional[
        Union["ConfigPlan", Callable[["StepContext"], "ConfigPlan"]]
    ] = None,
    facet: Optional[Any] = None,
    facet_index: Optional[bool] = None,
    cache_mode: Optional[str] = None,
    cache_hydration: Optional[str] = None,
    cache_version: Optional[int] = None,
    validate_cached_outputs: Optional[str] = None,
    load_inputs: Optional[bool] = None,
    hash_inputs: Optional[Any] = None,
    tags: Optional[list[str]] = None,
    facet_from: Optional[list[str]] = None,
    facet_schema_version: Optional[Union[str, int]] = None,
    description: Optional[str] = None,
    **extra: Any,
):
    """
    Attach metadata to a function without changing execution behavior.

    This decorator lets you attach defaults such as ``outputs`` or ``tags`` to a
    function. ``Tracker.run`` and ``ScenarioContext.run`` read this metadata.
    Callable values are resolved at runtime with a StepContext.
    """
    return define_step_decorator(
        model=model,
        name_template=name_template,
        outputs=outputs,
        schema_outputs=schema_outputs,
        output_paths=output_paths,
        inputs=inputs,
        input_keys=input_keys,
        optional_input_keys=optional_input_keys,
        config=config,
        config_plan=config_plan,
        facet=facet,
        facet_index=facet_index,
        cache_mode=cache_mode,
        cache_hydration=cache_hydration,
        cache_version=cache_version,
        validate_cached_outputs=validate_cached_outputs,
        load_inputs=load_inputs,
        hash_inputs=hash_inputs,
        tags=tags,
        facet_from=facet_from,
        facet_schema_version=facet_schema_version,
        description=description,
        **extra,
    )


def require_runtime_kwargs(*names: str) -> Callable[[Callable[..., Any]], Callable]:
    """
    Enforce the presence of required runtime_kwargs when a step is executed.

    Use this decorator to catch missing runtime-only inputs early, before
    executing a run. It validates that each required name is present in the
    `runtime_kwargs` passed to `Tracker.run` or `ScenarioContext.run`.
    """
    return require_runtime_kwargs_decorator(*names)


@contextmanager
def scenario(
    name: str,
    tracker: Optional["Tracker"] = None,
    *,
    enabled: bool = True,
    **kwargs: Any,
):
    """
    Context manager for grouping multiple execution steps into a semantic scenario.

    A scenario creates a parent "header" run that aggregates metadata, lineage,
    and artifacts from all nested steps. This enables multi-stage simulations
    to be tracked as a single unit while maintaining fine-grained provenance
    for each internal component.

    Parameters
    ----------
    name : str
        Unique identifier or name for the scenario header.
    tracker : Optional[Tracker], optional
        The Tracker instance to use for persistence. Defaults to the active
        global tracker.
    enabled : bool, default True
        If False, returns a no-op context that executes functions without
        recording provenance, preserving the Scenario API ergonomics for debugging.
    **kwargs : Any
        Additional metadata (e.g., tags, config) forwarded to the header run.

    Yields
    ------
    ScenarioContext
        An active scenario context with an internal Coupler for artifact chaining.
    """
    if not enabled:
        yield NoopScenarioContext(name, **kwargs)
        return
    tr = _resolve_tracker(tracker)
    with tr.scenario(name, **kwargs) as sc:
        yield sc


@contextmanager
def noop_scenario(name: str, **kwargs: Any):
    """
    Creates a scenario context that executes without provenance tracking.

    This is useful for debugging or running simulations where you want the
    ergonomics of the Consist scenario API (like the Coupler and RunResult)
    but do not want to record any metadata or artifacts to the database.

    Parameters
    ----------
    name : str
        Name of the scenario (for display/logging purposes).
    **kwargs : Any
        Additional arguments forwarded to the noop context.
    """
    yield NoopScenarioContext(name, **kwargs)


@contextmanager
def single_step_scenario(
    name: str,
    step_name: Optional[str] = None,
    tracker: Optional["Tracker"] = None,
    **kwargs: Any,
):
    """
    Convenience wrapper that exposes a single step scenario.

    Parameters
    ----------
    name : str
        Name of the scenario header.
    step_name : Optional[str], optional
        Name for the single step; defaults to ``name``.
    tracker : Optional[Tracker], optional
        Tracker to execute the scenario; defaults to the active tracker.
    **kwargs : Any
        Arguments forwarded to ``Tracker.scenario``.

    Yields
    ------
    ScenarioContext
        Scenario context manager for the single step.
    """
    tr = _resolve_tracker(tracker)
    with tr.scenario(name, **kwargs) as sc:
        with sc.trace(step_name or name):
            yield sc


@contextmanager
def start_run(
    run_id: str,
    model: str,
    tracker: Optional["Tracker"] = None,
    **kwargs: Any,
) -> Iterator["Tracker"]:
    """
    Initiate and manage a Consist run.

    This context manager marks the beginning of a discrete unit of work (a "run").
    All artifacts logged within this context will be associated with this run.

    Parameters
    ----------
    run_id : str
        A unique identifier for this run.
    model : str
        The name of the model or system being run.
    tracker : Optional[Tracker]
        The tracker instance to use. Defaults to the active global tracker.
    **kwargs : Any
        Additional parameters for the run, such as `tags`, `config`, or `inputs`.

    Yields
    ------
    Tracker
        The active tracker instance.

    Example
    -------
    ```python
    with consist.start_run("run_123", "my_model"):
        consist.log_artifact("data.csv", "input_data")
    ```
    """
    tr = _resolve_tracker(tracker)
    with tr.start_run(run_id=run_id, model=model, **kwargs) as active:
        yield active


def run(
    fn: Optional[Callable[..., Any]] = None,
    name: Optional[str] = None,
    *,
    tracker: Optional["Tracker"] = None,
    cache_options: Optional[CacheOptions] = None,
    output_policy: Optional[OutputPolicyOptions] = None,
    execution_options: Optional[ExecutionOptions] = None,
    **kwargs: Any,
) -> RunResult:
    """
    Execute a computational task as a tracked and cached Consist run.

    This high-level entrypoint encapsulates the execution of a callable within
    a managed provenance context. It automates the lifecycle of the run,
    including signature computation for cache identity, artifact capture,
    and result persistence.

    Parameters
    ----------
    fn : Optional[Callable]
        The computational function to execute.
    name : Optional[str]
        A semantic identifier for the run. If omitted, the function's
        `__name__` is utilized as the default.
    tracker : Optional[Tracker]
        The Tracker instance responsible for provenance and caching.
        If None, the active global tracker is resolved.
    cache_options : Optional[CacheOptions]
        Grouped cache controls for run execution.
    output_policy : Optional[OutputPolicyOptions]
        Grouped output mismatch/missing policy controls.
    execution_options : Optional[ExecutionOptions]
        Grouped runtime execution controls.
    **kwargs : Any
        Arguments forwarded to `Tracker.run`, including `inputs`, `config`,
        `tags`, and other core run metadata.

    Returns
    -------
    RunResult
        A container holding the function's return value and the
        immutable `Run` record.
    """
    raise_legacy_policy_kwargs_error(
        api_name="consist.run",
        kwargs=kwargs,
    )
    tr = _resolve_tracker(tracker)
    return tr.run(
        fn=fn,
        name=name,
        cache_options=cache_options,
        output_policy=output_policy,
        execution_options=execution_options,
        **kwargs,
    )


def ref(run_result: RunResult, key: Optional[str] = None) -> Artifact:
    """
    Select a specific output artifact from a ``RunResult``.

    Parameters
    ----------
    run_result : RunResult
        Result object returned by ``consist.run``/``tracker.run``.
    key : Optional[str], optional
        Output key to select. If omitted, ``run_result`` must have exactly one
        output.

    Returns
    -------
    Artifact
        Selected artifact reference, suitable for ``inputs=...``.
    """
    return resolve_run_result_output(run_result, key=key)


def _set_ref_mapping_value(
    output_map: Dict[str, Artifact], key: str, artifact: Artifact
) -> None:
    if key in output_map:
        raise ValueError(
            f"Duplicate input key {key!r} while building refs mapping. "
            "Use unique keys or keyword aliases in consist.refs(...)."
        )
    output_map[key] = artifact


def _add_run_result_refs(
    output_map: Dict[str, Artifact],
    run_result: RunResult,
    keys: Optional[Iterable[str]],
) -> None:
    if keys is None:
        for output_key, artifact in run_result.outputs.items():
            _set_ref_mapping_value(output_map, output_key, artifact)
        return

    for key in keys:
        if not isinstance(key, str):
            raise TypeError(
                f"consist.refs(...) output keys must be strings (got {type(key)})."
            )
        _set_ref_mapping_value(
            output_map, key, resolve_run_result_output(run_result, key=key)
        )


def refs(
    *selectors: Union[RunResult, tuple[Any, ...], str, Mapping[str, str]],
    **aliases: Union[RunResult, tuple[RunResult, str]],
) -> Dict[str, Artifact]:
    """
    Build an ``inputs={...}`` mapping from one or more ``RunResult`` objects.

    Supported forms
    ---------------
    - Single result shorthand:
      ``consist.refs(prep, "households", "persons")``
    - Single result alias mapping:
      ``consist.refs(prep, {"hh": "households", "pp": "persons"})``
    - Multi-result positional groups:
      ``consist.refs((prep, "households"), (skim, "skims"))``
    - Keyword aliases:
      ``consist.refs(hh=(prep, "households"), skims=(skim, "skims"))``

    Returns
    -------
    Dict[str, Artifact]
        Mapping suitable for ``inputs=...``.
    """
    result: Dict[str, Artifact] = {}

    if selectors:
        first = selectors[0]
        if isinstance(first, RunResult):
            if len(selectors) == 1:
                _add_run_result_refs(result, first, keys=None)
            elif len(selectors) == 2 and isinstance(selectors[1], MappingABC):
                alias_map = cast(Mapping[Any, Any], selectors[1])
                if not alias_map:
                    raise ValueError(
                        "consist.refs(..., {alias: key}) alias mapping cannot be empty."
                    )
                for alias, key in alias_map.items():
                    if not isinstance(alias, str):
                        raise TypeError(
                            "consist.refs(..., {alias: key}) alias mapping keys must "
                            f"be strings (got {type(alias)})."
                        )
                    if not isinstance(key, str):
                        raise TypeError(
                            "consist.refs(..., {alias: key}) alias mapping values must "
                            f"be strings output keys (got {type(key)})."
                        )
                    _set_ref_mapping_value(
                        result, alias, resolve_run_result_output(first, key=key)
                    )
            else:
                keys = selectors[1:]
                if not all(isinstance(key, str) for key in keys):
                    raise TypeError(
                        "When consist.refs(...) starts with a RunResult, remaining "
                        "positional arguments must be output keys (str), or a single "
                        "alias mapping {alias: key}."
                    )
                _add_run_result_refs(result, first, keys=cast(Iterable[str], keys))
        else:
            for selector in selectors:
                if not isinstance(selector, tuple) or not selector:
                    raise TypeError(
                        "Positional selectors for consist.refs(...) must be non-empty "
                        "tuples of the form (run_result, *keys)."
                    )
                run_result = selector[0]
                if not isinstance(run_result, RunResult):
                    raise TypeError(
                        "Tuple selectors for consist.refs(...) must start with "
                        f"RunResult (got {type(run_result)})."
                    )
                _add_run_result_refs(
                    result,
                    run_result,
                    keys=cast(Optional[Iterable[str]], selector[1:] or None),
                )

    for alias, selector in aliases.items():
        if isinstance(selector, RunResult):
            artifact = resolve_run_result_output(selector)
        elif (
            isinstance(selector, tuple)
            and len(selector) == 2
            and isinstance(selector[0], RunResult)
            and isinstance(selector[1], str)
        ):
            artifact = resolve_run_result_output(selector[0], key=selector[1])
        else:
            raise TypeError(
                "Keyword selectors for consist.refs(...) must be RunResult or "
                "(RunResult, key)."
            )
        _set_ref_mapping_value(result, alias, artifact)

    if not result:
        raise ValueError("consist.refs(...) requires at least one selector.")

    return result


@contextmanager
def trace(
    name: str,
    *,
    tracker: Optional["Tracker"] = None,
    **kwargs: Any,
) -> Iterator["Tracker"]:
    """
    Create a nested tracing context within an active run.

    Use `trace` to break down a large run into smaller, logical steps. Each
    traced step is recorded as a sub-run with its own inputs and outputs.

    Parameters
    ----------
    name : str
        Semantic name for this step/trace.
    tracker : Optional[Tracker]
        The tracker instance to use.
    **kwargs : Any
        Additional metadata or parameters for this trace.

    Yields
    ------
    Tracker
        The active tracker instance.
    """
    tr = _resolve_tracker(tracker)
    with tr.trace(name=name, **kwargs) as active:
        yield active


def current_tracker() -> "Tracker":
    """
    Retrieves the active `Tracker` instance from the global context.

    If no run is active, this function falls back to the default tracker (if set
    via `consist.use_tracker` or `consist.set_current_tracker`).

    Returns
    -------
    Tracker
        The `Tracker` instance currently active in the execution context.

    Raises
    ------
    RuntimeError
        If no `Tracker` is active in the current context and no default tracker
        has been configured.
    """
    try:
        return get_active_tracker()
    except RuntimeError:
        default = get_default_tracker()
        if default is None:
            raise RuntimeError(
                "No active Consist run and no default tracker configured. "
                "Use consist.set_current_tracker(tracker) or "
                "with consist.use_tracker(tracker): ..."
            ) from None
        return default


def current_run() -> Optional[Run]:
    """
    Return the active `Run` record if one is in progress, otherwise ``None``.

    A `Run` record contains metadata about the current execution, such as its
    unique ID, model name, and start time.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return None
    current_consist = tracker.current_consist
    return current_consist.run if current_consist else None


def current_consist() -> Optional[ConsistRecord]:
    """
    Return the active `ConsistRecord` if one is in progress, otherwise ``None``.

    The `ConsistRecord` is the internal state object that tracks the active
    run's inputs, outputs, and metadata during execution.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return None
    return tracker.current_consist


def _require_active_run_tracker() -> "Tracker":
    """
    Return the tracker for the current active run context.

    Raises
    ------
    RuntimeError
        If there is no active run context.
    """
    tracker = get_active_tracker()
    if tracker.current_consist is None:
        raise RuntimeError(
            "No active Consist run found. "
            "Ensure you are within a run or active scenario step."
        )
    return tracker


def output_dir(namespace: Optional[str] = None) -> Path:
    """
    Resolve the managed output directory for the active run context.

    Parameters
    ----------
    namespace : Optional[str], optional
        Optional relative subdirectory under the active run's managed output
        directory.

    Returns
    -------
    Path
        Absolute path to the managed output directory.

    Raises
    ------
    RuntimeError
        If called outside an active run context.
    """
    return RunContext(_require_active_run_tracker()).output_dir(namespace=namespace)


def output_path(key: str, ext: str = "parquet") -> Path:
    """
    Resolve a deterministic managed output path for the active run context.

    Parameters
    ----------
    key : str
        Artifact key used as the output filename stem.
    ext : str, default "parquet"
        File extension to append to the output filename.

    Returns
    -------
    Path
        Absolute managed output path for the current run.

    Raises
    ------
    RuntimeError
        If called outside an active run context.
    """
    return RunContext(_require_active_run_tracker()).output_path(key=key, ext=ext)


# --- Cache helpers ---


def cached_artifacts(direction: str = "output") -> Dict[str, Artifact]:
    """
    Returns hydrated cached artifacts for the active run, if any.

    Parameters
    ----------
    direction : str, default "output"
        "output" or "input".

    Returns
    -------
    Dict[str, Artifact]
        Mapping from artifact key to Artifact, or empty dict if no cache hit.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return {}
    return tracker.cached_artifacts(direction=direction)


def cached_output(key: Optional[str] = None) -> Optional[Artifact]:
    """
    Fetch a hydrated cached output artifact for the active run.

    Parameters
    ----------
    key : Optional[str]
        Specific artifact key to look up; defaults to the first available artifact.

    Returns
    -------
    Optional[Artifact]
        Cached artifact instance or ``None`` if no cache hit exists.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return None
    return tracker.cached_output(key=key)


def get_run_result(
    run_id: str,
    *,
    keys: Optional[Iterable[str]] = None,
    validate: Literal["lazy", "strict", "none"] = "lazy",
    tracker: Optional["Tracker"] = None,
) -> RunResult:
    """
    Retrieve a historical run as a ``RunResult`` with selected outputs.

    Parameters
    ----------
    run_id : str
        Identifier of the historical run.
    keys : Optional[Iterable[str]], optional
        Optional output keys to include. If omitted, includes all outputs.
    validate : {"lazy", "strict", "none"}, default "lazy"
        Output validation policy forwarded to ``Tracker.get_run_result``.
    tracker : Optional[Tracker], optional
        Tracker instance to query. If omitted, resolves the active/default tracker.

    Returns
    -------
    RunResult
        Historical run metadata plus selected outputs.
    """
    tr = _resolve_tracker(tracker)
    return tr.get_run_result(run_id, keys=keys, validate=validate)


def get_artifact(
    run_id: str,
    key: Optional[str] = None,
    key_contains: Optional[str] = None,
    direction: str = "output",
) -> Optional[Artifact]:
    """
    Retrieve a single artifact from a historical run.

    Parameters
    ----------
    run_id : str
        Identifier of the run that produced the artifact.
    key : Optional[str], optional
        Exact artifact key to match.
    key_contains : Optional[str], optional
        Substring filter for artifact keys.
    direction : str, default "output"
        Either "input" or "output".

    Returns
    -------
    Optional[Artifact]
        Matching artifact or ``None`` if not found.
    """
    return current_tracker().get_run_artifact(
        run_id, key=key, key_contains=key_contains, direction=direction
    )


def register_artifact_facet_parser(
    prefix: str,
    parser_fn: Callable[[str], Optional[Any]],
    *,
    tracker: Optional["Tracker"] = None,
) -> None:
    """
    Register a key-prefix parser for deriving artifact facets at log time.

    The parser is invoked when logging an artifact without an explicit ``facet=``.
    """
    tr = _resolve_tracker(tracker)
    tr.register_artifact_facet_parser(prefix, parser_fn)


# --- Proxy Functions ---


def log_artifact(
    path: ArtifactRef,
    key: Optional[str] = None,
    direction: str = "output",
    schema: Optional[Type[SQLModel]] = None,
    driver: Optional[str] = None,
    content_hash: Optional[str] = None,
    force_hash_override: bool = False,
    validate_content_hash: bool = False,
    reuse_if_unchanged: bool = False,
    reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
    facet: Optional[Any] = None,
    facet_schema_version: Optional[Union[str, int]] = None,
    facet_index: bool = False,
    *,
    enabled: bool = True,
    **meta,
) -> ArtifactLike:
    """
    Logs an artifact (file or data reference) to the currently active run.

    This function is a convenient proxy to `consist.core.tracker.Tracker.log_artifact`.
    It automatically links the artifact to the current run context, handles path
    virtualization, and performs lineage discovery.

    Parameters
    ----------
    path : ArtifactRef
        A file path (str/Path) or an existing `Artifact` reference to be logged.
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
    reuse_if_unchanged : bool, default False
        If True and logging an output, reuse a prior artifact row when the content hash matches.
    reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
        Scope for output reuse checks. "same_uri" restricts reuse to the same URI,
        while "any_uri" allows reuse across different URIs with the same hash.
    facet : Optional[Any], optional
        Optional artifact-level facet payload (dict or Pydantic model).
    facet_schema_version : Optional[Union[str, int]], optional
        Optional schema version for artifact facet compatibility.
    facet_index : bool, default False
        If True, flatten scalar facet values for fast artifact querying.
    enabled : bool, default True
        If False, returns a noop artifact object without requiring an active run.
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
    if not enabled:
        return NoopRunContext().log_artifact(
            path=path,
            key=key,
            direction=direction,
            schema=schema,
            driver=driver,
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
    return get_active_tracker().log_artifact(
        path=path,
        key=key,
        direction=direction,
        schema=schema,
        driver=driver,
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


def log_artifacts(
    outputs: Mapping[str, ArtifactRef],
    *,
    direction: str = "output",
    driver: Optional[str] = None,
    metadata_by_key: Optional[Mapping[str, Dict[str, Any]]] = None,
    facets_by_key: Optional[Mapping[str, Any]] = None,
    facet_schema_versions_by_key: Optional[Mapping[str, Union[str, int]]] = None,
    facet_index: bool = False,
    reuse_if_unchanged: bool = False,
    reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
    enabled: bool = True,
    **shared_meta: Any,
) -> Mapping[str, ArtifactLike]:
    """
    Log multiple artifacts in a single call for efficiency.

    This function is a convenient proxy to `consist.core.tracker.Tracker.log_artifacts`.
    It logs a batch of related artifacts with optional per-key metadata customization.

    Parameters
    ----------
    outputs : Mapping[str, ArtifactRef]
        Mapping of key -> path/Artifact to log. Keys must be strings and explicitly
        chosen by the caller (not inferred from filenames).
    direction : str, default "output"
        "input" or "output" for the current run context.
    driver : Optional[str], optional
        Explicitly specify driver for all artifacts. If None, inferred from file extension.
    metadata_by_key : Optional[Mapping[str, Dict[str, Any]]], optional
        Per-key metadata overrides applied on top of shared metadata.
    facets_by_key : Optional[Mapping[str, Any]], optional
        Per-key artifact facet payloads.
    facet_schema_versions_by_key : Optional[Mapping[str, Union[str, int]]], optional
        Optional per-key schema versions for artifact facets.
    facet_index : bool, default False
        Whether to index scalar artifact facet fields in ``artifact_kv``.
    reuse_if_unchanged : bool, default False
        If True, reuse an existing logged artifact when the input path has not changed.
    reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
        Control reuse lookup scope. "same_uri" only reuses when the URI matches,
        while "any_uri" allows reuse across different URIs with the same content hash.
    enabled : bool, default True
        If False, returns noop artifact objects without requiring an active run.
    **shared_meta : Any
        Metadata key-value pairs applied to ALL logged artifacts.

    Returns
    -------
    Mapping[str, ArtifactLike]
        Mapping of key -> logged Artifact-like objects.

    Raises
    ------
    RuntimeError
        If called outside an active run context.
    ValueError
        If metadata_by_key contains keys not in outputs, or if any value is None.
    TypeError
        If mapping keys are not strings.

    Examples
    --------
    Log multiple outputs with shared metadata:
    ```python
    artifacts = consist.log_artifacts(
        {
            "persons": "results/persons.parquet",
            "households": "results/households.parquet",
            "jobs": "results/jobs.parquet"
        },
        year=2030,
        scenario="base"
    )
    # All three artifacts get year=2030 and scenario="base"
    ```

    Mix shared and per-key metadata:
    ```python
    artifacts = consist.log_artifacts(
        {
            "persons": "output/persons.parquet",
            "households": "output/households.parquet"
        },
        metadata_by_key={
            "households": {"role": "primary_unit", "weight": 1.0}
        },
        dataset_version="v2",
        simulation_id="run_001"
    )
    # persons gets: dataset_version="v2", simulation_id="run_001"
    # households gets: dataset_version="v2", simulation_id="run_001",
    #                  role="primary_unit", weight=1.0
    ```
    """
    if not enabled:
        ctx = NoopRunContext()
        artifacts: Dict[str, ArtifactLike] = {}
        per_key_meta = metadata_by_key or {}
        for key, ref in outputs.items():
            meta = dict(shared_meta)
            meta.update(per_key_meta.get(key, {}))
            facet = facets_by_key.get(key) if facets_by_key else None
            facet_schema_version = (
                facet_schema_versions_by_key.get(key)
                if facet_schema_versions_by_key
                else None
            )
            artifacts[str(key)] = ctx.log_artifact(
                ref,
                key=str(key),
                direction=direction,
                driver=driver,
                facet=facet,
                facet_schema_version=facet_schema_version,
                facet_index=facet_index,
                reuse_if_unchanged=reuse_if_unchanged,
                reuse_scope=reuse_scope,
                **meta,
            )
        return artifacts
    return get_active_tracker().log_artifacts(
        outputs,
        direction=direction,
        driver=driver,
        metadata_by_key=metadata_by_key,
        facets_by_key=facets_by_key,
        facet_schema_versions_by_key=facet_schema_versions_by_key,
        facet_index=facet_index,
        reuse_if_unchanged=reuse_if_unchanged,
        reuse_scope=reuse_scope,
        **shared_meta,
    )


def log_input(
    path: ArtifactRef,
    key: Optional[str] = None,
    *,
    schema: Optional[Type[SQLModel]] = None,
    driver: Optional[str] = None,
    content_hash: Optional[str] = None,
    force_hash_override: bool = False,
    validate_content_hash: bool = False,
    facet: Optional[Any] = None,
    facet_schema_version: Optional[Union[str, int]] = None,
    facet_index: bool = False,
    enabled: bool = True,
    **meta,
) -> ArtifactLike:
    """
    Log an input artifact to the active run.

    An input artifact represents a data source that the current run reads from.
    Logging it creates a lineage link, allowing Consist to track which versions
    of data produced which results.

    Parameters
    ----------
    path : ArtifactRef
        File path (str/Path) or an existing `Artifact` object.
    key : Optional[str]
        Semantic name for the artifact (e.g. "raw_households"). Required if `path`
        is a file path.
    schema : Optional[Type[SQLModel]]
        Optional SQLModel class defining the data structure.
    driver : Optional[str]
        Explicit format driver (e.g. "parquet"). Inferred from extension if None.
    content_hash : Optional[str]
        Precomputed hash to avoid re-hashing large files.
    force_hash_override : bool, default False
        Overwrite existing hash in the database if different from `content_hash`.
    validate_content_hash : bool, default False
        Re-hash the file on disk to ensure it matches the provided `content_hash`.
    facet : Optional[Any], optional
        Optional artifact-level facet payload.
    facet_schema_version : Optional[Union[str, int]], optional
        Optional facet schema version.
    facet_index : bool, default False
        Whether to index scalar facet fields for querying.
    enabled : bool, default True
        If False, returns a dummy artifact object for disconnected execution.
    **meta : Any
        Additional metadata fields to store with the artifact.

    Returns
    -------
    ArtifactLike
        The logged artifact reference.
    """
    return log_artifact(
        path=path,
        key=key,
        direction="input",
        schema=schema,
        driver=driver,
        content_hash=content_hash,
        force_hash_override=force_hash_override,
        validate_content_hash=validate_content_hash,
        facet=facet,
        facet_schema_version=facet_schema_version,
        facet_index=facet_index,
        enabled=enabled,
        **meta,
    )


def log_output(
    path: ArtifactRef,
    key: Optional[str] = None,
    *,
    schema: Optional[Type[SQLModel]] = None,
    driver: Optional[str] = None,
    content_hash: Optional[str] = None,
    force_hash_override: bool = False,
    validate_content_hash: bool = False,
    reuse_if_unchanged: bool = False,
    reuse_scope: Literal["same_uri", "any_uri"] = "same_uri",
    facet: Optional[Any] = None,
    facet_schema_version: Optional[Union[str, int]] = None,
    facet_index: bool = False,
    enabled: bool = True,
    **meta,
) -> ArtifactLike:
    """
    Log an output artifact produced by the current run.

    Parameters
    ----------
    path : ArtifactRef
        File path (str/Path) or an existing `Artifact` object.
    key : Optional[str]
        Semantic name for the artifact (e.g. "processed_results"). Required if `path`
        is a file path.
    schema : Optional[Type[SQLModel]]
        Optional SQLModel class defining the data structure.
    driver : Optional[str]
        Explicit format driver (e.g. "parquet"). Inferred from extension if None.
    content_hash : Optional[str]
        Precomputed hash to avoid re-hashing large files.
    force_hash_override : bool, default False
        Overwrite existing hash in the database if different from `content_hash`.
    validate_content_hash : bool, default False
        Re-hash the file on disk to ensure it matches the provided `content_hash`.
    reuse_if_unchanged : bool, default False
        If True, and the content hash matches a previous run's output, Consist may
        reuse that historical artifact record.
    reuse_scope : {"same_uri", "any_uri"}, default "same_uri"
        Whether to restrict reuse to the exact same file path or any path with the same hash.
    facet : Optional[Any], optional
        Optional artifact-level facet payload.
    facet_schema_version : Optional[Union[str, int]], optional
        Optional facet schema version.
    facet_index : bool, default False
        Whether to index scalar facet fields for querying.
    enabled : bool, default True
        If False, returns a dummy artifact object.
    **meta : Any
        Additional metadata fields to store.

    Returns
    -------
    ArtifactLike
        The logged artifact reference.
    """
    return log_artifact(
        path=path,
        key=key,
        direction="output",
        schema=schema,
        driver=driver,
        content_hash=content_hash,
        force_hash_override=force_hash_override,
        validate_content_hash=validate_content_hash,
        reuse_if_unchanged=reuse_if_unchanged,
        reuse_scope=reuse_scope,
        facet=facet,
        facet_schema_version=facet_schema_version,
        facet_index=facet_index,
        enabled=enabled,
        **meta,
    )


def ingest(
    artifact: Artifact,
    data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
    schema: Optional[Type[SQLModel]] = None,
    run: Optional[Run] = None,
) -> Any:
    """
    Ingests data associated with an `Artifact` into the active run's database.

    This function is a convenient proxy to `consist.core.tracker.Tracker.ingest`.
    It materializes data into the DuckDB, leveraging `dlt` for efficient loading
    and injecting provenance information. This is part of the "Hot Data Strategy".

    Parameters
    ----------
    artifact : Artifact
        The artifact object representing the data being ingested.
    data : Optional[Union[Iterable[Dict[str, Any]], Any]], optional
        The data to ingest. Can be an iterable of dictionaries (rows), a file path
        (str or Path), a Pandas DataFrame, or any other data type that `dlt`
        can handle. If `None`, Consist attempts to read the data directly
        from the artifact's resolved file path.
    schema : Optional[Type[SQLModel]], optional
        An optional SQLModel class that defines the expected schema for the ingested data.
        If provided, `dlt` will use this for strict validation and schema inference.
    run : Optional[Run], optional
        The `Run` context for ingestion. If provided, the data will be tagged with
        this run's ID (Offline Mode). If `None`, it defaults to the currently active run (Online Mode).

    Returns
    -------
    Any
        The result information from the underlying `dlt` ingestion process.

    Raises
    ------
    RuntimeError
        If no database is configured for the active `Tracker` or if `ingest` is
        called outside of an active run context.
    Exception
        Any exception raised by the underlying `dlt` ingestion process.
    """
    return get_active_tracker().ingest(
        artifact=artifact, data=data, schema=schema, run=run
    )


def log_dataframe(
    df: pd.DataFrame,
    key: str,
    schema: Optional[Type[SQLModel]] = None,
    direction: str = "output",
    tracker: Optional["Tracker"] = None,
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
    tracker : Optional[Tracker], optional
        Tracker instance to use; defaults to the active tracker.
        If None and no active run context exists, raises RuntimeError.
    path : Optional[Union[str, Path]], optional
        Output path; defaults to `<run_dir>/outputs/<run_subdir>/<key>.<driver>` for the
        active run as determined by the tracker's run subdir configuration.
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
    tr = tracker or current_tracker()
    # Resolve path and driver
    if path is None:
        base_dir = tr.run_artifact_dir()
        resolved_path = Path(base_dir) / f"{key}.{driver or 'parquet'}"
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
    art = tr.log_artifact(
        resolved_path, key=key, direction=direction, schema=schema, **meta_payload
    )
    if schema is not None:
        tr.ingest(art, df, schema=schema)
    return art


def register_views(*models: Type[SQLModel]) -> Dict[str, Type[SQLModel]]:
    """
    Register hybrid view models for the active tracker.

    Parameters
    ----------
    *models : Type[SQLModel]
        SQLModel classes describing the schema of hybrid views.

    Returns
    -------
    Dict[str, Type[SQLModel]]
        Mapping from model class name to the generated view model.
    """
    return {m.__name__: create_view_model(m) for m in models}


def find_run(tracker: Optional["Tracker"] = None, **filters: Any) -> Optional[Run]:
    """
    Convenience proxy for ``Tracker.find_run``.

    Parameters
    ----------
    tracker : Optional[Tracker], optional
        Tracker instance to query; defaults to the active tracker.
    **filters : Any
        Filter values forwarded to ``Tracker.find_run``.

    Returns
    -------
    Optional[Run]
        Matching run or ``None`` when no match exists.
    """
    tr = tracker or current_tracker()
    return tr.find_run(**filters)


def find_runs(
    tracker: Optional["Tracker"] = None, **filters: Any
) -> Union[list[Run], Dict[Hashable, Run]]:
    """
    Convenience proxy for ``Tracker.find_runs``.

    Parameters
    ----------
    tracker : Optional[Tracker], optional
        Tracker instance to query; defaults to the active tracker.
    **filters : Any
        Filter values forwarded to ``Tracker.find_runs``.

    Returns
    -------
    Union[list[Run], Dict[Hashable, Run]]
        Results returned by ``Tracker.find_runs``.
    """
    tr = tracker or current_tracker()
    return tr.find_runs(**filters)


@contextmanager
def db_session(tracker: Optional["Tracker"] = None) -> Iterator[Session]:
    """
    Provide a SQLModel ``Session`` connected to the tracker's database.

    Parameters
    ----------
    tracker : Optional[Tracker], optional
        Tracker instance supplying the engine; defaults to active tracker.

    Yields
    ------
    Session
        SQLModel session bound to the tracker engine.
    """
    tr = tracker or current_tracker()
    with Session(tr.engine) as session:
        yield session


def run_query(query: Executable, tracker: Optional["Tracker"] = None) -> list:
    """
    Execute a SQLModel/SQLAlchemy query via the tracker engine.

    Parameters
    ----------
    query : Executable
        Query object (``select``, etc.).
    tracker : Optional[Tracker], optional
        Tracker instance supplying the engine; defaults to the active tracker.

    Returns
    -------
    list
        Results of the executed query.
    """
    tr = tracker or current_tracker()
    with Session(tr.engine) as session:
        return session.exec(cast(Any, query)).all()


def config_run_query(
    table: Type[SQLModel],
    *,
    link_table: Type[SQLModel],
    table_name: Optional[str] = None,
    columns: Optional[Iterable[Any]] = None,
    where: Optional[Union[Iterable[Any], Any]] = None,
    join_on: Optional[Iterable[str]] = None,
) -> Executable:
    """
    Build a query joining a config cache table to its run-link table.

    This is a convenience helper for config adapter tables that are deduplicated
    by content hash and linked to runs via a separate link table.

    Parameters
    ----------
    table : Type[SQLModel]
        Config cache table to query (e.g., ActivitySimCoefficientsCache).
    link_table : Type[SQLModel]
        Link table storing run_id/content_hash (e.g., ActivitySimConfigIngestRunLink).
    table_name : Optional[str], optional
        Optional filter for link_table.table_name. Defaults to table.__tablename__
        if link_table has a table_name column.
    columns : Optional[Iterable[Any]], optional
        Columns to select. Defaults to (link_table.run_id, table).
    where : Optional[Union[Iterable[Any], Any]], optional
        Optional filter clauses to apply to the query.
    join_on : Optional[Iterable[str]], optional
        Column names to join on. Defaults to content_hash and file_name when available.

    Returns
    -------
    Executable
        SQL query joining the config table to runs.
    """
    if columns is None:
        if not hasattr(link_table, "run_id"):
            raise ValueError(
                "config_run_query requires link_table to have a run_id column "
                "when columns is not specified."
            )
        columns_list = [link_table.run_id, table]
    else:
        if isinstance(columns, (list, tuple, set)):
            columns_list = list(columns)
        else:
            columns_list = [columns]
        if not columns_list:
            raise ValueError("config_run_query requires at least one column.")

    if join_on is None:
        join_on = ["content_hash"]
        if hasattr(table, "file_name") and hasattr(link_table, "file_name"):
            join_on.append("file_name")
    else:
        join_on = list(join_on)
        if not join_on:
            raise ValueError("config_run_query join_on requires at least one column.")

    join_clauses = []
    for column in join_on:
        if not hasattr(table, column) or not hasattr(link_table, column):
            raise ValueError(
                f"config_run_query join_on column {column!r} must exist on table and link_table."
            )
        join_clauses.append(getattr(table, column) == getattr(link_table, column))

    stmt = (
        sa_select(*cast(tuple[Any, ...], tuple(columns_list)))
        .select_from(table)
        .join(link_table, and_(*join_clauses))
    )

    resolved_table_name = table_name
    if resolved_table_name is None and hasattr(link_table, "table_name"):
        resolved_table_name = getattr(table, "__tablename__", None)
    if resolved_table_name is not None:
        if not hasattr(link_table, "table_name"):
            raise ValueError(
                "config_run_query table_name provided but link_table has no table_name column."
            )
        stmt = stmt.where(getattr(link_table, "table_name") == resolved_table_name)

    if where is not None:
        if isinstance(where, (list, tuple, set)):
            clauses = list(where)
        else:
            clauses = [where]
        for clause in clauses:
            stmt = stmt.where(cast(Any, clause))

    return stmt


def config_run_rows(
    table: Type[SQLModel],
    *,
    link_table: Type[SQLModel],
    table_name: Optional[str] = None,
    columns: Optional[Iterable[Any]] = None,
    where: Optional[Union[Iterable[Any], Any]] = None,
    join_on: Optional[Iterable[str]] = None,
    tracker: Optional["Tracker"] = None,
) -> list:
    """
    Execute a config-to-run join query and return the results as a list of rows.

    This is a high-level wrapper around `config_run_query` and `run_query`. It is
    frequently used by integration adapters (like ActivitySim or BEAM) to retrieve
    configuration parameters that were active during a specific run.

    Parameters
    ----------
    table : Type[SQLModel]
        The configuration cache table to query.
    link_table : Type[SQLModel]
        The table that links configuration entries to specific run IDs.
    table_name : Optional[str]
        Optional filter for the link table's `table_name` column.
    columns : Optional[Iterable]
        Specific columns to select. Defaults to the run ID and the config record.
    where : Optional[Clause]
        Additional SQLAlchemy filter clauses.
    join_on : Optional[Iterable[str]]
        Column names to join on (e.g. `["content_hash"]`).
    tracker : Optional[Tracker]
        The tracker to use for database access.

    Returns
    -------
    list
        The results of the query.
    """
    query = config_run_query(
        table,
        link_table=link_table,
        table_name=table_name,
        columns=columns,
        where=where,
        join_on=join_on,
    )
    return run_query(query, tracker=tracker)


def pivot_facets(
    *,
    namespace: Optional[str],
    keys: Iterable[str],
    value_column: str = "value_num",
    value_columns: Optional[Mapping[str, str]] = None,
    label_prefix: str = "",
    label_map: Optional[Mapping[str, str]] = None,
    run_id_label: str = "run_id",
    table: Type[RunConfigKV] = RunConfigKV,
) -> Subquery:
    """
    Build a pivoted facet subquery keyed by run_id.

    This is a convenience helper for turning flattened config facets
    (``run_config_kv``) into a wide table suitable for joins.

    Parameters
    ----------
    namespace : Optional[str]
        Facet namespace to filter by (typically the model name). If ``None``,
        the namespace filter is skipped.
    keys : Iterable[str]
        Facet keys to pivot into columns.
    value_column : str, default "value_num"
        Default value column to read from. Must be one of:
        ``value_num``, ``value_str``, ``value_bool``, ``value_json``.
    value_columns : Optional[Mapping[str, str]], optional
        Optional per-key override of ``value_column``.
    label_prefix : str, default ""
        Optional prefix for pivoted column labels.
    label_map : Optional[Mapping[str, str]], optional
        Optional per-key label overrides.
    run_id_label : str, default "run_id"
        Label to use for the run id column in the returned subquery.
    table : Type[RunConfigKV], default RunConfigKV
        Table/model providing the facet KV rows.

    Returns
    -------
    Any
        A SQLAlchemy subquery with columns: ``run_id`` and one column per key.
    """
    keys_list = list(dict.fromkeys(keys))
    if not keys_list:
        raise ValueError("pivot_facets requires at least one key")

    valid_columns = {"value_num", "value_str", "value_bool", "value_json"}
    if value_column not in valid_columns:
        raise ValueError(
            f"pivot_facets value_column must be one of {sorted(valid_columns)}"
        )

    value_columns = value_columns or {}
    label_map = label_map or {}

    select_columns = [col(table.run_id).label(run_id_label)]
    for key in keys_list:
        column_name = value_columns.get(key, value_column)
        if column_name not in valid_columns:
            raise ValueError(
                f"pivot_facets value_columns[{key!r}] must be one of {sorted(valid_columns)}"
            )
        value_col = col(getattr(table, column_name))
        label = label_map.get(key, f"{label_prefix}{key}")
        select_columns.append(
            func.max(case((col(table.key) == key, value_col), else_=None)).label(label)
        )

    stmt = (
        select(*select_columns)
        .where(col(table.key).in_(keys_list))
        .group_by(col(table.run_id))
    )
    if namespace is not None:
        stmt = stmt.where(table.namespace == namespace)

    return stmt.subquery()


@contextmanager
def capture_outputs(
    directory: Union[str, Path], pattern: str = "*", recursive: bool = False
) -> Iterator[OutputCapture]:
    """
    Context manager to automatically capture and log new or modified files in a directory
    within the current active run context.

    This function is a convenient proxy to `consist.core.tracker.Tracker.capture_outputs`.
    It watches a specified `directory` for any file changes (creations or modifications)
    that occur within its `with` block. These changes are then automatically logged
    as output artifacts of the current Consist run.

    Parameters
    ----------
    directory : Union[str, Path]
        The path to the directory to monitor for new or modified files.
    pattern : str, default "*"
        A glob pattern (e.g., "*.csv", "data_*.parquet") to filter which files
        are captured within the specified directory. Defaults to all files.
    recursive : bool, default False
        If True, the capture will recursively scan subdirectories within `directory`
        for changes.

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
    with get_active_tracker().capture_outputs(directory, pattern, recursive) as capture:
        yield capture


# --- Data Loading ---


# Type Guards for artifact narrowing
# These enable runtime type checking with static type refinement


def is_dataframe_artifact(artifact: ArtifactLike) -> TypeGuard[DataFrameArtifact]:
    """
    Type guard: narrow artifact to tabular types (parquet, csv, h5_table).

    Use this to enable type-safe loading and IDE autocomplete:

    ```python
    if is_dataframe_artifact(artifact):
        rel = load(artifact)  # Type checker knows return is Relation
        df.head()  # IDE autocomplete works!
    ```

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is parquet, csv, or h5_table.
    """
    return artifact.driver in DriverType.dataframe_drivers()


def is_tabular_artifact(artifact: ArtifactLike) -> TypeGuard[TabularArtifact]:
    """
    Type guard: narrow artifact to any tabular format (parquet, csv, h5_table, json).

    Note: This is broader than `is_dataframe_artifact()`, so `load()` returns
    a Relation for tabular artifacts. Use `load_df()` for a pandas escape hatch.

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver produces tabular data.
    """
    return artifact.driver in DriverType.tabular_drivers()


def is_json_artifact(artifact: ArtifactLike) -> TypeGuard[JsonArtifact]:
    """
    Type guard: narrow artifact to JSON format.

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is json.
    """
    return artifact.driver == DriverType.JSON.value


def is_zarr_artifact(artifact: ArtifactLike) -> TypeGuard[ZarrArtifact]:
    """
    Type guard: narrow artifact to Zarr format.

    Use this when you know an artifact should be Zarr and want type-safe loading:

    ```python
    if is_zarr_artifact(artifact):
        ds = load(artifact)  # Type checker knows return is xarray.Dataset
        ds.dims  # IDE autocomplete works!
    ```

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is zarr.
    """
    return artifact.driver in DriverType.zarr_drivers()


def is_hdf_artifact(artifact: ArtifactLike) -> TypeGuard[HdfStoreArtifact]:
    """
    Type guard: narrow artifact to HDF5 format (h5 or hdf5).

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is h5 or hdf5.
    """
    return artifact.driver in DriverType.hdf_drivers()


def is_netcdf_artifact(artifact: ArtifactLike) -> TypeGuard[NetCdfArtifact]:
    """
    Type guard: narrow artifact to NetCDF format.

    Use this when you know an artifact should be NetCDF and want type-safe loading:

    ```python
    if is_netcdf_artifact(artifact):
        ds = load(artifact)  # Type checker knows return is xarray.Dataset
        ds.dims  # IDE autocomplete works!
    ```

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is netcdf.
    """
    return artifact.driver == DriverType.NETCDF.value


def is_openmatrix_artifact(artifact: ArtifactLike) -> TypeGuard[OpenMatrixArtifact]:
    """
    Type guard: narrow artifact to OpenMatrix format.

    Use this when you know an artifact should be OpenMatrix and want type-safe loading:

    ```python
    if is_openmatrix_artifact(artifact):
        matrix_data = load(artifact)  # Type checker knows return is appropriate type
    ```

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is openmatrix.
    """
    return artifact.driver == DriverType.OPENMATRIX.value


def is_spatial_artifact(artifact: ArtifactLike) -> TypeGuard[SpatialArtifact]:
    """
    Type guard: narrow artifact to spatial formats (GeoDataFrame outputs).

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to check.

    Returns
    -------
    bool
        True if artifact driver is geojson, shapefile, or geopackage.
    """
    return artifact.driver in DriverType.spatial_drivers()


# Overload signatures ordered from most specific to least specific
# Type checkers evaluate overloads in declaration order


@overload
def load(
    artifact: ZarrArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> "xarray.Dataset": ...


@overload
def load(
    artifact: NetCdfArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> "xarray.Dataset": ...


@overload
def load(
    artifact: OpenMatrixArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> "xarray.Dataset": ...


@overload
def load(
    artifact: SpatialArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> "geopandas.GeoDataFrame": ...


@overload
def load(
    artifact: HdfStoreArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> pd.HDFStore: ...


@overload
def load(
    artifact: DataFrameArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> duckdb.DuckDBPyRelation: ...


@overload
def load(
    artifact: JsonArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> duckdb.DuckDBPyRelation: ...


@overload
def load(
    artifact: TabularArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> duckdb.DuckDBPyRelation: ...


@overload
def load(
    artifact: Artifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> LoadResult: ...


@overload
def load(
    artifact: ArtifactLike,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> LoadResult: ...


def load(
    artifact: ArtifactLike,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> LoadResult:
    """
    Smart loader that retrieves data for an artifact from the best available source.

    This function attempts to load the data associated with an `Artifact` object.
    It prioritizes loading from disk (raw format) if the file exists. If the file
    is missing but the artifact is marked as ingested, it can optionally recover the
    data from the Consist DuckDB database ("Ghost Mode"). By default, DB recovery is
    only allowed when the artifact is an input to an active, non-cached run.

    Parameters
    ----------
    artifact : Artifact
        The Consist `Artifact` object whose data is to be loaded.
    tracker : Optional[Tracker], optional
        The `Tracker` instance to use for path resolution and database access.
        If `None`, the function attempts to use the active global tracker context.
        Explicitly passing a `tracker` is recommended for clarity or when
        no global context is available.
    db_fallback : str, default "inputs-only"
        Controls when the loader is allowed to fall back to DuckDB ("Ghost Mode") when
        the file is missing but the artifact is marked as ingested.

        - "inputs-only": allow DB fallback only if the artifact is declared as an input
          to the current active run AND the current run is not a cache hit.
        - "always": allow DB fallback whenever `artifact.meta["is_ingested"]` is true and
          a tracker with a DB connection is available.
        - "never": disable DB fallback entirely.
    **kwargs : Any
        Additional keyword arguments to pass to the underlying data loader function
        (e.g., `pd.read_parquet`, `pd.read_csv`, `xr.open_zarr`, `pd.read_sql`).

    Returns
    -------
    LoadResult
        The loaded data, typically a DuckDB Relation for tabular data, an xarray
        Dataset for array formats, or another data object depending on the artifact's
        `driver` and the data format.

    Raises
    ------
    RuntimeError
        If no `Tracker` instance can be resolved (neither provided nor active in context)
        and the artifact's absolute path is not directly resolvable.
        Also if the artifact is marked as ingested but no tracker with a DB connection is available.
    FileNotFoundError
        If the artifact's data cannot be found on disk or recovered from the database.
    """

    # 1. Resolve Tracker
    if tracker is None:
        try:
            tracker = get_active_tracker()
        except RuntimeError:
            tracker = None

    if tracker is None:
        tracker_ref = get_tracker_ref(artifact)
        if tracker_ref:
            attached_tracker = tracker_ref()
            if attached_tracker:
                tracker = attached_tracker

    if tracker is None:
        # If we have a resolved path available, allow direct disk loading without a tracker.
        if artifact.path and Path(artifact.path).exists():
            pass
        else:
            raise RuntimeError(
                "consist.load() requires a Tracker instance to resolve paths or access the database. "
                "Pass it explicitly: consist.load(art, tracker=my_tracker)"
            )

    # 2. Determine Physical Path
    # If we have a tracker, use it to resolve. Else fallback to runtime cache.
    if tracker:
        path = tracker.resolve_uri(artifact.container_uri)
    else:
        path = artifact.path

    # Only pass caller-provided kwargs to the loader; artifact.meta can contain
    # flags (e.g., schema_name) not accepted by pandas/xarray readers.
    load_kwargs = dict(kwargs)

    # Driver-specific hints from metadata
    if artifact.driver == "h5_table":
        if "table_path" not in load_kwargs:
            table_path = getattr(artifact, "table_path", None)
            if table_path:
                load_kwargs["table_path"] = table_path
    if artifact.driver in DriverType.array_drivers():
        if "array_path" not in load_kwargs:
            array_path = getattr(artifact, "array_path", None)
            if array_path:
                load_kwargs["array_path"] = array_path

    # 3. Try Disk Load (Priority 1)
    if Path(path).exists():
        return _load_from_disk(str(path), artifact.driver, **load_kwargs)

    # 4. Try Database Load (Priority 2 - Ghost Mode)
    if artifact.meta.get("is_ingested", False):
        if db_fallback not in {"inputs-only", "always", "never"}:
            raise ValueError(
                f"Invalid db_fallback={db_fallback!r}. Expected 'inputs-only', 'always', or 'never'."
            )

        if db_fallback == "never":
            raise FileNotFoundError(
                f"Artifact '{artifact.key}' (ID: {artifact.id}) not found.\n"
                f" - Disk Path: {path} (Missing)\n"
                f" - Database: Ingested, but db_fallback='never'\n"
                f"Hint: pass db_fallback='always' (or load within an active run where the artifact is a declared input)."
            )

        if db_fallback == "inputs-only":
            if not tracker or not tracker.current_consist:
                raise FileNotFoundError(
                    f"Artifact '{artifact.key}' (ID: {artifact.id}) not found.\n"
                    f" - Disk Path: {path} (Missing)\n"
                    f" - Database: Ingested, but no active run context\n"
                    f"Hint: call consist.load(...) within a tracker.start_run(..., inputs=[...]) context, "
                    f"or pass db_fallback='always'."
                )
            if getattr(tracker, "is_cached", False):
                raise FileNotFoundError(
                    f"Artifact '{artifact.key}' (ID: {artifact.id}) not found.\n"
                    f" - Disk Path: {path} (Missing)\n"
                    f" - Database: Ingested, but current run is a cache hit\n"
                    f"Hint: pass db_fallback='always' if you explicitly want DB recovery here."
                )
            is_declared_input = any(
                (a.id == artifact.id) or (a.container_uri == artifact.container_uri)
                for a in tracker.current_consist.inputs
            )
            if not is_declared_input:
                raise FileNotFoundError(
                    f"Artifact '{artifact.key}' (ID: {artifact.id}) not found.\n"
                    f" - Disk Path: {path} (Missing)\n"
                    f" - Database: Ingested, but artifact is not a declared input to the active run\n"
                    f"Hint: add it to inputs=[...] when starting the run, or pass db_fallback='always'."
                )

        if not tracker or not tracker.engine:
            raise RuntimeError(
                f"Artifact {artifact.key} is missing from disk, but marked as ingested. Provide a tracker with a DB connection to load it."
            )
        return _load_from_db(artifact, tracker, **kwargs)

    # 5. Failure
    raise FileNotFoundError(
        f"Artifact '{artifact.key}' (ID: {artifact.id}) not found.\n"
        f" - Disk Path: {path} (Missing)\n"
        f" - Database: Not Ingested\n"
        f"The run that produced this output may need to be re-run."
    )


def _load_from_disk(path: str, driver: str, **kwargs: Any) -> LoadResult:
    """
    Dispatches to the correct file reader based on the artifact's driver.

    This internal helper returns DuckDB Relations for tabular formats and
    format-specific objects for array/spatial formats.

    Parameters
    ----------
    path : str
        The absolute file system path to the data file.
    driver : str
        The driver string indicating the format of the file (e.g., "parquet", "csv", "zarr", "h5_table").
    **kwargs : Any
        Additional keyword arguments to pass to the specific file reading function.

    Returns
    -------
    LoadResult
        The loaded data, typically a DuckDB Relation for tabular formats, an
        xarray Dataset for array formats, or another format-specific data object.

    Raises
    ------
    ImportError
        If a required library for a specific driver (e.g., `xarray` for Zarr, `tables` for HDF5)
        is not installed.
    ValueError
        If an unsupported `driver` is provided, or if essential metadata (like `table_path`
        for 'h5_table' driver) is missing.
    """
    if driver in DriverType.tabular_drivers():
        conn = duckdb.connect()
        load_kwargs = dict(kwargs)
        table_path = load_kwargs.pop("table_path", None)
        nrows = load_kwargs.pop("nrows", None)
        info = TableInfo(
            role=Path(path).stem,
            table_path=table_path,
            container_uri=path,
            driver=driver,
            schema_id=None,
        )
        relation = TABLE_DRIVERS.get(driver).load(info, conn, **load_kwargs)
        if nrows is not None:
            relation = relation.limit(int(nrows))
        # Keep the connection alive for downstream relation materialization.
        _RELATION_CONNECTIONS[relation] = conn
        _maybe_warn_relation_leaks()
        return relation
    elif driver in DriverType.array_drivers():
        info = ArrayInfo(
            role=Path(path).stem,
            array_path=kwargs.get("array_path"),
            container_uri=path,
            driver=driver,
            schema_id=None,
        )
        return ARRAY_DRIVERS.get(driver).load(info)
    elif driver in ("geojson", "shapefile", "geopackage"):
        if gpd is None:
            raise ImportError("geopandas required for spatial formats")
        return gpd.read_file(path, **kwargs)
    elif driver in ("h5", "hdf5"):
        if not tables:
            raise ImportError("PyTables is required.")
        return pd.HDFStore(path, mode="r")
    else:
        # Fallback for unknown drivers?
        raise ValueError(f"Unsupported driver for disk load: {driver}")


def _load_from_db(
    artifact: ArtifactLike, tracker: "Tracker", **kwargs: Any
) -> duckdb.DuckDBPyRelation:
    """
    Recovers data for an artifact from the Consist DuckDB database as a Relation.
    """
    if not tracker.db_path:
        raise RuntimeError("Tracker has no DuckDB path configured.")
    table_name = artifact.meta.get("dlt_table_name") or artifact.key
    if not isinstance(table_name, str) or not table_name:
        raise RuntimeError(f"Invalid table name for DuckDB load: {table_name!r}")
    artifact_id = str(artifact.id).replace("'", "''")
    conn = duckdb.connect(tracker.db_path, read_only=True)
    quoted_table = _quote_ident(table_name)
    relation = conn.sql(
        f"SELECT * FROM global_tables.{quoted_table} "
        f"WHERE consist_artifact_id = '{artifact_id}'"
    )
    nrows = kwargs.get("nrows")
    if nrows is not None:
        relation = relation.limit(int(nrows))
    # Keep the connection alive for downstream relation materialization.
    _RELATION_CONNECTIONS[relation] = conn
    _maybe_warn_relation_leaks()
    return relation


def _close_relation_connection(relation: duckdb.DuckDBPyRelation) -> None:
    conn = _RELATION_CONNECTIONS.pop(relation, None)
    if conn is None:
        conn = getattr(relation, "_consist_conn", None)
        if conn is None:
            return
    try:
        conn.close()
    finally:
        try:
            delattr(relation, "_consist_conn")
        except Exception:
            pass


_RELATION_CONNECTIONS: "weakref.WeakKeyDictionary[duckdb.DuckDBPyRelation, duckdb.DuckDBPyConnection]" = weakref.WeakKeyDictionary()


class RelationConnectionLeakWarning(RuntimeWarning):
    """Warning emitted when relation connections appear to accumulate."""


def active_relation_count() -> int:
    """Return the number of active DuckDB relations tracked by Consist."""
    return len(_RELATION_CONNECTIONS)


def _relation_warn_threshold() -> int:
    raw = os.getenv("CONSIST_RELATION_WARN_THRESHOLD", "")
    if not raw:
        return 100
    try:
        value = int(raw)
    except ValueError:
        return 100
    return max(1, value)


def _maybe_warn_relation_leaks() -> None:
    count = active_relation_count()
    threshold = _relation_warn_threshold()
    if count < threshold:
        return
    message = (
        "Consist has %d active DuckDB relations. "
        "This may indicate unclosed relations. "
        "Use consist.to_df(..., close=True) or "
        "consist.load_relation(...) to ensure connections are closed."
    )
    logging.warning(message, count)
    warnings.warn(message % count, RelationConnectionLeakWarning, stacklevel=2)


def to_df(relation: duckdb.DuckDBPyRelation, *, close: bool = True) -> pd.DataFrame:
    """
    Convert a DuckDB Relation to a pandas DataFrame.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Relation to materialize into a DataFrame.
    close : bool, default True
        Whether to close the underlying DuckDB connection after materialization.
        Use `close=False` if you plan to continue using the relation.
    """
    if not hasattr(relation, "df"):
        raise TypeError("to_df expects a DuckDB Relation.")
    try:
        return relation.df()
    finally:
        if close:
            _close_relation_connection(relation)


def load_df(
    artifact: ArtifactLike,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    close: bool = True,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Load a tabular artifact and return a pandas DataFrame.

    Parameters
    ----------
    artifact : ArtifactLike
        Artifact to load.
    tracker : Optional[Tracker]
        Tracker to use for resolving paths or DB fallback.
    db_fallback : str, default "inputs-only"
        Controls when DB recovery is allowed for ingested artifacts.
    close : bool, default True
        Whether to close the underlying DuckDB connection after materialization
        when the load returns a Relation.
    **kwargs : Any
        Additional loader options.
    """
    result = load(artifact, tracker=tracker, db_fallback=db_fallback, **kwargs)
    if isinstance(result, duckdb.DuckDBPyRelation):
        # Relations are intended to be short-lived; close the underlying
        # connection by default after materialization. TODO: consider a bulk
        # materializer that reuses a shared connection for batch loads.
        return to_df(result, close=close)
    if isinstance(result, pd.DataFrame):
        return result
    if isinstance(result, pd.Series):
        return result.to_frame()
    raise TypeError("load_df requires a tabular artifact.")


@contextmanager
def load_relation(
    artifact: ArtifactLike,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> Iterator[duckdb.DuckDBPyRelation]:
    """
    Context manager that yields a DuckDB Relation and ensures the underlying
    connection is closed on exit.
    """
    result = load(artifact, tracker=tracker, db_fallback=db_fallback, **kwargs)
    if not isinstance(result, duckdb.DuckDBPyRelation):
        raise TypeError("load_relation requires a tabular artifact.")
    try:
        yield result
    finally:
        _close_relation_connection(result)


def log_meta(**kwargs: Any) -> None:
    """
    Updates the active run's metadata with the provided key-value pairs.

    This function is a convenient proxy to `consist.core.tracker.Tracker.log_meta`.
    It allows users to log additional information about the current run, such as
    performance metrics, experimental parameters, or tags, directly to the run's
    metadata. This information is then persisted to both the JSON log and the
    DuckDB database.

    Parameters
    ----------
    **kwargs : Any
        Arbitrary key-value pairs to merge into the `meta` dictionary of
        the current run. Existing keys will be updated, and new keys will be added.

    Raises
    ------
    RuntimeError
        If called when no `Tracker` is active in the current context.
    """
    get_active_tracker().log_meta(**kwargs)
