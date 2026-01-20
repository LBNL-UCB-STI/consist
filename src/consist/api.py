from contextlib import contextmanager
import importlib
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
    overload,
    runtime_checkable,
)

import pandas as pd
from sqlalchemy import MetaData, Table, and_, case, func, select as sa_select, Subquery
from sqlalchemy.sql import Executable
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import SQLModel, Session, select

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
from consist.core.noop import NoopRunContext, NoopScenarioContext
from consist.core.views import create_view_model
from consist.core.workflow import OutputCapture
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord, Run, RunResult
from consist.models.run_config_kv import RunConfigKV
from consist.core.tracker import Tracker
from consist.types import ArtifactRef, DriverType

if TYPE_CHECKING:
    import xarray

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

T = TypeVar("T", bound=SQLModel)
LoadResult = Union[pd.DataFrame, pd.Series, "xarray.Dataset", pd.HDFStore]


@runtime_checkable
class ArtifactLike(Protocol):
    """
    Structural typing for artifact-like objects.

    Any object with `driver`, `uri`, `meta` attributes and a `path` property
    can be used where ArtifactLike is expected.
    """

    driver: str
    uri: str
    meta: Dict[str, Any]

    @property
    def path(self) -> Path: ...


class DataFrameArtifact(ArtifactLike, Protocol):
    """Artifact that loads as pandas DataFrame or Series."""

    driver: Literal["parquet", "csv", "h5_table"]


class TabularArtifact(ArtifactLike, Protocol):
    """Artifact that loads as tabular data (DataFrame, Series, or both)."""

    driver: Literal["parquet", "csv", "h5_table", "json"]


class JsonArtifact(ArtifactLike, Protocol):
    """Artifact that loads as pandas DataFrame or Series from JSON."""

    driver: Literal["json"]


class ZarrArtifact(ArtifactLike, Protocol):
    """Artifact that loads as xarray.Dataset."""

    driver: Literal["zarr"]


class HdfStoreArtifact(ArtifactLike, Protocol):
    """Artifact that loads as pandas HDFStore."""

    driver: Literal["h5", "hdf5"]


def view(model: Type[T], name: Optional[str] = None) -> Type[T]:
    """
    Create a SQLModel class backed by a Consist hybrid view.

    Parameters
    ----------
    model : Type[T]
        Base SQLModel describing the schema.
    name : Optional[str], optional
        Optional override for the generated view name.

    Returns
    -------
    Type[T]
        SQLModel subclass with ``table=True`` pointing at the hybrid view.
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
    Set the default tracker used by Consist entrypoints outside active runs.

    Returns the previously configured default tracker, if any.
    """
    return set_default_tracker(tracker)


def define_step(
    *,
    outputs: Optional[list[str]] = None,
    tags: Optional[list[str]] = None,
    description: Optional[str] = None,
):
    """
    Attach metadata to a function without changing execution behavior.

    This decorator lets you attach defaults such as ``outputs`` or ``tags`` to a
    function. ``Tracker.run`` and ``ScenarioContext.run`` read this metadata.
    """
    return define_step_decorator(outputs=outputs, tags=tags, description=description)


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
    Proxy for ``Tracker.scenario`` to avoid importing the tracker directly.

    Parameters
    ----------
    name : str
        Name of the scenario (used for the header run ID).
    tracker : Optional[Tracker], optional
        Tracker instance to use; defaults to the active global tracker.
    enabled : bool, default True
        If False, returns a noop scenario context that executes without provenance
        tracking while preserving Coupler/RunResult ergonomics.
    **kwargs : Any
        Additional arguments forwarded to ``Tracker.scenario``.

    Yields
    ------
    ScenarioContext
        Scenario context manager.
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
    Scenario context that executes without provenance tracking.

    Provides Coupler/RunResult compatibility for disabled Consist use cases.
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
    Context manager to initiate and manage a Consist run with a default tracker.
    """
    tr = _resolve_tracker(tracker)
    with tr.start_run(run_id=run_id, model=model, **kwargs) as active:
        yield active


def run(
    fn: Optional[Callable[..., Any]] = None,
    name: Optional[str] = None,
    *,
    tracker: Optional["Tracker"] = None,
    **kwargs: Any,
) -> RunResult:
    """
    Execute a function-shaped run using the default tracker.
    """
    tr = _resolve_tracker(tracker)
    return tr.run(fn=fn, name=name, **kwargs)


@contextmanager
def trace(
    name: str,
    *,
    tracker: Optional["Tracker"] = None,
    **kwargs: Any,
) -> Iterator["Tracker"]:
    """
    Manual tracing context manager using the default tracker.
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
    Return the active run if one is in progress, otherwise ``None``.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return None
    current_consist = tracker.current_consist
    return current_consist.run if current_consist else None


def current_consist() -> Optional[ConsistRecord]:
    """
    Return the active Consist record if one is in progress, otherwise ``None``.
    """
    try:
        tracker = get_active_tracker()
    except RuntimeError:
        return None
    return tracker.current_consist


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
        **meta,
    )


def log_artifacts(
    outputs: Mapping[str, ArtifactRef],
    *,
    direction: str = "output",
    driver: Optional[str] = None,
    metadata_by_key: Optional[Mapping[str, Dict[str, Any]]] = None,
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
        artifacts: Dict[str, Artifact] = {}
        per_key_meta = metadata_by_key or {}
        for key, ref in outputs.items():
            meta = dict(shared_meta)
            meta.update(per_key_meta.get(key, {}))
            artifacts[str(key)] = ctx.log_artifact(
                ref, key=str(key), direction=direction, driver=driver, **meta
            )
        return artifacts
    return get_active_tracker().log_artifacts(
        outputs,
        direction=direction,
        driver=driver,
        metadata_by_key=metadata_by_key,
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
    enabled: bool = True,
    **meta,
) -> ArtifactLike:
    """
    Log an input artifact to the active run, or return a noop artifact when disabled.

    Use `content_hash` to reuse a known hash; set `force_hash_override=True` to
    overwrite existing hashes, or `validate_content_hash=True` to validate against disk.
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
    enabled: bool = True,
    **meta,
) -> ArtifactLike:
    """
    Log an output artifact to the active run, or return a noop artifact when disabled.

    Use `content_hash` to reuse a known hash; set `force_hash_override=True` to
    overwrite existing hashes, or `validate_content_hash=True` to validate against disk.
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
        return session.exec(query).all()


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
        select(*columns_list).select_from(table).join(link_table, and_(*join_clauses))
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
            stmt = stmt.where(clause)

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
    Execute a config run query and return rows.

    This is a convenience wrapper around ``config_run_query`` + ``run_query``.
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

    select_columns = [table.run_id.label(run_id_label)]
    for key in keys_list:
        column_name = value_columns.get(key, value_column)
        if column_name not in valid_columns:
            raise ValueError(
                f"pivot_facets value_columns[{key!r}] must be one of {sorted(valid_columns)}"
            )
        value_col = getattr(table, column_name)
        label = label_map.get(key, f"{label_prefix}{key}")
        select_columns.append(
            func.max(case((table.key == key, value_col), else_=None)).label(label)
        )

    stmt = (
        select(*select_columns).where(table.key.in_(keys_list)).group_by(table.run_id)
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
    Type guard: narrow artifact to DataFrame-compatible types (parquet, csv, h5_table).

    Use this to enable type-safe loading and IDE autocomplete:

    ```python
    if is_dataframe_artifact(artifact):
        df = load(artifact)  # Type checker knows return is DataFrame
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
    `DataFrame | Series` for tabular artifacts. Use `is_dataframe_artifact()`
    for the narrower DataFrame-only return type.

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
) -> pd.DataFrame: ...


@overload
def load(
    artifact: JsonArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> pd.DataFrame | pd.Series: ...


@overload
def load(
    artifact: TabularArtifact,
    tracker: Optional["Tracker"] = None,
    *,
    db_fallback: str = "inputs-only",
    **kwargs: Any,
) -> pd.DataFrame | pd.Series: ...


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
    **kwargs : Any
        Additional keyword arguments to pass to the underlying data loader function
        (e.g., `pd.read_parquet`, `pd.read_csv`, `xr.open_zarr`, `pd.read_sql`).
    db_fallback : str, default "inputs-only"
        Controls when the loader is allowed to fall back to DuckDB ("Ghost Mode") when
        the file is missing but the artifact is marked as ingested.

        - "inputs-only": allow DB fallback only if the artifact is declared as an input
          to the current active run AND the current run is not a cache hit.
        - "always": allow DB fallback whenever `artifact.meta["is_ingested"]` is true and
          a tracker with a DB connection is available.
        - "never": disable DB fallback entirely.

    Returns
    -------
    LoadResult
        The loaded data, typically a Pandas DataFrame, an xarray Dataset (for Zarr),
        or another data object depending on the artifact's `driver` and the data format.

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
        tracker_ref = getattr(artifact, "_tracker", None)
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
        path = tracker.resolve_uri(artifact.uri)
    else:
        path = artifact.path

    # Only pass caller-provided kwargs to the loader; artifact.meta can contain
    # flags (e.g., schema_name) not accepted by pandas/xarray readers.
    load_kwargs = dict(kwargs)

    # Driver-specific hints from metadata
    if artifact.driver == "h5_table":
        if "table_path" not in load_kwargs:
            table_path = artifact.meta.get("table_path") or artifact.meta.get(
                "sub_path"
            )
            if table_path:
                load_kwargs["table_path"] = table_path

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
                (a.id == artifact.id) or (a.uri == artifact.uri)
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

    This internal helper function is responsible for reading data directly from
    a file path using appropriate libraries (e.g., pandas for Parquet/CSV, xarray for Zarr).

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
        The loaded data, typically a Pandas DataFrame, an xarray Dataset, or
        another format-specific data object.

    Raises
    ------
    ImportError
        If a required library for a specific driver (e.g., `xarray` for Zarr, `tables` for HDF5)
        is not installed.
    ValueError
        If an unsupported `driver` is provided, or if essential metadata (like `table_path`
        for 'h5_table' driver) is missing.
    """
    if driver == "parquet":
        return pd.read_parquet(path, **kwargs)
    elif driver == "csv":
        return pd.read_csv(path, **kwargs)
    elif driver == "zarr":
        if xr is None:
            raise ImportError("xarray required for Zarr")
        return xr.open_zarr(path, consolidated=False, **kwargs)
    elif driver == "json":
        return pd.read_json(path, **kwargs)
    elif driver == "h5_table":
        if not tables:
            raise ImportError("PyTables is required (pip install tables)")

        # In this pattern, the Artifact URI points to the physical H5 file.
        # The 'sub_path' or 'table_path' in metadata (passed via kwargs if not in artifact)
        # tells us where to look.

        # Note: consist.load(artifact) automatically passes artifact.meta as kwargs?
        # No, we need to handle that in the main load() function or here.
        # Let's handle it here by checking kwargs or assuming the caller (load) passed it.

        key = kwargs.get("table_path") or kwargs.get("sub_path")
        if not key:
            raise ValueError(
                f"Loading 'h5_table' requires 'table_path' in metadata. File: {path}"
            )

        return pd.read_hdf(path, key=key)

    elif driver in ("h5", "hdf5"):
        if not tables:
            raise ImportError("PyTables is required.")
        return pd.HDFStore(path, mode="r")
    else:
        # Fallback for unknown drivers?
        raise ValueError(f"Unsupported driver for disk load: {driver}")


def _load_from_db(
    artifact: Artifact, tracker: "Tracker", **kwargs: Any
) -> pd.DataFrame:
    """
    Recovers data for an artifact from the Consist DuckDB database.

    This function is used when the artifact's file is not found on disk but it has
    been previously ingested into the database. It constructs a SQL query to
    retrieve the data from the appropriate global table, filtering by the artifact's ID.

    Parameters
    ----------
    artifact : Artifact
        The `Artifact` object whose data is to be recovered from the database.
    tracker : Tracker
        The `Tracker` instance, necessary to access the database engine.
    **kwargs : Any
        Additional keyword arguments to pass to `pd.read_sql`.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the recovered data.

    Raises
    ------
    RuntimeError
        If the data cannot be loaded from the database, for example, if the table
        does not exist or a database error occurs.
    """
    table_name = artifact.key
    metadata = MetaData()
    try:
        table = Table(
            table_name,
            metadata,
            schema="global_tables",
            autoload_with=tracker.engine,
        )
    except SQLAlchemyError as e:
        raise RuntimeError(
            f"Failed to reflect DB table 'global_tables.{table_name}': {e}"
        ) from e

    if "consist_artifact_id" not in table.c:
        raise RuntimeError(
            f"Table 'global_tables.{table_name}' is missing consist_artifact_id."
        )

    stmt = sa_select(table).where(table.c.consist_artifact_id == str(artifact.id))
    try:
        return pd.read_sql(stmt, tracker.engine, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to load from DB table '{table_name}': {e}") from e


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
