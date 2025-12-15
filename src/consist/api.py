from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union, Any, Type, Iterable, Dict, TYPE_CHECKING

import pandas as pd
import tempfile
from sqlalchemy import text

# Internal imports
from consist.core.context import get_active_tracker
from consist.core.views import create_view_model
from consist.models.artifact import Artifact
from consist.models.run import Run
from consist.core.tracker import Tracker

if TYPE_CHECKING:
    # Type-only imports already handled above; kept for static checkers
    pass

# Import loaders for specific formats
try:
    import xarray as xr
except ImportError:
    xr = None

try:
    import tables
except ImportError:
    tables = None

# In consist/api.py

from typing import TypeVar
from sqlmodel import SQLModel, Session

T = TypeVar("T", bound=SQLModel)


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


@contextmanager
def scenario(name: str, tracker: Optional["Tracker"] = None, **kwargs: Any):
    """
    Proxy for ``Tracker.scenario`` to avoid importing the tracker directly.

    Parameters
    ----------
    name : str
        Name of the scenario (used for the header run ID).
    tracker : Optional[Tracker], optional
        Tracker instance to use; defaults to the active global tracker.
    **kwargs : Any
        Additional arguments forwarded to ``Tracker.scenario``.

    Yields
    ------
    ScenarioContext
        Scenario context manager.
    """
    tr = tracker or current_tracker()
    with tr.scenario(name, **kwargs) as sc:
        yield sc


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
    tr = tracker or current_tracker()
    with tr.scenario(name, **kwargs) as sc:
        with sc.step(step_name or name):
            yield sc


def current_tracker() -> "Tracker":
    """
    Retrieves the currently active `Tracker` instance from the global context.

    This function provides a way to access the `Tracker` object that is managing
    the current run. It's particularly useful when you need to interact with
    Consist's features (like logging artifacts or querying run history) from
    within a function that doesn't explicitly receive the `Tracker` object as an argument.

    Returns
    -------
    Tracker
        The `Tracker` instance currently active in the execution context.

    Raises
    ------
    RuntimeError
        If no `Tracker` is active in the current context. This typically happens
        if called outside of a `consist.start_run` block or a `@consist.task` decorated function.
    """
    return get_active_tracker()


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
    return current_tracker().cached_artifacts(direction=direction)


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
    return current_tracker().cached_output(key=key)


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
    return current_tracker().get_artifact(
        run_id, key=key, key_contains=key_contains, direction=direction
    )


# --- Proxy Functions ---


def log_artifact(
    path: Union[str, Artifact],
    key: Optional[str] = None,
    direction: str = "output",
    schema: Optional[Type[SQLModel]] = None,
    driver: Optional[str] = None,
    **meta,
) -> Artifact:
    """
    Logs an artifact (file or data reference) to the currently active run.

    This function is a convenient proxy to `consist.core.tracker.Tracker.log_artifact`.
    It automatically links the artifact to the current run context, handles path
    virtualization, and performs lineage discovery.

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
    return get_active_tracker().log_artifact(
        path=path, key=key, direction=direction, schema=schema, driver=driver, **meta
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
    path : Optional[Union[str, Path]], optional
        Output path; defaults to a temporary file in the tracker run directory.
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
        base_dir = getattr(tr, "run_dir", None) or Path(tempfile.mkdtemp())
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


def find_runs(tracker: Optional["Tracker"] = None, **filters: Any):
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
    list
        Results returned by ``Tracker.find_runs``.
    """
    tr = tracker or current_tracker()
    return tr.find_runs(**filters)


@contextmanager
def db_session(tracker: Optional["Tracker"] = None) -> Session:
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


def run_query(query: Any, tracker: Optional["Tracker"] = None) -> list:
    """
    Execute a SQLModel/SQLAlchemy query via the tracker engine.

    Parameters
    ----------
    query : Any
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


@contextmanager
def capture_outputs(
    directory: Union[str, Path], pattern: str = "*", recursive: bool = False
) -> Any:
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


def load(
    artifact: Artifact, tracker: Optional["Tracker"] = None, **kwargs: Any
) -> Union[pd.DataFrame, "xr.Dataset", Any]:
    """
    Smart loader that retrieves data for an artifact from the best available source.

    This function attempts to load the data associated with an `Artifact` object.
    It prioritizes loading from disk (raw format) if the file exists. If the file
    is missing but the artifact is marked as ingested, it attempts to recover the data
    from the Consist DuckDB database ("Ghost Mode").

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

    Returns
    -------
    Union[pd.DataFrame, xarray.Dataset, Any]
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
        return _load_from_disk(path, artifact.driver, **load_kwargs)

    # 4. Try Database Load (Priority 2 - Ghost Mode)
    if artifact.meta.get("is_ingested", False):
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


def _load_from_disk(path: str, driver: str, **kwargs: Any) -> Any:
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
    Any
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
    query = f"SELECT * FROM global_tables.{table_name} WHERE consist_artifact_id = '{artifact.id}'"
    try:
        return pd.read_sql(text(query), tracker.engine, **kwargs)
    except Exception as e:
        # If table not found, provide helpful error
        raise RuntimeError(f"Failed to load from DB table '{table_name}': {e}")


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
