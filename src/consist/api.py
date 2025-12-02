import pandas as pd
from pathlib import Path
from typing import Optional, Union, Any, Type, Iterable, Dict, TYPE_CHECKING
from contextlib import contextmanager

from sqlalchemy import text

# Internal imports
from consist.models.artifact import Artifact
from consist.models.run import Run
from consist.core.context import get_active_tracker
from sqlmodel import SQLModel

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

# Import loaders for specific formats
try:
    import xarray as xr
except ImportError:
    xr = None

try:
    import tables
except ImportError:
    tables = None


# --- Core Access ---


def current_tracker() -> "Tracker":
    """
    Returns the currently active Tracker instance.
    Useful for introspection (e.g. accessing .last_run) inside deep functions.
    """
    return get_active_tracker()


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

    Args:
        path (Union[str, Artifact]): The file path (str) or an existing `Artifact` object.
        key (Optional[str]): A semantic name for the artifact. Required if `path` is a string.
        direction (str): "input" or "output". Defaults to "output".
        schema (Optional[Type[SQLModel]]): Optional SQLModel class for schema metadata.
        driver (Optional[str]): Explicitly specify the driver (e.g., 'h5_table').
                                If None, inferred from file extension.
        **meta: Additional metadata for the artifact.

    Returns:
        Artifact: The created or updated `Artifact` object.
    """
    return get_active_tracker().log_artifact(
        path=path, key=key, direction=direction, schema=schema, driver=driver, **meta
    )


def ingest(
    artifact: Artifact,
    data: Optional[Union[Iterable[Dict[str, Any]], Any]] = None,
    schema: Optional[Type[SQLModel]] = None,
    run: Optional[Run] = None,
):
    """
    Ingests data associated with an `Artifact` into the active run's database.

    This function is a convenient proxy to `consist.core.tracker.Tracker.ingest`.
    It materializes data into the DuckDB, leveraging `dlt` for efficient loading
    and injecting provenance information.

    Args:
        artifact (Artifact): The artifact object representing the data being ingested.
        data (Optional[Union[Iterable[Dict[str, Any]], Any]]): The data to ingest. Can be a
                                                              file path, DataFrame, or iterable of dicts.
        schema (Optional[Type[SQLModel]]): An optional SQLModel class defining the schema.
        run (Optional[Run]): The run context for ingestion. Defaults to the active run.
    """
    return get_active_tracker().ingest(
        artifact=artifact, data=data, schema=schema, run=run
    )


@contextmanager
def capture_outputs(
    directory: Union[str, Any], pattern: str = "*", recursive: bool = False
):
    """
    Watches a directory for changes within the current active run context.
    Proxy to `tracker.capture_outputs`.

    Usage:
        import consist

        # Inside a function where you don't have the 'tracker' object:
        with consist.capture_outputs("./outputs"):
             legacy_code.run()
    """
    with get_active_tracker().capture_outputs(directory, pattern, recursive) as capture:
        yield capture


# --- Data Loading ---


def load(
    artifact: Artifact, tracker: Optional["Tracker"] = None, **kwargs
) -> Union[pd.DataFrame, "xr.Dataset", Any]:
    """
    Smart loader that retrieves data for an artifact from the best available source.

    Priority:
    1. Disk (Fastest, Raw Format)
    2. Database (Ghost Mode, if ingested)

    Args:
        artifact: The Consist Artifact to load.
        tracker: The Tracker instance (required for path resolution/DB access).
                 If None, attempts to use the active global tracker context.
        **kwargs: Arguments passed to the underlying loader (e.g. read_parquet, read_sql).

    Returns:
        DataFrame, xarray.Dataset, or other data object depending on the artifact driver.
    """

    # 1. Resolve Tracker
    if tracker is None:
        try:
            tracker = get_active_tracker()
        except RuntimeError:
            tracker = None

    if tracker is None:
        # If we have an absolute path cached in runtime, we might get lucky without a tracker
        # but usually we need the tracker to resolve 'inputs://' or access the DB.
        if artifact.abs_path and Path(artifact.abs_path).exists():
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
        path = artifact.abs_path

    load_kwargs = artifact.meta.copy()
    load_kwargs.update(kwargs)

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


def _load_from_disk(path: str, driver: str, **kwargs):
    """Dispatches to the correct file reader."""
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


def _load_from_db(artifact: Artifact, tracker: "Tracker", **kwargs):
    """Recovers data from the global tables."""
    table_name = artifact.key
    query = f"SELECT * FROM global_tables.{table_name} WHERE consist_artifact_id = '{artifact.id}'"
    try:
        return pd.read_sql(text(query), tracker.engine, **kwargs)
    except Exception as e:
        # If table not found, provide helpful error
        raise RuntimeError(f"Failed to load from DB table '{table_name}': {e}")


def log_meta(**kwargs):
    """
    Updates the active run's metadata with the provided key-value pairs.
    Useful for logging metrics, tags, or execution stats at runtime.

    Usage:
        consist.log_meta(accuracy=0.95, rows_processed=1000)
    """
    get_active_tracker().log_meta(**kwargs)