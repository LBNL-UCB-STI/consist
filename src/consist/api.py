# src/consist/api.py

import pandas as pd
from pathlib import Path
from typing import Optional, Union, Any

# Internal imports
from consist.models.artifact import Artifact
from consist.core.context import get_active_tracker

# Import loaders for specific formats
try:
    import xarray as xr
except ImportError:
    xr = None

try:
    import tables
except ImportError:
    tables = None


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
        tracker = get_active_tracker()

    if tracker is None:
        # If we have an absolute path cached in runtime, we might get lucky without a tracker
        # but usually we need the tracker to resolve 'inputs://' or access the DB.
        if artifact.abs_path and Path(artifact.abs_path).exists():
            pass  # We can proceed strictly on disk
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
    load_kwargs.update(kwargs)  # User kwargs override meta

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
        # This represents the WHOLE file.
        # We probably can't return a single DataFrame.
        # Return the store object?
        if not tables:
            raise ImportError("PyTables is required.")
        return pd.HDFStore(path, mode="r")
    else:
        # Fallback for unknown drivers?
        raise ValueError(f"Unsupported driver for disk load: {driver}")


def _load_from_db(artifact: Artifact, tracker: "Tracker", **kwargs):
    """Recovers data from the global tables."""
    # dlt normalizes table names. We attempt to guess the table name.
    # 1. Exact key match (normalized)
    # 2. Schema name match
    # 3. Filename match

    # For now, simplistic approach: Try the key.
    # We really should store the 'table_name' in artifact.meta during ingest to avoid guessing.
    # But for now, let's look for the key.

    # We query specific rows for this artifact ID to ensure we don't get mixed data
    # from other runs that updated the same table.

    # Note: This requires us to know the table name.
    # Improvement for Phase 2.1: Store 'dlt_table_name' in artifact.meta during ingest!

    table_name = artifact.key  # Simple assumption for now

    query = f"SELECT * FROM global_tables.{table_name} WHERE consist_artifact_id = '{artifact.id}'"

    try:
        return pd.read_sql(query, tracker.engine, **kwargs)
    except Exception as e:
        # If table not found, provide helpful error
        raise RuntimeError(f"Failed to load from DB table '{table_name}': {e}")
