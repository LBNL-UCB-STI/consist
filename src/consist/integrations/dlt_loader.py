"""
Consist dlt (Data Load Tool) Integration Module

Provides the integration layer between Consist and ``dlt`` over a single shared
``StoreLedger`` DuckDB connection, injecting Consist provenance system columns
(``consist_run_id``, ``consist_artifact_id``, ``consist_year``, ``consist_iteration``).
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
import random
import time
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    get_args,
    get_origin,
    runtime_checkable,
)
import uuid

import duckdb
import pandas as pd
from sqlalchemy import JSON, inspect as sa_inspect
from sqlmodel import SQLModel

from consist.artifact import Artifact
from consist.core.gtfs import load_gtfs_member_df
from consist.core.netcdf_utils import resolve_netcdf_engine
from consist.location import lookup_live_ledger_by_path
from consist.models.run import Run
from consist.store.detect import ConsistError
from consist.tools.file_batches import yield_file_batches
from consist.types import DriverType

if TYPE_CHECKING:
    from consist.store.ledger import StoreLedger

try:
    import dlt
except ImportError:
    dlt = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


def _get_zarr() -> Any:
    try:
        return importlib.import_module("zarr")
    except ImportError:
        return None


def _get_xarray() -> Any:
    try:
        return importlib.import_module("xarray")
    except ImportError:
        return None


def _get_h5py() -> Any:
    try:
        return importlib.import_module("h5py")
    except ImportError:
        return None


def _get_openmatrix() -> Any:
    try:
        return importlib.import_module("openmatrix")
    except ImportError:
        return None


def _get_geopandas() -> Any:
    try:
        return importlib.import_module("geopandas")
    except ImportError:
        return None


_DLT_EXCLUDED_SYSTEM_COLS: frozenset[str] = frozenset(
    {
        "consist_artifact_id",
        "consist_run_id",
        "consist_year",
        "consist_iteration",
    }
)

_RETRYABLE_DUCKDB_LOCK_MARKERS = (
    "database is locked",
    "could not set lock on file",
    "conflicting lock is held",
    "io error",
)


@runtime_checkable
class _HasMatrixAttrs(Protocol):
    attrs: Any


def _is_retryable_duckdb_lock_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_DUCKDB_LOCK_MARKERS)


def _retry_sleep(
    attempt: int, *, base_sleep_seconds: float, max_sleep_seconds: float
) -> None:
    delay = min(
        (base_sleep_seconds * (1.5**attempt)) + random.uniform(0.05, 0.2),
        max_sleep_seconds,
    )
    time.sleep(delay)


def _open_netcdf_dataset(path: str, engine: Optional[str]) -> Any:
    xr = _get_xarray()
    if xr is None:
        raise ImportError("xarray is required to open NetCDF datasets")
    if engine:
        try:
            return xr.open_dataset(path, engine=engine)
        except Exception:
            return xr.open_dataset(path)
    return xr.open_dataset(path)


def _retry_pipeline_load(
    pipeline: Any,
    *,
    retries: int,
    lock_base_sleep_seconds: float,
    lock_max_sleep_seconds: float,
    exc: Exception,
) -> Any:
    retries = max(1, int(retries))
    for attempt in range(retries):
        if attempt == retries - 1:
            raise exc
        logging.warning(
            "[Consist][dlt] Load failed due to DuckDB lock (attempt %d/%d): %s",
            attempt + 1,
            retries,
            exc,
        )
        _retry_sleep(
            attempt,
            base_sleep_seconds=max(0.0, float(lock_base_sleep_seconds)),
            max_sleep_seconds=max(0.0, float(lock_max_sleep_seconds)),
        )
        try:
            return pipeline.load()
        except Exception as retry_exc:
            if not _is_retryable_duckdb_lock_error(retry_exc):
                raise
            exc = retry_exc
    raise exc


def _json_dumps(value: Dict[str, Any]) -> str:
    return json.dumps(value, default=str)


def _handle_zarr_metadata(path: str) -> Iterable[Dict[str, Any]]:
    """Extract and yield structural metadata from a Zarr array store."""
    xr = _get_xarray()
    if xr is None or _get_zarr() is None:
        raise ImportError("xarray and zarr are required for Zarr ingestion.")

    try:
        ds = xr.open_zarr(path, consolidated=False)
        for var_name, da in ds.data_vars.items():
            yield {
                "variable_name": var_name,
                "variable_type": "data",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": da.attrs,
            }
        for coord_name, da in ds.coords.items():
            yield {
                "variable_name": coord_name,
                "variable_type": "coordinate",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": da.attrs,
            }
    except Exception as e:
        raise ValueError(f"Failed to extract Zarr metadata from {path}: {e}")


def _handle_netcdf_metadata(path: str) -> Iterable[Dict[str, Any]]:
    """Extract and yield structural metadata from a NetCDF file."""
    if _get_xarray() is None:
        raise ImportError(
            "xarray is required for NetCDF ingestion (pip install xarray netCDF4 h5netcdf)"
        )

    try:
        engine = resolve_netcdf_engine()
        ds = _open_netcdf_dataset(path, engine)
        for var_name, da in ds.data_vars.items():
            yield {
                "variable_name": var_name,
                "variable_type": "data",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": _json_dumps(dict(da.attrs)) if da.attrs else "{}",
            }
        for coord_name, da in ds.coords.items():
            yield {
                "variable_name": coord_name,
                "variable_type": "coordinate",
                "dims": list(da.dims),
                "shape": list(da.shape),
                "dtype": str(da.dtype),
                "attributes": _json_dumps(dict(da.attrs)) if da.attrs else "{}",
            }
    except Exception as e:
        raise ValueError(f"Failed to extract NetCDF metadata from {path}: {e}")


def _handle_openmatrix_metadata(path: str) -> Iterable[Dict[str, Any]]:
    """Extract and yield structural metadata from an OpenMatrix (OMX) file."""
    openmatrix = _get_openmatrix()
    h5py = _get_h5py()
    if openmatrix:
        try:
            with openmatrix.open_file(path, mode="r") as f:
                matrix_names = f.list_matrices()
                for matrix_name in matrix_names:
                    matrix = f[matrix_name]
                    attrs_json = (
                        _json_dumps(dict(matrix.attrs))
                        if isinstance(matrix, _HasMatrixAttrs)
                        else "{}"
                    )
                    yield {
                        "matrix_name": matrix_name,
                        "shape": list(matrix.shape),
                        "dtype": str(matrix.dtype),
                        "n_rows": matrix.shape[0] if len(matrix.shape) >= 1 else None,
                        "n_cols": matrix.shape[1] if len(matrix.shape) >= 2 else None,
                        "attributes": attrs_json,
                    }
            return
        except Exception as e:
            if not h5py:
                raise ValueError(
                    f"Failed to extract OpenMatrix metadata from {path}: {e}"
                )
            logging.debug(
                "[Consist] OpenMatrix reader failed (%s); falling back to h5py.",
                e,
            )

    if not h5py:
        raise ImportError(
            "h5py or openmatrix is required for OpenMatrix (pip install h5py openmatrix)"
        )
    h5py_mod = h5py

    try:
        with h5py_mod.File(path, "r") as f:

            def walk_matrices(group: Any, prefix: str = "") -> Iterable[Dict[str, Any]]:
                for key, item in group.items():
                    if isinstance(item, h5py_mod.Dataset):
                        yield {
                            "matrix_name": key,
                            "path": prefix + "/" + key if prefix else "/" + key,
                            "shape": list(item.shape),
                            "dtype": str(item.dtype),
                            "n_rows": item.shape[0] if len(item.shape) >= 1 else None,
                            "n_cols": item.shape[1] if len(item.shape) >= 2 else None,
                            "attributes": (
                                _json_dumps(dict(item.attrs)) if item.attrs else "{}"
                            ),
                        }
                    elif isinstance(item, h5py_mod.Group):
                        yield from walk_matrices(
                            item, prefix + "/" + key if prefix else "/" + key
                        )

            for matrix_record in walk_matrices(f):
                yield matrix_record
    except Exception as e:
        raise ValueError(f"Failed to extract OpenMatrix metadata from {path}: {e}")


def _format_spatial_crs(crs: Any) -> str | None:
    if crs is None:
        return None
    try:
        pyproj = importlib.import_module("pyproj")
        normalized_crs = pyproj.CRS.from_user_input(crs)
        authority = normalized_crs.to_authority()
        if authority is not None:
            return f"{authority[0]}:{authority[1]}"
        return normalized_crs.to_string()
    except (ImportError, TypeError, ValueError):
        return str(crs)


def _handle_spatial_metadata(
    path: str, driver: str = "geojson"
) -> Iterable[Dict[str, Any]]:
    """Extract and yield basic metadata from spatial files."""
    geopandas = _get_geopandas()
    if geopandas is None:
        raise ImportError(
            "geopandas is required for spatial ingestion. "
            'Install with `pip install "consist[spatial]"`.'
        )

    try:
        if driver == "geoparquet":
            gdf = geopandas.read_parquet(path)
        else:
            gdf = geopandas.read_file(path)
        geometry_name = gdf.geometry.name
        geometry_series = gdf.geometry if geometry_name else None
        geometry_types = []
        if geometry_series is not None:
            geometry_types = geometry_series.geom_type.dropna().unique().tolist()

        yield {
            "bounds": gdf.total_bounds.tolist(),
            "crs": _format_spatial_crs(gdf.crs),
            "feature_count": len(gdf),
            "geometry_types": geometry_types,
            "geometry_column": geometry_name,
            "column_names": list(gdf.columns),
        }
    except Exception as e:
        raise ValueError(f"Failed to extract spatial metadata from {path}: {e}")


def _handle_parquet_path(path: str, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    if pa and pq:
        pq_mod = pq

        def parquet_stream() -> Iterator[pd.DataFrame]:
            pf = pq_mod.ParquetFile(path)
            for batch in pf.iter_batches():
                df_batch = batch.to_pandas()
                for k, v in ctx.items():
                    if v is not None:
                        df_batch[k] = v
                yield df_batch

        return parquet_stream(), True
    if pd:
        df = pd.read_parquet(path)
        for k, v in ctx.items():
            if v is not None:
                df[k] = v
        return df, True
    raise ImportError(f"Pandas or PyArrow required for Parquet: {path}")


def _handle_csv_path(path: str, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    if pd:

        def csv_stream() -> Iterator[pd.DataFrame]:
            for df_batch in pd.read_csv(path, chunksize=100_000):
                for k, v in ctx.items():
                    if v is not None:
                        df_batch[k] = v
                yield df_batch

        return csv_stream(), True
    raise ImportError(f"Pandas required for CSV: {path}")


def _schema_model_table_name(schema_model: Optional[Type[SQLModel]]) -> Optional[str]:
    if schema_model is None:
        return None
    for cls in schema_model.__mro__:
        if "__tablename__" in cls.__dict__ and isinstance(
            cls.__dict__["__tablename__"], str
        ):
            return cls.__dict__["__tablename__"]
    return None


def _is_additive_schema_model(schema_model: Optional[Type[SQLModel]]) -> bool:
    if schema_model is None:
        return False
    for cls in schema_model.__mro__:
        if "__consist_additive_schema__" in cls.__dict__:
            return bool(cls.__dict__["__consist_additive_schema__"])
    return False


def _reset_search_path(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("SET search_path = 'main'")
    except duckdb.Error:
        pass


def ingest_artifact(
    artifact: Artifact,
    run_context: Run,
    db_path: str | os.PathLike[str] | None = None,
    data_iterable: Optional[Union[Iterable[Any], str, Path, pd.DataFrame]] = None,
    schema_model: Optional[Type[SQLModel]] = None,
    lock_retries: int = 20,
    lock_base_sleep_seconds: float = 0.1,
    lock_max_sleep_seconds: float = 2.0,
    *,
    ledger: Optional[StoreLedger] = None,
) -> Tuple[Any, str]:
    """
    Ingest artifact data into DuckDB ``global_tables`` using ``dlt`` over ``StoreLedger``.
    """
    if dlt is None:
        raise ImportError(
            "Optional dependency 'dlt' is required for ingestion. "
            'Install with `pip install "consist[ingest]"`.'
        )

    resolved_ledger = ledger
    if (
        resolved_ledger is None
        and db_path is not None
        and os.fspath(db_path) != ":memory:"
    ):
        resolved_ledger = lookup_live_ledger_by_path(Path(db_path))

    if resolved_ledger is not None:
        if resolved_ledger.read_only:
            raise PermissionError("Cannot ingest into a read-only StoreLedger")
        if resolved_ledger._tx_depth != 0:
            raise ConsistError(
                "Cannot run dlt ingestion inside an active StoreLedger.transaction(); "
                "run ingestion outside transaction boundaries (ledger._tx_depth == 0)."
            )

    data_source: Iterable[Any]
    if isinstance(data_iterable, (str, Path)):
        file_path = os.fspath(data_iterable)

        if artifact.driver == "parquet":
            data_source = yield_file_batches(file_path, driver="parquet")

        elif artifact.driver == "csv":
            data_source = yield_file_batches(file_path, driver="csv")

        elif artifact.driver in DriverType.table_path_drivers():
            table_path = artifact.member
            if not table_path:
                raise ValueError(
                    f"Artifact '{artifact.key}' missing member/table_path."
                )
            if artifact.driver == "h5_table":
                data_source = _yield_h5_batches(file_path, table_path)
            else:
                data_source = [load_gtfs_member_df(file_path, table_path)]

        elif artifact.driver == "json":
            data_source = pd.read_json(file_path).to_dict(orient="records")

        elif artifact.driver == "zarr":
            data_source = _handle_zarr_metadata(file_path)

        elif artifact.driver == "netcdf":
            data_source = _handle_netcdf_metadata(file_path)

        elif artifact.driver == "openmatrix":
            data_source = _handle_openmatrix_metadata(file_path)

        elif artifact.driver == "geoparquet":
            spatial_mode = artifact.meta.get("spatial_ingest_mode")
            if spatial_mode == "wkt":
                raise ValueError(
                    "GeoParquet does not support spatial_ingest_mode='wkt'. "
                    "Use the default metadata ingestion or convert geometry to WKT before logging."
                )
            data_source = _handle_spatial_metadata(file_path, artifact.driver)

        elif artifact.driver in {"geojson", "shapefile", "geopackage"}:
            spatial_mode = artifact.meta.get("spatial_ingest_mode")
            if spatial_mode == "wkt":
                geopandas = _get_geopandas()
                if geopandas is None:
                    raise ImportError(
                        "geopandas is required for spatial ingestion. "
                        'Install with `pip install "consist[spatial]"`.'
                    )
                gdf = geopandas.read_file(file_path)
                geometry_name = gdf.geometry.name
                if not geometry_name:
                    raise ValueError(
                        "Spatial ingest mode 'wkt' requires geometry data."
                    )
                wkt_column = "geometry_wkt"
                if wkt_column in gdf.columns:
                    wkt_column = f"{wkt_column}_1"
                table_df = gdf.copy()
                table_df[wkt_column] = table_df.geometry.to_wkt()
                table_df = table_df.drop(columns=[geometry_name])
                data_source = [table_df]
            else:
                data_source = _handle_spatial_metadata(file_path, artifact.driver)

        else:
            raise ValueError(f"Ingestion not supported for driver: {artifact.driver}")

    elif data_iterable is not None:
        if isinstance(data_iterable, pd.DataFrame):
            data_source = [data_iterable]
        else:
            data_source = data_iterable
    else:
        raise ValueError("No data provided for ingestion.")

    schema_table_name = _schema_model_table_name(schema_model)
    desired_table_name = str(schema_table_name or artifact.key)

    additive_schema = _is_additive_schema_model(schema_model)
    allowed_keys: Optional[Set[str]] = None
    if schema_model and not additive_schema:
        allowed_keys = set(schema_model.model_fields.keys())
        allowed_keys.update(_DLT_EXCLUDED_SYSTEM_COLS)

    def _enrich_rows(source: Iterable[Any]) -> Iterator[Any]:
        for item in source:
            if isinstance(item, pd.DataFrame):
                batch = item.copy(deep=False)
                batch["consist_run_id"] = run_context.id
                batch["consist_artifact_id"] = str(artifact.id)
                batch["consist_year"] = run_context.year
                batch["consist_iteration"] = run_context.iteration

                if allowed_keys:
                    current_cols = set(batch.columns)
                    extra_cols = current_cols - allowed_keys
                    if extra_cols:
                        schema_name = (
                            schema_model.__name__
                            if schema_model is not None
                            else "unknown"
                        )
                        raise ValueError(
                            f"Schema Contract Violation: Found undefined columns {extra_cols} "
                            f"in artifact '{artifact.key}'. Strict Schema '{schema_name}' "
                            "does not allow new columns."
                        )
                yield batch

            elif isinstance(item, dict):
                row_copy = dict(item)
                row_copy["consist_run_id"] = run_context.id
                row_copy["consist_artifact_id"] = str(artifact.id)
                row_copy["consist_year"] = run_context.year
                row_copy["consist_iteration"] = run_context.iteration

                if allowed_keys:
                    current_keys = set(row_copy.keys())
                    extra_cols = current_keys - allowed_keys
                    if extra_cols:
                        raise ValueError(
                            f"Schema Contract Violation: Found undefined columns {extra_cols} "
                            f"in artifact '{artifact.key}'."
                        )
                yield row_copy

            else:
                yield item

    system_columns = {
        "consist_run_id": {"data_type": "text", "nullable": True},
        "consist_artifact_id": {"data_type": "text", "nullable": True},
        "consist_year": {"data_type": "bigint", "nullable": True},
        "consist_iteration": {"data_type": "bigint", "nullable": True},
    }

    schema_contract = None
    if schema_model:
        schema_contract = {
            "tables": "evolve",
            "columns": "evolve" if additive_schema else "freeze",
            "data_type": "freeze",
        }
        columns = _sqlmodel_to_dlt_columns(schema_model)
        columns.update(system_columns)
    else:
        columns = system_columns

    enriched_rows = _enrich_rows(data_source)
    resource_factory = cast(Any, dlt.resource)
    resource = resource_factory(
        enriched_rows,
        name=desired_table_name,
        write_disposition="append",
        columns=columns,
        schema_contract=schema_contract,
    )

    pipeline_uid = uuid.uuid4().hex[:8]
    if resolved_ledger is not None:
        with resolved_ledger._lock:
            if resolved_ledger.read_only:
                raise PermissionError("Cannot ingest into a read-only StoreLedger")
            if resolved_ledger._tx_depth != 0:
                raise ConsistError(
                    "Cannot run dlt ingestion inside an active StoreLedger.transaction()"
                )
            destination = dlt.destinations.duckdb(
                credentials=resolved_ledger.connection
            )
            pipeline = dlt.pipeline(
                pipeline_name=f"consist_ingest_{pipeline_uid}",
                destination=destination,
                dataset_name="global_tables",
            )
            try:
                info = pipeline.run(resource)
            finally:
                _reset_search_path(resolved_ledger.connection)
    else:
        if db_path is None:
            raise ValueError("Either ledger or db_path is required for dlt ingestion.")
        pipeline = dlt.pipeline(
            pipeline_name=f"consist_ingest_{pipeline_uid}",
            destination=dlt.destinations.duckdb(os.fspath(db_path)),
            dataset_name="global_tables",
        )
        try:
            info = pipeline.run(resource)
        except Exception as exc:
            if not _is_retryable_duckdb_lock_error(exc):
                raise
            info = _retry_pipeline_load(
                pipeline,
                retries=lock_retries,
                lock_base_sleep_seconds=lock_base_sleep_seconds,
                lock_max_sleep_seconds=lock_max_sleep_seconds,
                exc=exc,
            )

    real_table_name = pipeline.default_schema.naming.normalize_table_identifier(
        str(desired_table_name)
    )
    return info, real_table_name


def _sqlmodel_to_dlt_columns(model: Type[SQLModel]) -> Dict[str, Dict[str, Any]]:
    """Convert a SQLModel class definition into a ``dlt`` columns dictionary."""

    def _normalize_types(annotation: Any) -> set[Any]:
        origin = get_origin(annotation)
        if origin is None:
            return {annotation}
        if origin in {list, dict, tuple}:
            return {origin}
        if origin is Annotated:
            args = get_args(annotation)
            return _normalize_types(args[0]) if args else {annotation}
        args = get_args(annotation)
        if not args:
            return {annotation}
        normalized: set[Any] = set()
        for arg in args:
            if arg is type(None):
                continue
            normalized |= _normalize_types(arg)
        return normalized or {annotation}

    mapper = sa_inspect(model, raiseerr=False)
    sa_columns = mapper.columns if mapper is not None else {}
    columns: Dict[str, Dict[str, Any]] = {}
    for name, field in model.model_fields.items():
        sa_col = sa_columns.get(name)
        py_type = field.annotation
        normalized = _normalize_types(py_type)
        dlt_type = "text"
        if (
            (sa_col is not None and isinstance(sa_col.type, JSON))
            or dict in normalized
            or list in normalized
        ):
            dlt_type = "json"
        elif float in normalized:
            dlt_type = "double"
        elif int in normalized:
            dlt_type = "bigint"
        elif bool in normalized:
            dlt_type = "bool"
        columns[name] = {"data_type": dlt_type, "nullable": not field.is_required()}
    return columns


def _yield_h5_batches(path: str, key: str) -> Iterable[pd.DataFrame]:
    from importlib.util import find_spec

    if find_spec("tables") is None:
        raise ImportError("PyTables is required.")
    with pd.HDFStore(path, mode="r") as store:
        if key not in store:
            raise KeyError(f"Key '{key}' not found in HDF5.")
        iterator = pd.read_hdf(path, key=key, chunksize=50000, iterator=True)
        for chunk in iterator:
            yield chunk
