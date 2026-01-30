"""
Driver registry for tabular and array loaders.

Note: We intentionally support only a vetted subset of load kwargs per driver.
If you need additional options for a driver, add them explicitly in that
driver's `_SUPPORTED_LOAD_KWARGS` and (optionally) `_LOAD_ALIASES`.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Sequence, Tuple, Iterable

import duckdb

try:
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore[misc,assignment]


class TableInfo(BaseModel):
    role: str
    table_path: Optional[str]
    container_uri: str
    driver: str
    schema_id: Optional[str] = None


class Driver(Protocol):
    def discover(self, container_uri: str) -> list[TableInfo]: ...

    def load(self, info: TableInfo, conn: duckdb.DuckDBPyConnection, **kwargs: Any): ...

    def schema(self, info: TableInfo, conn: duckdb.DuckDBPyConnection): ...

    def supported_load_kwargs(self) -> set[str]: ...


class ArrayInfo(BaseModel):
    role: str
    array_path: Optional[str]
    container_uri: str
    driver: str
    schema_id: Optional[str] = None


class ArrayDriver(Protocol):
    def discover(self, container_uri: str) -> list[ArrayInfo]: ...

    def load(self, info: ArrayInfo): ...

    def schema(self, info: ArrayInfo): ...

    def to_relation(
        self,
        info: ArrayInfo,
        conn: duckdb.DuckDBPyConnection,
        *,
        view_name: Optional[str] = None,
    ): ...


class DriverRegistry:
    def __init__(self) -> None:
        self._drivers: Dict[str, Driver] = {}
        self._ext_map: Dict[str, str] = {}

    def register(
        self, name: str, driver: Driver, handles_exts: Optional[Sequence[str]]
    ):
        self._drivers[name] = driver
        for ext in handles_exts or []:
            if not ext:
                continue
            normalized = ext.lower()
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            self._ext_map[normalized] = name

    def get(self, name: str) -> Driver:
        return self._drivers[name]

    def resolve_driver(self, path: Path) -> Optional[str]:
        suffixes = [s.lower() for s in path.suffixes]
        if len(suffixes) >= 2:
            composite = "".join(suffixes[-2:])
            if composite in self._ext_map:
                return self._ext_map[composite]
        if suffixes:
            ext = suffixes[-1]
            if ext in self._ext_map:
                return self._ext_map[ext]
        return None


class ArrayDriverRegistry:
    def __init__(self) -> None:
        self._drivers: Dict[str, ArrayDriver] = {}
        self._ext_map: Dict[str, str] = {}

    def register(
        self, name: str, driver: ArrayDriver, handles_exts: Optional[Sequence[str]]
    ) -> None:
        self._drivers[name] = driver
        for ext in handles_exts or []:
            if not ext:
                continue
            normalized = ext.lower()
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            self._ext_map[normalized] = name

    def get(self, name: str) -> ArrayDriver:
        return self._drivers[name]

    def resolve_driver(self, path: Path) -> Optional[str]:
        suffixes = [s.lower() for s in path.suffixes]
        if len(suffixes) >= 2:
            composite = "".join(suffixes[-2:])
            if composite in self._ext_map:
                return self._ext_map[composite]
        if suffixes:
            ext = suffixes[-1]
            if ext in self._ext_map:
                return self._ext_map[ext]
        return None


def _safe_duckdb_path(path: str) -> str:
    return path.replace("'", "''")


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _normalize_columns(columns: Any) -> list[str]:
    if columns is None:
        return []
    if isinstance(columns, str):
        return [columns]
    if isinstance(columns, Iterable):
        return [str(col) for col in columns]
    raise TypeError("columns must be a string or iterable of strings.")


def _validate_load_kwargs(
    *,
    driver_name: str,
    kwargs: Dict[str, Any],
    supported: set[str],
    aliases: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    alias_map = aliases or {}
    for key, value in kwargs.items():
        mapped = alias_map.get(key, key)
        normalized[mapped] = value
    unknown = set(normalized) - supported
    if unknown:
        allowed = ", ".join(sorted(supported))
        unknown_list = ", ".join(sorted(unknown))
        raise ValueError(
            f"Unsupported load kwargs for driver '{driver_name}': {unknown_list}. "
            f"Supported: {allowed}"
        )
    return normalized


class ParquetDriver:
    _SUPPORTED_LOAD_KWARGS = {"columns"}
    _LOAD_ALIASES: Dict[str, str] = {}

    def discover(self, container_uri: str) -> list[TableInfo]:
        stem = Path(container_uri).stem
        return [
            TableInfo(
                role=stem,
                table_path=None,
                container_uri=container_uri,
                driver="parquet",
                schema_id=None,
            )
        ]

    def supported_load_kwargs(self) -> set[str]:
        return set(self._SUPPORTED_LOAD_KWARGS)

    def load(self, info: TableInfo, conn, **kwargs: Any):
        normalized = _validate_load_kwargs(
            driver_name="parquet",
            kwargs=kwargs,
            supported=self._SUPPORTED_LOAD_KWARGS,
            aliases=self._LOAD_ALIASES,
        )
        path = _safe_duckdb_path(info.container_uri)
        columns = _normalize_columns(normalized.get("columns"))
        if columns:
            col_sql = ", ".join(_quote_ident(col) for col in columns)
            return conn.sql(f"SELECT {col_sql} FROM read_parquet('{path}')")
        return conn.sql(f"SELECT * FROM read_parquet('{path}')")

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class CsvDriver:
    _SUPPORTED_LOAD_KWARGS = {"columns", "delimiter", "header"}
    _LOAD_ALIASES = {"sep": "delimiter"}

    def discover(self, container_uri: str) -> list[TableInfo]:
        stem = Path(container_uri).stem
        return [
            TableInfo(
                role=stem,
                table_path=None,
                container_uri=container_uri,
                driver="csv",
                schema_id=None,
            )
        ]

    def supported_load_kwargs(self) -> set[str]:
        return set(self._SUPPORTED_LOAD_KWARGS)

    def load(self, info: TableInfo, conn, **kwargs: Any):
        normalized = _validate_load_kwargs(
            driver_name="csv",
            kwargs=kwargs,
            supported=self._SUPPORTED_LOAD_KWARGS,
            aliases=self._LOAD_ALIASES,
        )
        path = _safe_duckdb_path(info.container_uri)
        options: list[str] = []
        delimiter = normalized.get("delimiter")
        if delimiter is not None:
            escaped = str(delimiter).replace("'", "''")
            options.append(f"delim='{escaped}'")
        header = normalized.get("header")
        if header is not None:
            options.append(f"header={'true' if bool(header) else 'false'}")
        options_sql = ", " + ", ".join(options) if options else ""
        read_expr = f"read_csv_auto('{path}'{options_sql})"
        columns = _normalize_columns(normalized.get("columns"))
        if columns:
            col_sql = ", ".join(_quote_ident(col) for col in columns)
            return conn.sql(f"SELECT {col_sql} FROM {read_expr}")
        return conn.sql(f"SELECT * FROM {read_expr}")

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class JsonDriver:
    _SUPPORTED_LOAD_KWARGS = {
        "orient",
        "dtype",
        "convert_axes",
        "convert_dates",
        "precise_float",
        "date_unit",
        "encoding",
        "lines",
        "compression",
        "typ",
    }
    _LOAD_ALIASES: Dict[str, str] = {}

    def discover(self, container_uri: str) -> list[TableInfo]:
        stem = Path(container_uri).stem
        return [
            TableInfo(
                role=stem,
                table_path=None,
                container_uri=container_uri,
                driver="json",
                schema_id=None,
            )
        ]

    def supported_load_kwargs(self) -> set[str]:
        return set(self._SUPPORTED_LOAD_KWARGS)

    def load(self, info: TableInfo, conn, **kwargs: Any):
        normalized = _validate_load_kwargs(
            driver_name="json",
            kwargs=kwargs,
            supported=self._SUPPORTED_LOAD_KWARGS,
            aliases=self._LOAD_ALIASES,
        )
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise ImportError("pandas required for json staging.") from exc
        df = pd.read_json(info.container_uri, **normalized)
        return conn.from_df(df)

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class H5TableDriver:
    _SUPPORTED_LOAD_KWARGS = {"columns", "where", "start", "stop"}
    _LOAD_ALIASES: Dict[str, str] = {}

    def discover(self, container_uri: str) -> list[TableInfo]:
        return []

    def supported_load_kwargs(self) -> set[str]:
        return set(self._SUPPORTED_LOAD_KWARGS)

    def load(self, info: TableInfo, conn, **kwargs: Any):
        normalized = _validate_load_kwargs(
            driver_name="h5_table",
            kwargs=kwargs,
            supported=self._SUPPORTED_LOAD_KWARGS,
            aliases=self._LOAD_ALIASES,
        )
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise ImportError("pandas required for HDF5 staging.") from exc
        table_path = info.table_path
        if not table_path:
            raise ValueError("HDF5 table load requires table_path.")
        df = pd.read_hdf(info.container_uri, key=table_path, **normalized)
        return conn.from_df(df)

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class ZarrArrayDriver:
    def discover(self, container_uri: str) -> list[ArrayInfo]:
        stem = Path(container_uri).stem
        return [
            ArrayInfo(
                role=stem,
                array_path=None,
                container_uri=container_uri,
                driver="zarr",
                schema_id=None,
            )
        ]

    def load(self, info: ArrayInfo):
        try:
            xr = importlib.import_module("xarray")
        except ImportError as exc:  # pragma: no cover
            raise ImportError("xarray required for Zarr.") from exc
        return xr.open_zarr(info.container_uri, consolidated=False)

    def schema(self, info: ArrayInfo):
        return None

    def to_relation(self, info: ArrayInfo, conn, *, view_name: Optional[str] = None):
        raise NotImplementedError


class NetcdfArrayDriver:
    def discover(self, container_uri: str) -> list[ArrayInfo]:
        stem = Path(container_uri).stem
        return [
            ArrayInfo(
                role=stem,
                array_path=None,
                container_uri=container_uri,
                driver="netcdf",
                schema_id=None,
            )
        ]

    def load(self, info: ArrayInfo):
        try:
            xr = importlib.import_module("xarray")
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "xarray required for NetCDF (pip install xarray netCDF4 h5netcdf)."
            ) from exc
        try:
            from consist.core.netcdf_utils import resolve_netcdf_engine
        except Exception:
            return xr.open_dataset(info.container_uri)
        engine = resolve_netcdf_engine()
        if engine:
            try:
                return xr.open_dataset(info.container_uri, engine=engine)
            except Exception:
                return xr.open_dataset(info.container_uri)
        return xr.open_dataset(info.container_uri)

    def schema(self, info: ArrayInfo):
        return None

    def to_relation(self, info: ArrayInfo, conn, *, view_name: Optional[str] = None):
        raise NotImplementedError


class OpenMatrixArrayDriver:
    def discover(self, container_uri: str) -> list[ArrayInfo]:
        stem = Path(container_uri).stem
        return [
            ArrayInfo(
                role=stem,
                array_path=None,
                container_uri=container_uri,
                driver="openmatrix",
                schema_id=None,
            )
        ]

    def load(self, info: ArrayInfo):
        try:
            xr = importlib.import_module("xarray")
        except ImportError as exc:  # pragma: no cover
            raise ImportError("xarray required for OpenMatrix arrays.") from exc
        try:
            omx = importlib.import_module("openmatrix")
        except Exception:
            omx = None
        if omx is None:
            try:
                h5py = importlib.import_module("h5py")
            except Exception as exc:
                raise ImportError(
                    "openmatrix or h5py required for OpenMatrix arrays."
                ) from exc
            with h5py.File(info.container_uri, "r") as f:
                data_vars: Dict[str, Tuple[Tuple[str, str], Any]] = {}
                for name, obj in f.items():
                    if hasattr(obj, "shape"):
                        data_vars[name] = (("row", "col"), obj[()])
                return xr.Dataset(data_vars=data_vars)

        try:
            with omx.open_file(info.container_uri, mode="r") as f:
                data_vars: Dict[str, Tuple[Tuple[str, str], Any]] = {}
                for name in f.list_matrices():
                    data_vars[name] = (("row", "col"), f[name][:])
                return xr.Dataset(data_vars=data_vars)
        except Exception:
            # Fall back to raw HDF5 layout for OMX-like files without /data groups.
            try:
                h5py = importlib.import_module("h5py")
            except Exception as exc:
                raise ImportError(
                    "openmatrix or h5py required for OpenMatrix arrays."
                ) from exc
            with h5py.File(info.container_uri, "r") as f:
                data_vars: Dict[str, Tuple[Tuple[str, str], Any]] = {}
                for name, obj in f.items():
                    if hasattr(obj, "shape"):
                        data_vars[name] = (("row", "col"), obj[()])
                return xr.Dataset(data_vars=data_vars)

    def schema(self, info: ArrayInfo):
        return None

    def to_relation(self, info: ArrayInfo, conn, *, view_name: Optional[str] = None):
        raise NotImplementedError


TABLE_DRIVERS = DriverRegistry()
ARRAY_DRIVERS = ArrayDriverRegistry()

TABLE_DRIVERS.register(
    "parquet", ParquetDriver(), handles_exts=[".parquet", ".parquet.gz"]
)
TABLE_DRIVERS.register("csv", CsvDriver(), handles_exts=[".csv", ".csv.gz"])
TABLE_DRIVERS.register("json", JsonDriver(), handles_exts=[".json", ".json.gz"])
TABLE_DRIVERS.register("h5_table", H5TableDriver(), handles_exts=None)

ARRAY_DRIVERS.register("zarr", ZarrArrayDriver(), handles_exts=[".zarr"])
ARRAY_DRIVERS.register(
    "netcdf", NetcdfArrayDriver(), handles_exts=[".nc", ".nc4", ".netcdf"]
)
ARRAY_DRIVERS.register("openmatrix", OpenMatrixArrayDriver(), handles_exts=[".omx"])
