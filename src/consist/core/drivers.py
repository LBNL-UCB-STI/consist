from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, Sequence, Tuple

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

    def load(self, info: TableInfo, conn: duckdb.DuckDBPyConnection): ...

    def schema(self, info: TableInfo, conn: duckdb.DuckDBPyConnection): ...


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


class ParquetDriver:
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

    def load(self, info: TableInfo, conn):
        path = _safe_duckdb_path(info.container_uri)
        return conn.sql(f"SELECT * FROM read_parquet('{path}')")

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class CsvDriver:
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

    def load(self, info: TableInfo, conn):
        path = _safe_duckdb_path(info.container_uri)
        return conn.sql(f"SELECT * FROM read_csv_auto('{path}')")

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class JsonDriver:
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

    def load(self, info: TableInfo, conn):
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise ImportError("pandas required for json staging.") from exc
        df = pd.read_json(info.container_uri)
        return conn.from_df(df)

    def schema(self, info: TableInfo, conn):
        relation = self.load(info, conn)
        return relation


class H5TableDriver:
    def discover(self, container_uri: str) -> list[TableInfo]:
        return []

    def load(self, info: TableInfo, conn):
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise ImportError("pandas required for HDF5 staging.") from exc
        table_path = info.table_path
        if not table_path:
            raise ValueError("HDF5 table load requires table_path.")
        df = pd.read_hdf(info.container_uri, key=table_path)
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
