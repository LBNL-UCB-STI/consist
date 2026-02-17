from pathlib import Path

import duckdb
import pandas as pd

from consist.core.drivers import (
    ARRAY_DRIVERS,
    TABLE_DRIVERS,
    ArrayDriverRegistry,
    DriverRegistry,
    TableInfo,
)


class _DummyTableDriver:
    def discover(self, container_uri: str):
        return []

    def load(self, info, conn, **kwargs):
        return None

    def schema(self, info, conn):
        return None

    def supported_load_kwargs(self) -> set[str]:
        return set()


class _DummyArrayDriver:
    def discover(self, container_uri: str):
        return []

    def load(self, info):
        return None

    def schema(self, info):
        return None

    def to_relation(self, info, conn, *, view_name=None):
        return None


def test_table_driver_registry_extension_mapping():
    assert TABLE_DRIVERS.resolve_driver(Path("data.csv")) == "csv"
    assert TABLE_DRIVERS.resolve_driver(Path("data.csv.gz")) == "csv"
    assert TABLE_DRIVERS.resolve_driver(Path("data.parquet")) == "parquet"
    assert TABLE_DRIVERS.resolve_driver(Path("data.json")) == "json"
    assert TABLE_DRIVERS.resolve_driver(Path("data.h5")) is None


def test_array_driver_registry_extension_mapping():
    assert ARRAY_DRIVERS.resolve_driver(Path("matrix.zarr")) == "zarr"
    assert ARRAY_DRIVERS.resolve_driver(Path("climate.nc")) == "netcdf"
    assert ARRAY_DRIVERS.resolve_driver(Path("demand.omx")) == "openmatrix"


def test_table_registry_normalizes_extensions_and_prefers_composite_lookup():
    registry = DriverRegistry()
    driver = _DummyTableDriver()
    registry.register("single", driver, handles_exts=["csv", "GZ"])
    registry.register("composite", driver, handles_exts=["tar.gz"])

    assert registry.resolve_driver(Path("DATA.CSV")) == "single"
    assert registry.resolve_driver(Path("bundle.tar.gz")) == "composite"
    assert registry.resolve_driver(Path("bundle.GZ")) == "single"


def test_array_registry_normalizes_extensions_and_returns_none_for_unknown():
    registry = ArrayDriverRegistry()
    driver = _DummyArrayDriver()
    registry.register("array", driver, handles_exts=["NC", ".ZARR", ""])

    assert registry.resolve_driver(Path("climate.nc")) == "array"
    assert registry.resolve_driver(Path("grid.ZARR")) == "array"
    assert registry.resolve_driver(Path("grid.unknown")) is None


def test_csv_driver_loads_relation(tmp_path):
    path = tmp_path / "sample.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(path, index=False)

    conn = duckdb.connect()
    info = TableInfo(
        role="sample",
        table_path=None,
        container_uri=str(path),
        driver="csv",
        schema_id=None,
    )
    relation = TABLE_DRIVERS.get("csv").load(info, conn)
    df = relation.df()
    assert list(df.columns) == ["a", "b"]
    assert df["a"].tolist() == [1, 2]
