from pathlib import Path

import duckdb
import pandas as pd

from consist.core.drivers import ARRAY_DRIVERS, TABLE_DRIVERS, TableInfo


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
