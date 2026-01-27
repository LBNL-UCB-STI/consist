from __future__ import annotations

from typing import get_args

from consist.types import DriverLiteral


def test_driver_literal_contains_expected_values() -> None:
    values = set(get_args(DriverLiteral))
    assert values >= {
        "parquet",
        "csv",
        "zarr",
        "netcdf",
        "openmatrix",
        "geojson",
        "shapefile",
        "geopackage",
        "json",
        "h5_table",
        "h5",
        "hdf5",
        "other",
    }
