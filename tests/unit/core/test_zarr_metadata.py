import numpy as np
import pytest
from consist.integrations.dlt_loader import _handle_zarr_metadata

pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")


def test_extract_zarr_structure(tmp_path):
    """
    Verifies that the `_handle_zarr_metadata` function correctly extracts and yields
    structured metadata (variables, dimensions, shapes, data types, attributes)
    from a Zarr store.

    This unit test ensures that Consist can correctly introspect Zarr datasets
    to build a metadata catalog without loading the potentially large raw data.

    What happens:
    1. A dummy Zarr store (`test_data.zarr`) is created in a temporary directory.
       This Zarr store contains an `xarray.Dataset` with one data variable ("temperature")
       and one coordinate ("x").
    2. The `_handle_zarr_metadata` function is called with the path to this dummy store,
       and its generator output is converted into a list of records.

    What's checked:
    - The `records` list contains exactly two entries, corresponding to the "temperature"
      data variable and the "x" coordinate.
    - Each record's `variable_name`, `variable_type`, `dims`, `shape`, `dtype`, and
      `attributes` are correctly extracted and match the expected values from the
      original `xarray.Dataset`.
    """
    # 1. Create a dummy Zarr store
    store_path = tmp_path / "test_data.zarr"

    # Create Dataset with Data Variable + Coordinate
    ds = xr.Dataset(
        data_vars={
            "temperature": (
                ("x", "y"),
                np.zeros((10, 20), dtype="float32"),
                {"units": "C"},
            )
        },
        coords={"x": np.arange(10)},
    )
    ds.to_zarr(store_path)

    # 2. Extract Metadata
    metadata_gen = _handle_zarr_metadata(str(store_path))
    records = list(metadata_gen)

    # 3. Verify
    # We expect 2 records: 'temperature' (data) and 'x' (coordinate)
    assert len(records) == 2

    # Check 'temperature'
    temp_rec = next(r for r in records if r["variable_name"] == "temperature")
    assert temp_rec["variable_type"] == "data"
    assert temp_rec["dims"] == ["x", "y"]
    assert temp_rec["shape"] == [10, 20]
    assert temp_rec["dtype"] == "float32"
    assert temp_rec["attributes"]["units"] == "C"

    # Check 'x'
    x_rec = next(r for r in records if r["variable_name"] == "x")
    assert x_rec["variable_type"] == "coordinate"
    assert x_rec["shape"] == [10]
