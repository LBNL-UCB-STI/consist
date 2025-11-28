import pytest
import numpy as np

# Skip if dependencies missing
zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from consist.integrations.dlt_loader import _handle_zarr_metadata


def test_extract_zarr_structure(tmp_path):
    """
    Verifies that _handle_zarr_metadata yields correct dictionary records
    describing the variables and coordinates of a Zarr store.
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
