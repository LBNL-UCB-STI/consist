import pytest
import numpy as np
from pathlib import Path
from consist.core.tracker import Tracker
from consist.core.matrix import MatrixViewFactory

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")


@pytest.fixture
def tracker_matrix(tmp_path):
    """
    Sets up a Consist Tracker configured for testing matrix (Zarr/xarray) workflows.

    This fixture initializes a Tracker with a temporary run directory and database,
    crucially setting `hashing_strategy="fast"`. This is necessary because Zarr
    directories can be very large, and performing a full content hash (`"full"` strategy)
    is computationally expensive and often impractical for directories. The "fast"
    strategy hashes metadata (size, mtime) which is sufficient for many matrix-style
    inputs where bitwise reproducibility is managed at a different level or where
    performance is paramount.
    """
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "matrix.duckdb")
    # hashing_strategy="fast" required for directories
    tracker = Tracker(run_dir=run_dir, db_path=db_path, hashing_strategy="fast")
    return tracker


def create_zarr(path: Path, value_fill: float, year: int):
    """
    Creates a simple Zarr dataset at the given path, filled with a specified value.

    This helper function is used by tests to generate dummy Zarr data for matrix workflows.

    Args:
        path (Path): The filesystem path where the Zarr dataset should be created.
        value_fill (float): The value to fill the 'traffic' data variable with.
        year (int): A dummy year value, which is not directly used in the Zarr dataset
                    but is provided for context if needed by consuming tests.
    """
    ds = xr.Dataset({"traffic": (("zone",), np.full((5,), value_fill))})
    # Note: We don't need to put 'year' inside the zarr anymore,
    # Consist injects it from the Run metadata.
    ds.to_zarr(path)


def test_zarr_lifecycle(tracker_matrix):
    """
    Tests the full lifecycle of Zarr (matrix) data tracking and viewing in Consist.

    This integration test verifies that Consist can:
    1.  Log Zarr datasets as artifacts.
    2.  Ingest Zarr metadata (not the raw data) into the database.
    3.  Create a virtual `xarray.Dataset` view that combines multiple Zarr artifacts
        logged across different runs, with system metadata (like `year`) injected
        as coordinates.

    What happens:
    - Two separate runs (`run_2020` and `run_2030`) are executed.
    - Each run creates a Zarr dataset (representing a "traffic" matrix for a given year)
      and logs it as a "congestion" artifact.
    - The Zarr artifacts are then ingested, which means their metadata is cataloged
      in the DuckDB database.
    - A `MatrixViewFactory` is used to create a unified `xarray.Dataset` view over
      all "congestion" artifacts.

    What's checked:
    - The `xarray.Dataset` view is successfully created.
    - Consist system metadata, specifically `year`, is correctly injected as an `xarray`
      coordinate in the combined dataset.
    - The data from the different Zarr sources is correctly accessible and separated
      by their `run_id` (implicitly via year coordinate), demonstrating proper stacking.
    """
    # --- Run 1: Year 2020 ---
    with tracker_matrix.start_run("run_2020", "traffic_model", year=2020) as t:
        # FIX: Unique filename prevents overwrite
        zarr_path = t.run_dir / "congestion_2020.zarr"
        create_zarr(zarr_path, value_fill=10.0, year=2020)

        art = t.log_artifact(zarr_path, key="congestion", driver="zarr")
        t.ingest(art)

    # --- Run 2: Year 2030 ---
    with tracker_matrix.start_run("run_2030", "traffic_model", year=2030) as t:
        # FIX: Unique filename prevents overwrite
        zarr_path = t.run_dir / "congestion_2030.zarr"
        create_zarr(zarr_path, value_fill=20.0, year=2030)

        art = t.log_artifact(zarr_path, key="congestion", driver="zarr")
        t.ingest(art)

    # --- Verification ---
    factory = MatrixViewFactory(tracker_matrix)
    ds_view = factory.load_matrix_view("congestion")

    print("\nVirtual View:\n", ds_view)

    # Check that 'year' is now a coordinate
    assert "year" in ds_view.coords

    # Query using .sel()
    # Now that we use concat(dim='run_id'), 'year' is a coordinate along 'run_id'.
    # We first swap dims to index by year easily, or just boolean index.

    # Option A: Boolean indexing (Standard xarray)
    ds_2020 = ds_view.isel(run_id=(ds_view.year == 2020))
    ds_2030 = ds_view.isel(run_id=(ds_view.year == 2030))

    val_2020 = ds_2020.traffic.values[0][0]  # [Run, Zone] -> [0,0]
    val_2030 = ds_2030.traffic.values[0][0]

    assert val_2020 == 10.0
    assert val_2030 == 20.0
