import pytest
import numpy as np
from pathlib import Path
from consist.core.tracker import Tracker
from consist.core.matrix import MatrixViewFactory

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")


@pytest.fixture
def tracker_matrix(tmp_path: Path) -> Tracker:
    """
    Pytest fixture that sets up a Consist `Tracker` configured for testing matrix (Zarr/xarray) workflows.

    This fixture initializes a `Tracker` with a temporary run directory and a DuckDB database.
    Crucially, it sets the `hashing_strategy` to "fast" because Zarr directories can be
    very large, and performing a full content hash (`"full"` strategy) is computationally
    expensive and often impractical for directories. The "fast" strategy hashes metadata
    (size, mtime) which is sufficient for many matrix-style inputs where bitwise
    reproducibility is managed at a different level or where performance is paramount.

    Parameters
    ----------
    tmp_path : Path
        The built-in pytest fixture for creating unique temporary directories
        for each test.

    Returns
    -------
    Tracker
        A fully configured `Tracker` instance, with `hashing_strategy` set to "fast",
        ready for use in matrix workflow tests.
    """
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "matrix.duckdb")
    # hashing_strategy="fast" required for directories
    tracker = Tracker(run_dir=run_dir, db_path=db_path, hashing_strategy="fast")
    return tracker


def create_zarr(path: Path, value_fill: float, year: int) -> None:
    """
    Creates a simple Zarr dataset at the given path, filled with a specified value.

    This helper function is used by tests to generate dummy Zarr data for matrix
    workflows. The created dataset contains a single data variable named 'traffic'
    with one dimension 'zone'.

    Parameters
    ----------
    path : Path
        The filesystem path where the Zarr dataset (which is a directory)
        should be created.
    value_fill : float
        The scalar value to fill all elements of the 'traffic' data variable with.
    year : int
        A dummy year value, which is not directly used in the Zarr dataset
        content but can be used for context or metadata in Consist runs.
    """
    ds = xr.Dataset({"traffic": (("zone",), np.full((5,), value_fill))})
    # Note: We don't need to put 'year' inside the zarr anymore,
    # Consist injects it from the Run metadata.
    ds.to_zarr(path)


def test_zarr_lifecycle(tracker_matrix):
    """
    Tests the full lifecycle of Zarr (matrix) data tracking and viewing in Consist.

    This integration test verifies that Consist can:
    1.  **Log Zarr datasets as artifacts**: Correctly log Zarr directories as `Artifact`s.
    2.  **Ingest Zarr metadata**: Ingest the *metadata* (schema, dimensions, attributes)
        of Zarr datasets into the DuckDB, rather than the potentially massive raw data.
    3.  **Create a virtual xarray.Dataset view**: Combine multiple Zarr artifacts
        logged across different runs into a unified `xarray.Dataset` view, with
        Consist system metadata (like `year`) injected as coordinates.

    What happens:
    1. Two separate Consist runs (`run_2020` and `run_2030`) are executed.
    2. In each run:
        - A unique Zarr dataset (representing a "traffic" matrix for a given `year`)
          is created using `create_zarr`.
        - This Zarr dataset is logged as a "congestion" artifact.
        - The artifact's metadata is ingested into the DuckDB.
    3. A `MatrixViewFactory` is then used to create a unified `xarray.Dataset` view
       over all "congestion" artifacts.

    What's checked:
    - The `xarray.Dataset` view is successfully created and is not empty.
    - The `year` metadata (from the Consist `Run` context) is correctly injected
      as a coordinate within the combined `xarray.Dataset`.
    - Data from the different Zarr sources is correctly accessible and can be
      selected by `year` coordinate, confirming proper stacking and metadata injection.
      Specifically, the 'traffic' values for 2020 and 2030 match their expected fill values.
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


def test_matrix_view_filters(tracker_matrix):
    """
    Verify MatrixViewFactory filters by run_ids and parent_id.
    """
    # --- Run A (Scenario A) ---
    with tracker_matrix.start_run(
        "run_A", "traffic_simulation", year=2020, parent_run_id="scenario_A"
    ) as t:
        zarr_path = t.run_dir / "skims_A.zarr"
        create_zarr(zarr_path, value_fill=1.0, year=2020)
        art = t.log_artifact(zarr_path, key="skims", driver="zarr")
        t.ingest(art)

    # --- Run B (Scenario B) ---
    with tracker_matrix.start_run(
        "run_B", "traffic_simulation", year=2020, parent_run_id="scenario_B"
    ) as t:
        zarr_path = t.run_dir / "skims_B.zarr"
        create_zarr(zarr_path, value_fill=2.0, year=2020)
        art = t.log_artifact(zarr_path, key="skims", driver="zarr")
        t.ingest(art)

    factory = MatrixViewFactory(tracker_matrix)

    # Filter by run_ids
    ds_run_a = factory.load_matrix_view("skims", run_ids=["run_A"])
    assert len(ds_run_a.run_id) == 1
    assert ds_run_a.run_id.values[0] == "run_A"

    # Filter by parent_id + model + status
    ds_scenario_a = factory.load_matrix_view(
        "skims",
        parent_id="scenario_A",
        model="traffic_simulation",
        status="completed",
    )
    assert len(ds_scenario_a.run_id) == 1
    assert ds_scenario_a.run_id.values[0] == "run_A"
