import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from consist.core.tracker import Tracker
from consist.core.matrix import MatrixViewFactory

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")


@pytest.fixture
def tracker_matrix(tmp_path):
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "matrix.duckdb")
    # hashing_strategy="fast" required for directories
    tracker = Tracker(run_dir=run_dir, db_path=db_path, hashing_strategy="fast")
    return tracker


def create_zarr(path: Path, value_fill: float, year: int):
    ds = xr.Dataset(
        {"traffic": (("zone",), np.full((5,), value_fill))}
    )
    # Note: We don't need to put 'year' inside the zarr anymore,
    # Consist injects it from the Run metadata.
    ds.to_zarr(path)


def test_zarr_lifecycle(tracker_matrix):
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