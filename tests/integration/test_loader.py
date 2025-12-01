# tests/integration/test_loader.py
import numpy as np
import pytest
import pandas as pd
from consist.core.tracker import Tracker
from consist.api import load
from consist.models.artifact import Artifact

# Check for optional dependencies for Zarr testing
try:
    import xarray as xr
    import zarr

    has_zarr = True
except ImportError:
    has_zarr = False


def test_loader_priority_and_ghost_mode(tmp_path):
    """
    Verifies the Universal Loader priority:
    1. Disk (Primary)
    2. Database (Fallback / Ghost Mode)
    3. Failure
    """
    # Setup
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # Create dummy data
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    file_path = run_dir / "data.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)

    # --- Phase 1: Standard Logging (Cold Data) ---
    with tracker.start_run("run_1", model="model_A"):
        artifact = tracker.log_artifact(file_path, key="my_table")

    # TEST 1: Load from Disk
    # Should work immediately
    loaded_df = load(artifact, tracker=tracker)
    pd.testing.assert_frame_equal(df, loaded_df)

    # --- Phase 2: Ingestion (Hot Data) ---
    # We ingest the data into DuckDB
    with tracker.start_run("run_ingest", model="model_A", inputs=[artifact]):
        tracker.ingest(artifact, df)
        # Note: Ingest updates the artifact metadata in the DB,
        # but our local 'artifact' variable might be stale.
        # Let's fetch the fresh artifact from DB to ensure we have 'dlt_table_name' / 'is_ingested'
        fresh_artifact = tracker.get_artifact(
            artifact.key
        )  # Assuming get_artifact helper exists, or manual fetch:

        # Manual fetch for test robustness if get_artifact isn't exposed yet
        from sqlmodel import Session, select

        with Session(tracker.engine) as session:
            fresh_artifact = session.exec(
                select(Artifact).where(Artifact.id == artifact.id)
            ).one()

    # --- Phase 3: Ghost Mode (Delete File) ---
    file_path.unlink()  # DELETE THE FILE
    assert not file_path.exists()

    # TEST 2: Load from DB (Ghost Mode)
    # The loader should detect file is missing, check metadata, find it's ingested, and query DB.
    ghost_df = load(fresh_artifact, tracker=tracker)

    # Note: DB roundtrip might change column types (e.g. object -> string),
    # so we may need loose comparison or type casting.
    # dlt/duckdb usually handles simple types well.

    assert len(ghost_df) == 3
    assert ghost_df.iloc[0]["val"] == "a"

    # --- Phase 4: Failure Mode ---
    # Create a fake artifact that exists nowhere
    fake_artifact = Artifact(
        key="fake",
        uri="inputs://fake.csv",
        driver="csv",
        abs_path=str(run_dir / "fake.csv"),
    )

    with pytest.raises(FileNotFoundError):
        load(fake_artifact, tracker=tracker)


def test_loader_drivers(tmp_path):
    """
    Verifies that consist.load() dispatches to the correct reader
    based on the artifact driver (CSV, Zarr).
    """
    run_dir = tmp_path / "runs_drivers"
    tracker = Tracker(run_dir=run_dir)  # No DB needed for disk loading tests

    # --- 1. CSV Test ---
    csv_path = run_dir / "test.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    df_csv = pd.DataFrame({"col1": [10, 20], "col2": ["x", "y"]})
    df_csv.to_csv(csv_path, index=False)

    with tracker.start_run("run_csv", model="model_A"):
        art_csv = tracker.log_artifact(csv_path, key="csv_data")

    assert art_csv.driver == "csv"

    loaded_csv = load(art_csv, tracker=tracker)
    assert isinstance(loaded_csv, pd.DataFrame)
    assert len(loaded_csv) == 2
    assert loaded_csv.iloc[0]["col1"] == 10

    # --- 2. Zarr Test ---
    if has_zarr:
        zarr_path = run_dir / "test.zarr"

        # Create a simple xarray Dataset
        ds = xr.Dataset(
            {"temperature": (("x", "y"), np.random.rand(2, 2))},
            coords={"x": [1, 2], "y": [3, 4]},
        )
        ds.to_zarr(zarr_path)

        with tracker.start_run("run_zarr", model="model_A"):
            # Zarr is a directory, log_artifact should handle it
            art_zarr = tracker.log_artifact(zarr_path, key="zarr_matrix")

        assert art_zarr.driver == "zarr"

        loaded_ds = load(art_zarr, tracker=tracker)
        assert isinstance(loaded_ds, xr.Dataset)
        assert "temperature" in loaded_ds
        assert loaded_ds.dims["x"] == 2
