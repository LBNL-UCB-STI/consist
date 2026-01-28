# tests/integration/test_loader.py
from importlib.util import find_spec
from pathlib import Path

import numpy as np
import pytest
import pandas as pd
from consist.core.tracker import Tracker
from consist.api import load, load_df
from consist.models.artifact import Artifact

HAS_ZARR = find_spec("xarray") is not None and find_spec("zarr") is not None


def test_loader_priority_and_ghost_mode(tracker: Tracker):
    """
    Tests the Consist Universal Loader's priority mechanism and "Ghost Mode" functionality.

    This test verifies that the `consist.load()` function correctly prioritizes
    reading data: first from the physical disk file, and then falling back to
    the database if the file is missing but the artifact was previously ingested.
    It also checks for proper error handling when data is neither on disk nor in the DB.

    What happens:
    1. A `Tracker` is initialized, and a dummy Parquet file (`data.parquet`) is created.
    2. **Phase 1 (Standard Logging)**: The Parquet file is logged as an `Artifact` ("my_table")
       within a run. `consist.load()` is then used to load this artifact from disk.
    3. **Phase 2 (Ingestion)**: The same artifact is ingested into the DuckDB, marking
       it as "is_ingested" in its metadata.
    4. **Phase 3 (Ghost Mode)**: The original `data.parquet` file is deliberately deleted.
       `consist.load()` is then called for the ingested artifact.
    5. **Phase 4 (Failure Mode)**: A completely fake `Artifact` (not existing anywhere)
       is created, and `consist.load()` is called for it.

    What's checked:
    -   **Phase 1**: `consist.load_df()` successfully loads the DataFrame from the physical file.
    -   **Phase 3**: After the file is deleted, `consist.load_df()` successfully retrieves the
        data from the DuckDB, demonstrating "Ghost Mode".
    -   **Phase 4**: `consist.load()` raises a `FileNotFoundError` for the non-existent artifact.
    """
    # Create dummy data
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    file_path = tracker.run_dir / "data.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)

    # --- Phase 1: Standard Logging (Cold Data) ---
    with tracker.start_run("run_1", model="model_A"):
        artifact = tracker.log_artifact(file_path, key="my_table")

    # TEST 1: Load from Disk
    # Should work immediately
    loaded_df = load_df(artifact, tracker=tracker)
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
    ghost_df = load_df(fresh_artifact, tracker=tracker, db_fallback="always")

    # Note: DB roundtrip might change column types (e.g. object -> string),
    # so we may need loose comparison or type casting.
    # dlt/duckdb usually handles simple types well.

    assert len(ghost_df) == 3
    assert ghost_df.iloc[0]["val"] == "a"

    # --- Phase 4: Failure Mode ---
    # Create a fake artifact that exists nowhere
    fake_artifact = Artifact(
        key="fake",
        container_uri="inputs://fake.csv",
        driver="csv",
    )
    fake_artifact.abs_path = str(tracker.run_dir / "fake.csv")

    with pytest.raises(FileNotFoundError):
        load(fake_artifact, tracker=tracker)


def test_loader_db_fallback_policy_paths(tracker: Tracker):
    """
    Covers `db_fallback` policy behavior for DB recovery ("Ghost Mode").

    We explicitly validate four distinct paths:

    1) "always": DB recovery works even outside an active run.
    2) "inputs-only": DB recovery works when the artifact is a declared input to an
       active, non-cached run.
    3) "inputs-only": DB recovery is blocked when the artifact is NOT a declared input.
    4) "inputs-only": if the artifact IS a declared input but cannot be loaded from disk
       AND is not ingested, `load()` raises `FileNotFoundError`.
    """
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    file_path = tracker.run_dir / "data.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)

    # Produce an artifact (so it has provenance/run_id) and ingest it so DB recovery is possible.
    with tracker.start_run("produce_csv", model="producer"):
        artifact = tracker.log_artifact(file_path, key="my_csv", driver="csv")

    with tracker.start_run("ingest_csv", model="ingester", inputs=[artifact]):
        tracker.ingest(artifact, df)

    # Refresh the artifact so `is_ingested` is present in metadata.
    from sqlmodel import Session, select

    with Session(tracker.engine) as session:
        ingested_artifact = session.exec(
            select(Artifact).where(Artifact.id == artifact.id)
        ).one()

    # Delete the file to force any recovery to come from DuckDB.
    file_path.unlink()
    assert not file_path.exists()

    # 1) "always": recovery succeeds even without an active run.
    df_always = load_df(ingested_artifact, tracker=tracker, db_fallback="always")
    assert len(df_always) == 3
    assert df_always.iloc[0]["val"] == "a"

    # 2) "inputs-only": recovery succeeds when declared as an input to an uncached run.
    # Use overwrite to ensure this is NOT a cache hit; policy is meant to allow DB
    # fallback only for inputs to an actually-executing run.
    with tracker.start_run(
        "consumer_with_input",
        model="consumer",
        inputs=[ingested_artifact],
        cache_mode="overwrite",
    ):
        df_inputs_only_ok = load_df(ingested_artifact, tracker=tracker)
        assert len(df_inputs_only_ok) == 3

    # 3) "inputs-only": recovery is blocked when artifact is not declared as an input.
    with tracker.start_run(
        "consumer_without_input", model="consumer", cache_mode="overwrite"
    ):
        with pytest.raises(FileNotFoundError):
            load(ingested_artifact, tracker=tracker)

    # 4) "inputs-only": declared input but not ingested and missing on disk => FileNotFoundError.
    other_path = tracker.run_dir / "other.csv"
    df.to_csv(other_path, index=False)
    with tracker.start_run("produce_other_csv", model="producer"):
        cold_artifact = tracker.log_artifact(other_path, key="other_csv", driver="csv")
    other_path.unlink()
    assert not other_path.exists()

    with tracker.start_run(
        "consumer_missing_input",
        model="consumer",
        inputs=[cold_artifact],
        cache_mode="overwrite",
    ):
        with pytest.raises(FileNotFoundError):
            load(cold_artifact, tracker=tracker)


def test_loader_drivers(run_dir: Path):
    """
    Verifies that `consist.load()` correctly dispatches to the appropriate
    underlying reader based on the artifact's specified `driver`.

    This test ensures that Consist can seamlessly load data from various
    file formats (CSV, Zarr) by invoking the correct parsing logic.

    What happens:
    1. A `Tracker` is initialized without a database (as this test focuses on disk loading).
    2. **CSV Test**:
       - A dummy CSV file (`test.csv`) is created with sample data.
       - The CSV file is logged as an `Artifact` with `driver="csv"`.
       - `consist.load()` is called for the CSV artifact.
    3. **Zarr Test** (if xarray/zarr are installed):
       - A dummy Zarr store (`test.zarr`) is created from an xarray Dataset.
       - The Zarr directory is logged as an `Artifact` with `driver="zarr"`.
       - `consist.load()` is called for the Zarr artifact.

    What's checked:
    -   **CSV Test**:
        - The logged artifact's driver is correctly identified as "csv".
        - `consist.load_df()` returns a Pandas DataFrame.
        - The loaded DataFrame has the correct number of rows and column values.
    -   **Zarr Test**:
        - The logged artifact's driver is correctly identified as "zarr".
        - `consist.load()` returns an xarray Dataset.
        - The loaded Dataset contains the expected variable and dimensions.
    """
    tracker = Tracker(run_dir=run_dir)  # No DB needed for disk loading tests

    # --- 1. CSV Test ---
    csv_path = run_dir / "test.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    df_csv = pd.DataFrame({"col1": [10, 20], "col2": ["x", "y"]})
    df_csv.to_csv(csv_path, index=False)

    with tracker.start_run("run_csv", model="model_A"):
        art_csv = tracker.log_artifact(csv_path, key="csv_data")

    assert art_csv.driver == "csv"

    loaded_csv = load_df(art_csv, tracker=tracker)
    assert isinstance(loaded_csv, pd.DataFrame)
    assert len(loaded_csv) == 2
    assert loaded_csv.iloc[0]["col1"] == 10

    # --- 2. Zarr Test ---
    if HAS_ZARR:
        import xarray as xr

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


def test_loader_json_and_h5_table(run_dir: Path):
    """
    Verify additional driver dispatches: JSON (DataFrame) and HDF5 table (DataFrame).
    """
    tracker = Tracker(run_dir=run_dir)

    json_path = run_dir / "payload.json"
    df_json = pd.DataFrame({"col": [1, 2, 3]})
    df_json.to_json(json_path, orient="records")

    with tracker.start_run("run_json", model="model_A"):
        art_json = tracker.log_artifact(json_path, key="json_payload", driver="json")

    loaded_json = load_df(art_json, tracker=tracker)
    assert isinstance(loaded_json, pd.DataFrame)
    assert loaded_json["col"].tolist() == [1, 2, 3]

    pytest.importorskip("tables")
    h5_path = run_dir / "payload.h5"
    df_h5 = pd.DataFrame({"value": [10, 20]})
    df_h5.to_hdf(h5_path, key="table", mode="w")

    with tracker.start_run("run_h5", model="model_A"):
        art_h5 = tracker.log_artifact(
            h5_path, key="h5_payload", driver="h5_table", table_path="table"
        )

    loaded_h5 = load_df(art_h5, tracker=tracker)
    assert isinstance(loaded_h5, pd.DataFrame)
    assert loaded_h5["value"].tolist() == [10, 20]


def test_loader_type_guard_dispatch(run_dir: Path):
    """
    Integration coverage for type-guard driven load dispatch.

    This mirrors the recommended pattern:
    - use type guards to narrow the artifact
    - call load() with the narrowed type
    """
    from consist import (
        is_dataframe_artifact,
        is_json_artifact,
        is_zarr_artifact,
    )

    tracker = Tracker(run_dir=run_dir)

    csv_path = run_dir / "guard.csv"
    df_csv = pd.DataFrame({"col": [1, 2]})
    df_csv.to_csv(csv_path, index=False)

    json_path = run_dir / "guard.json"
    df_json = pd.DataFrame({"col": [3, 4]})
    df_json.to_json(json_path, orient="records")

    with tracker.start_run("run_guard_csv", model="model_A"):
        art_csv = tracker.log_artifact(csv_path, key="csv_guard", driver="csv")

    with tracker.start_run("run_guard_json", model="model_A"):
        art_json = tracker.log_artifact(json_path, key="json_guard", driver="json")

    assert is_dataframe_artifact(art_csv)
    loaded_csv = load_df(art_csv, tracker=tracker)
    assert isinstance(loaded_csv, pd.DataFrame)
    assert loaded_csv["col"].tolist() == [1, 2]

    assert is_json_artifact(art_json)
    loaded_json = load_df(art_json, tracker=tracker)
    assert isinstance(loaded_json, pd.DataFrame)

    if HAS_ZARR:
        import xarray as xr

        zarr_path = run_dir / "guard.zarr"
        ds = xr.Dataset({"temp": (("x", "y"), np.random.rand(2, 2))})
        ds.to_zarr(zarr_path)

        with tracker.start_run("run_guard_zarr", model="model_A"):
            art_zarr = tracker.log_artifact(zarr_path, key="zarr_guard")

        assert is_zarr_artifact(art_zarr)
        loaded_ds = load(art_zarr, tracker=tracker)
        assert isinstance(loaded_ds, xr.Dataset)
