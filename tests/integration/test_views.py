# tests/integration/test_views.py

import pytest
import pandas as pd
from typing import Optional
from sqlmodel import SQLModel, Field, text
from consist.core.tracker import Tracker


# --- Models ---

class Household(SQLModel, table=True):
    __tablename__ = "households"
    id: int = Field(primary_key=True)
    income: float
    persons: Optional[int] = Field(default=None)


# --- Tests ---

def test_hybrid_view_generation(tracker, tmp_path):
    """
    Tests the "happy path" for hybrid view generation, where both "hot" and "cold" data exist.

    This test verifies that `ViewFactory` can successfully create a SQL view that unions
    data from a materialized database table ("hot" data) and a raw Parquet file ("cold" data).
    It specifically checks that the view gracefully handles forward schema drift, where the
    "hot" data contains a column that the "cold" data lacks.

    What happens:
    1. A Parquet file is created representing "cold" data, missing a 'persons' column.
    2. A run is executed to log this file as a 'households' artifact.
    3. A separate run ingests "hot" data (which includes the 'persons' column) into the
       database, also for the 'households' concept.
    4. A hybrid view `v_households` is created.

    What's checked:
    - The final view contains records from both the hot and cold sources.
    - The record from the "cold" source has a `NULL` (or `NaN`) value for the 'persons' column.
    - The record from the "hot" source has the correct value for the 'persons' column.
    """
    # 1. Cold Data (Missing 'persons')
    cold_df = pd.DataFrame({"id": [1], "income": [50000.0]})
    cold_path = tmp_path / "cold.parquet"
    cold_df.to_parquet(cold_path)

    with tracker.start_run("run_cold", "model_x", year=2010):
        tracker.log_artifact(str(cold_path), key="households", driver="parquet")

    # 2. Hot Data (Has 'persons')
    hot_data = [{"id": 2, "income": 75000.0, "persons": 3}]
    with tracker.start_run("run_hot", "model_x", year=2011):
        tracker.ingest(
            artifact=tracker.log_artifact("dummy", "households"),
            data=hot_data,
            schema=Household
        )

    # 3. Verify
    tracker.create_view("v_households", "households")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_households ORDER BY consist_year", conn)

        assert len(df) == 2
        # Check Cold Row (year 2010)
        assert df.iloc[0]['consist_year'] == 2010
        assert pd.isna(df.iloc[0]['persons'])
        # Check Hot Row (year 2011)
        assert df.iloc[1]['consist_year'] == 2011
        assert df.iloc[1]['persons'] == 3.0


def test_pure_cold_view(tracker, tmp_path):
    """
    Tests the creation of a view when only "cold" (file-based) data exists.

    This test is critical for ensuring that Consist can operate in a "file-only" mode,
    where no data has been materialized into the database yet. It verifies that the
    `ViewFactory` can correctly create a view that unions multiple file-based artifacts.

    What happens:
    1. Two distinct Parquet files are created.
    2. Two separate runs are executed, each logging one of the files under the same
       concept key ('zones').
    3. A view `v_zones` is created for the 'zones' concept. At this point, no
       `global_tables.zones` table exists in the database.

    What's checked:
    - The resulting view contains the data from both Parquet files.
    - The Consist system columns (e.g., `consist_run_id`) are correctly injected into
      the view's output, even for file-only sources.
    """
    # Create two parquet files
    df1 = pd.DataFrame({"id": [1], "val": ["A"]})
    df2 = pd.DataFrame({"id": [2], "val": ["B"]})

    p1 = tmp_path / "f1.parquet"
    p2 = tmp_path / "f2.parquet"
    df1.to_parquet(p1)
    df2.to_parquet(p2)

    with tracker.start_run("r1", "m", year=2020):
        tracker.log_artifact(str(p1), key="zones", driver="parquet")

    with tracker.start_run("r2", "m", year=2021):
        tracker.log_artifact(str(p2), key="zones", driver="parquet")

    # Create view (No 'global_tables.zones' exists)
    tracker.create_view("v_zones", "zones")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_zones ORDER BY id", conn)
        assert len(df) == 2
        assert df['val'].tolist() == ["A", "B"]
        # Ensure system columns were injected
        assert "consist_run_id" in df.columns


def test_bidirectional_schema_drift(tracker, tmp_path):
    """
    Tests the view's ability to handle bidirectional schema drift between "hot" and "cold" data.

    This test verifies that DuckDB's `UNION ALL BY NAME` feature correctly handles cases
    where the file-based data has a column that the database table lacks, and vice-versa.
    The final view should contain all columns from both sources, with `NULL` values filling
    in where a column is absent for a given record.

    What happens:
    1. A "cold" Parquet file is created with a `legacy_col` but no `new_col`.
    2. A "hot" data record is ingested into the database with a `new_col` but no `legacy_col`.
    3. A hybrid view is created to union these two sources.

    What's checked:
    - The record originating from the "cold" source has the correct value for `legacy_col`
      and a `NULL` value for `new_col`.
    - The record originating from the "hot" source has a `NULL` value for `legacy_col`
      and the correct value for `new_col`.
    """
    # Cold: Has 'legacy_col', missing 'new_col'
    cold_df = pd.DataFrame({"id": [1], "legacy_col": ["old"]})
    cold_path = tmp_path / "drift.parquet"
    cold_df.to_parquet(cold_path)

    with tracker.start_run("r_old", "m"):
        tracker.log_artifact(str(cold_path), key="drift_test", driver="parquet")

    # Hot: Has 'new_col', missing 'legacy_col'
    # We define a dynamic model for this
    class DriftModel(SQLModel, table=True):
        __tablename__ = "drift_test"
        id: int = Field(primary_key=True)
        new_col: str

    hot_data = [{"id": 2, "new_col": "fresh"}]
    with tracker.start_run("r_new", "m"):
        tracker.ingest(
            artifact=tracker.log_artifact("dummy", "drift_test"),
            data=hot_data,
            schema=DriftModel
        )

    tracker.create_view("v_drift", "drift_test")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_drift ORDER BY id", conn)

        # Row 1 (Cold): legacy=old, new=NaN
        assert df.iloc[0]['legacy_col'] == "old"
        assert pd.isna(df.iloc[0]['new_col'])

        # Row 2 (Hot): legacy=NaN, new=fresh
        assert pd.isna(df.iloc[1]['legacy_col'])
        assert df.iloc[1]['new_col'] == "fresh"


def test_empty_state_safety(tracker):
    """
    Tests the system's resilience when creating a view for a concept that does not exist.

    This test ensures that requesting a view for a `concept_key` that has neither
    materialized data nor any file-based artifacts does not cause an error. The expected
    behavior is the creation of a valid, but empty, SQL view.

    What happens:
    - `create_view` is called with a `concept_key` ("ghost_concept") that has no associated
      artifacts or database tables.

    What's checked:
    - The operation completes without raising an exception.
    - The resulting view (`v_ghost`) can be queried (e.g., `SELECT count(*)`).
    - Querying the view returns an empty result set.
    """
    tracker.create_view("v_ghost", "ghost_concept")

    with tracker.engine.connect() as conn:
        # Should not raise table not found
        count = conn.execute(text("SELECT count(*) FROM v_ghost")).scalar()
        assert count == 0

        # Ensure it's select-able
        df = pd.read_sql("SELECT * FROM v_ghost", conn)
        assert len(df) == 0


def test_view_with_mounts(tmp_path):
    """
    Tests that the ViewFactory correctly handles virtualized URIs using mounts.

    This test ensures that when an artifact is logged with a path that falls under a
    configured mount point, the `ViewFactory` can correctly resolve the virtualized URI
    (e.g., "inputs://...") back to an absolute file path that DuckDB can use to read the file.

    What happens:
    1. A Tracker is initialized with a specific `mounts` dictionary mapping "inputs" to a temp directory.
    2. A Parquet file is created inside this mounted directory.
    3. An artifact is logged using the file's absolute path.
    4. A view is created for the concept associated with the artifact.

    What's checked:
    - The `Artifact` table in the database correctly stores the virtualized URI ("inputs://...").
    - The created view can be queried successfully, proving that the `ViewFactory` was able
      to resolve the virtualized URI back to an absolute path for DuckDB's `read_parquet` function.
    - The data in the view is correct.
    """
    # Setup tracker with mounts (Cannot use default fixture here)
    data_dir = tmp_path / "data_mount"
    data_dir.mkdir()

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
        mounts={"inputs": str(data_dir)}
    )

    # Create file inside the mount point
    df = pd.DataFrame({"x": [100]})
    p_path = data_dir / "mounted.parquet"
    df.to_parquet(p_path)

    # Log using absolute path (Tracker should virtualize it to inputs://)
    with tracker.start_run("r_mount", "m"):
        # We simulate the user providing the absolute path,
        # which Tracker converts to inputs:// internally.
        tracker.log_artifact(str(p_path), key="mounted_data", driver="parquet")

    # Verify internal storage is virtualized
    with tracker.engine.connect() as conn:
        # We query the artifact table directly to prove it stored the virtualization
        uris = pd.read_sql("SELECT uri FROM artifact WHERE key='mounted_data'", conn)['uri'].tolist()
        assert uris[0] == "inputs://mounted.parquet"

    # Create View (This tests if ViewFactory correctly resolves inputs:// back to absolute)
    tracker.create_view("v_mounted", "mounted_data")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_mounted", conn)
        assert len(df) == 1
        assert df.iloc[0]['x'] == 100