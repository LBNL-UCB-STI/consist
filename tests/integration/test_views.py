# tests/integration/test_views.py

import pandas as pd
from typing import Optional
from sqlmodel import SQLModel, Field, text
from consist.core.tracker import Tracker


# --- Models ---


class Household(SQLModel, table=True):
    """
    A SQLModel schema representing a household entity, used for testing
    data ingestion and hybrid view generation with varying schemas.

    This model defines a basic household structure with an ID, income, and
    an optional number of persons, which helps in demonstrating how Consist
    handles schema drift between different data sources.

    Attributes
    ----------
    id : int
        A unique identifier for the household, serving as the primary key.
    income : float
        The income of the household.
    persons : Optional[int], default None
        The number of persons in the household. This field is optional
        to facilitate testing schema drift scenarios.
    """

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
    1. A `Tracker` is initialized.
    2. **Cold Data Setup**: A Parquet file (`cold.parquet`) is created representing "cold"
       data. This DataFrame contains 'id' and 'income' columns but explicitly *lacks*
       a 'persons' column. This file is logged as a 'households' artifact within "run_cold".
    3. **Hot Data Setup**: A list of dictionaries representing "hot" data is prepared.
       This data *includes* the 'persons' column. It is then ingested into the database
       as a 'households' artifact within "run_hot", using the `Household` SQLModel schema.
    4. A hybrid view named `v_households` is created for the 'households' concept.

    What's checked:
    - The final view `v_households` contains exactly two records, one from the cold source
      and one from the hot source.
    - The record originating from the "cold" source (year 2010) has a `NULL` (or `NaN`)
      value for the 'persons' column, demonstrating correct schema reconciliation.
    - The record originating from the "hot" source (year 2011) has the correct integer
      value (3) for the 'persons' column.
    - The `consist_year` column is correctly populated for both records, confirming
      the injection of run metadata into the view.
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
            schema=Household,
        )

    # 3. Verify
    tracker.create_view("v_households", "households")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_households ORDER BY consist_year", conn)

        assert len(df) == 2
        # Check Cold Row (year 2010)
        assert df.iloc[0]["consist_year"] == 2010
        assert pd.isna(df.iloc[0]["persons"])
        # Check Hot Row (year 2011)
        assert df.iloc[1]["consist_year"] == 2011
        assert df.iloc[1]["persons"] == 3.0


def test_pure_cold_view(tracker, tmp_path):
    """
    Tests the creation of a hybrid view when only "cold" (file-based) data exists
    for a given concept, without any corresponding "hot" (ingested) data.

    This test is critical for ensuring that Consist can operate in a "file-only" mode,
    where no data has been materialized into the database. It verifies that the
    `ViewFactory` can correctly create a view that unions multiple file-based artifacts
    and injects Consist's system columns.

    What happens:
    1. Two distinct Parquet files (`f1.parquet`, `f2.parquet`) are created with sample data.
    2. Two separate runs ("r1", "r2") are executed. Each logs one of the Parquet files
       under the same concept key ('zones'), effectively creating two "cold" artifacts.
       No data is ingested into the database for this concept.
    3. A hybrid view named `v_zones` is created for the 'zones' concept.

    What's checked:
    - The `create_view` operation completes successfully without errors, even though
      no `global_tables.zones` materialized table exists.
    - Querying the resulting view `v_zones` returns all records from both Parquet files.
    - The Consist system columns (e.g., `consist_run_id`) are correctly injected into
      the view's output, associated with their respective runs.
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
        assert df["val"].tolist() == ["A", "B"]
        # Ensure system columns were injected
        assert "consist_run_id" in df.columns


def test_bidirectional_schema_drift(tracker, tmp_path):
    """
    Tests the hybrid view's ability to handle bidirectional schema drift between
    "hot" and "cold" data sources.

    This test verifies that DuckDB's `UNION ALL BY NAME` feature, used by Consist's
    `ViewFactory`, correctly handles scenarios where columns present in the file-based
    data are missing from the database table, and vice-versa. The final view should
    contain all columns from both sources, with `NULL` values filling in for missing data.

    What happens:
    1. **Cold Data Setup**: A Parquet file (`drift.parquet`) is created representing
       "cold" data. This file contains an `id` and a `legacy_col` but *no* `new_col`.
       It is logged as a 'drift_test' artifact within "r_old".
    2. **Hot Data Setup**: A list of dictionaries is prepared representing "hot" data.
       This data contains an `id` and a `new_col` but *no* `legacy_col`. It is ingested
       into the database as a 'drift_test' artifact within "r_new", using a dynamically
       defined `DriftModel` schema.
    3. A hybrid view named `v_drift` is created for the 'drift_test' concept.

    What's checked:
    - The `v_drift` view successfully combines records from both sources.
    - The record originating from the "cold" source (id=1) correctly shows its `legacy_col`
      value and `NULL` (or `NaN`) for `new_col`.
    - The record originating from the "hot" source (id=2) correctly shows its `new_col`
      value and `NULL` (or `NaN`) for `legacy_col`.
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
            schema=DriftModel,
        )

    tracker.create_view("v_drift", "drift_test")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_drift ORDER BY id", conn)

        # Row 1 (Cold): legacy=old, new=NaN
        assert df.iloc[0]["legacy_col"] == "old"
        assert pd.isna(df.iloc[0]["new_col"])

        # Row 2 (Hot): legacy=NaN, new=fresh
        assert pd.isna(df.iloc[1]["legacy_col"])
        assert df.iloc[1]["new_col"] == "fresh"


def test_empty_state_safety(tracker):
    """
    Tests the `ViewFactory`'s resilience when creating a view for a concept that has
    no associated data ("hot" or "cold").

    This test ensures that requesting a view for a `concept_key` that has neither
    materialized data in the database nor any file-based artifacts does not cause
    an error. The expected behavior is the creation of a valid, but empty, SQL view,
    allowing downstream queries to run without crashing.

    What happens:
    - `tracker.create_view` is called with a `concept_key` ("ghost_concept") that
      is known not to have any corresponding `Artifact`s or database tables.

    What's checked:
    - The `create_view` operation completes without raising an exception.
    - Querying the resulting view (`v_ghost`) for a `COUNT(*)` returns 0.
    - A `pd.read_sql` query on the view also returns an empty DataFrame,
      confirming the view's existence and emptiness.
    """
    tracker.create_view("v_ghost", "ghost_concept")

    with tracker.engine.connect() as conn:
        # Should not raise table not found
        count = conn.execute(text("SELECT count(*) FROM v_ghost")).scalar()
        assert count == 0

        # Ensure it's select-able
        df = pd.read_sql("SELECT * FROM v_ghost", conn)
        assert len(df) == 0


import pytest
from pathlib import Path


@pytest.fixture
def mounted_tracker(tmp_path: Path):
    """
    Provides a Tracker instance configured with a specific 'inputs' mount for testing
    path virtualization.
    """
    data_dir = tmp_path / "data_mount"
    data_dir.mkdir()
    t = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
        mounts={"inputs": str(data_dir)},
    )
    yield t, data_dir
    if t.engine:
        t.engine.dispose()


def test_view_with_mounts(mounted_tracker):
    """
    Tests that the `ViewFactory` correctly resolves virtualized URIs using configured mounts
    when creating a hybrid view.

    This test verifies a core aspect of Consist's "Path Virtualization" where portable
    URIs (e.g., "inputs://...") stored in the provenance can be translated back into
    absolute file system paths that DuckDB can use to read the actual data.

    What happens:
    1. A `Tracker` is initialized with a specific `mounts` dictionary that maps the
       "inputs" scheme to a temporary `data_dir`.
    2. A Parquet file (`mounted.parquet`) is created inside this `data_dir`.
    3. An artifact is logged using the file's absolute path. Consist's `Tracker`
       should automatically convert this into a virtualized URI ("inputs://mounted.parquet").
    4. A hybrid view named `v_mounted` is created for the "mounted_data" concept.

    What's checked:
    - The `Artifact` table in the database correctly stores the virtualized URI
      ("inputs://mounted.parquet") for the logged artifact.
    - The created view (`v_mounted`) can be queried successfully. This implicitly
      proves that the `ViewFactory` correctly resolved the virtualized URI back
      to an absolute path that DuckDB's `read_parquet` function could access.
    - The data retrieved from `v_mounted` is correct, confirming the integrity of
      the data loading process through the virtualized path.
    """
    tracker, data_dir = mounted_tracker
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
        uris = pd.read_sql("SELECT uri FROM artifact WHERE key='mounted_data'", conn)[
            "uri"
        ].tolist()
        assert uris[0] == "inputs://mounted.parquet"

    # Create View (This tests if ViewFactory correctly resolves inputs:// back to absolute)
    tracker.create_view("v_mounted", "mounted_data")

    with tracker.engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM v_mounted", conn)
        assert len(df) == 1
        assert df.iloc[0]["x"] == 100


def test_hybrid_view_with_multiple_cold_schema_drift(tracker, tmp_path):
    """
    Tests the `ViewFactory`'s ability to create a hybrid view from multiple "cold"
    (file-based) artifacts, where each file has a different schema, demonstrating
    robust schema drift handling with only file-based data.

    This test verifies that DuckDB's `UNION ALL BY NAME` capability (used by Consist)
    correctly merges data from Parquet files with differing columns, ensuring that
    all available data is present and missing columns are filled with `NULL`s.

    What happens:
    1. A `Tracker` is initialized.
    2. **First Cold Data**: A Parquet file (`file1.parquet`) is created with columns
       `['id', 'value_a']`. It is logged as a 'drift_concept' artifact within "run_cold_schema_1".
    3. **Second Cold Data**: Another Parquet file (`file2.parquet`) is created for the
       same 'drift_concept', but with an additional column `value_b` (`['id', 'value_a', 'value_b']`).
       It is logged as a 'drift_concept' artifact within "run_cold_schema_2".
    4. A hybrid view named `v_drift_concept` is created over the 'drift_concept' artifacts.

    What's checked:
    - The final view contains all four rows from both Parquet files.
    - Rows originating from `file1.parquet` (from `run_cold_schema_1`) correctly have `NaN`
      for the `value_b` column, as it was missing in their original schema.
    - Rows originating from `file2.parquet` (from `run_cold_schema_2`) correctly have the
      `value_b` data.
    - The `consist_run_id` system column is correctly associated with its respective runs
      for all records in the view.
    """
    # 1. First Cold Data (Missing 'value_b')
    df1 = pd.DataFrame({"id": [1, 2], "value_a": [10, 20]})
    path1 = tmp_path / "file1.parquet"
    df1.to_parquet(path1)

    run_id1 = "run_cold_schema_1"
    with tracker.start_run(run_id1, "model_drift", year=2023):
        tracker.log_artifact(str(path1), key="drift_concept", driver="parquet")

    # 2. Second Cold Data (Has 'value_b')
    df2 = pd.DataFrame({"id": [3, 4], "value_a": [30, 40], "value_b": ["x", "y"]})
    path2 = tmp_path / "file2.parquet"
    df2.to_parquet(path2)

    run_id2 = "run_cold_schema_2"
    with tracker.start_run(run_id2, "model_drift", year=2024):
        tracker.log_artifact(str(path2), key="drift_concept", driver="parquet")

    # 3. Create the hybrid view
    tracker.create_view("v_drift_concept", "drift_concept")

    # 4. Query the view and perform assertions
    with tracker.engine.connect() as conn:
        df_result = pd.read_sql(
            "SELECT consist_run_id, id, value_a, value_b FROM v_drift_concept ORDER BY id",
            conn,
        )

        # Assertions
        assert len(df_result) == 4
        assert df_result["id"].tolist() == [1, 2, 3, 4]
        assert df_result["value_a"].tolist() == [10, 20, 30, 40]

        # Check value_b for rows from run1 (should be NaN)
        assert pd.isna(df_result.iloc[0]["value_b"])
        assert pd.isna(df_result.iloc[1]["value_b"])

        # Check value_b for rows from run2 (should have values)
        assert df_result.iloc[2]["value_b"] == "x"
        assert df_result.iloc[3]["value_b"] == "y"

        # Check run_id associations
        assert df_result.iloc[0]["consist_run_id"] == run_id1
        assert df_result.iloc[1]["consist_run_id"] == run_id1
        assert df_result.iloc[2]["consist_run_id"] == run_id2
        assert df_result.iloc[3]["consist_run_id"] == run_id2
