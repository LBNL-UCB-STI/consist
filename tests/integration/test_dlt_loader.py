# tests/integration/test_dlt_loader.py
import pytest
from typing import Optional
from sqlmodel import SQLModel, Field, Session, text
import pandas as pd


# Define a test schema for Strict Mode
class MockTable(SQLModel, table=True):
    __tablename__ = "mock_data"
    id: Optional[int] = Field(default=None, primary_key=True)
    val: str


def test_ingest_artifact_strict_dicts(tracker, engine):
    """
    Tests the `ingest` method in "Strict Mode".

    This integration test verifies that `dlt_loader` correctly ingests data into DuckDB when a
    specific SQLModel schema is provided. It checks that data is loaded into the correct table
    and that the Consist-specific system columns (e.g., `consist_run_id`, `consist_year`) are
    automatically added and populated with the correct context from the run.

    What happens:
    1. A run is started using the provided `tracker` fixture.
    2. An artifact is logged, associating it with a specific SQLModel schema (`MockTable`).
    3. A list of mock data records is created.
    4. The test fixture's SQLAlchemy engine is disposed to simulate a separate process,
       allowing the tracker's internal `ingest` method to acquire a lock on the database.
    5. The tracker's `ingest` method is called with the artifact, data, and schema.

    What's checked:
    - After ingestion, the database is queried to ensure the data was loaded into the
      `global_tables.mock_data` table.
    - The content of the loaded data is verified.
    - The presence and correctness of the automatically injected system columns (`consist_run_id`,
      `consist_year`) are confirmed.
    """

    # 1. Setup Run
    with tracker.start_run("run_dict", "test_model", year=2030):
        # Log artifact (doesn't need to exist physically for dict ingestion)
        artifact = tracker.log_artifact(
            "inputs/manual.json", "my_data", schema=MockTable
        )

        # Raw Data (Dicts)
        raw_data = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]

        # Dispose test fixture engine to allow dlt to lock the DB
        engine.dispose()

        # Ingest
        tracker.ingest(artifact=artifact, data=raw_data, schema=MockTable)

    # 2. Verify in DB
    with Session(engine) as session:
        # Note: dlt creates tables in 'global_tables' schema by default
        result = session.exec(
            text("SELECT * FROM global_tables.mock_data ORDER BY id")
        ).fetchall()

        assert len(result) == 2
        row = result[0]

        # Check Business Data
        assert row.id == 1
        assert row.val == "A"

        # Check Consist System Columns
        # Access via _mapping for safety if row is a tuple-like object
        assert row._mapping["consist_run_id"] == "run_dict"
        assert row._mapping["consist_year"] == 2030


def test_ingest_vectorized_pandas(tracker, engine):
    """
    Test 2: High-Performance Mode.
    Ingesting a Pandas DataFrame directly (Vectorized).
    """
    df = pd.DataFrame({"id": [10, 20, 30], "val": ["X", "Y", "Z"]})

    with tracker.start_run("run_pandas", "test_model", iteration=5):
        artifact = tracker.log_artifact("inputs/df.csv", "my_data_df", schema=MockTable)

        engine.dispose()

        # Ingest DataFrame directly
        tracker.ingest(artifact=artifact, data=df, schema=MockTable)

    with Session(engine) as session:
        # Check that data was appended to the SAME table (mock_data)
        # dlt handles merging schemas if compatible
        result = session.exec(
            text(
                "SELECT * FROM global_tables.mock_data WHERE consist_run_id='run_pandas' ORDER BY id"
            )
        ).fetchall()

        assert len(result) == 3
        assert result[0].val == "X"
        assert result[0]._mapping["consist_iteration"] == 5  # Check iteration injection


def test_ingest_streaming_parquet(tracker, engine, tmp_path):
    """
    Test 3: Memory-Efficient Mode.
    Ingesting directly from a Parquet file artifact (no data argument).
    """
    run_dir = tmp_path / "stream_run"
    run_dir.mkdir()

    # Create physical parquet file
    df = pd.DataFrame({"id": [100, 101], "val": ["Stream", "Stream"]})
    fpath = run_dir / "stream.parquet"
    df.to_parquet(fpath)

    # Initialize a specific tracker pointing to this dir (or use fixture if compatible)
    # The fixture 'tracker' points to 'run_dir' fixture, which is separate.
    # We can just write to the fixture's run_dir.
    # Let's use the file we just created.

    with tracker.start_run("run_stream", "test_model"):
        # Log the file
        # Note: We must pass absolute path or valid relative path
        artifact = tracker.log_artifact(
            str(fpath), key="my_data_stream", schema=MockTable
        )

        engine.dispose()

        # Ingest WITHOUT passing 'data'
        # This triggers the auto-load streaming logic in tracker.ingest -> dlt_loader
        tracker.ingest(artifact=artifact, schema=MockTable)

    with Session(engine) as session:
        result = session.exec(
            text(
                "SELECT * FROM global_tables.mock_data WHERE consist_run_id='run_stream' ORDER BY id"
            )
        ).fetchall()

        assert len(result) == 2
        assert result[0].id == 100
        assert result[0].val == "Stream"


def test_schema_drift_warning(tracker, engine):
    """
    Test 4: Schema Auditing.
    Verifies that passing a DataFrame with extra columns triggers a warning
    when a Strict Schema is provided.
    """
    df_drift = pd.DataFrame(
        {"id": [999], "val": ["Ghost"], "ghost_col": [123]}  # <--- Not in MockTable
    )

    with tracker.start_run("run_drift", "test_model"):
        artifact = tracker.log_artifact(
            "inputs/ghost.csv", "my_data_ghost", schema=MockTable
        )
        engine.dispose()

        # Should warn about 'ghost_col'
        with pytest.warns(UserWarning, match="ghost_col"):
            tracker.ingest(artifact=artifact, data=df_drift, schema=MockTable)
