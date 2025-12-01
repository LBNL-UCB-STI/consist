# tests/integration/test_dlt_loader.py
import pytest
from typing import Optional
from sqlmodel import SQLModel, Field, Session, text
import pandas as pd
from dlt.pipeline.exceptions import PipelineStepFailed


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

        # Should raise an error about 'ghost_col'
        # dlt catches the ValueError from the generator and wraps it in PipelineStepFailed
        with pytest.raises(PipelineStepFailed) as exc:
            tracker.ingest(artifact=artifact, data=df_drift, schema=MockTable)

        # Verify our custom error message
        assert "undefined columns" in str(exc.value).lower()
        assert "ghost_col" in str(exc.value)


class StrictPerson(SQLModel, table=True):
    __tablename__ = "strict_person"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    age: int


def test_ingest_with_invalid_data_in_strict_mode(tracker, engine):
    """
    Tests that the dlt_loader in strict mode correctly raises an exception when it encounters
    data that does not conform to the provided SQLModel schema.

    What happens:
    1. A strict schema `StrictPerson` is defined, expecting `age` to be an integer.
    2. A list of data is created, containing one valid record and one invalid record
       (where `age` is a string).
    3. The tracker attempts to ingest this data using the strict schema.

    What's checked:
    - The `tracker.ingest` call raises a `dlt.pipeline.exceptions.PipelineStepFailed` exception.
    - The exception message confirms a data validation error related to the `age` column.
    - No data is loaded into the target `strict_person` table, as the transaction is aborted.
    """
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": "thirty"},
    ]

    with tracker.start_run("run_strict_invalid", "test_model"):
        artifact = tracker.log_artifact(
            "inputs/people.json", "people", schema=StrictPerson
        )
        engine.dispose()

        with pytest.raises(PipelineStepFailed) as excinfo:
            tracker.ingest(artifact=artifact, data=data, schema=StrictPerson)

        # Check the exception message for details
        msg = str(excinfo.value).lower()
        # It might fail on type parsing OR contract violation depending on dlt version/config
        # We accept either as proof of strict enforcement
        assert "contract" in msg or "parse" in msg

    # Verify that NO data was ingested because the transaction failed
    with Session(engine) as session:
        result = session.exec(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'global_tables' AND table_name = 'strict_person'"
            )
        ).first()
        assert result is None, "Table 'strict_person' should not have been created."
