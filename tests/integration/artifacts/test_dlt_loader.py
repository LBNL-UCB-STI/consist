# tests/integration/test_dlt_loader.py
import pytest
from typing import Optional
from sqlmodel import SQLModel, Field, Session, text
import pandas as pd
from dlt.pipeline.exceptions import PipelineStepFailed


# Define a test schema for Strict Mode
class MockTable(SQLModel, table=True):
    """
    A mock SQLModel schema used for testing the `dlt_loader`'s ingestion capabilities
    in "Strict Mode" and schema enforcement scenarios.

    This class defines a simple table structure with an `id` and `val` column,
    which is used to guide `dlt` during data ingestion and validate that
    Consist can correctly handle user-defined schemas.

    Attributes
    ----------
    id : Optional[int]
        A unique identifier for each record, serving as the primary key.
    val : str
        A string value associated with each record.
    """

    __tablename__ = "mock_data"
    id: Optional[int] = Field(default=None, primary_key=True)
    val: str


def test_ingest_artifact_strict_dicts(tracker, engine):
    """
    Tests the `tracker.ingest()` method's ability to ingest a list of dictionaries
    ("scalar" ingestion) while enforcing a strict `SQLModel` schema and automatically
    injecting Consist system columns.

    This integration test verifies that `dlt_loader` correctly processes and
    loads data into DuckDB when a specific `SQLModel` schema is provided.
    It confirms that data is loaded into the correct table, and that the
    Consist-specific provenance columns (`consist_run_id`, `consist_year`) are
    automatically added and populated with the correct context from the run.

    What happens:
    1. A run is started using the provided `tracker` fixture with a specific `year`.
    2. An artifact is logged, associating it with a `MockTable` schema.
    3. A list of mock data records (dictionaries) is created.
    4. The test fixture's SQLAlchemy engine is temporarily disposed to simulate a
       separate process or thread, ensuring the `tracker.ingest` method can
       acquire a lock on the database for `dlt` operations.
    5. The `tracker.ingest` method is called with the artifact, raw data, and `MockTable` schema.

    What's checked:
    - After ingestion, a query to the DuckDB confirms the data was loaded into the
      `global_tables.mock_data` table.
    - The content of the loaded data (id, val) is verified to match the input.
    - The presence and correctness of the automatically injected system columns
      (`consist_run_id`, `consist_year`) are confirmed in the loaded data.
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
    Tests the `tracker.ingest()` method's ability to ingest a Pandas DataFrame
    directly ("vectorized" ingestion) while enforcing a strict `SQLModel` schema
    and automatically injecting Consist system columns.

    This test verifies the "High-Performance Mode" of ingestion, where `dlt_loader`
    can efficiently process entire DataFrames as single batches. It also confirms
    that the system can append to an existing table if compatible schemas are used.

    What happens:
    1. A Pandas DataFrame `df` is created with sample data.
    2. A run is started with a specific `iteration`.
    3. An artifact is logged, associating it with the `MockTable` schema.
    4. The test fixture's SQLAlchemy engine is disposed.
    5. The `tracker.ingest` method is called with the artifact, the Pandas DataFrame,
       and the `MockTable` schema.

    What's checked:
    - The data from the Pandas DataFrame is successfully appended to the `global_tables.mock_data`
      table (which was potentially created by a previous test).
    - The content of the loaded data is verified.
    - The automatically injected `consist_iteration` column is present and correctly populated.
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
    Tests the `tracker.ingest()` method's ability to ingest data directly from
    a Parquet file artifact without providing the `data` argument ("Memory-Efficient Mode").

    This test verifies that `dlt_loader` can automatically detect that the `data`
    argument is `None` and stream the data from the artifact's file path,
    demonstrating efficient processing for large files that might not fit into memory.

    What happens:
    1. A temporary directory is created for the run.
    2. A Pandas DataFrame is created and saved as a Parquet file (`stream.parquet`).
    3. A run is started.
    4. The Parquet file is logged as an artifact ("my_data_stream") with a `MockTable` schema.
    5. The test fixture's SQLAlchemy engine is disposed.
    6. The `tracker.ingest` method is called with the artifact and schema, but `data=None`.

    What's checked:
    - The data from the Parquet file is successfully ingested into the
      `global_tables.mock_data` table.
    - The content of the loaded data is verified.
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
    Tests that `tracker.ingest()` in strict mode correctly raises an exception
    when encountering data with extra columns not defined in the provided `SQLModel` schema.

    This verifies Consist's "Schema Auditing" capability, ensuring that unexpected
    schema changes in input data are detected and reported, preventing data quality
    issues in the provenance database.

    What happens:
    1. A Pandas DataFrame `df_drift` is created that includes an extra column (`ghost_col`)
       not present in the `MockTable` schema.
    2. A run is started.
    3. An artifact is logged, associating it with the `MockTable` schema.
    4. The test fixture's SQLAlchemy engine is disposed.
    5. The `tracker.ingest` method is called with the artifact, the drifting DataFrame,
       and the `MockTable` schema.

    What's checked:
    - The `tracker.ingest` call raises a `dlt.pipeline.exceptions.PipelineStepFailed`
      exception, as expected in strict mode when extra columns are present.
    - The exception message contains phrases like "undefined columns" and mentions
      the extra column "ghost_col", confirming the nature of the schema violation.
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
    """
    A strict SQLModel schema for a 'Person' entity, used to test data validation
    during ingestion.

    This schema defines the expected structure for person records, including
    an integer `age` field, which is critical for testing that `dlt_loader`
    correctly rejects data that does not conform to the specified types.

    Attributes
    ----------
    id : Optional[int]
        A unique integer identifier for the person, serving as the primary key.
    name : str
        The name of the person.
    age : int
        The age of the person, expected to be an integer.
    """

    __tablename__ = "strict_person"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    age: int


def test_ingest_with_invalid_data_in_strict_mode(tracker, engine):
    """
    Tests that `dlt_loader` in strict mode correctly raises an exception
    when it encounters data with type mismatches that do not conform to
    the provided `SQLModel` schema.

    This verifies the "Data Validation" aspect of Consist's ingestion,
    ensuring that only data meeting predefined schema constraints is
    materialized into the database, preventing corrupt or inconsistent records.

    What happens:
    1. A strict `SQLModel` schema `StrictPerson` is defined, expecting `age` to be an integer.
    2. A list of data records is prepared, containing one valid record and one
       invalid record (where `age` is a string "thirty" instead of an integer).
    3. A run is started, and an artifact ("people") is logged with the `StrictPerson` schema.
    4. The test fixture's SQLAlchemy engine is disposed.
    5. The `tracker.ingest` method is called with the artifact, the mixed data,
       and the `StrictPerson` schema.

    What's checked:
    - The `tracker.ingest` call raises a `dlt.pipeline.exceptions.PipelineStepFailed`
      exception, confirming that the ingestion process aborted due to invalid data.
    - The exception message contains keywords indicating a schema contract violation
      or data parsing error related to the `age` column.
    - After the failed ingestion, the database is queried to ensure that the
      `strict_person` table was *not* created, verifying that the transaction
      was rolled back completely.
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


def test_ingest_missing_dlt_dependency_raises(tracker, monkeypatch, tmp_path) -> None:
    """
    Missing dlt dependency should raise ImportError during ingestion.
    """
    from consist.integrations import dlt_loader

    monkeypatch.setattr(dlt_loader, "dlt", None)

    data_path = tmp_path / "data.json"
    data_path.write_text('[{"id": 1}]', encoding="utf-8")

    with tracker.start_run("run_missing_dlt", "test_model"):
        artifact = tracker.log_artifact(data_path, "missing_dlt", driver="json")
        with pytest.raises(ImportError):
            tracker.ingest(artifact=artifact, data=[{"id": 1}])
