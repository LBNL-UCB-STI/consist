# tests/integration/test_dlt_loader.py
import pytest
from typing import Optional
from sqlmodel import SQLModel, Field
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


def test_ingest_artifact_strict_dicts(tracker):
    """
    Tests the `tracker.ingest()` method's ability to ingest a list of dictionaries
    ("scalar" ingestion) while enforcing a strict `SQLModel` schema and automatically
    injecting Consist system columns.
    """

    # 1. Setup Run
    with tracker.start_run("run_dict", "test_model", year=2030):
        # Log artifact (doesn't need to exist physically for dict ingestion)
        artifact = tracker.log_artifact(
            "inputs/manual.json", "my_data", schema=MockTable
        )

        # Raw Data (Dicts)
        raw_data = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]

        # Ingest
        tracker.ingest(artifact=artifact, data=raw_data, schema=MockTable)

    # 2. Verify in DB
    result = tracker.query_df(
        "SELECT id, val, consist_run_id, consist_year, consist_iteration "
        "FROM global_tables.mock_data ORDER BY id"
    )

    assert len(result) == 2
    row = result.iloc[0]

    # Check Business Data
    assert row["id"] == 1
    assert row["val"] == "A"

    # Check Consist System Columns
    assert row["consist_run_id"] == "run_dict"
    assert row["consist_year"] == 2030


def test_ingest_vectorized_pandas(tracker):
    """
    Tests the `tracker.ingest()` method's ability to ingest a Pandas DataFrame
    directly ("vectorized" ingestion) while enforcing a strict `SQLModel` schema
    and automatically injecting Consist system columns.
    """
    df = pd.DataFrame({"id": [10, 20, 30], "val": ["X", "Y", "Z"]})

    with tracker.start_run("run_pandas", "test_model", iteration=5):
        artifact = tracker.log_artifact("inputs/df.csv", "my_data_df", schema=MockTable)

        # Ingest DataFrame directly
        tracker.ingest(artifact=artifact, data=df, schema=MockTable)

    result = tracker.query_df(
        "SELECT id, val, consist_run_id, consist_year, consist_iteration "
        "FROM global_tables.mock_data "
        "WHERE consist_run_id = ? "
        "ORDER BY id",
        ["run_pandas"],
    )

    assert len(result) == 3
    assert result.iloc[0]["val"] == "X"
    assert result.iloc[0]["consist_iteration"] == 5


def test_ingest_streaming_parquet(tracker, tmp_path):
    """
    Tests the `tracker.ingest()` method's ability to ingest data directly from
    a Parquet file artifact without providing the `data` argument ("Memory-Efficient Mode").
    """
    run_dir = tmp_path / "stream_run"
    run_dir.mkdir()

    # Create physical parquet file
    df = pd.DataFrame({"id": [100, 101], "val": ["Stream", "Stream"]})
    fpath = run_dir / "stream.parquet"
    df.to_parquet(fpath)

    with tracker.start_run("run_stream", "test_model"):
        artifact = tracker.log_artifact(
            str(fpath), key="my_data_stream", schema=MockTable
        )

        # Ingest WITHOUT passing 'data'
        tracker.ingest(artifact=artifact, schema=MockTable)

    result = tracker.query_df(
        "SELECT id, val, consist_run_id, consist_year, consist_iteration "
        "FROM global_tables.mock_data "
        "WHERE consist_run_id = ? "
        "ORDER BY id",
        ["run_stream"],
    )

    assert len(result) == 2
    assert result.iloc[0]["id"] == 100
    assert result.iloc[0]["val"] == "Stream"


def test_schema_drift_warning(tracker):
    """
    Tests that `tracker.ingest()` in strict mode correctly raises an exception
    when encountering data with extra columns not defined in the provided `SQLModel` schema.
    """
    df_drift = pd.DataFrame(
        {"id": [999], "val": ["Ghost"], "ghost_col": [123]}  # <--- Not in MockTable
    )

    with tracker.start_run("run_drift", "test_model"):
        artifact = tracker.log_artifact(
            "inputs/ghost.csv", "my_data_ghost", schema=MockTable
        )

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
    """

    __tablename__ = "strict_person"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    age: int


def test_ingest_with_invalid_data_in_strict_mode(tracker):
    """
    Tests that `dlt_loader` in strict mode correctly raises an exception
    when it encounters data with type mismatches that do not conform to
    the provided `SQLModel` schema.
    """
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": "thirty"},
    ]

    with tracker.start_run("run_strict_invalid", "test_model"):
        artifact = tracker.log_artifact(
            "inputs/people.json", "people", schema=StrictPerson
        )

        with pytest.raises(PipelineStepFailed) as excinfo:
            tracker.ingest(artifact=artifact, data=data, schema=StrictPerson)

        # Check the exception message for details
        msg = str(excinfo.value).lower()
        assert "contract" in msg or "parse" in msg

    # Verify that NO data was ingested because the transaction failed
    result = tracker.query_df(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'global_tables' AND table_name = 'strict_person'"
    )
    assert result.empty, "Table 'strict_person' should not have been created."


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
