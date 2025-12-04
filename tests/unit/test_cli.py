import pandas as pd
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
import pytest
from sqlmodel import Session, SQLModel, create_engine
from datetime import datetime
import uuid

from consist import Tracker
from consist.cli import app
from consist.models.run import Run, RunArtifactLink
from consist.models.artifact import Artifact

runner = CliRunner()


# --- Helpers ---

@pytest.fixture
def mock_data(tmp_path):
    """Creates mock data objects."""
    # R1 -> A1 (out)
    # R2 -> A1 (in), A2 (in), A3 (out)
    run1 = Run(
        id="run1",
        model_name="create_data",
        status="completed",
        created_at=datetime(2025, 1, 1, 12, 0),
        started_at=datetime(2025, 1, 1, 12, 0),
        ended_at=datetime(2025, 1, 1, 12, 5),
        tags=["dev", "initial"],
    )

    csv_path = tmp_path / "raw.csv"
    pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).to_csv(csv_path, index=False)

    art1 = Artifact(
        id=uuid.uuid4(),
        key="raw_data",
        uri=str(csv_path),
        driver="csv",
        run_id="run1",
        hash="abc",
    )
    link1 = RunArtifactLink(run_id="run1", artifact_id=art1.id, direction="output")

    run2 = Run(
        id="run2",
        model_name="process_data",
        status="failed",
        created_at=datetime(2025, 1, 2, 12, 0),
        started_at=datetime(2025, 1, 2, 12, 0),
        ended_at=datetime(2025, 1, 2, 12, 10),
        tags=["prod"],
    )
    art2 = Artifact(
        id=uuid.uuid4(),
        key="params",
        uri="config/params.json",
        driver="json",
        hash="def",
    )
    art3 = Artifact(
        id=uuid.uuid4(),
        key="processed_data",
        uri="data/processed.parquet",
        driver="parquet",
        run_id="run2",
        hash="ghi",
    )
    link2_in1 = RunArtifactLink(run_id="run2", artifact_id=art1.id, direction="input")
    link2_in2 = RunArtifactLink(run_id="run2", artifact_id=art2.id, direction="input")
    link2_out = RunArtifactLink(run_id="run2", artifact_id=art3.id, direction="output")

    run3 = Run(
        id="run3",
        model_name="create_data",
        status="completed",
        created_at=datetime(2025, 1, 3, 12, 0),
        started_at=datetime(2025, 1, 3, 12, 0),
        ended_at=datetime(2025, 1, 3, 12, 5),
        tags=["dev"],
    )
    art4 = Artifact(
        id=uuid.uuid4(),
        key="more_raw_data",
        uri="data/more_raw.csv",
        driver="csv",
        run_id="run3",
        hash="jkl",
    )
    link3 = RunArtifactLink(run_id="run3", artifact_id=art4.id, direction="output")

    return [run1, art1, link1, run2, art2, art3, link2_in1, link2_in2, link2_out, run3, art4, link3]


@pytest.fixture
def mock_db_session(mock_data, tmp_path):
    """
    Creates an isolated in-memory DB populated with mock data for CLI testing.
    We do NOT use the global tracker fixture here because we want full control
    over the data population for specific CLI outputs.
    """
    db_path = str(tmp_path / "cli_test.db")
    engine = create_engine(f"duckdb:///{db_path}")

    # CRITICAL: Only create the core tables, ignoring MockTable from other tests
    core_tables = [Run.__table__, Artifact.__table__, RunArtifactLink.__table__]

    with engine.connect() as connection:
        with connection.begin():
            SQLModel.metadata.drop_all(connection, tables=core_tables)
            SQLModel.metadata.create_all(connection, tables=core_tables)

    with Session(engine) as session:
        for item in mock_data:
            session.add(item)
        session.commit()
        yield session

    engine.dispose()


# --- Tests ---

def test_runs_no_db():
    with patch("pathlib.Path.exists", return_value=False):
        result = runner.invoke(app, ["runs", "--db-path", "nonexistent.db"])
        assert result.exit_code != 0
        assert "Database not found at" in result.stdout


def test_runs_with_db(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), patch(
            "consist.cli.get_tracker"
    ) as mock_get_tracker:
        mock_tracker = MagicMock()
        mock_tracker.engine = mock_db_session.get_bind()
        mock_get_tracker.return_value = mock_tracker

        # We pass a dummy path because get_tracker is mocked anyway
        result = runner.invoke(app, ["runs", "--db-path", "mock.db"])

        assert result.exit_code == 0
        assert "Recent Runs" in result.stdout
        assert "run3" in result.stdout
        assert "run2" in result.stdout
        assert "run1" in result.stdout


def test_runs_filter_by_model(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--model", "create_data"])
        assert result.exit_code == 0
        assert "run1" in result.stdout
        assert "run3" in result.stdout
        assert "run2" not in result.stdout


def test_runs_filter_by_tag(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--tag", "prod"])
        assert result.exit_code == 0
        assert "run2" in result.stdout
        assert "run1" not in result.stdout


def test_runs_filter_by_status(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--status", "failed"])
        assert result.exit_code == 0
        assert "run2" in result.stdout
        assert "run1" not in result.stdout


def test_artifacts_with_db(mock_db_session, tmp_path):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["artifacts", "run2"])
        assert result.exit_code == 0
        assert "Artifacts for Run run2" in result.stdout
        assert "raw_data" in result.stdout
        assert "processed_data" in result.stdout


def test_lineage_with_db(mock_db_session, tmp_path):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["lineage", "processed_data"])
        assert result.exit_code == 0
        assert "Lineage for Artifact: processed_data" in result.stdout
        assert "Run: run2" in result.stdout
        assert "Input: raw_data" in result.stdout


def test_summary_command(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["summary"])
        assert result.exit_code == 0
        assert "Database Summary" in result.stdout
        assert "Runs: 3" in result.stdout
        assert "create_data" in result.stdout


def test_preview_command_success(mock_db_session):
    with patch("pathlib.Path.exists", return_value=True), \
            patch("consist.cli.get_tracker"), \
            patch("consist.cli.queries.get_artifact_preview") as mock_get_preview:
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_get_preview.return_value = mock_df

        result = runner.invoke(app, ["preview", "raw_data"])

        assert result.exit_code == 0
        assert "Preview: raw_data" in result.stdout
        assert "col1" in result.stdout
        assert "a" in result.stdout


def test_preview_command_artifact_not_found(mock_db_session, tmp_path):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "nonexistent_artifact"])
        assert result.exit_code == 1
        assert "Artifact 'nonexistent_artifact' not found" in result.stdout


def test_preview_command_unsupported_driver(mock_db_session, tmp_path):
    with patch("pathlib.Path.exists", return_value=True), patch("consist.cli.get_tracker") as m:
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "params"])
        assert result.exit_code == 1
        assert "Preview is not supported for driver 'json'" in result.stdout