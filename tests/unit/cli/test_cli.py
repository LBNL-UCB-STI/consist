"""TODO: add unit coverage for query/helper utilities when expanded (tools.queries, pagination)."""

import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlmodel import SQLModel, Session, create_engine
from typer.testing import CliRunner

from consist import Tracker
from consist.cli import ConsistShell, app
from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink

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

    return [
        run1,
        art1,
        link1,
        run2,
        art2,
        art3,
        link2_in1,
        link2_in2,
        link2_out,
        run3,
        art4,
        link3,
    ]


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
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as mock_get_tracker,
    ):
        mock_tracker = MagicMock()
        # For MagicMock, we CAN assign to 'engine' even if it's a property on the real class
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
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--model", "create_data"])
        assert result.exit_code == 0
        assert "run1" in result.stdout
        assert "run3" in result.stdout
        assert "run2" not in result.stdout


def test_runs_filter_by_tag(mock_db_session):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--tag", "prod"])
        assert result.exit_code == 0
        assert "run2" in result.stdout
        assert "run1" not in result.stdout


def test_runs_filter_by_status(mock_db_session):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--status", "failed"])
        assert result.exit_code == 0
        assert "run2" in result.stdout
        assert "run1" not in result.stdout


def test_runs_json_output(mock_db_session):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert any(item["id"] == "run1" for item in data)
        run2 = next(item for item in data if item["id"] == "run2")
        assert run2["tags"] == ["prod"]


def test_cli_runner_fixture_runs_json(cli_runner, tracker, sample_csv):
    # Create a run using the shared tracker and log an output so it persists
    with tracker.start_run("fixture_run", "demo") as t:
        t.log_output(sample_csv("fixture.csv"), key="fixture")

    result = cli_runner.invoke(app, ["runs", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert any(item["id"] == "fixture_run" for item in data)


def test_show_uses_renderer(mock_db_session):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m_tracker,
        patch("consist.cli._render_run_details") as render_details,
    ):
        tracker = MagicMock()
        tracker.engine = mock_db_session.get_bind()
        tracker.get_run.return_value = MagicMock()
        m_tracker.return_value = tracker

        result = runner.invoke(app, ["show", "run1"])

        assert result.exit_code == 0
        render_details.assert_called_once()


def test_artifacts_with_db(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        # We need a real Tracker to exercise the get_artifacts_for_run delegation logic
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        # Fix: Inject the mock engine into the DatabaseManager, not the Tracker property
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["artifacts", "run2"])
        assert result.exit_code == 0
        assert "Artifacts for Run run2" in result.stdout
        assert "raw_data" in result.stdout
        assert "processed_data" in result.stdout


def test_lineage_with_db(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        # Fix: Inject the mock engine into the DatabaseManager
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["lineage", "processed_data"])
        assert result.exit_code == 0
        assert "Lineage for Artifact: processed_data" in result.stdout
        assert "Run: run2" in result.stdout
        assert "Input: raw_data" in result.stdout


def test_summary_command(mock_db_session):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        m.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["summary"])
        assert result.exit_code == 0
        assert "Database Summary" in result.stdout
        assert "Runs: 3" in result.stdout
        assert "create_data" in result.stdout


def test_preview_command_success(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
        patch("consist.load") as mock_load,
    ):
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_load.return_value = mock_df

        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "raw_data"])

        assert result.exit_code == 0
        assert "Preview: raw_data" in result.stdout
        assert "col1" in result.stdout
        assert "a" in result.stdout


def test_preview_command_artifact_not_found(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        # Fix: Inject the mock engine into the DatabaseManager
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "nonexistent_artifact"])
        assert result.exit_code == 1
        assert "Artifact 'nonexistent_artifact' not found" in result.stdout


def test_preview_command_unsupported_loaded_type(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
        patch("consist.load") as mock_load,
    ):
        mock_load.return_value = {"not": "a dataframe"}

        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        # Fix: Inject the mock engine into the DatabaseManager
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "params"])
        assert result.exit_code == 0
        assert "Preview not implemented" in result.stdout


def test_schema_command_success_json(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
        patch("consist.load") as mock_load,
    ):
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_load.return_value = mock_df

        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["schema", "raw_data", "--json"])
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["type"] == "dataframe"
        assert payload["driver"] == "csv"
        assert payload["columns"] == 2
        assert payload["dtypes"]["col1"] in {"int64", "int32"}
        assert payload["dtypes"]["col2"] == "object"


def test_schema_command_artifact_not_found(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.db.engine = mock_db_session.get_bind()
        m.return_value = tracker

        result = runner.invoke(app, ["schema", "nonexistent_artifact"])
        assert result.exit_code == 1
        assert "Artifact 'nonexistent_artifact' not found" in result.stdout


def test_summary_no_runs_exits_cleanly(tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m_tracker,
        patch("consist.cli.queries.get_summary") as m_summary,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        tracker.db.engine = create_engine("duckdb:///:memory:")
        m_tracker.return_value = tracker
        m_summary.return_value = {
            "total_runs": 0,
            "completed_runs": 0,
            "failed_runs": 0,
            "total_artifacts": 0,
            "first_run_at": datetime(2025, 1, 1),
            "last_run_at": datetime(2025, 1, 1),
            "models_distribution": [],
        }

        result = runner.invoke(app, ["summary"])
        assert result.exit_code == 0
        assert "No runs found in the database" in result.stdout


# --- Shell command tests ---


def test_shell_command_invokes_cmdloop():
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m_tracker,
        patch("consist.cli.ConsistShell") as m_shell,
    ):
        tracker = MagicMock()
        m_tracker.return_value = tracker
        result = runner.invoke(app, ["shell", "--db-path", "mock.db"])
        assert result.exit_code == 0
        m_shell.assert_called_once_with(tracker)
        m_shell.return_value.cmdloop.assert_called_once()


def test_shell_runs_parsing_calls_helper():
    tracker = MagicMock()
    shell = ConsistShell(tracker)
    with patch("consist.cli._render_runs_table") as render:
        shell.do_runs("--limit 5 --model create --status failed --tag dev --tag prod")
    render.assert_called_once_with(tracker, 5, "create", ["dev", "prod"], "failed")


def test_shell_artifacts_checks_existence(capsys):
    tracker = MagicMock()
    tracker.get_run.return_value = None
    shell = ConsistShell(tracker)
    shell.do_artifacts("missing_run")
    out = capsys.readouterr().out
    assert "not found" in out


def test_shell_artifacts_calls_renderer():
    tracker = MagicMock()
    tracker.get_run.return_value = MagicMock()
    shell = ConsistShell(tracker)
    with patch("consist.cli._render_artifacts_table") as render:
        shell.do_artifacts("run1")
    render.assert_called_once_with(tracker, "run1")


def test_shell_summary_uses_renderer():
    tracker = MagicMock()
    tracker.engine = MagicMock()
    shell = ConsistShell(tracker)
    fake_summary = {"total_runs": 1}
    with (
        patch("consist.cli.Session") as session_cls,
        patch("consist.cli.queries.get_summary", return_value=fake_summary),
        patch("consist.cli._render_summary") as render,
    ):
        session_ctx = MagicMock()
        session_cls.return_value.__enter__.return_value = session_ctx
        shell.do_summary("")
        render.assert_called_once_with(fake_summary)


def test_shell_scenarios_limit_parsing_flag():
    tracker = MagicMock()
    shell = ConsistShell(tracker)
    with patch("consist.cli._render_scenarios") as render:
        shell.do_scenarios("--limit 5")
    render.assert_called_once_with(tracker, 5)


def test_shell_scenarios_limit_parsing_positional():
    tracker = MagicMock()
    shell = ConsistShell(tracker)
    with patch("consist.cli._render_scenarios") as render:
        shell.do_scenarios("3")
    render.assert_called_once_with(tracker, 3)


def test_shell_help_lists_preview_and_schema(capsys):
    tracker = MagicMock()
    shell = ConsistShell(tracker)
    shell.onecmd("help")
    out = capsys.readouterr().out
    assert "preview" in out
    assert "schema" in out
