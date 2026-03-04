"""TODO: add unit coverage for query/helper utilities when expanded (tools.queries)."""

import json
from pathlib import Path
import re
import uuid
from datetime import datetime
from typing import Optional, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, Session, create_engine, text
from typer.testing import CliRunner

from consist import Tracker
from consist.cli import (
    ConsistShell,
    _render_run_details,
    _render_scenarios,
    _tracker_session,
    app,
    find_db_path,
)
from consist.core.maintenance import (
    DatabaseMaintenance,
    MergeResult,
    PurgePlan,
    PurgeResult,
    RebuildResult,
)
from consist.core.persistence import DatabaseManager
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
        config_hash=None,
        git_hash=None,
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
        container_uri=str(csv_path),
        driver="csv",
        run_id="run1",
        hash="abc",
    )
    link1 = RunArtifactLink(run_id="run1", artifact_id=art1.id, direction="output")

    run2 = Run(
        id="run2",
        model_name="process_data",
        config_hash=None,
        git_hash=None,
        status="failed",
        created_at=datetime(2025, 1, 2, 12, 0),
        started_at=datetime(2025, 1, 2, 12, 0),
        ended_at=datetime(2025, 1, 2, 12, 10),
        tags=["prod"],
    )
    art2 = Artifact(
        id=uuid.uuid4(),
        key="params",
        container_uri="config/params.json",
        driver="json",
        hash="def",
    )
    art3 = Artifact(
        id=uuid.uuid4(),
        key="processed_data",
        container_uri="data/processed.parquet",
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
        config_hash=None,
        git_hash=None,
        status="completed",
        created_at=datetime(2025, 1, 3, 12, 0),
        started_at=datetime(2025, 1, 3, 12, 0),
        ended_at=datetime(2025, 1, 3, 12, 5),
        tags=["dev"],
    )
    art4 = Artifact(
        id=uuid.uuid4(),
        key="more_raw_data",
        container_uri="data/more_raw.csv",
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
    core_tables = [
        getattr(Run, "__table__"),
        getattr(Artifact, "__table__"),
        getattr(RunArtifactLink, "__table__"),
    ]

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


def test_cli_version_option_prints_version_and_exits_cleanly():
    with patch("consist.cli._resolve_cli_version", return_value="9.9.9"):
        result = runner.invoke(app, ["--version"])
        short_result = runner.invoke(app, ["-V"])

    assert result.exit_code == 0
    assert "consist 9.9.9" in result.stdout
    assert short_result.exit_code == 0
    assert "consist 9.9.9" in short_result.stdout


def test_cli_root_without_command_keeps_usage_exit_code():
    result = runner.invoke(app, [])

    assert result.exit_code == 2


def test_tracker_session_uses_db_session_scope(tmp_path):
    db = DatabaseManager(str(tmp_path / "cli_session.db"))
    tracker = MagicMock()
    tracker.engine = create_engine("duckdb:///:memory:")
    tracker.db = db

    with patch.object(db, "session_scope", wraps=db.session_scope) as session_scope:
        with _tracker_session(tracker) as session:
            assert session is not None
        assert session_scope.call_count == 1


def test_tracker_session_falls_back_to_engine(mock_db_session):
    tracker = MagicMock()
    tracker.engine = mock_db_session.get_bind()
    tracker.db = None

    with _tracker_session(tracker) as session:
        assert session.get_bind() == mock_db_session.get_bind()


def test_find_db_path_prefers_explicit_path(monkeypatch):
    monkeypatch.setenv("CONSIST_DB", "/tmp/from_env.duckdb")
    assert find_db_path("/tmp/explicit.duckdb") == "/tmp/explicit.duckdb"


def test_find_db_path_uses_env_when_no_explicit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    env_db = tmp_path / "env.duckdb"
    monkeypatch.setenv("CONSIST_DB", str(env_db))
    assert find_db_path() == str(env_db)


def test_find_db_path_uses_cwd_provenance_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CONSIST_DB", raising=False)
    (tmp_path / "provenance.duckdb").write_text("", encoding="utf-8")
    assert find_db_path() == "provenance.duckdb"


@pytest.mark.parametrize("subdir", ["data", "db", ".consist"])
def test_find_db_path_uses_common_subdirs(tmp_path, monkeypatch, subdir):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CONSIST_DB", raising=False)
    subdir_path = tmp_path / subdir
    subdir_path.mkdir(parents=True, exist_ok=True)
    (subdir_path / "provenance.duckdb").write_text("", encoding="utf-8")
    assert find_db_path() == str(Path(subdir) / "provenance.duckdb")


def test_find_db_path_defaults_when_nothing_found(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CONSIST_DB", raising=False)
    assert find_db_path() == "provenance.duckdb"


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


def test_runs_json_output_handles_non_serializable_meta(mock_db_session):
    run = MagicMock()
    run.id = "run_with_set_meta"
    run.model_name = "demo"
    run.status = "completed"
    run.parent_run_id = None
    run.year = 2025
    run.created_at = datetime(2025, 1, 1, 12, 0)
    run.duration_seconds = 1.0
    run.tags = ["demo"]
    run.meta = {"bad_set": {1, 2}}

    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m_tracker,
        patch("consist.cli.queries.get_runs", return_value=[run]) as m_get_runs,
    ):
        m_tracker.return_value.engine = mock_db_session.get_bind()
        result = runner.invoke(app, ["runs", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 1
    assert payload[0]["id"] == "run_with_set_meta"
    assert isinstance(payload[0]["meta"]["bad_set"], str)
    m_get_runs.assert_called_once()


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


def test_render_run_details_includes_config_and_meta(capsys):
    run = MagicMock()
    run.id = "run_details"
    run.model_name = "demo_model"
    run.status = "completed"
    run.parent_run_id = "scenario_a"
    run.year = 2030
    run.created_at = datetime(2025, 1, 1, 12, 0)
    run.duration_seconds = 300.0
    run.tags = ["dev", "test"]
    run.signature = "abcdef1234567890"
    run.config = {"threshold": 0.5}
    run.meta = {"note": "hello"}

    _render_run_details(run)

    out = capsys.readouterr().out
    assert "Run Details" in out
    assert "Configuration" in out
    assert "threshold" in out
    assert "Metadata:" in out
    assert "note" in out


def test_artifacts_with_db(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        # We need a real Tracker to exercise the get_artifacts_for_run delegation logic
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        # Fix: Inject the mock engine into the DatabaseManager, not the Tracker property
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        result = runner.invoke(app, ["artifacts", "run2"])
        assert result.exit_code == 0
        assert "Artifacts for Run run2" in result.stdout
        assert "raw_data" in result.stdout
        assert "processed_data" in result.stdout


def test_artifacts_query_mode_with_param(cli_runner, tracker, sample_csv):
    with tracker.start_run("run_query_artifacts", "beam"):
        tracker.log_output(
            sample_csv("facet_output.csv"),
            key="facet_output",
            facet={
                "artifact_family": "linkstats_unmodified_phys_sim_iter_parquet",
                "phys_sim_iteration": 2,
            },
            facet_schema_version="1",
            facet_index=True,
        )

    result = cli_runner.invoke(
        app,
        [
            "artifacts",
            "--param",
            "beam.phys_sim_iteration=2",
            "--family-prefix",
            "linkstats_unmodified",
        ],
        env={"COLUMNS": "200"},
    )
    assert result.exit_code == 0
    assert "Artifact Query Results" in result.stdout
    assert "facet_output" in result.stdout
    assert "1" in result.stdout


def test_views_create_grouped_command(cli_runner, tracker, tmp_path):
    path = tmp_path / "grouped_cli.parquet"
    pd.DataFrame({"id": [1], "value": [1.0]}).to_parquet(path)

    with tracker.start_run(
        "run_grouped_cli", "beam", year=2018, iteration=0, cache_mode="overwrite"
    ):
        artifact = tracker.log_artifact(
            str(path),
            key="grouped_cli_key",
            driver="parquet",
            profile_file_schema=True,
            facet={"artifact_family": "grouped_cli", "year": 2018, "iteration": 0},
            facet_index=True,
        )

    schema_id = artifact.meta.get("schema_id")
    assert schema_id is not None

    result = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_grouped_cli",
            "--schema-id",
            schema_id,
            "--namespace",
            "beam",
            "--param",
            "artifact_family=grouped_cli",
            "--attach-facet",
            "year",
            "--driver",
            "parquet",
        ],
    )
    assert result.exit_code == 0
    assert "Created grouped view 'v_grouped_cli'" in result.stdout

    with tracker.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM v_grouped_cli")).scalar()
    assert count == 1


def test_views_create_grouped_command_missing_file_error(cli_runner, tracker, tmp_path):
    path = tmp_path / "grouped_cli_missing.parquet"
    pd.DataFrame({"id": [1], "value": [1.0]}).to_parquet(path)

    with tracker.start_run(
        "run_grouped_cli_missing",
        "beam",
        year=2018,
        iteration=0,
        cache_mode="overwrite",
    ):
        artifact = tracker.log_artifact(
            str(path),
            key="grouped_cli_missing_key",
            driver="parquet",
            profile_file_schema=True,
            facet={
                "artifact_family": "grouped_cli_missing",
                "year": 2018,
                "iteration": 0,
            },
            facet_index=True,
        )

    schema_id = artifact.meta.get("schema_id")
    assert schema_id is not None
    path.unlink()

    result = cli_runner.invoke(
        app,
        [
            "views",
            "create",
            "v_grouped_cli_missing",
            "--schema-id",
            schema_id,
            "--namespace",
            "beam",
            "--param",
            "artifact_family=grouped_cli_missing",
            "--driver",
            "parquet",
            "--missing-files",
            "error",
            "--mode",
            "cold_only",
        ],
    )

    assert result.exit_code == 1
    normalized_stdout = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout).replace("\n", "")
    assert "grouped_cli_missing.parquet" in normalized_stdout


def test_lineage_with_db(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        # Fix: Inject the mock engine into the DatabaseManager
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
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


def test_render_scenarios_no_results_prints_empty_message(tracker, capsys):
    _render_scenarios(tracker, limit=5)

    out = capsys.readouterr().out
    assert "No scenarios found." in out


def test_render_scenarios_populated_prints_rows(tracker, capsys):
    with Session(tracker.engine) as session:
        session.add_all(
            [
                Run(
                    id="scenario_child_1",
                    model_name="demo",
                    config_hash=None,
                    git_hash=None,
                    status="completed",
                    parent_run_id="scenario_x",
                    created_at=datetime(2025, 1, 1, 12, 0),
                    started_at=datetime(2025, 1, 1, 12, 0),
                ),
                Run(
                    id="scenario_child_2",
                    model_name="demo",
                    config_hash=None,
                    git_hash=None,
                    status="completed",
                    parent_run_id="scenario_x",
                    created_at=datetime(2025, 1, 1, 12, 5),
                    started_at=datetime(2025, 1, 1, 12, 5),
                ),
            ]
        )
        session.commit()

    _render_scenarios(tracker, limit=5)

    out = capsys.readouterr().out
    assert "Scenarios" in out
    assert "scenario_x" in out
    assert "2" in out


def test_scenario_command_no_runs_returns_exit_1(cli_runner):
    result = cli_runner.invoke(app, ["scenario", "missing_scenario"])
    assert result.exit_code == 1
    assert "No runs found for scenario 'missing_scenario'" in result.stdout


def test_scenario_command_lists_matching_runs(cli_runner, tracker):
    with Session(tracker.engine) as session:
        session.add(
            Run(
                id="scenario_run_1",
                model_name="transport",
                config_hash=None,
                git_hash=None,
                status="completed",
                parent_run_id="scenario_2025",
                year=2025,
                created_at=datetime(2025, 1, 1, 12, 0),
                started_at=datetime(2025, 1, 1, 12, 0),
            )
        )
        session.commit()

    result = cli_runner.invoke(app, ["scenario", "scenario_2025"])
    assert result.exit_code == 0
    assert "Runs in Scenario: scenario_2025" in result.stdout
    assert "transport" in result.stdout
    assert "2025" in result.stdout


def test_preview_command_success(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
        patch("consist.load") as mock_load,
    ):
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_load.return_value = mock_df

        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
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
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
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
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        result = runner.invoke(app, ["preview", "params"])
        assert result.exit_code == 0
        assert "Preview not implemented" in result.stdout


def test_schema_export_command_success_stdout(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        with patch.object(
            tracker,
            "export_schema_sqlmodel",
            return_value="class MyTable(SQLModel, table=True):\n    pass\n",
        ):
            result = runner.invoke(
                app,
                [
                    "schema",
                    "export",
                    "--artifact-id",
                    "00000000-0000-0000-0000-000000000000",
                ],
            )
        assert result.exit_code == 0
        assert "class MyTable" in result.stdout


def test_schema_export_command_requires_selector(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        result = runner.invoke(app, ["schema", "export"])
        assert result.exit_code == 2
        assert (
            "Provide exactly one of --schema-id, --artifact-id, or --artifact-key"
            in result.stdout
        )


def test_schema_export_command_schema_not_found_exits_1(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        with patch.object(tracker, "export_schema_sqlmodel", side_effect=KeyError()):
            result = runner.invoke(
                app,
                [
                    "schema",
                    "export",
                    "--artifact-id",
                    "00000000-0000-0000-0000-000000000000",
                ],
            )
        assert result.exit_code == 1
        assert "Captured schema not found" in result.stdout


def test_schema_export_command_rejects_invalid_prefer_source(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        result = runner.invoke(
            app,
            [
                "schema",
                "export",
                "--artifact-id",
                "00000000-0000-0000-0000-000000000000",
                "--prefer-source",
                "bogus",
            ],
        )
        assert result.exit_code == 2
        assert "--prefer-source must be either 'file' or 'duckdb'" in result.stdout


def test_schema_export_command_passes_prefer_source(mock_db_session, tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
        tracker.db.engine = cast(Engine, mock_db_session.get_bind())
        m.return_value = tracker

        with patch.object(
            tracker,
            "export_schema_sqlmodel",
            return_value="class MyTable(SQLModel, table=True):\n    pass\n",
        ) as mock_export:
            result = runner.invoke(
                app,
                [
                    "schema",
                    "export",
                    "--artifact-id",
                    "00000000-0000-0000-0000-000000000000",
                    "--prefer-source",
                    "duckdb",
                ],
            )
        assert result.exit_code == 0
        assert mock_export.call_args.kwargs["prefer_source"] == "duckdb"


def test_validate_paginates_artifacts(tmp_path, monkeypatch):
    db_path = tmp_path / "validate_test.duckdb"
    engine = create_engine(f"duckdb:///{db_path}")

    core_tables = [getattr(Artifact, "__table__")]
    with engine.connect() as connection:
        with connection.begin():
            SQLModel.metadata.drop_all(connection, tables=core_tables)
            SQLModel.metadata.create_all(connection, tables=core_tables)

    present_path = tmp_path / "present.txt"
    present_path.write_text("ok", encoding="utf-8")
    present_path_2 = tmp_path / "present_2.txt"
    present_path_2.write_text("ok", encoding="utf-8")

    missing_path = tmp_path / "missing.txt"

    artifacts = [
        Artifact(
            id=uuid.uuid4(),
            key="present",
            container_uri=str(present_path),
            driver="txt",
            run_id="run_a",
            hash="hash_present",
            created_at=datetime(2025, 1, 1, 0, 0),
        ),
        Artifact(
            id=uuid.uuid4(),
            key="missing",
            container_uri=str(missing_path),
            driver="txt",
            run_id="run_b",
            hash="hash_missing",
            created_at=datetime(2025, 1, 1, 0, 1),
        ),
        Artifact(
            id=uuid.uuid4(),
            key="present_2",
            container_uri=str(present_path_2),
            driver="txt",
            run_id="run_c",
            hash="hash_present_2",
            created_at=datetime(2025, 1, 1, 0, 2),
        ),
    ]

    with Session(engine) as session:
        session.add_all(artifacts)
        session.commit()

    tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
    assert tracker.db is not None
    tracker.db.engine = engine

    monkeypatch.setenv("CONSIST_VALIDATE_BATCH_SIZE", "2")

    with patch("consist.cli.get_tracker", return_value=tracker):
        result = runner.invoke(app, ["validate", "--db-path", str(db_path)])

    assert result.exit_code == 0
    assert "Found 1 missing artifacts" in result.stdout
    assert "missing" in result.stdout
    assert "present_2.txt" not in result.stdout

    engine.dispose()


def test_summary_no_runs_exits_cleanly(tmp_path):
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("consist.cli.get_tracker") as m_tracker,
        patch("consist.cli.queries.get_summary") as m_summary,
    ):
        tracker = Tracker(run_dir=tmp_path, db_path=":memory:")
        assert tracker.db is not None
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


def _seed_db_for_maintenance_cli(db_path: Path) -> None:
    db = DatabaseManager(str(db_path))
    artifact_id = uuid.uuid4()
    now = datetime(2025, 1, 1, 12, 0)
    try:
        with db.session_scope() as session:
            session.add(
                Run(
                    id="cli_run",
                    model_name="demo",
                    config_hash=None,
                    git_hash=None,
                    status="completed",
                    created_at=now,
                    started_at=now,
                    ended_at=now,
                )
            )
            session.add(
                Artifact(
                    id=artifact_id,
                    key="cli_artifact",
                    container_uri="outputs://cli_artifact.parquet",
                    driver="parquet",
                    run_id="cli_run",
                )
            )
            session.add(
                RunArtifactLink(
                    run_id="cli_run",
                    artifact_id=artifact_id,
                    direction="output",
                )
            )
            session.commit()
    finally:
        db.engine.dispose()


def _seed_unscoped_cache_global_table(db_path: Path, table_name: str) -> None:
    db = DatabaseManager(str(db_path))
    try:
        with db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                f"CREATE TABLE global_tables.{table_name} (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                f"""
                INSERT INTO global_tables.{table_name} (content_hash)
                VALUES ('hash_a')
                """
            )
    finally:
        db.engine.dispose()


def _write_cli_snapshot(
    json_dir: Path, *, run_id: str, artifact_id: Optional[uuid.UUID] = None
) -> None:
    payload = {
        "run": {"id": run_id, "model_name": "demo"},
        "inputs": [],
        "outputs": [],
        "config": {},
        "facet": {},
    }
    if artifact_id is not None:
        payload["outputs"] = [
            {
                "id": str(artifact_id),
                "key": "rebuilt_output",
                "container_uri": "outputs://rebuilt_output.parquet",
                "driver": "parquet",
            }
        ]
    json_dir.mkdir(parents=True, exist_ok=True)
    (json_dir / f"{run_id}.json").write_text(json.dumps(payload), encoding="utf-8")


def test_db_inspect_json_output_includes_expected_keys(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_maintenance_inspect.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["db", "inspect", "--json", "--db-path", str(db_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "total_runs",
        "runs_by_status",
        "total_artifacts",
        "orphaned_artifact_count",
        "zombie_run_ids",
        "global_table_sizes",
        "db_file_size_mb",
        "json_snapshot_count",
        "json_db_parity",
    }


def test_db_doctor_json_output_includes_expected_keys(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_maintenance_doctor.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["db", "doctor", "--json", "--db-path", str(db_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "zombie_run_ids",
        "completed_without_end_time",
        "dangling_parent_run_ids",
        "artifacts_with_missing_producing_run",
        "global_table_schema_drift",
    }


def test_db_snapshot_json_output_includes_expected_keys(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_snapshot_source.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)
    out_path = tmp_path / "snapshots" / "cli_snapshot.duckdb"

    result = runner.invoke(
        app,
        [
            "db",
            "snapshot",
            "--out",
            str(out_path),
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["snapshot_path"] == str(out_path)
    assert payload["checkpoint"] is True
    assert payload["sidecar_path"] == str(
        out_path.with_name(f"{out_path.stem}.snapshot_meta.json")
    )
    assert out_path.exists()
    assert Path(payload["sidecar_path"]).exists()


def test_db_snapshot_no_checkpoint_reflected_in_json_output(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_snapshot_no_checkpoint_source.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)
    out_path = tmp_path / "snapshots" / "cli_snapshot_no_checkpoint.duckdb"

    result = runner.invoke(
        app,
        [
            "db",
            "snapshot",
            "--out",
            str(out_path),
            "--no-checkpoint",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["checkpoint"] is False
    assert payload["snapshot_path"] == str(out_path)
    assert payload["sidecar_path"] == str(
        out_path.with_name(f"{out_path.stem}.snapshot_meta.json")
    )


def test_db_rebuild_json_output_is_parseable(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_rebuild_target.duckdb"
    db = DatabaseManager(str(db_path))
    db.engine.dispose()
    snapshot_dir = tmp_path / "cli_rebuild_snapshots"
    _write_cli_snapshot(
        snapshot_dir,
        run_id="cli_rebuild_run",
        artifact_id=uuid.uuid4(),
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "rebuild",
            "--json-dir",
            str(snapshot_dir),
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "json_files_scanned",
        "runs_inserted",
        "runs_already_present",
        "artifacts_inserted",
        "errors",
        "dry_run",
    }
    assert payload["json_files_scanned"] == 1
    assert payload["runs_inserted"] == 1
    assert payload["dry_run"] is False


def test_db_rebuild_passes_full_mode_to_maintenance(tmp_path, monkeypatch):
    maintenance = MagicMock()
    maintenance.rebuild_from_json.return_value = RebuildResult(
        json_files_scanned=0,
        runs_inserted=0,
        runs_already_present=0,
        artifacts_inserted=0,
        errors=[],
        dry_run=False,
    )
    monkeypatch.setattr(
        "consist.cli._maintenance_service", lambda _db_path: maintenance
    )

    result = runner.invoke(
        app,
        [
            "db",
            "rebuild",
            "--json-dir",
            str(tmp_path),
            "--mode",
            "full",
            "--db-path",
            str(tmp_path / "target.duckdb"),
        ],
    )

    assert result.exit_code == 0
    assert maintenance.rebuild_from_json.call_count == 1
    kwargs = maintenance.rebuild_from_json.call_args.kwargs
    assert kwargs["mode"] == "full"
    assert "Mode" in result.stdout
    assert "full" in result.stdout


def test_db_rebuild_defaults_mode_to_minimal(tmp_path, monkeypatch):
    maintenance = MagicMock()
    maintenance.rebuild_from_json.return_value = RebuildResult(
        json_files_scanned=0,
        runs_inserted=0,
        runs_already_present=0,
        artifacts_inserted=0,
        errors=[],
        dry_run=False,
    )
    monkeypatch.setattr(
        "consist.cli._maintenance_service", lambda _db_path: maintenance
    )

    result = runner.invoke(
        app,
        [
            "db",
            "rebuild",
            "--json-dir",
            str(tmp_path),
            "--db-path",
            str(tmp_path / "target.duckdb"),
            "--json",
        ],
    )

    assert result.exit_code == 0
    assert maintenance.rebuild_from_json.call_count == 1
    kwargs = maintenance.rebuild_from_json.call_args.kwargs
    assert kwargs["mode"] == "minimal"
    payload = json.loads(result.stdout)
    assert payload["json_files_scanned"] == 0


def test_db_compact_json_output_is_parseable(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_compact_target.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "compact",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {"compacted": True}


def test_db_export_json_output_is_parseable(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_export_source.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)
    shard_path = tmp_path / "cli_export_shard.duckdb"

    result = runner.invoke(
        app,
        [
            "db",
            "export",
            "cli_run",
            "--out",
            str(shard_path),
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "run_ids",
        "artifact_count",
        "out_path",
        "ingested_rows",
        "ingested_table_modes",
        "unscoped_cache_tables_skipped",
        "snapshots_copied",
    }
    assert payload["run_ids"] == ["cli_run"]
    assert payload["out_path"] == str(shard_path)
    assert shard_path.exists()


def test_db_export_dry_run_json_output_is_parseable_and_no_writes(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "cli_export_dry_run_source.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)
    shard_path = tmp_path / "cli_export_dry_run_shard.duckdb"

    result = runner.invoke(
        app,
        [
            "db",
            "export",
            "cli_run",
            "--out",
            str(shard_path),
            "--dry-run",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "run_ids",
        "artifact_count",
        "out_path",
        "ingested_rows",
        "ingested_table_modes",
        "unscoped_cache_tables_skipped",
        "snapshots_copied",
    }
    assert payload["run_ids"] == ["cli_run"]
    assert payload["out_path"] == str(shard_path)
    assert not shard_path.exists()


def test_db_export_non_json_warns_when_unscoped_cache_tables_skipped(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "cli_export_warn_source.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    _seed_unscoped_cache_global_table(db_path, "cache_export_warn")
    monkeypatch.chdir(tmp_path)
    shard_path = tmp_path / "cli_export_warn_shard.duckdb"

    result = runner.invoke(
        app,
        [
            "db",
            "export",
            "cli_run",
            "--out",
            str(shard_path),
            "--include-data",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    assert "Warning: skipped unscoped cache table(s) during export" in result.stdout
    assert "cache_export_warn" in result.stdout


def test_db_export_no_children_excludes_descendants(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_export_no_children_source.duckdb"
    db = DatabaseManager(str(db_path))
    parent_artifact_id = uuid.uuid4()
    child_artifact_id = uuid.uuid4()
    try:
        with db.session_scope() as session:
            session.add_all(
                [
                    Run(
                        id="parent_run",
                        model_name="demo",
                        config_hash=None,
                        git_hash=None,
                        status="completed",
                    ),
                    Run(
                        id="child_run",
                        model_name="demo",
                        config_hash=None,
                        git_hash=None,
                        status="completed",
                        parent_run_id="parent_run",
                    ),
                    Artifact(
                        id=parent_artifact_id,
                        key="parent_art",
                        container_uri="outputs://parent.parquet",
                        driver="parquet",
                        run_id="parent_run",
                    ),
                    Artifact(
                        id=child_artifact_id,
                        key="child_art",
                        container_uri="outputs://child.parquet",
                        driver="parquet",
                        run_id="child_run",
                    ),
                    RunArtifactLink(
                        run_id="parent_run",
                        artifact_id=parent_artifact_id,
                        direction="output",
                    ),
                    RunArtifactLink(
                        run_id="child_run",
                        artifact_id=child_artifact_id,
                        direction="output",
                    ),
                ]
            )
            session.commit()
    finally:
        db.engine.dispose()

    monkeypatch.chdir(tmp_path)
    shard_path = tmp_path / "cli_export_no_children_shard.duckdb"
    result = runner.invoke(
        app,
        [
            "db",
            "export",
            "parent_run",
            "--out",
            str(shard_path),
            "--no-children",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run_ids"] == ["parent_run"]


def test_db_merge_json_output_is_parseable(tmp_path, monkeypatch):
    source_db_path = tmp_path / "cli_merge_source.duckdb"
    target_db_path = tmp_path / "cli_merge_target.duckdb"
    shard_path = tmp_path / "cli_merge_shard.duckdb"
    _seed_db_for_maintenance_cli(source_db_path)
    target_db = DatabaseManager(str(target_db_path))
    target_db.engine.dispose()

    source_db = DatabaseManager(str(source_db_path))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "source_runs"
    )
    try:
        source_maintenance.export(
            "cli_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )
    finally:
        source_db.engine.dispose()

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        [
            "db",
            "merge",
            str(shard_path),
            "--conflict",
            "skip",
            "--json",
            "--db-path",
            str(target_db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "shard_path",
        "runs_merged",
        "runs_skipped",
        "artifacts_merged",
        "ingested_tables_merged",
        "unscoped_cache_tables_skipped",
        "conflicts_detected",
        "snapshots_merged",
        "incompatible_global_tables_skipped",
    }
    assert payload["runs_merged"] == ["cli_run"]
    merged_db = DatabaseManager(str(target_db_path))
    try:
        with merged_db.session_scope() as session:
            assert session.get(Run, "cli_run") is not None
    finally:
        merged_db.engine.dispose()


def test_db_merge_dry_run_json_output_is_parseable_and_no_writes(tmp_path, monkeypatch):
    source_db_path = tmp_path / "cli_merge_dry_run_source.duckdb"
    target_db_path = tmp_path / "cli_merge_dry_run_target.duckdb"
    shard_path = tmp_path / "cli_merge_dry_run_shard.duckdb"
    _seed_db_for_maintenance_cli(source_db_path)
    target_db = DatabaseManager(str(target_db_path))
    target_db.engine.dispose()

    source_db = DatabaseManager(str(source_db_path))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "source_runs"
    )
    try:
        source_maintenance.export(
            "cli_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )
    finally:
        source_db.engine.dispose()

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        [
            "db",
            "merge",
            str(shard_path),
            "--conflict",
            "skip",
            "--dry-run",
            "--json",
            "--db-path",
            str(target_db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "shard_path",
        "runs_merged",
        "runs_skipped",
        "artifacts_merged",
        "ingested_tables_merged",
        "unscoped_cache_tables_skipped",
        "conflicts_detected",
        "snapshots_merged",
        "incompatible_global_tables_skipped",
    }
    assert payload["runs_merged"] == ["cli_run"]

    merged_db = DatabaseManager(str(target_db_path))
    try:
        with merged_db.session_scope() as session:
            assert session.get(Run, "cli_run") is None
    finally:
        merged_db.engine.dispose()


def test_db_merge_non_json_warns_when_unscoped_cache_tables_skipped(
    tmp_path, monkeypatch
):
    source_db_path = tmp_path / "cli_merge_warn_source.duckdb"
    target_db_path = tmp_path / "cli_merge_warn_target.duckdb"
    shard_path = tmp_path / "cli_merge_warn_shard.duckdb"
    _seed_db_for_maintenance_cli(source_db_path)
    target_db = DatabaseManager(str(target_db_path))
    target_db.engine.dispose()

    source_db = DatabaseManager(str(source_db_path))
    source_maintenance = DatabaseMaintenance(
        source_db, run_dir=tmp_path / "source_runs"
    )
    try:
        source_maintenance.export(
            "cli_run",
            shard_path,
            include_data=False,
            include_snapshots=False,
        )
    finally:
        source_db.engine.dispose()

    shard_db = DatabaseManager(str(shard_path))
    try:
        with shard_db.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.cache_merge_warn (content_hash VARCHAR)"
            )
            conn.exec_driver_sql(
                """
                INSERT INTO global_tables.cache_merge_warn (content_hash)
                VALUES ('hash_a')
                """
            )
    finally:
        shard_db.engine.dispose()

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        [
            "db",
            "merge",
            str(shard_path),
            "--conflict",
            "skip",
            "--db-path",
            str(target_db_path),
        ],
    )

    assert result.exit_code == 0
    assert "Warning: skipped unscoped cache table(s) during merge" in result.stdout
    assert "cache_merge_warn" in result.stdout


def test_db_merge_non_json_warns_when_incompatible_global_tables_skipped(
    tmp_path, monkeypatch
):
    maintenance = MagicMock()
    maintenance.merge.return_value = MergeResult(
        shard_path=tmp_path / "mock_shard.duckdb",
        runs_merged=["cli_run"],
        runs_skipped=[],
        artifacts_merged=0,
        ingested_tables_merged=[],
        unscoped_cache_tables_skipped=[],
        conflicts_detected=[],
        snapshots_merged=0,
        incompatible_global_tables_skipped={
            "link_bad": "missing required target column 'run_id' for mode 'run_link'"
        },
    )
    monkeypatch.setattr(
        "consist.cli._maintenance_service", lambda _db_path: maintenance
    )

    result = runner.invoke(
        app,
        [
            "db",
            "merge",
            str(tmp_path / "mock_shard.duckdb"),
            "--conflict",
            "skip",
            "--db-path",
            str(tmp_path / "target.duckdb"),
        ],
    )

    assert result.exit_code == 0
    assert "Warning: skipped incompatible global table(s) during merge" in result.stdout
    assert "link_bad" in result.stdout
    assert "missing required target column 'run_id'" in result.stdout


def test_db_purge_dry_run_json_output_is_parseable(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_purge_dry_run.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "purge",
            "cli_run",
            "--dry-run",
            "--prune-cache",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {"plan", "executed", "ingested_data_skipped"}
    assert payload["executed"] is False
    assert payload["plan"]["run_ids"] == ["cli_run"]


def test_db_purge_passes_prune_cache_flag_to_maintenance(tmp_path, monkeypatch):
    maintenance = MagicMock()
    maintenance.purge.return_value = PurgeResult(
        plan=PurgePlan(
            run_ids=["cli_run"],
            child_run_ids=[],
            orphaned_artifact_ids=[],
            json_files=[],
            disk_files=[],
            ingested_data={},
            ingested_table_modes={},
        ),
        executed=True,
        ingested_data_skipped=False,
    )
    monkeypatch.setattr(
        "consist.cli._maintenance_service", lambda _db_path: maintenance
    )

    result = runner.invoke(
        app,
        [
            "db",
            "purge",
            "cli_run",
            "--yes",
            "--prune-cache",
            "--db-path",
            str(tmp_path / "target.duckdb"),
        ],
    )

    assert result.exit_code == 0
    assert maintenance.purge.call_count == 1
    kwargs = maintenance.purge.call_args.kwargs
    assert kwargs["delete_ingested_data"] is False
    assert kwargs["prune_cache"] is True


def test_db_purge_non_dry_run_with_yes_executes(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_purge_execute.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "purge",
            "cli_run",
            "--yes",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["executed"] is True
    db = DatabaseManager(str(db_path))
    try:
        with db.session_scope() as session:
            assert session.get(Run, "cli_run") is None
    finally:
        db.engine.dispose()


def test_db_purge_without_yes_can_be_cancelled(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_purge_cancel.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        ["db", "purge", "cli_run", "--db-path", str(db_path)],
        input="n\n",
    )

    assert result.exit_code == 0
    assert "Confirm purge scope:" in result.stdout
    assert "runs=1" in result.stdout
    assert "snapshots=" in result.stdout
    assert "disk_files=disabled" in result.stdout
    assert "ingested_global=preserve" in result.stdout
    assert "Purge cancelled." in result.stdout
    db = DatabaseManager(str(db_path))
    try:
        with db.session_scope() as session:
            assert session.get(Run, "cli_run") is not None
    finally:
        db.engine.dispose()


def test_root_help_includes_command_group_descriptions():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Schema inspection, profiling" in result.stdout
    assert "Materialized and hybrid SQL view management" in result.stdout
    assert "Database maintenance and recovery commands" in result.stdout


def test_db_fix_status_json_output_contains_expected_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_fix_status.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "fix-status",
            "cli_run",
            "running",
            "--force",
            "--reason",
            "manual_fix",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {"id", "status", "ended_at", "updated_at", "meta"}
    assert payload["id"] == "cli_run"
    assert payload["status"] == "running"
    assert payload["ended_at"] is None
    assert payload["meta"]["status_fix_reason"] == "manual_fix"


def test_db_fix_status_terminal_to_running_requires_force(tmp_path, monkeypatch):
    db_path = tmp_path / "cli_fix_status_requires_force.duckdb"
    _seed_db_for_maintenance_cli(db_path)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "db",
            "fix-status",
            "cli_run",
            "running",
            "--json",
            "--db-path",
            str(db_path),
        ],
    )

    assert result.exit_code == 2
    assert "--force" in result.stdout


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
        m_shell.assert_called_once_with(
            tracker,
            trust_db=False,
            db_path="mock.db",
            run_dir=None,
        )
        m_shell.return_value.cmdloop.assert_called_once()


def test_shell_uses_env_db_when_db_path_omitted(tmp_path, monkeypatch):
    env_db = tmp_path / "env_provenance.duckdb"
    engine = create_engine(f"duckdb:///{env_db}")
    with engine.connect():
        pass
    engine.dispose()
    monkeypatch.setenv("CONSIST_DB", str(env_db))

    with patch("consist.cli.ConsistShell") as m_shell:
        result = runner.invoke(app, ["shell"])

    assert result.exit_code == 0
    normalized_stdout = result.stdout.replace("\n", "")
    assert "Loaded database:" in normalized_stdout
    assert str(env_db) in normalized_stdout
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
