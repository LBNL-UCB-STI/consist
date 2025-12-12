from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from sqlmodel import SQLModel
from typer.testing import CliRunner

# Import core library classes
from consist.core.tracker import Tracker
from consist.core import context
from consist.core.identity import IdentityManager

# Import specific models
from consist.models.run import Run, RunArtifactLink
from consist.models.artifact import Artifact
from consist.models.config_facet import ConfigFacet
from consist.models.run_config_kv import RunConfigKV


# --- Global Test Configuration ---


def pytest_addoption(parser):
    """Adds the --persist-db-path option to pytest for manual DB inspection."""
    parser.addoption(
        "--persist-db-path",
        action="store",
        default=None,
        help="Specify a file path to create a persistent DuckDB for manual inspection (e.g., test_debug.db).",
    )


# --- Core Fixtures ---


@pytest.fixture(autouse=True)
def reset_context():
    """
    Automatically resets the global Consist context before and after each test.
    This prevents state (like active runs) from leaking between tests.
    """
    if hasattr(context, "_TRACKER_STACK"):
        context._TRACKER_STACK.clear()
    yield
    if hasattr(context, "_TRACKER_STACK"):
        context._TRACKER_STACK.clear()


@pytest.fixture(autouse=True)
def stable_code_version(request):
    """
    Stabilize the code hash for tests so cache/signature behavior is deterministic
    even when the working tree is dirty during local development.

    Skip for IdentityManager unit tests that explicitly validate git behavior.
    """
    if "tests/unit/core/test_identity.py" in request.node.nodeid:
        yield
    else:
        with patch.object(
            IdentityManager, "get_code_version", return_value="static_test_hash"
        ):
            yield


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    """
    Provides a isolated temporary directory for run outputs.
    Exposed as a fixture so tests can access the file system location directly.
    """
    path = tmp_path / "consist_runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def tracker(request, run_dir: Path, tmp_path: Path) -> Tracker:
    """
    Provides a fresh, isolated Tracker instance for each test function.

    Handles:
    1. DB Persistence via --persist-db-path
    2. Schema filtering (fixing the SERIAL/MockTable error)
    3. Transaction/Table cleaning (fixing IntegrityErrors)
    """
    persist_path = request.config.getoption("--persist-db-path")

    if persist_path:
        # Use the user-provided path for persistence
        db_path = str(Path(persist_path).resolve())
    else:
        # Standard test isolation: use a temp file
        db_path = str(tmp_path / "provenance.db")

    # Initialize the Tracker
    test_tracker = Tracker(run_dir=run_dir, db_path=db_path)

    # --- CRITICAL DATABASE SETUP ---
    # We restrict operations to ONLY these tables to prevent 'create_all'
    # from trying to build 'MockTable' (from dlt tests) which uses 'SERIAL' types.
    core_tables = [
        Run.__table__,
        Artifact.__table__,
        RunArtifactLink.__table__,
        ConfigFacet.__table__,
        RunConfigKV.__table__,
    ]

    # Clean Slate Policy:
    # We drop and recreate ONLY the core tables before every test.
    if test_tracker.engine:
        with test_tracker.engine.connect() as connection:
            with connection.begin():
                SQLModel.metadata.drop_all(connection, tables=core_tables)
                SQLModel.metadata.create_all(connection, tables=core_tables)
                # Ensure run.parent_run_id FK is NOT ENFORCED (DuckDB limitation workaround)
                if test_tracker.db:
                    test_tracker.db._relax_run_parent_fk()

    yield test_tracker

    # Teardown
    if test_tracker.engine:
        test_tracker.engine.dispose()


@pytest.fixture
def engine(tracker: Tracker):
    """Provides direct access to the active tracker's SQLAlchemy engine."""
    return tracker.engine


@pytest.fixture
def cli_runner(tracker: Tracker) -> CliRunner:
    """
    Provides a Typer CliRunner with get_tracker patched to use the shared test tracker.
    Simplifies CLI command testing without repetitive patching.
    """
    runner = CliRunner()
    with patch("consist.cli.get_tracker", return_value=tracker):
        yield runner


@pytest.fixture
def sample_csv(tmp_path):
    """
    Factory fixture to generate sample CSV files with optional custom columns.
    """

    def _make_csv(name: str, rows: int = 5, **columns):
        path = tmp_path / name
        if not columns:
            columns = {"value": range(rows), "category": ["a"] * rows}
        pd.DataFrame(columns).to_csv(path, index=False)
        return path

    return _make_csv
