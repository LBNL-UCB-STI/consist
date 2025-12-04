import pytest
from pathlib import Path
from sqlmodel import SQLModel

# Import core library classes
from consist.core.tracker import Tracker
from consist.core import context

# Import specific models
from consist.models.run import Run, RunArtifactLink
from consist.models.artifact import Artifact


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
    core_tables = [Run.__table__, Artifact.__table__, RunArtifactLink.__table__]

    # Clean Slate Policy:
    # We drop and recreate ONLY the core tables before every test.
    if test_tracker.engine:
        with test_tracker.engine.connect() as connection:
            with connection.begin():
                SQLModel.metadata.drop_all(connection, tables=core_tables)
                SQLModel.metadata.create_all(connection, tables=core_tables)

    yield test_tracker

    # Teardown
    if test_tracker.engine:
        test_tracker.engine.dispose()


@pytest.fixture
def engine(tracker: Tracker):
    """Provides direct access to the active tracker's SQLAlchemy engine."""
    return tracker.engine
