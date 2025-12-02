# tests/conftest.py
import pytest
from pathlib import Path

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink
from sqlmodel import create_engine, SQLModel
from consist.core.tracker import Tracker


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    """
    Pytest fixture that creates and returns a temporary directory for Consist run artifacts.

    This fixture provides an isolated directory for each test function, ensuring that
    tests do not interfere with each other's file-based outputs and providing a clean
    slate for each test involving filesystem operations.

    Parameters
    ----------
    tmp_path : Path
        The built-in pytest fixture for creating unique temporary directories
        for each test.

    Returns
    -------
    Path
        The path to the created 'runs' subdirectory within the temporary test directory.
    """
    d = tmp_path / "runs"
    d.mkdir()
    return d


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """
    Pytest fixture that returns the path to a temporary, empty DuckDB database file.

    This ensures that each test function operates on a clean, isolated database,
    preventing state from leaking between tests and providing a consistent
    starting point for database-related assertions.

    Parameters
    ----------
    tmp_path : Path
        The built-in pytest fixture for creating unique temporary directories
        for each test.

    Returns
    -------
    str
        The string path to the temporary DuckDB database file (e.g., ".../test_provenance.duckdb").
    """
    return str(tmp_path / "test_provenance.duckdb")


@pytest.fixture
def engine(db_path: str):
    """
    Pytest fixture that provides a SQLAlchemy engine connected to the test database.

    This fixture initializes the database by creating all necessary `SQLModel` tables
    (`Run`, `Artifact`, `RunArtifactLink`), ensuring the database is ready to be
    used by test functions for direct SQLModel interactions.

    Parameters
    ----------
    db_path : str
        The path to the temporary DuckDB file, provided by the `db_path` fixture.

    Yields
    ------
    sqlalchemy.engine.Engine
        A SQLAlchemy `Engine` instance connected to the test DuckDB database.
        The engine is automatically disposed after the test.
    """
    eng = create_engine(f"duckdb:///{db_path}")

    SQLModel.metadata.create_all(
        eng, tables=[Run.__table__, Artifact.__table__, RunArtifactLink.__table__]
    )
    yield eng  # Use yield to ensure engine is disposed after test
    eng.dispose()


@pytest.fixture
def tracker(run_dir: Path, db_path: str) -> Tracker:
    """
    Pytest fixture that provides a fully initialized Consist `Tracker` instance for tests.

    This fixture combines the `run_dir` and `db_path` fixtures to create a complete,
    isolated environment for testing the `Tracker`'s functionality. The returned
    `Tracker` is configured to use a temporary directory for run logs and a dedicated
    DuckDB file for provenance tracking.

    Parameters
    ----------
    run_dir : Path
        The temporary run directory, provided by the `run_dir` fixture.
    db_path : str
        The path to the temporary database, provided by the `db_path` fixture.

    Returns
    -------
    Tracker
        A fully configured `Tracker` instance ready for use in a test function.
        The `Tracker`'s database engine is explicitly disposed after the test.
    """
    t = Tracker(run_dir=run_dir, db_path=db_path)
    yield t
    if t.engine:
        t.engine.dispose()
