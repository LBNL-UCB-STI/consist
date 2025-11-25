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
    A pytest fixture that creates and returns a temporary directory for run artifacts.

    This fixture provides an isolated directory for each test function, ensuring that
    tests do not interfere with each other's file-based outputs.

    Args:
        tmp_path (Path): The built-in pytest fixture for creating temporary directories.

    Returns:
        Path: The path to the created 'runs' subdirectory.
    """
    d = tmp_path / "runs"
    d.mkdir()
    return d


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """
    A pytest fixture that returns the path to a temporary, empty DuckDB database file.

    This ensures that each test function operates on a clean, isolated database,
    preventing state from leaking between tests.

    Args:
        tmp_path (Path): The built-in pytest fixture for creating temporary directories.

    Returns:
        str: The string path to the temporary database file.
    """
    return str(tmp_path / "test_provenance.duckdb")


@pytest.fixture
def engine(db_path: str):
    """
    A pytest fixture that provides a SQLAlchemy engine connected to the test database.

    This fixture initializes the database by creating all necessary SQLModel tables,
    ensuring the database is ready to be used by the test function.

    Args:
        db_path (str): The path to the temporary DuckDB file, provided by the `db_path` fixture.

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy Engine instance connected to the test database.
    """
    eng = create_engine(f"duckdb:///{db_path}")

    SQLModel.metadata.create_all(
        eng, tables=[Run.__table__, Artifact.__table__, RunArtifactLink.__table__]
    )
    return eng


@pytest.fixture
def tracker(run_dir: Path, db_path: str) -> Tracker:
    """
    A pytest fixture that provides a fully initialized Consist Tracker instance for tests.

    This fixture combines the `run_dir` and `db_path` fixtures to create a complete,
    isolated environment for testing the Tracker's functionality.

    Args:
        run_dir (Path): The temporary run directory, provided by the `run_dir` fixture.
        db_path (str): The path to the temporary database, provided by the `db_path` fixture.

    Returns:
        Tracker: A fully configured `Tracker` instance ready for use in a test.
    """
    return Tracker(run_dir=run_dir, db_path=db_path)
