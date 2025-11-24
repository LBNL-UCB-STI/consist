# tests/conftest.py
import pytest
from pathlib import Path

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink
from sqlmodel import create_engine, SQLModel, Session
from consist.core.tracker import Tracker

@pytest.fixture
def run_dir(tmp_path):
    """Creates a temporary directory for a run."""
    d = tmp_path / "runs"
    d.mkdir()
    return d

@pytest.fixture
def db_path(tmp_path):
    """Returns a path to a fresh DuckDB file."""
    return str(tmp_path / "test_provenance.duckdb")


@pytest.fixture
def engine(db_path):
    """Returns a SQLAlchemy engine connected to the test DB."""
    eng = create_engine(f"duckdb:///{db_path}")

    SQLModel.metadata.create_all(
        eng,
        tables=[Run.__table__, Artifact.__table__, RunArtifactLink.__table__]
    )
    return eng

@pytest.fixture
def tracker(run_dir, db_path):
    """Returns a fully initialized Tracker instance."""
    return Tracker(run_dir=run_dir, db_path=db_path)