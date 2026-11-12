from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from consist.core import context
from consist.core.identity import IdentityManager
from consist.core.persistence import close_all_tracked_relations
from consist.core.tracker import Tracker


def pytest_addoption(parser):
    """Adds the --persist-db-path option to pytest for manual DB inspection."""
    parser.addoption(
        "--persist-db-path",
        action="store",
        default=None,
        help="Specify a file path to create a persistent DuckDB for manual inspection (e.g., test_debug.db).",
    )


@pytest.fixture(autouse=True)
def reset_context():
    """
    Automatically resets the global Consist context before and after each test.
    This prevents state (like active runs) from leaking between tests.
    """
    context._TRACKER_STACK.set(())
    context._DEFAULT_TRACKER.set(None)
    yield
    context._TRACKER_STACK.set(())
    context._DEFAULT_TRACKER.set(None)
    close_all_tracked_relations()


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
    Provides an isolated temporary directory for run outputs.
    Exposed as a fixture so tests can access the file system location directly.
    """
    path = tmp_path / "consist_runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def tracker(request, run_dir: Path, tmp_path: Path) -> Iterator[Tracker]:
    """Provides a fresh, isolated Tracker instance for each test function."""
    yield from _build_tracker(request, run_dir, tmp_path)


@pytest.fixture
def gtfs_tracker(request, run_dir: Path, tmp_path: Path) -> Iterator[Tracker]:
    """Provides a Tracker with built-in GTFS schemas registered."""
    yield from _build_tracker(request, run_dir, tmp_path, builtin_schemas=("gtfs",))


def _build_tracker(
    request,
    run_dir: Path,
    tmp_path: Path,
    *,
    builtin_schemas: tuple[str, ...] = (),
) -> Iterator[Tracker]:
    persist_path = request.config.getoption("--persist-db-path")

    if persist_path:
        resolved = Path(persist_path).resolve()
        resolved.unlink(missing_ok=True)
        Path(f"{resolved}.wal").unlink(missing_ok=True)
        db_path = str(resolved)
    else:
        db_path = str(tmp_path / "provenance.db")

    test_tracker = Tracker(
        run_dir=run_dir,
        db_path=db_path,
        builtin_schemas=list(builtin_schemas),
    )

    yield test_tracker

    test_tracker.close()


@pytest.fixture
def db(tracker: Tracker):
    """Provides direct access to the active tracker's DatabaseManager."""
    return tracker.db


@pytest.fixture
def cli_runner(tracker: Tracker) -> Iterator[CliRunner]:
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


@pytest.fixture
def write_netcdf():
    """
    Writes a NetCDF file using the best available xarray backend.

    Falls back to the default backend when h5netcdf is not installed, and skips
    tests if the underlying NetCDF backend fails with HDF errors.
    """
    pytest.importorskip("xarray")
    from consist.core.netcdf_utils import resolve_netcdf_engine

    def _write(dataset, path: Path) -> None:
        engine = resolve_netcdf_engine()
        try:
            if engine:
                dataset.to_netcdf(path, engine=engine)
            else:
                dataset.to_netcdf(path)
        except Exception as exc:
            message = str(exc)
            if (
                "NetCDF: HDF error" in message
                or "Problem with HDF5 dimscales" in message
            ):
                pytest.skip(
                    "NetCDF backend failed with HDF5 errors. "
                    "Install h5netcdf or use a compatible netCDF4 build."
                )
            raise

    return _write
