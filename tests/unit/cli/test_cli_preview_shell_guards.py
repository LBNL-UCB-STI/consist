"""Preview/shell tests that document CLI error-handling behavior."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from consist.cli import ConsistShell, app


runner = CliRunner()


def test_preview_shows_driver_specific_hint_for_missing_optional_dependency(
    cli_runner,
    tracker,
    tmp_path: Path,
) -> None:
    """
    `preview` should guide users to the right extras when optional deps are missing.
    """
    artifact_path = tmp_path / "artifact.zarr"
    artifact_path.write_text("placeholder", encoding="utf-8")

    with tracker.start_run("run_preview_zarr", "model"):
        tracker.log_artifact(
            str(artifact_path),
            key="preview_zarr_artifact",
            driver="zarr",
            direction="output",
        )

    with patch("consist.load", side_effect=ImportError("zarr is not installed")):
        result = cli_runner.invoke(app, ["preview", "preview_zarr_artifact"])

    assert result.exit_code == 1
    assert "Missing optional dependency while loading artifact" in result.stdout
    assert "install Zarr support" in result.stdout


def test_shell_runs_reports_parse_error_for_invalid_limit(capsys) -> None:
    """Shell `runs --limit` should surface bounded-int parse errors to users."""
    shell = ConsistShell(MagicMock())

    shell.do_runs("--limit not-an-int")

    out = capsys.readouterr().out
    assert "limit must be an integer" in out


def test_shell_preview_requires_artifact_key(capsys) -> None:
    """Shell preview should reject empty invocation with a direct usage error."""
    shell = ConsistShell(MagicMock())

    shell.do_preview("")

    out = capsys.readouterr().out
    assert "artifact_key required" in out


def test_shell_preview_reports_missing_artifact(capsys) -> None:
    """Shell preview should clearly report when the artifact lookup fails."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = None
    shell = ConsistShell(tracker)

    shell.do_preview("missing_artifact")

    out = capsys.readouterr().out
    assert "Artifact 'missing_artifact' not found." in out


def test_preview_reports_unsupported_driver_value_error(
    cli_runner,
    tracker,
    tmp_path: Path,
) -> None:
    """`preview` should show a driver-specific error when load raises ValueError."""
    artifact_path = tmp_path / "artifact.csv"
    artifact_path.write_text("x\n1\n", encoding="utf-8")

    with tracker.start_run("run_preview_value_error", "model"):
        tracker.log_artifact(
            str(artifact_path),
            key="preview_bad_driver_artifact",
            driver="csv",
            direction="output",
        )

    with patch("consist.load", side_effect=ValueError("bad driver config")):
        result = cli_runner.invoke(app, ["preview", "preview_bad_driver_artifact"])

    assert result.exit_code == 1
    assert "Unsupported artifact driver 'csv'" in result.stdout


def test_preview_reports_generic_loading_error(
    cli_runner, tracker, tmp_path: Path
) -> None:
    """Unexpected loading exceptions should be surfaced with a generic error message."""
    artifact_path = tmp_path / "artifact.parquet"
    artifact_path.write_text("placeholder", encoding="utf-8")

    with tracker.start_run("run_preview_generic_error", "model"):
        tracker.log_artifact(
            str(artifact_path),
            key="preview_generic_error_artifact",
            driver="parquet",
            direction="output",
        )

    with patch("consist.load", side_effect=RuntimeError("boom")):
        result = cli_runner.invoke(app, ["preview", "preview_generic_error_artifact"])

    assert result.exit_code == 1
    assert "Error loading artifact: boom" in result.stdout


def test_preview_renders_dimensions_for_xarray_like_dataset(
    cli_runner,
    tracker,
    tmp_path: Path,
) -> None:
    """`preview` should render dataset dimensions when xarray-like objects are loaded."""

    class FakeDataset:
        def __init__(self) -> None:
            self.sizes = {"time": 3, "zone": 2}
            self.data_vars: dict[str, object] = {}

    class FakeXarrayModule:
        Dataset = FakeDataset
        DataArray = pd.Series  # placeholder class to satisfy isinstance tuple

    artifact_path = tmp_path / "artifact.nc"
    artifact_path.write_text("placeholder", encoding="utf-8")

    with tracker.start_run("run_preview_xarray", "model"):
        tracker.log_artifact(
            str(artifact_path),
            key="preview_xarray_artifact",
            driver="netcdf",
            direction="output",
        )

    with (
        patch("consist.load", return_value=FakeDataset()),
        patch("consist.cli._optional_xarray", return_value=FakeXarrayModule),
    ):
        result = cli_runner.invoke(app, ["preview", "preview_xarray_artifact"])

    assert result.exit_code == 0
    assert "Dimensions" in result.stdout
    assert "time" in result.stdout
    assert "No data variables found." in result.stdout


def test_shell_schema_profile_requires_artifact_key(capsys) -> None:
    """Shell schema_profile should require an artifact key argument."""
    shell = ConsistShell(MagicMock())

    shell.do_schema_profile("")

    out = capsys.readouterr().out
    assert "artifact_key required" in out


def test_shell_schema_profile_reports_missing_artifact(capsys) -> None:
    """Shell schema_profile should clearly report lookup failures."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = None
    shell = ConsistShell(tracker)

    shell.do_schema_profile("missing_artifact")

    out = capsys.readouterr().out
    assert "Artifact 'missing_artifact' not found." in out
