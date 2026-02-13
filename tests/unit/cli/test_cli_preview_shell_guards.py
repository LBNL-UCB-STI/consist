"""Preview/shell tests that document CLI error-handling behavior."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
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
    assert "install optional support with pip install -e '.[zarr]'" in result.stdout


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


def test_preview_reports_mount_diagnostics_on_missing_file() -> None:
    """CLI preview should use mount diagnostic formatting for missing files."""
    tracker = MagicMock()
    tracker.mounts = {"inputs": "/workspace/current"}
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        run_id=None,
        key="missing_artifact",
        driver="parquet",
        container_uri="inputs://data/missing.parquet",
        meta={"mount_root": "/workspace/original"},
    )
    tracker.resolve_uri.return_value = "/workspace/current/data/missing.parquet"

    hint = object()
    with (
        patch("consist.cli.get_tracker", return_value=tracker),
        patch("consist.load", side_effect=FileNotFoundError),
        patch(
            "consist.tools.mount_diagnostics.build_mount_resolution_hint",
            return_value=hint,
        ),
        patch(
            "consist.tools.mount_diagnostics.format_missing_artifact_mount_help",
            return_value="Mount diagnostics: configure inputs mount.",
        ) as format_help,
    ):
        result = runner.invoke(app, ["preview", "missing_artifact"])

    assert result.exit_code == 1
    assert "Artifact file not found at: inputs://data/missing.parquet" in result.stdout
    assert "Mount diagnostics: configure inputs mount." in result.stdout
    format_help.assert_called_once_with(
        hint, resolved_path="/workspace/current/data/missing.parquet"
    )


def test_shell_preview_reports_mount_diagnostics_on_missing_file(capsys) -> None:
    """Shell preview should show mount diagnostics when a file cannot be resolved."""
    tracker = MagicMock()
    tracker.mounts = {"inputs": "/workspace/current"}
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        run_id=None,
        key="missing_artifact",
        driver="csv",
        container_uri="inputs://data/missing.csv",
        meta={"mount_root": "/workspace/original"},
    )
    tracker.resolve_uri.return_value = "/workspace/current/data/missing.csv"

    shell = ConsistShell(tracker)
    with (
        patch("consist.load", side_effect=FileNotFoundError),
        patch(
            "consist.tools.mount_diagnostics.build_mount_resolution_hint",
            return_value=object(),
        ),
        patch(
            "consist.tools.mount_diagnostics.format_missing_artifact_mount_help",
            return_value="Mount diagnostics for shell preview.",
        ),
    ):
        shell.do_preview("missing_artifact")

    out = capsys.readouterr().out
    assert "Artifact file not found at: inputs://data/missing.csv" in out
    assert "Mount diagnostics for shell preview." in out


def test_shell_schema_stub_requires_artifact_key(capsys) -> None:
    """Shell schema_stub should require an artifact key argument."""
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub("")

    out = capsys.readouterr().out
    assert "artifact_key required" in out


def test_shell_schema_stub_reports_missing_artifact(capsys) -> None:
    """Shell schema_stub should report a missing artifact lookup."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = None
    shell = ConsistShell(tracker)

    shell.do_schema_stub("missing_artifact")

    out = capsys.readouterr().out
    assert "Artifact 'missing_artifact' not found." in out


def test_shell_schema_stub_reports_missing_captured_schema(capsys) -> None:
    """Shell schema_stub should handle KeyError as missing captured schema."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(id="artifact-id")
    tracker.export_schema_sqlmodel.side_effect = KeyError("schema missing")
    shell = ConsistShell(tracker)

    shell.do_schema_stub("artifact_key")

    out = capsys.readouterr().out
    assert "Captured schema not found for this artifact." in out


def test_shell_schema_stub_surfaces_value_error(capsys) -> None:
    """Shell schema_stub should print ValueError text from export failures."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(id="artifact-id")
    tracker.export_schema_sqlmodel.side_effect = ValueError("invalid class name")
    shell = ConsistShell(tracker)

    shell.do_schema_stub("artifact_key")

    out = capsys.readouterr().out
    assert "invalid class name" in out


def test_shell_schema_profile_unsupported_loaded_type_without_db_profile(
    capsys,
) -> None:
    """Shell schema_profile should report unsupported loaded type when no DB profile exists."""
    tracker = MagicMock()
    tracker.mounts = {}
    tracker.db = MagicMock()
    tracker.db.get_artifact_schema_for_artifact.return_value = None
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        run_id=None,
        key="artifact_key",
        driver="bin",
        container_uri="inputs://data/object.bin",
        meta={},
    )
    shell = ConsistShell(tracker)

    with (
        patch("consist.load", return_value=object()),
        patch("consist.cli._optional_xarray", return_value=None),
    ):
        shell.do_schema_profile("artifact_key")

    out = capsys.readouterr().out
    assert "Schema not implemented for loaded type: object" in out
