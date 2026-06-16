"""Preview/shell tests that document CLI error-handling behavior."""

from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from consist.cli import ConsistShell, app


runner = CliRunner()


def _write_gtfs_bundle_zip(root: Path) -> Path:
    feed_dir = root / "feed"
    feed_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "agency_id": "A1",
                "agency_name": "Transit",
                "agency_url": "https://example.com",
                "agency_timezone": "America/Los_Angeles",
            }
        ]
    ).to_csv(feed_dir / "agency.txt", index=False)
    pd.DataFrame(
        [
            {
                "route_id": "R1",
                "agency_id": "A1",
                "route_short_name": "1",
                "route_type": 3,
            }
        ]
    ).to_csv(feed_dir / "routes.txt", index=False)
    pd.DataFrame(
        [
            {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
        ]
    ).to_csv(feed_dir / "trips.txt", index=False)
    (feed_dir / "README.txt").write_text("ignored by GTFS preview", encoding="utf-8")

    zip_path = root / "feed.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for file_path in sorted(feed_dir.iterdir()):
            zf.write(file_path, arcname=file_path.name)
    return zip_path


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


def test_preview_renders_gtfs_bundle_summary(
    cli_runner, tracker, tmp_path: Path
) -> None:
    """`preview` should summarize a GTFS bundle instead of trying to load it as a table."""
    feed_zip = _write_gtfs_bundle_zip(tmp_path)

    with tracker.start_run("run_preview_gtfs_bundle", "model"):
        tracker.log_artifact(
            str(feed_zip),
            key="preview_gtfs_bundle",
            driver="gtfs",
            direction="output",
        )

    result = cli_runner.invoke(app, ["preview", "preview_gtfs_bundle"])

    assert result.exit_code == 0
    assert "GTFS Bundle Summary" in result.stdout
    assert "Member Tables" in result.stdout
    assert "agency.txt" in result.stdout
    assert "routes.txt" in result.stdout
    assert "trips.txt" in result.stdout
    assert "Preview a specific table member" in result.stdout


def test_preview_renders_legacy_unknown_driver_gtfs_bundle_summary(
    cli_runner, tracker, tmp_path: Path
) -> None:
    """Legacy GTFS bundle artifacts may have driver='unknown' but gtfs_bundle metadata."""
    feed_zip = _write_gtfs_bundle_zip(tmp_path)

    with tracker.start_run("run_preview_legacy_gtfs_bundle", "model"):
        tracker.log_artifact(
            str(feed_zip),
            key="preview_legacy_gtfs_bundle",
            driver="unknown",
            direction="output",
            gtfs_bundle=True,
        )

    result = cli_runner.invoke(app, ["preview", "preview_legacy_gtfs_bundle"])

    assert result.exit_code == 0
    assert "GTFS Bundle Summary" in result.stdout
    assert "Member Tables" in result.stdout
    assert "agency.txt" in result.stdout


def test_preview_renders_gtfs_selected_service_summary(
    cli_runner, tracker, tmp_path: Path
) -> None:
    """`preview` should summarize a selected-service parent without loading it."""
    manifest_path = tmp_path / "selected_service_manifest.json"
    manifest_path.write_text('{"service_date": "2024-01-01"}\n', encoding="utf-8")
    trips_path = tmp_path / "trips.csv"
    trips_path.write_text("feed_key,trip_id\nfeed,T1\n", encoding="utf-8")
    routes_path = tmp_path / "routes.csv"
    routes_path.write_text("feed_key,route_id\nfeed,R1\n", encoding="utf-8")
    notes_path = tmp_path / "notes.txt"
    notes_path.write_text("not a selected GTFS table\n", encoding="utf-8")

    with tracker.start_run("run_preview_gtfs_selected_service", "model"):
        manifest = tracker.log_artifact(
            manifest_path,
            key="preview_gtfs_selected_service_manifest",
            driver="json",
            direction="output",
        )
        parent = tracker.log_artifact(
            manifest_path,
            key="preview_gtfs_selected_service",
            driver="gtfs_selected_service",
            direction="output",
            gtfs_selected_service=True,
            gtfs_manifest_artifact_id=str(manifest.id),
            service_date="2024-01-01",
            source_feed_count=1,
            table_count=2,
            source_bundle_hash="a" * 64,
            service_slice_hash="b" * 64,
        )
        tracker.log_artifact(
            trips_path,
            key="preview_gtfs_selected_service_trips",
            driver="csv",
            direction="output",
            parent_artifact_id=parent.id,
            gtfs_selected_table=True,
            gtfs_table_name="trips",
        )
        tracker.log_artifact(
            routes_path,
            key="preview_gtfs_selected_service_routes",
            driver="csv",
            direction="output",
            parent_artifact_id=parent.id,
            gtfs_selected_table=True,
            gtfs_table_name="routes",
        )
        tracker.log_artifact(
            notes_path,
            key="preview_gtfs_selected_service_notes",
            driver="txt",
            direction="output",
            parent_artifact_id=parent.id,
        )

    result = cli_runner.invoke(app, ["preview", "preview_gtfs_selected_service"])

    assert result.exit_code == 0
    assert "GTFS Selected Service Summary" in result.stdout
    assert "Service date" in result.stdout
    assert "2024-01-01" in result.stdout
    assert "Selected Tables" in result.stdout
    assert "trips" in result.stdout
    assert "routes" in result.stdout
    assert "preview_gtfs_selected_service_trips" in result.stdout
    assert "Other children" in result.stdout
    assert "preview_gtfs_selected_service_notes" not in result.stdout


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
    assert "consist schema capture-file --artifact-id artifact-id" in out


def test_shell_schema_stub_surfaces_value_error(capsys) -> None:
    """Shell schema_stub should print ValueError text from export failures."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(id="artifact-id")
    tracker.export_schema_sqlmodel.side_effect = ValueError("invalid class name")
    shell = ConsistShell(tracker)

    shell.do_schema_stub("artifact_key")

    out = capsys.readouterr().out
    assert "invalid class name" in out


def test_shell_schema_stub_rejects_key_and_artifact_id_together(capsys) -> None:
    """Shell schema_stub should reject ambiguous selector combinations."""
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub(
        "artifact_key --artifact-id 00000000-0000-0000-0000-000000000001"
    )

    out = capsys.readouterr().out
    assert "Provide exactly one selector" in out


def test_shell_preview_supports_hash_selector(capsys) -> None:
    tracker = MagicMock()
    tracker.get_artifact.return_value = None
    shell = ConsistShell(tracker)
    shell._lookup_artifact_by_hash_prefix = MagicMock(
        return_value=SimpleNamespace(
            id="artifact-id-123",
            key="resolved_by_hash",
            run_id="run-1",
            driver="csv",
            container_uri="inputs://file.csv",
            meta={},
        )
    )

    with patch(
        "consist.cli._load_artifact_with_diagnostics",
        return_value=pd.DataFrame({"x": [1]}),
    ):
        shell.do_preview("--hash deadbeef")

    shell._lookup_artifact_by_hash_prefix.assert_called_once_with(
        "deadbeef", command_name="preview"
    )


def test_shell_preview_supports_hash_equals_selector(capsys) -> None:
    tracker = MagicMock()
    shell = ConsistShell(tracker)
    shell._lookup_artifact_by_hash_prefix = MagicMock(
        return_value=SimpleNamespace(
            id="artifact-id-123",
            key="resolved_by_hash",
            run_id="run-1",
            driver="csv",
            container_uri="inputs://file.csv",
            meta={},
        )
    )

    with patch(
        "consist.cli._load_artifact_with_diagnostics",
        return_value=pd.DataFrame({"x": [1]}),
    ):
        shell.do_preview("--hash=deadbeef")

    shell._lookup_artifact_by_hash_prefix.assert_called_once_with(
        "deadbeef", command_name="preview"
    )


def test_shell_preview_rejects_hash_with_positional_selector(capsys) -> None:
    shell = ConsistShell(MagicMock())

    shell.do_preview("artifact_key --hash deadbeef")

    out = capsys.readouterr().out
    assert "Provide either a positional selector or --hash, not both." in out


def test_shell_preview_requires_hash_value(capsys) -> None:
    shell = ConsistShell(MagicMock())

    shell.do_preview("--hash")

    out = capsys.readouterr().out
    assert "--hash requires a prefix value" in out


def test_shell_schema_stub_supports_hash_selector(capsys) -> None:
    tracker = MagicMock()
    tracker.export_schema_sqlmodel.return_value = "class ExportedModel(SQLModel): ..."
    shell = ConsistShell(tracker)
    shell._lookup_artifact_by_hash_prefix = MagicMock(
        return_value=SimpleNamespace(id="artifact-id-123")
    )

    shell.do_schema_stub("--hash deadbeef")

    _ = capsys.readouterr().out
    shell._lookup_artifact_by_hash_prefix.assert_called_once_with(
        "deadbeef",
        command_name="schema_stub",
        run_id=None,
    )
    tracker.export_schema_sqlmodel.assert_called_once_with(
        artifact_id="artifact-id-123",
        class_name=None,
        table_name=None,
        abstract=True,
        include_system_cols=False,
        include_stats_comments=True,
    )


def test_shell_schema_stub_supports_hash_equals_selector(capsys) -> None:
    tracker = MagicMock()
    tracker.export_schema_sqlmodel.return_value = "class ExportedModel(SQLModel): ..."
    shell = ConsistShell(tracker)
    shell._lookup_artifact_by_hash_prefix = MagicMock(
        return_value=SimpleNamespace(id="artifact-id-123")
    )

    shell.do_schema_stub("--hash=deadbeef")

    _ = capsys.readouterr().out
    shell._lookup_artifact_by_hash_prefix.assert_called_once_with(
        "deadbeef",
        command_name="schema_stub",
        run_id=None,
    )


def test_shell_schema_stub_requires_hash_value(capsys) -> None:
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub("--hash")

    out = capsys.readouterr().out
    assert "--hash requires a prefix value" in out


def test_shell_schema_stub_rejects_hash_with_positional_selector(capsys) -> None:
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub("artifact_key --hash deadbeef")

    out = capsys.readouterr().out
    assert "Provide exactly one selector" in out


def test_shell_schema_stub_rejects_run_id_with_artifact_id(capsys) -> None:
    """Shell schema_stub should enforce run-id usage only with artifact_key lookup."""
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub(
        "--artifact-id 00000000-0000-0000-0000-000000000001 --run-id run-123"
    )

    out = capsys.readouterr().out
    assert "--run-id can only be used with artifact_key selection." in out


def test_shell_schema_stub_rejects_invalid_source(capsys) -> None:
    """Shell schema_stub should validate --source values."""
    shell = ConsistShell(MagicMock())

    shell.do_schema_stub("artifact_key --source bad_source")

    out = capsys.readouterr().out
    assert "--source must be one of: file|duckdb|user_provided" in out


def test_shell_schema_profile_supports_hash_selector_with_db_profile(capsys) -> None:
    tracker = MagicMock()
    tracker.mounts = {}
    tracker.db = MagicMock()
    tracker.db.get_artifact_schema_for_artifact.return_value = (
        SimpleNamespace(summary_json={"column_count": 1}),
        [SimpleNamespace(name="trip_id", logical_type="integer", nullable=False)],
    )
    shell = ConsistShell(tracker)
    shell._lookup_artifact_by_hash_prefix = MagicMock(
        return_value=SimpleNamespace(
            id="artifact-id",
            run_id=None,
            key="trips",
            driver="parquet",
            container_uri="inputs://data/trips.parquet",
            meta={},
        )
    )

    shell.do_schema_profile("--hash deadbeef")

    out = capsys.readouterr().out
    shell._lookup_artifact_by_hash_prefix.assert_called_once_with(
        "deadbeef", command_name="schema_profile"
    )
    assert "Schema: trips" in out
    assert "trip_id" in out
    assert "integer" in out


def test_shell_schema_profile_rejects_hash_with_positional_selector(capsys) -> None:
    shell = ConsistShell(MagicMock())

    shell.do_schema_profile("artifact_key --hash deadbeef")

    out = capsys.readouterr().out
    assert "Provide either a positional selector or --hash, not both." in out


def test_shell_schema_stub_uses_run_id_and_prints_selection_explainability(
    capsys,
) -> None:
    """Shell schema_stub should use run-scoped key lookup and print selection details."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(id="artifact-id-123")
    tracker.select_artifact_schema_for_artifact.return_value = SimpleNamespace(
        schema_id="schema-id-123",
        source="file",
        candidate_count=3,
        selection_rule="default source order file>duckdb",
    )
    tracker.export_schema_sqlmodel.return_value = "class ExportedModel(SQLModel): ..."
    shell = ConsistShell(tracker)

    shell.do_schema_stub("artifact_key --run-id run-123 --source file")

    out = capsys.readouterr().out
    tracker.get_artifact.assert_called_once_with("artifact_key", run_id="run-123")
    tracker.select_artifact_schema_for_artifact.assert_called_once_with(
        artifact_id="artifact-id-123",
        source="file",
        strict_source=True,
    )
    tracker.export_schema_sqlmodel.assert_called_once_with(
        schema_id="schema-id-123",
        class_name=None,
        table_name=None,
        abstract=True,
        include_system_cols=False,
        include_stats_comments=True,
    )
    assert "Schema selection: source=file, schema_id=schema-id-123, candidates=3" in out
    assert "Selection rule: default source order file>duckdb" in out


def test_shell_schema_stub_allows_artifact_id_selector(capsys) -> None:
    """Shell schema_stub should support direct artifact-id selection."""
    tracker = MagicMock()
    tracker.get_artifact.return_value = SimpleNamespace(id="artifact-id-123")
    tracker.export_schema_sqlmodel.return_value = "class ExportedModel(SQLModel): ..."
    shell = ConsistShell(tracker)

    shell.do_schema_stub("--artifact-id 00000000-0000-0000-0000-000000000001")

    _ = capsys.readouterr().out
    tracker.get_artifact.assert_called_once_with("00000000-0000-0000-0000-000000000001")
    tracker.export_schema_sqlmodel.assert_called_once_with(
        artifact_id="artifact-id-123",
        class_name=None,
        table_name=None,
        abstract=True,
        include_system_cols=False,
        include_stats_comments=True,
    )


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


def test_shell_schema_profile_h5_container_hint_for_unsupported_type(
    capsys,
) -> None:
    """Shell schema_profile should point HDF5 containers at h5_table artifacts."""
    tracker = MagicMock()
    tracker.mounts = {}
    tracker.db = MagicMock()
    tracker.db.get_artifact_schema_for_artifact.return_value = None
    tracker.get_artifact.return_value = SimpleNamespace(
        id="artifact-id",
        run_id=None,
        key="h5_container",
        driver="h5",
        container_uri="inputs://data/pipeline.h5",
        meta={},
    )
    shell = ConsistShell(tracker)
    hdf_store_like = type("HDFStore", (), {})()

    with (
        patch("consist.load", return_value=hdf_store_like),
        patch("consist.cli._optional_xarray", return_value=None),
    ):
        shell.do_schema_profile("h5_container")

    out = capsys.readouterr().out
    assert "Schema not implemented for loaded type: HDFStore" in out
    assert "Container-level HDF5 schema is not supported." in out
    assert "inspect sibling `h5_table` artifacts" in out
