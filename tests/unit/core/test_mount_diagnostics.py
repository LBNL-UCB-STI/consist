"""
Unit tests for mount-related diagnostics and metadata.

These tests ensure Consist records enough mount context at artifact log time to
produce actionable error messages later when a mount-backed URI cannot be resolved.
"""

from __future__ import annotations

from pathlib import Path

from consist.core.tracker import Tracker
from consist.tools.mount_diagnostics import (
    build_mount_resolution_hint,
    format_missing_artifact_mount_help,
)


def test_log_artifact_records_mount_metadata(tmp_path: Path) -> None:
    """
    Records mount metadata at log time for later debugging.

    This checks that when a filesystem path is virtualized to a mount URI
    (e.g., ``inputs://foo.csv``), Consist stores ``mount_scheme`` and
    ``mount_root`` in the artifact meta, and stores a mounts snapshot on the run.
    """
    inputs_root = tmp_path / "inputs_root"
    inputs_root.mkdir(parents=True)
    data_path = inputs_root / "data.csv"
    data_path.write_text("a,b\n1,2\n", encoding="utf-8")

    tracker = Tracker(run_dir=tmp_path / "run", mounts={"inputs": str(inputs_root)})
    with tracker.start_run(run_id="step", model="test"):
        art = tracker.log_artifact(data_path, key="data", direction="input")

    assert art.container_uri == "inputs://data.csv"
    assert art.meta["mount_scheme"] == "inputs"
    assert art.meta["mount_root"] == str(inputs_root.resolve())

    assert tracker.last_run is not None
    assert tracker.last_run.run.meta["mounts"]["inputs"] == str(inputs_root)


def test_missing_artifact_help_includes_mount_root_mismatch() -> None:
    """
    Explains mount root mismatches when a mount-backed artifact is missing.

    This checks the diagnostic message includes both the recorded mount root
    (captured at log time) and the currently configured mount root (used at
    resolution time).
    """
    uri = "inputs://nested/file.csv"
    hint = build_mount_resolution_hint(
        uri,
        artifact_meta={"mount_scheme": "inputs", "mount_root": "/old/inputs"},
        mounts={"inputs": "/new/inputs"},
    )
    assert hint is not None

    msg = format_missing_artifact_mount_help(
        hint, resolved_path="/new/inputs/nested/file.csv"
    )
    assert "Mount root mismatch" in msg
    assert "logged mount_root" in msg
    assert "/old/inputs" in msg
    assert "/new/inputs" in msg


def test_missing_artifact_help_mentions_unconfigured_mount() -> None:
    """
    Explains when a mount URI scheme is not configured.

    This checks that when an artifact URI uses a scheme (e.g., ``inputs://``)
    that is not present in the current Tracker mounts, the help text suggests
    configuring mounts for the session.
    """
    uri = "inputs://data.csv"
    hint = build_mount_resolution_hint(
        uri,
        artifact_meta={"mount_root": "/some/inputs"},
        mounts={},
    )
    assert hint is not None

    msg = format_missing_artifact_mount_help(hint, resolved_path="/nowhere/data.csv")
    assert "not configured" in msg
    assert "Fix: pass `mounts={...}`" in msg
