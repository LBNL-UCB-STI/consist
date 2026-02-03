from pathlib import Path

import pytest

from consist.core.tracker import Tracker
from consist.integrations.containers.api import (
    _build_container_manifest,
    _ensure_output_within_run_dir,
    _validate_host_path,
)


def test_manifest_omits_env_values():
    manifest = _build_container_manifest(
        image="img:v1",
        image_digest="sha:123",
        command=["run"],
        environment={"SECRET": "do-not-store"},
        working_dir=None,
        backend_type="docker",
        volumes={"/host": "/container"},
    )

    assert "environment" not in manifest
    assert "environment_hash" in manifest
    assert "do-not-store" not in str(manifest)


def test_validate_host_path_requires_mounts_when_strict(tmp_path: Path) -> None:
    host_path = tmp_path / "data"
    with pytest.raises(ValueError, match="require configured tracker mounts"):
        _validate_host_path(host_path, allowed_roots=[], strict_mounts=True)


def test_validate_host_path_allows_absolute_when_not_strict(
    tmp_path: Path,
) -> None:
    host_path = tmp_path / "data"
    resolved = _validate_host_path(host_path, allowed_roots=[], strict_mounts=False)
    assert resolved == host_path.resolve()


def test_validate_host_path_resolves_relative_to_first_root(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "root").resolve()
    rel_path = Path("nested") / "file.txt"
    resolved = _validate_host_path(rel_path, allowed_roots=[root], strict_mounts=True)
    assert resolved == (root / rel_path).resolve()


def test_validate_host_path_rejects_outside_roots(tmp_path: Path) -> None:
    allowed = (tmp_path / "allowed").resolve()
    disallowed = (tmp_path / "other" / "file.txt").resolve()
    allowed.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError, match="outside allowed roots"):
        _validate_host_path(disallowed, allowed_roots=[allowed], strict_mounts=True)


def test_ensure_output_within_run_dir_enforces_when_strict(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir)
    outside_path = tmp_path / "outside" / "file.txt"
    outside_path.parent.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError, match="outside run_dir"):
        _ensure_output_within_run_dir(
            outside_path.resolve(), tracker, strict_mounts=True
        )


def test_ensure_output_within_run_dir_allows_external_when_enabled(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    tracker = Tracker(run_dir=run_dir, allow_external_paths=True)
    outside_path = tmp_path / "outside" / "file.txt"
    outside_path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_output_within_run_dir(outside_path.resolve(), tracker, strict_mounts=True)
