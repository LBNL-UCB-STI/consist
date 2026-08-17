from pathlib import Path

import pytest
from sqlmodel import SQLModel

from consist.core.tracker import Tracker
from consist.integrations.containers import api as container_api
from consist.integrations.containers.api import (
    _build_container_manifest,
    _ensure_output_within_run_dir,
    _validate_host_path,
    run_container,
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


def test_run_container_applies_output_log_kwargs(monkeypatch, tmp_path: Path) -> None:
    class TaggedOutput(SQLModel):
        item_id: int
        value: int

    output_path = tmp_path / "runs" / "out.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    class FakeDockerBackend:
        def __init__(self, pull_latest: bool = False) -> None:
            self.pull_latest = pull_latest

        def resolve_image_digest(self, image: str) -> str:
            return f"{image}@sha256:test"

        def run(self, **kwargs) -> bool:
            output_path.write_text("item_id,value\n1,2\n", encoding="utf-8")
            return True

    monkeypatch.setattr(container_api, "DockerBackend", FakeDockerBackend)

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "state.duckdb",
        allow_external_paths=True,
    )

    result = run_container(
        tracker=tracker,
        run_id="container_spec_test",
        image="example:latest",
        command=["python", "-V"],
        volumes={},
        inputs=[],
        outputs={"out": output_path},
        strict_mounts=False,
        output_log_kwargs={
            "out": {
                "schema": TaggedOutput,
                "strict_schema": False,
                "driver": "csv",
            }
        },
    )

    artifact = result.artifacts["out"]
    assert artifact.driver == "csv"
    assert artifact.meta["schema_name"] == "TaggedOutput"
    assert artifact.meta.get("has_strict_schema") is not True
