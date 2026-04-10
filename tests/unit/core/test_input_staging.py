from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import consist.api as consist_api
from consist.core.fs import FileSystemManager
from consist.core.materialize import (
    StagedInput,
    StagedInputsResult,
    stage_artifact,
    stage_inputs,
)
from consist.models.artifact import Artifact


def _checksum(path: Path) -> str:
    if path.is_dir():
        digest = hashlib.sha256()
        for child in sorted(path.rglob("*")):
            if child.is_file():
                digest.update(child.relative_to(path).as_posix().encode("utf-8"))
                digest.update(b"\0")
                digest.update(child.read_bytes())
        return digest.hexdigest()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tracker(
    run_dir: Path,
    *,
    resolve_uri,
    allow_external_paths: bool = True,
    mounts: dict[str, str] | None = None,
):
    return SimpleNamespace(
        run_dir=run_dir,
        fs=FileSystemManager(run_dir, mounts),
        allow_external_paths=allow_external_paths,
        resolve_uri=resolve_uri,
        identity=SimpleNamespace(compute_file_checksum=_checksum),
    )


def _artifact(
    key: str,
    container_uri: str,
    *,
    driver: str = "csv",
    hash_value: str | None = None,
    meta: dict | None = None,
    abs_path: Path | None = None,
) -> Artifact:
    artifact = Artifact(
        key=key,
        container_uri=container_uri,
        driver=driver,
        hash=hash_value,
        meta=dict(meta or {}),
    )
    if abs_path is not None:
        artifact.abs_path = str(abs_path)
    return artifact


def test_stage_artifact_copies_file_and_detaches_artifact(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))

    result = stage_artifact(tracker, artifact, destination)

    assert isinstance(result, StagedInput)
    assert result.status == "staged"
    assert result.resolvable is True
    assert result.path == destination.resolve()
    assert result.artifact.as_path() == destination.resolve()
    assert destination.read_text(encoding="utf-8") == "value\n1\n"


def test_stage_artifact_preserves_existing_identical_destination(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.csv"
    destination = tmp_path / "runs" / "staged" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination.write_text("value\n1\n", encoding="utf-8")
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "preserved_existing"
    assert result.resolvable is True
    assert result.path == destination.resolve()
    assert result.artifact.as_path() == destination.resolve()


def test_stage_artifact_stages_directories(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "bundle"
    (source / "nested").mkdir(parents=True, exist_ok=True)
    (source / "nested" / "data.txt").write_text("payload", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "bundle"
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact(
        "bundle",
        "./inputs/bundle",
        driver="zarr",
    )

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "staged"
    assert (destination / "nested" / "data.txt").read_text(encoding="utf-8") == "payload"


def test_stage_artifact_uses_runtime_abs_path_when_canonical_resolution_missing(
    tmp_path: Path,
) -> None:
    source = tmp_path / "fallback" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _tracker(
        tmp_path / "runs",
        resolve_uri=lambda _uri: str(tmp_path / "missing" / "source.csv"),
    )
    artifact = _artifact(
        "source",
        "./inputs/source.csv",
        abs_path=source,
        hash_value=_checksum(source),
    )

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "staged"
    assert destination.read_text(encoding="utf-8") == "value\n1\n"


def test_stage_artifact_uses_recovery_roots_when_source_is_not_resolved(
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "archive"
    source = archive_root / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _tracker(
        tmp_path / "runs",
        resolve_uri=lambda _uri: str(tmp_path / "missing" / "source.csv"),
    )
    artifact = _artifact(
        "source",
        "./inputs/source.csv",
        meta={"recovery_roots": [archive_root]},
        hash_value=_checksum(source),
    )

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "staged"
    assert destination.read_text(encoding="utf-8") == "value\n1\n"


def test_stage_artifact_reports_missing_source(tmp_path: Path) -> None:
    source = tmp_path / "missing.csv"
    destination = tmp_path / "runs" / "staged" / "missing.csv"
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact("missing", "./missing.csv")

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "missing_source"
    assert result.resolvable is False
    assert "source path missing" in (result.message or "")


def test_stage_artifact_reports_hash_mismatch_when_validation_applies(
    tmp_path: Path,
) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact("source", "./inputs/source.csv", hash_value="deadbeef")

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "failed"
    assert "Content hash mismatch" in (result.message or "")
    assert not destination.exists()


def test_stage_artifact_rejects_symlink_destination(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    real_destination = tmp_path / "runs" / "real.csv"
    real_destination.parent.mkdir(parents=True, exist_ok=True)
    real_destination.write_text("value\n2\n", encoding="utf-8")
    destination = tmp_path / "runs" / "linked.csv"
    destination.symlink_to(real_destination)
    tracker = _tracker(tmp_path / "runs", resolve_uri=lambda _uri: str(source))
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "failed"
    assert "Symlink detected in destination path" in (result.message or "")


def test_stage_artifact_rejects_external_destination_when_disallowed(
    tmp_path: Path,
) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    tracker = _tracker(
        tmp_path / "runs",
        resolve_uri=lambda _uri: str(source),
        allow_external_paths=False,
    )
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))
    destination = tmp_path / "outside.csv"

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "failed"
    assert "outside allowed" in (result.message or "")


def test_stage_inputs_returns_ordered_keyed_results(tmp_path: Path) -> None:
    source_a = tmp_path / "inputs" / "a.csv"
    source_b = tmp_path / "inputs" / "b.csv"
    source_a.parent.mkdir(parents=True, exist_ok=True)
    source_a.write_text("value\n1\n", encoding="utf-8")
    source_b.write_text("value\n2\n", encoding="utf-8")
    dest_a = tmp_path / "runs" / "staged" / "a.csv"
    dest_b = tmp_path / "runs" / "staged" / "b.csv"
    dest_b.parent.mkdir(parents=True, exist_ok=True)
    dest_b.write_text("value\n2\n", encoding="utf-8")
    tracker = _tracker(
        tmp_path / "runs",
        resolve_uri=lambda uri: str(source_a if uri.endswith("a.csv") else source_b),
    )
    inputs = {
        "b": _artifact("b", "./inputs/b.csv", hash_value=_checksum(source_b)),
        "a": _artifact("a", "./inputs/a.csv", hash_value=_checksum(source_a)),
    }
    destinations = {"b": dest_b, "a": dest_a}

    result = stage_inputs(tracker, inputs, destinations)

    assert isinstance(result, StagedInputsResult)
    assert list(result.keys()) == ["b", "a"]
    assert result["b"].status == "preserved_existing"
    assert result["a"].status == "staged"
    assert result.paths["a"] == dest_a.resolve()
    assert result.paths["b"] == dest_b.resolve()


def test_api_stage_artifact_delegates_to_core(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    calls: dict[str, object] = {}
    tracker = SimpleNamespace()
    artifact = _artifact("source", "./inputs/source.csv")

    def _fake_stage(tr, art, destination, **kwargs):
        calls.update(tr=tr, art=art, destination=destination, kwargs=kwargs)
        return sentinel

    monkeypatch.setattr(consist_api, "stage_artifact_core", _fake_stage)

    result = consist_api.stage_artifact(
        artifact,
        destination="/tmp/staged.csv",
        tracker=tracker,
        overwrite=True,
        mode="copy",
    )

    assert result is sentinel
    assert calls["tr"] is tracker
    assert calls["art"] is artifact
    assert calls["destination"] == Path("/tmp/staged.csv")
    assert calls["kwargs"]["overwrite"] is True


def test_api_stage_inputs_delegates_to_core(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    calls: dict[str, object] = {}
    tracker = SimpleNamespace()
    artifact = _artifact("source", "./inputs/source.csv")

    def _fake_stage(tr, inputs_by_key, destinations_by_key, **kwargs):
        calls.update(
            tr=tr,
            inputs_by_key=inputs_by_key,
            destinations_by_key=destinations_by_key,
            kwargs=kwargs,
        )
        return sentinel

    monkeypatch.setattr(consist_api, "stage_inputs_core", _fake_stage)

    result = consist_api.stage_inputs(
        {"source": artifact},
        destinations_by_key={"source": "/tmp/staged.csv"},
        tracker=tracker,
        overwrite=True,
        mode="copy",
    )

    assert result is sentinel
    assert calls["tr"] is tracker
    assert calls["inputs_by_key"] == {"source": artifact}
    assert calls["destinations_by_key"] == {"source": Path("/tmp/staged.csv")}
    assert calls["kwargs"]["overwrite"] is True
