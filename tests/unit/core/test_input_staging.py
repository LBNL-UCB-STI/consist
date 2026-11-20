from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import consist.api as consist_api
from consist import Tracker
from consist.core.materialize import (
    StagedInput,
    StagedInputsResult,
    stage_artifact,
    stage_inputs,
)
from consist.digest import Digest, compute_digest
from consist.models.artifact import Artifact


def _checksum(path: Path) -> Digest:
    return compute_digest(path, driver="directory" if path.is_dir() else "file")


def _db_less_tracker(
    run_dir: Path,
    *,
    allow_external_paths: bool = True,
    mounts: dict[str, str] | None = None,
) -> Tracker:
    return Tracker(
        run_dir=run_dir,
        db_path=None,
        mounts=mounts,
        allow_external_paths=allow_external_paths,
    )


def _artifact(
    key: str,
    uri: str,
    *,
    driver: str = "csv",
    hash_value: Digest | str | None = None,
    meta: dict | None = None,
    abs_path: Path | None = None,
) -> Artifact:
    digest_obj: Digest | None
    if isinstance(hash_value, Digest) or hash_value is None:
        digest_obj = hash_value
    else:
        digest_obj = Digest(
            "sha256:file", hashlib.sha256(hash_value.encode("utf-8")).hexdigest()
        )
    artifact = Artifact(
        key=key,
        uri=uri,
        driver=driver,
        digest=digest_obj,
        meta=dict(meta or {}),
    )
    if abs_path is not None:
        artifact = artifact.with_abs_path(abs_path)
    return artifact


def test_stage_artifact_copies_file_and_detaches_artifact(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _db_less_tracker(tmp_path)
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))

    result = stage_artifact(tracker, artifact, destination)

    assert isinstance(result, StagedInput)
    assert result.status == "staged"
    assert result.resolvable is True
    assert result.path == destination.resolve()
    assert result.artifact.digest == artifact.digest
    assert result.artifact.as_path() == destination.resolve()
    assert destination.read_text(encoding="utf-8") == "value\n1\n"


def test_stage_artifact_preserves_existing_identical_destination(
    tmp_path: Path,
) -> None:
    source = tmp_path / "inputs" / "source.csv"
    destination = tmp_path / "runs" / "staged" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination.write_text("value\n1\n", encoding="utf-8")
    tracker = _db_less_tracker(tmp_path)
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
    tracker = _db_less_tracker(tmp_path)
    artifact = _artifact(
        "bundle",
        "./inputs/bundle",
        driver="zarr",
        hash_value=_checksum(source),
    )

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "staged"
    assert result.artifact.digest == artifact.digest
    assert (destination / "nested" / "data.txt").read_text(
        encoding="utf-8"
    ) == "payload"


def test_stage_artifact_preserves_missing_hash_on_detached_artifact(
    tmp_path: Path,
) -> None:
    source = tmp_path / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _db_less_tracker(tmp_path)
    artifact = _artifact("source", "./inputs/source.csv")

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "staged"
    assert result.artifact.digest is None


def test_stage_artifact_uses_runtime_abs_path_when_canonical_resolution_missing(
    tmp_path: Path,
) -> None:
    source = tmp_path / "fallback" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "runs" / "staged" / "source.csv"
    tracker = _db_less_tracker(tmp_path / "runs")
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
    artifact = _artifact(
        "source",
        "./inputs/source.csv",
        hash_value=_checksum(source),
    )
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "prov.duckdb",
        allow_external_paths=True,
    )
    try:
        tracker.db.sync_artifact(artifact, run_id=None, direction="output")
        tracker.set_artifact_recovery_roots(artifact, [archive_root])

        result = stage_artifact(tracker, artifact, destination)
    finally:
        tracker.close()

    assert result.status == "staged"
    assert destination.read_text(encoding="utf-8") == "value\n1\n"


@pytest.mark.parametrize(
    ("use_named_mount", "expected_mount_name"),
    [
        pytest.param(True, "archive", id="named-mount"),
        pytest.param(False, "abs", id="abs-mount"),
    ],
)
def test_stage_artifact_rejects_escaping_symlink_recovery_source(
    tmp_path: Path,
    use_named_mount: bool,
    expected_mount_name: str,
) -> None:
    """A recovery source escaping its root through a symlink is never followed (C1)."""
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / "source.csv"
    outside_file.write_text("value\noutside\n", encoding="utf-8")
    archive_root = tmp_path / "archive"
    (archive_root / "inputs").mkdir(parents=True)
    (archive_root / "inputs").rmdir()
    (archive_root / "inputs").symlink_to(outside, target_is_directory=True)
    destination = tmp_path / "runs" / "staged" / "source.csv"
    artifact = _artifact("source", "./inputs/source.csv")
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=tmp_path / "prov.duckdb",
        mounts={"archive": str(archive_root)} if use_named_mount else None,
        allow_external_paths=True,
    )
    try:
        tracker.db.sync_artifact(artifact, run_id=None, direction="output")
        tracker.set_artifact_recovery_roots(artifact, [archive_root])
        rows = tracker.db.get_artifact_recovery_locations(artifact.id)
        assert rows and all(row.mount_name == expected_mount_name for row in rows)

        result = stage_artifact(
            tracker, artifact, destination, validate_content_hash="never"
        )
    finally:
        tracker.close()

    assert result.status == "missing_source"
    assert result.resolvable is False
    assert not destination.exists()
    assert outside_file.read_text(encoding="utf-8") == "value\noutside\n"


def test_stage_artifact_reports_missing_source(tmp_path: Path) -> None:
    destination = tmp_path / "runs" / "staged" / "missing.csv"
    tracker = _db_less_tracker(tmp_path)
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
    tracker = _db_less_tracker(tmp_path)
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
    tracker = _db_less_tracker(tmp_path)
    artifact = _artifact("source", "./inputs/source.csv", hash_value=_checksum(source))

    result = stage_artifact(tracker, artifact, destination)

    assert result.status == "failed"
    assert "Symlink detected in destination path" in (result.message or "")


def test_stage_artifact_rejects_external_destination_when_disallowed(
    tmp_path: Path,
) -> None:
    source = tmp_path / "runs" / "inputs" / "source.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("value\n1\n", encoding="utf-8")
    tracker = _db_less_tracker(
        tmp_path / "runs",
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
    tracker = _db_less_tracker(tmp_path)
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


def test_api_stage_artifact_delegates_to_tracker_method(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sentinel = object()
    calls: dict[str, object] = {}
    artifact = _artifact("source", "./inputs/source.csv")

    def _fake_stage(art, **kwargs):
        calls.update(art=art, kwargs=kwargs)
        return sentinel

    tracker = _db_less_tracker(tmp_path / "runs")
    monkeypatch.setattr(tracker, "stage_artifact", _fake_stage)

    result = consist_api.stage_artifact(
        artifact,
        destination="/tmp/staged.csv",
        tracker=tracker,
        overwrite=True,
        mode="copy",
    )

    assert result is sentinel
    assert calls["art"] is artifact
    assert calls["kwargs"]["overwrite"] is True
    assert calls["kwargs"]["destination"] == "/tmp/staged.csv"
    assert calls["kwargs"]["mode"] == "copy"


def test_api_stage_inputs_delegates_to_tracker_method(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sentinel = object()
    calls: dict[str, object] = {}
    artifact = _artifact("source", "./inputs/source.csv")

    def _fake_stage(inputs_by_key, **kwargs):
        calls.update(
            inputs_by_key=inputs_by_key,
            kwargs=kwargs,
        )
        return sentinel

    tracker = _db_less_tracker(tmp_path / "runs")
    monkeypatch.setattr(tracker, "stage_inputs", _fake_stage)

    result = consist_api.stage_inputs(
        {"source": artifact},
        destinations_by_key={"source": "/tmp/staged.csv"},
        tracker=tracker,
        overwrite=True,
        mode="copy",
    )

    assert result is sentinel
    assert calls["inputs_by_key"] == {"source": artifact}
    assert calls["kwargs"]["overwrite"] is True
    assert calls["kwargs"]["destinations_by_key"] == {"source": "/tmp/staged.csv"}
    assert calls["kwargs"]["mode"] == "copy"
