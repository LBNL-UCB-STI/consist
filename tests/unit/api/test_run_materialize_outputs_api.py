from __future__ import annotations

from collections.abc import Mapping
import hashlib
import os
from pathlib import Path
from typing import Sequence
import uuid

import pytest

import consist
import consist.core.materialize as consist_materialize
from consist.api import hydrate_run_outputs as hydrate_run_outputs_api
from consist.api import (
    hydrate_run_outputs_to_destinations as hydrate_run_outputs_to_destinations_api,
)
from consist.api import materialize_run_outputs as materialize_run_outputs_api
from consist.api import archive_artifact as archive_artifact_api
from consist.api import archive_current_run_outputs as archive_current_run_outputs_api
from consist.api import archive_run_outputs as archive_run_outputs_api
from consist.api import archive_run_output_files as archive_run_output_files_api
from consist.api import (
    register_artifact_recovery_copy as register_artifact_recovery_copy_api,
)
from consist.api import (
    register_run_output_recovery_copies as register_run_output_recovery_copies_api,
)
from consist.api import set_artifact_recovery_roots as set_artifact_recovery_roots_api
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact, ArchivedOutputs
from consist.models.run import Run


def test_tracker_forwards_file_output_archiving_to_archive_service(
    tmp_path: Path,
) -> None:
    """Tracker keeps the public archive API while the service owns its behavior."""

    class ArchiveService:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []
            self.result = object()

        def archive_run_output_files(
            self,
            run_id: str,
            recovery_root: Path,
            *,
            keys: Sequence[str] | None,
            preserve_existing: bool,
            verify: bool,
            append: bool,
        ) -> object:
            self.calls.append(
                (run_id, recovery_root, keys, preserve_existing, verify, append)
            )
            return self.result

    tracker = Tracker(run_dir=tmp_path)
    service = ArchiveService()
    tracker._archive_service = service

    result = tracker.archive_run_output_files(
        "producer",
        tmp_path / "archive",
        keys=["result"],
        preserve_existing=False,
        verify=False,
        append=False,
    )

    assert result is service.result
    assert service.calls == [
        ("producer", tmp_path / "archive", ["result"], False, False, False)
    ]


def _hydrated_result(
    outputs: dict[str, tuple[str | None, str, bool, str | None]],
) -> consist_materialize.HydratedRunOutputsResult:
    hydrated_outputs: dict[str, consist_materialize.HydratedRunOutput] = {}
    for key, (path, status, resolvable, message) in outputs.items():
        hydrated_outputs[key] = consist_materialize.HydratedRunOutput(
            key=key,
            artifact=Artifact(
                id=uuid.uuid4(),
                key=key,
                container_uri=f"./{key}.csv",
                driver="csv",
                meta={},
            ),
            path=Path(path) if path is not None else None,
            status=status,
            message=message,
            resolvable=resolvable,
        )
    return consist_materialize.HydratedRunOutputsResult(outputs=hydrated_outputs)


def test_tracker_materialize_run_outputs_requires_db(tmp_path: Path) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=None)

    with pytest.raises(RuntimeError, match="tracker has no database configured"):
        tracker.materialize_run_outputs("missing", target_root=tracker.run_dir)


def test_tracker_materialize_run_outputs_requires_existing_run(
    tracker: Tracker,
) -> None:
    with pytest.raises(KeyError, match="Run 'missing' was not found"):
        tracker.materialize_run_outputs("missing", target_root=tracker.run_dir)


def test_tracker_materialize_run_outputs_rejects_external_target_root_when_disallowed(
    tracker: Tracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    outside = tracker.run_dir.parent / "outside"

    with pytest.raises(
        ValueError,
        match="outside allowed base|configured mount root|allow_external_paths",
    ):
        tracker.materialize_run_outputs("run_1", target_root=outside)


def test_tracker_materialize_run_outputs_folds_hydrated_results(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hydrated_result = _hydrated_result(
        {
            "from_fs": (
                str((tracker.run_dir / "copied.csv").resolve()),
                "materialized_from_filesystem",
                True,
                None,
            ),
            "from_db": (
                str((tracker.run_dir / "exported.csv").resolve()),
                "materialized_from_db",
                True,
                None,
            ),
            "keep-me": (
                str((tracker.run_dir / "existing.csv").resolve()),
                "preserved_existing",
                True,
                None,
            ),
            "already-unmapped": (None, "skipped_unmapped", False, None),
            "missing-source": (
                str((tracker.run_dir / "missing.csv").resolve()),
                "missing_source",
                False,
                "source path missing",
            ),
            "bad": (
                str((tracker.run_dir / "bad.csv").resolve()),
                "failed",
                False,
                "copy failed",
            ),
        }
    )
    calls: dict[str, object] = {}

    def _fake_hydrate(self, run_id: str, **kwargs):
        calls["run_id"] = run_id
        calls["kwargs"] = kwargs
        return hydrated_result

    monkeypatch.setattr(Tracker, "hydrate_run_outputs", _fake_hydrate)

    result = tracker.materialize_run_outputs(
        "run_1",
        target_root=tracker.run_dir / "restored",
        source_root=tracker.run_dir / "archive",
        keys=("a", "b"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": tracker.run_dir / "restored",
            "source_root": tracker.run_dir / "archive",
            "keys": ("a", "b"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert result.materialized_from_filesystem == {
        "from_fs": str((tracker.run_dir / "copied.csv").resolve()),
    }
    assert result.materialized_from_db == {
        "from_db": str((tracker.run_dir / "exported.csv").resolve())
    }
    assert result.skipped_existing == ["keep-me"]
    assert result.skipped_unmapped == ["already-unmapped"]
    assert result.skipped_missing_source == ["missing-source"]
    assert result.failed == [("bad", "copy failed")]


def test_tracker_hydrate_run_outputs_delegates_to_core_helper(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}
    hydrated_result = _hydrated_result({})

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(
        *,
        tracker,
        run,
        target_root,
        source_root,
        keys,
        allowed_base,
        preserve_existing,
        on_missing,
        db_fallback,
    ):
        calls.update(
            tracker=tracker,
            run=run,
            target_root=target_root,
            source_root=source_root,
            keys=keys,
            allowed_base=allowed_base,
            preserve_existing=preserve_existing,
            on_missing=on_missing,
            db_fallback=db_fallback,
        )
        return hydrated_result

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    result = tracker.hydrate_run_outputs(
        "run_1",
        target_root=tracker.run_dir / "restored",
        source_root=tmp_path / "archive",
        keys=("a", "b"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result is hydrated_result
    assert calls == {
        "tracker": tracker,
        "run": run,
        "target_root": (tracker.run_dir / "restored").resolve(),
        "source_root": (tmp_path / "archive").resolve(),
        "keys": ("a", "b"),
        "allowed_base": (tracker.run_dir.resolve(),),
        "preserve_existing": False,
        "on_missing": "raise",
        "db_fallback": "never",
    }


def test_set_artifact_recovery_roots_persists_and_invalidates_cache(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "table.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run("producer_recovery_roots", model="producer"):
        tracker.log_artifact(output_path, key="table", direction="output")

    cached = tracker.get_run_outputs("producer_recovery_roots")["table"]
    archive_a = tmp_path / "archive_a"
    archive_b = tmp_path / "archive_b"

    updated = set_artifact_recovery_roots_api(
        cached,
        [archive_a, archive_a],
        tracker=tracker,
    )
    assert updated.meta["recovery_roots"] == [str(archive_a.resolve())]

    refreshed = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert refreshed.meta["recovery_roots"] == [str(archive_a.resolve())]

    tracker.set_artifact_recovery_roots(refreshed, [archive_b], append=True)
    appended = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert appended.meta["recovery_roots"] == [
        str(archive_a.resolve()),
        str(archive_b.resolve()),
    ]

    tracker.set_artifact_recovery_roots(appended, [], append=False)
    cleared = tracker.get_run_outputs("producer_recovery_roots")["table"]
    assert "recovery_roots" not in cleared.meta


def test_log_artifact_normalizes_recovery_roots_metadata(tracker: Tracker) -> None:
    output_path = tracker.run_dir / "outputs" / "normalized.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive = tracker.run_dir.parent / "archive_root"

    with tracker.start_run("producer_log_recovery_roots", model="producer"):
        artifact = tracker.log_artifact(
            output_path,
            key="normalized",
            direction="output",
            recovery_roots=[archive, archive],
        )

    assert artifact.meta["recovery_roots"] == [str(archive.resolve())]
    assert artifact.recovery_roots == [str(archive.resolve())]


def test_archive_artifact_copies_bytes_and_records_recovery_root(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "archived.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive_root = tmp_path / "archive_copy"

    with tracker.start_run("producer_archive_copy", model="producer"):
        artifact = tracker.log_artifact(output_path, key="archived", direction="output")

    archived_path = archive_artifact_api(artifact, archive_root, tracker=tracker)

    assert archived_path == (archive_root / "outputs" / "archived.csv").resolve()
    assert archived_path.read_text(encoding="utf-8") == "value\n1\n"
    assert output_path.exists()
    refreshed = tracker.get_run_outputs("producer_archive_copy")["archived"]
    assert refreshed.meta["recovery_roots"] == [str(archive_root.resolve())]


def test_register_artifact_recovery_copy_verifies_existing_file_without_copying(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "adopted.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive"

    with tracker.start_run("producer_adopt_copy", model="producer"):
        artifact = tracker.log_artifact(output_path, key="adopted", direction="output")

    expected_path = recovery_root / "outputs" / "adopted.csv"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.write_text("value\n1\n", encoding="utf-8")

    result = register_artifact_recovery_copy_api(
        artifact,
        recovery_root,
        tracker=tracker,
    )

    assert result.status == "registered"
    assert result.expected_path == expected_path.resolve()
    assert result.metadata_updated is True
    assert output_path.exists()
    refreshed = tracker.get_run_outputs("producer_adopt_copy")["adopted"]
    assert refreshed.recovery_roots == [str(recovery_root.resolve())]


def test_register_artifact_recovery_copy_missing_file_blocks_metadata_update(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "missing_archive.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_missing"

    with tracker.start_run("producer_adopt_missing", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="missing_archive", direction="output"
        )

    result = tracker.register_artifact_recovery_copy(artifact, recovery_root)

    assert result.status == "missing_copy"
    assert result.metadata_updated is False
    refreshed = tracker.get_run_outputs("producer_adopt_missing")["missing_archive"]
    assert "recovery_roots" not in refreshed.meta


def test_register_artifact_recovery_copy_hash_mismatch_blocks_metadata_update(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "mismatch.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_mismatch"

    with tracker.start_run("producer_adopt_mismatch", model="producer"):
        artifact = tracker.log_artifact(output_path, key="mismatch", direction="output")

    expected_path = recovery_root / "outputs" / "mismatch.csv"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.write_text("value\n999\n", encoding="utf-8")

    result = tracker.register_artifact_recovery_copy(artifact, recovery_root)

    assert result.status == "hash_mismatch"
    assert result.metadata_updated is False
    refreshed = tracker.get_run_outputs("producer_adopt_mismatch")["mismatch"]
    assert "recovery_roots" not in refreshed.meta


def test_register_artifact_recovery_copy_uses_full_hash_when_tracker_is_fast(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs_fast",
        db_path=str(tmp_path / "fast.duckdb"),
        hashing_strategy="fast",
    )
    try:
        output_path = tracker.run_dir / "outputs" / "fast_hash.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_text = "value\n1\n"
        output_path.write_text(output_text, encoding="utf-8")
        recovery_root = tmp_path / "external_archive_fast"

        with tracker.start_run("producer_adopt_fast", model="producer"):
            artifact = tracker.log_artifact(
                output_path, key="fast_hash", direction="output"
            )

        expected_path = recovery_root / "outputs" / "fast_hash.csv"
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        expected_path.write_text(output_text, encoding="utf-8")
        content_hash = hashlib.sha256(output_text.encode("utf-8")).hexdigest()

        result = tracker.register_artifact_recovery_copy(
            artifact,
            recovery_root,
            content_hash=content_hash,
        )

        assert result.status == "registered"
        refreshed = tracker.get_run_outputs("producer_adopt_fast")["fast_hash"]
        assert refreshed.recovery_roots == [str(recovery_root.resolve())]
    finally:
        if tracker.engine:
            tracker.engine.dispose()


def test_register_artifact_recovery_copy_fast_hash_requires_byte_hash(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs_fast_unverifiable",
        db_path=str(tmp_path / "fast_unverifiable.duckdb"),
        hashing_strategy="fast",
    )
    try:
        output_path = tracker.run_dir / "outputs" / "fast_unverifiable.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("value\n1\n", encoding="utf-8")
        recovery_root = tmp_path / "external_archive_fast_unverifiable"

        with tracker.start_run("producer_adopt_fast_unverifiable", model="producer"):
            artifact = tracker.log_artifact(
                output_path, key="fast_unverifiable", direction="output"
            )

        expected_path = recovery_root / "outputs" / "fast_unverifiable.csv"
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        expected_path.write_text("value\n1\n", encoding="utf-8")

        result = tracker.register_artifact_recovery_copy(artifact, recovery_root)

        assert result.status == "unverifiable_hash"
        assert result.expected_path == expected_path.resolve()
        assert result.metadata_updated is False
        refreshed = tracker.get_run_outputs("producer_adopt_fast_unverifiable")[
            "fast_unverifiable"
        ]
        assert "recovery_roots" not in refreshed.meta
    finally:
        if tracker.engine:
            tracker.engine.dispose()


def test_register_artifact_recovery_copy_verify_false_adopts_existing_copy(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs_verify_false",
        db_path=str(tmp_path / "verify_false.duckdb"),
        hashing_strategy="fast",
    )
    try:
        output_path = tracker.run_dir / "outputs" / "verify_false.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("value\n1\n", encoding="utf-8")
        recovery_root = tmp_path / "external_archive_verify_false"

        with tracker.start_run("producer_adopt_verify_false", model="producer"):
            artifact = tracker.log_artifact(
                output_path, key="verify_false", direction="output"
            )

        expected_path = recovery_root / "outputs" / "verify_false.csv"
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        expected_path.write_text("different bytes\n", encoding="utf-8")

        result = tracker.register_artifact_recovery_copy(
            artifact,
            recovery_root,
            verify=False,
        )

        assert result.status == "registered"
        assert result.expected_path == expected_path.resolve()
        assert result.metadata_updated is True
        refreshed = tracker.get_run_outputs("producer_adopt_verify_false")[
            "verify_false"
        ]
        assert refreshed.recovery_roots == [str(recovery_root.resolve())]
    finally:
        if tracker.engine:
            tracker.engine.dispose()


def test_register_artifact_recovery_copy_empty_content_hash_is_explicit(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "empty_hash.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_empty_hash"

    with tracker.start_run("producer_adopt_empty_hash", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="empty_hash", direction="output"
        )

    expected_path = recovery_root / "outputs" / "empty_hash.csv"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.write_text("value\n1\n", encoding="utf-8")

    result = tracker.register_artifact_recovery_copy(
        artifact,
        recovery_root,
        content_hash="",
    )

    assert result.status == "hash_mismatch"
    assert result.expected_path == expected_path.resolve()
    assert result.metadata_updated is False
    refreshed = tracker.get_run_outputs("producer_adopt_empty_hash")["empty_hash"]
    assert "recovery_roots" not in refreshed.meta


def test_register_artifact_recovery_copy_rejects_same_metadata_wrong_bytes(
    tmp_path: Path,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs_fast_same_metadata",
        db_path=str(tmp_path / "fast_same_metadata.duckdb"),
        hashing_strategy="fast",
    )
    try:
        output_path = tracker.run_dir / "outputs" / "same_metadata.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_text = "value\n1\n"
        output_path.write_text(output_text, encoding="utf-8")
        recovery_root = tmp_path / "external_archive_same_metadata"

        with tracker.start_run("producer_adopt_same_metadata", model="producer"):
            artifact = tracker.log_artifact(
                output_path, key="same_metadata", direction="output"
            )

        expected_path = recovery_root / "outputs" / "same_metadata.csv"
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        expected_path.write_text("value\n9\n", encoding="utf-8")
        source_stat = output_path.stat()
        os.utime(
            expected_path,
            ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
        )
        content_hash = hashlib.sha256(output_text.encode("utf-8")).hexdigest()

        result = tracker.register_artifact_recovery_copy(
            artifact,
            recovery_root,
            content_hash=content_hash,
        )

        assert result.status == "hash_mismatch"
        assert result.metadata_updated is False
        refreshed = tracker.get_run_outputs("producer_adopt_same_metadata")[
            "same_metadata"
        ]
        assert "recovery_roots" not in refreshed.meta
    finally:
        if tracker.engine:
            tracker.engine.dispose()


def test_register_artifact_recovery_copy_unmappable_layout_blocks_clearly(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key="absolute",
        container_uri=str((tmp_path / "absolute.csv").resolve()),
        driver="csv",
        meta={},
    )

    result = tracker.register_artifact_recovery_copy(
        artifact,
        tmp_path / "external_archive_unmappable",
    )

    assert result.status == "skipped_unmapped"
    assert result.expected_path is None
    assert "root-only recovery metadata" in str(result.message)
    assert result.metadata_updated is False


def test_register_artifact_recovery_copy_symlink_destination_blocks_metadata_update(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "symlink.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_symlink"

    with tracker.start_run("producer_adopt_symlink", model="producer"):
        artifact = tracker.log_artifact(output_path, key="symlink", direction="output")

    expected_path = recovery_root / "outputs" / "symlink.csv"
    expected_path.parent.mkdir(parents=True, exist_ok=True)
    expected_path.symlink_to(output_path)

    result = tracker.register_artifact_recovery_copy(artifact, recovery_root)

    assert result.status == "symlink_destination"
    assert result.metadata_updated is False
    refreshed = tracker.get_run_outputs("producer_adopt_symlink")["symlink"]
    assert "recovery_roots" not in refreshed.meta


def test_archive_artifact_unmappable_layout_has_actionable_error(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key="absolute",
        container_uri=str((tmp_path / "absolute.csv").resolve()),
        driver="csv",
        meta={},
    )

    with pytest.raises(
        ValueError,
        match="Use managed output paths|cannot be recovered from root-only recovery metadata",
    ):
        tracker.archive_artifact(artifact, tmp_path / "archive")


def test_archive_artifact_move_updates_runtime_path(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "moved.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_move"

    with tracker.start_run("producer_archive_move", model="producer"):
        artifact = tracker.log_artifact(output_path, key="moved", direction="output")

    archived_path = tracker.archive_artifact(artifact, archive_root, mode="move")

    assert archived_path.read_text(encoding="utf-8") == "value\n2\n"
    assert not output_path.exists()
    assert artifact.abs_path == str(archived_path.resolve())
    assert artifact.path == archived_path


def test_archive_current_run_outputs_uses_active_run(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "current.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n4\n", encoding="utf-8")
    archive_root = tmp_path / "archive_current"

    with tracker.start_run("producer_archive_current", model="producer"):
        tracker.log_artifact(output_path, key="current", direction="output")
        archived = tracker.archive_current_run_outputs(archive_root, mode="copy")

    archived_path = (archive_root / "outputs" / "current.csv").resolve()
    assert archived == {"current": archived_path}
    assert archived_path.read_text(encoding="utf-8") == "value\n4\n"
    refreshed = tracker.get_run_outputs("producer_archive_current")["current"]
    assert refreshed.recovery_roots == [str(archive_root.resolve())]


def test_archive_current_run_outputs_requires_active_run(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="requires an active run context"):
        tracker.archive_current_run_outputs(tmp_path / "archive_current")


def test_archive_artifact_prefers_historical_source_over_current_workspace(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "provenance.db")
    tracker_a = Tracker(run_dir=tmp_path / "runs_a", db_path=db_path)
    tracker_b = Tracker(run_dir=tmp_path / "runs_b", db_path=db_path)

    source_path = tracker_a.run_dir / "outputs" / "shared.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("historical\n", encoding="utf-8")

    with tracker_a.start_run("producer_archive_history", model="producer"):
        tracker_a.log_artifact(source_path, key="shared", direction="output")

    current_path = tracker_b.run_dir / "outputs" / "shared.csv"
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("current\n", encoding="utf-8")

    historical_artifact = tracker_b.get_run_outputs("producer_archive_history")[
        "shared"
    ]
    archive_root = tmp_path / "archive_history"
    archived_path = tracker_b.archive_artifact(historical_artifact, archive_root)

    assert archived_path.read_text(encoding="utf-8") == "historical\n"

    if tracker_a.engine:
        tracker_a.engine.dispose()
    if tracker_b.engine:
        tracker_b.engine.dispose()


def test_archive_artifact_move_rolls_back_on_metadata_failure(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tracker.run_dir / "outputs" / "rollback.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n3\n", encoding="utf-8")
    archive_root = tmp_path / "archive_rollback"

    with tracker.start_run("producer_archive_rollback", model="producer"):
        artifact = tracker.log_artifact(output_path, key="rollback", direction="output")

    def _raise_update(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(tracker.db, "update_artifact_meta", _raise_update)

    with pytest.raises(RuntimeError, match="db down"):
        tracker.archive_artifact(artifact, archive_root, mode="move")

    assert output_path.read_text(encoding="utf-8") == "value\n3\n"
    assert not (archive_root / "outputs" / "rollback.csv").exists()


def test_archive_artifact_copy_is_idempotent_for_matching_file(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retry.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n4\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retry_copy"

    with tracker.start_run("producer_archive_retry_copy", model="producer"):
        artifact = tracker.log_artifact(output_path, key="retry", direction="output")

    first = tracker.archive_artifact(artifact, archive_root, mode="copy")
    second = tracker.archive_artifact(artifact, archive_root, mode="copy")

    assert first == second
    assert second.read_text(encoding="utf-8") == "value\n4\n"


def test_archive_artifact_move_is_idempotent_for_matching_file(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retry_move.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n5\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retry_move"

    with tracker.start_run("producer_archive_retry_move", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="retry_move", direction="output"
        )

    first = tracker.archive_artifact(artifact, archive_root, mode="move")
    second = tracker.archive_artifact(artifact, archive_root, mode="move")

    assert first == second
    assert second.read_text(encoding="utf-8") == "value\n5\n"
    assert not output_path.exists()


def test_archive_run_outputs_archives_selected_keys(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "a.csv"
    b_path = tracker.run_dir / "outputs" / "b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_bulk"

    with tracker.start_run("producer_archive_bulk", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    archived = archive_run_outputs_api(
        "producer_archive_bulk",
        archive_root,
        keys=["a"],
        tracker=tracker,
    )

    assert archived == {"a": (archive_root / "outputs" / "a.csv").resolve()}
    outputs = tracker.get_run_outputs("producer_archive_bulk")
    assert outputs["a"].meta["recovery_roots"] == [str(archive_root.resolve())]
    assert "recovery_roots" not in outputs["b"].meta


def test_archive_run_output_files_copies_verifies_and_registers_selected_file(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "reported.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive_root = tmp_path / "archive_reported"

    with tracker.start_run("producer_archive_reported", model="producer"):
        tracker.log_artifact(output_path, key="reported", direction="output")

    report = archive_run_output_files_api(
        "producer_archive_reported", archive_root, tracker=tracker
    )

    result = report["reported"]
    assert result.copy_status == "copied"
    assert result.verification_status == "verified"
    assert result.metadata_committed is True
    assert result.source_path == output_path.resolve()
    assert result.target_path == (archive_root / "outputs" / "reported.csv").resolve()
    assert result.target_path.read_text(encoding="utf-8") == "value\n1\n"
    assert report.complete is True
    assert report.summary == "copied=1 verified=1 metadata_committed=1"


def test_archive_run_output_files_validates_unknown_keys_before_creating_root(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "known.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive_root = tmp_path / "missing_key_archive"

    with tracker.start_run("producer_archive_missing_key", model="producer"):
        tracker.log_artifact(output_path, key="known", direction="output")

    with pytest.raises(KeyError, match="Requested output keys were not found"):
        tracker.archive_run_output_files(
            "producer_archive_missing_key", archive_root, keys=["missing"]
        )

    assert not archive_root.exists()


def test_archive_run_output_files_preserves_matching_existing_target(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retained.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retained"

    with tracker.start_run("producer_archive_retained", model="producer"):
        tracker.log_artifact(output_path, key="retained", direction="output")

    target = archive_root / "outputs" / "retained.csv"
    target.parent.mkdir(parents=True)
    target.write_text("value\n2\n", encoding="utf-8")

    report = tracker.archive_run_output_files("producer_archive_retained", archive_root)

    assert report["retained"].copy_status == "preserved_existing"
    assert report["retained"].verification_status == "verified"
    assert report["retained"].metadata_committed is True
    assert target.read_text(encoding="utf-8") == "value\n2\n"


def test_archive_run_output_files_rejects_symlink_destination(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "linked.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n3\n", encoding="utf-8")
    archive_root = tmp_path / "archive_linked" / "nested"
    outside = tmp_path / "outside"
    outside.mkdir()
    archive_root.parent.symlink_to(outside, target_is_directory=True)

    with tracker.start_run("producer_archive_linked", model="producer"):
        tracker.log_artifact(output_path, key="linked", direction="output")

    report = tracker.archive_run_output_files("producer_archive_linked", archive_root)

    assert report["linked"].copy_status == "symlink_destination"
    assert report["linked"].metadata_committed is False
    assert not (outside / "nested" / "outputs" / "linked.csv").exists()


def test_archive_run_output_files_rejects_symlink_source_ancestor(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    external_outputs = tmp_path / "external_outputs"
    output_dir = tracker.run_dir / "outputs"
    output_path = output_dir / "source_linked.csv"
    output_dir.mkdir(parents=True)
    output_path.write_text("value\n4\n", encoding="utf-8")

    with tracker.start_run("producer_archive_source_linked", model="producer"):
        tracker.log_artifact(output_path, key="source_linked", direction="output")

    output_dir.rename(external_outputs)
    output_dir.symlink_to(external_outputs, target_is_directory=True)

    report = tracker.archive_run_output_files(
        "producer_archive_source_linked", tmp_path / "archive_source_linked"
    )

    assert report["source_linked"].copy_status == "symlink_source"
    assert report["source_linked"].metadata_committed is False


def test_find_existing_recovery_source_path_default_validator_receives_resolved_path(
    tracker: Tracker,
) -> None:
    output_path = tracker.run_dir / "outputs" / "validator.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n", encoding="utf-8")
    with tracker.start_run("producer_validator_path", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="validator", direction="output"
        )
    run = tracker.get_run("producer_validator_path")
    assert run is not None
    seen: list[Path] = []

    _, source, _ = consist_materialize.find_existing_recovery_source_path(
        tracker,
        artifact=artifact,
        run=run,
        source_root=None,
        source_validator=lambda candidate: seen.append(candidate) or True,
    )

    assert seen == [output_path.resolve()]
    assert source == output_path.resolve()


def test_archive_run_output_files_rejects_symlink_recovery_root_before_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "root_linked.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n5\n", encoding="utf-8")
    external_root = tmp_path / "external_root"
    (external_root / "outputs").mkdir(parents=True)
    (external_root / "outputs" / "root_linked.csv").write_text(
        "value\n5\n", encoding="utf-8"
    )
    linked_root = tmp_path / "linked_recovery_root"
    linked_root.symlink_to(external_root, target_is_directory=True)

    with tracker.start_run("producer_archive_root_linked", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="root_linked", direction="output"
        )

    output_path.unlink()
    artifact.meta = {"recovery_roots": [str(linked_root)]}
    monkeypatch.setattr(
        tracker, "get_run_outputs", lambda _run_id: {"root_linked": artifact}
    )
    report = tracker.archive_run_output_files(
        "producer_archive_root_linked", tmp_path / "archive_root_linked"
    )

    assert report["root_linked"].copy_status == "symlink_source"
    assert report["root_linked"].metadata_committed is False


def test_archive_run_output_files_rejects_symlinked_historical_root_before_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "historical_linked.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n11\n", encoding="utf-8")
    external_root = tmp_path / "external_historical_root"
    (external_root / "outputs").mkdir(parents=True)
    (external_root / "outputs" / "historical_linked.csv").write_text(
        "value\n11\n", encoding="utf-8"
    )
    linked_root = tmp_path / "linked_historical_root"
    linked_root.symlink_to(external_root, target_is_directory=True)

    with tracker.start_run("producer_archive_historical_linked", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="historical_linked", direction="output"
        )
    historical_run = tracker.get_run("producer_archive_historical_linked")
    assert historical_run is not None
    historical_run.meta = {"_physical_run_dir": str(linked_root)}
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: historical_run)
    monkeypatch.setattr(
        tracker,
        "get_run_outputs",
        lambda _run_id: {"historical_linked": artifact},
    )

    report = tracker.archive_run_output_files(
        "producer_archive_historical_linked", tmp_path / "archive_historical_linked"
    )

    assert report["historical_linked"].copy_status == "symlink_source"


def test_archive_run_output_files_rejects_output_set_roles_before_copy(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key="set",
        container_uri="./outputs/set",
        driver="artifact_set",
        meta={"artifact_set": True},
    )
    monkeypatch.setattr(tracker, "get_run_outputs", lambda _run_id: {"set": artifact})

    report = tracker.archive_run_output_files("output_set_run", tmp_path / "archive")

    assert report["set"].copy_status == "unsupported_directory"
    assert report["set"].metadata_committed is False
    assert not (tmp_path / "archive").exists()


@pytest.mark.parametrize(
    ("key", "meta"),
    [
        ("manifest", {"output_set_manifest": True}),
        ("member", {"output_set_member": True}),
    ],
)
def test_archive_run_output_files_rejects_output_set_manifest_and_member(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
    key: str,
    meta: dict[str, bool],
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key=key,
        container_uri=f"./outputs/{key}.json",
        driver="json",
        meta=meta,
    )
    monkeypatch.setattr(tracker, "get_run_outputs", lambda _run_id: {key: artifact})

    report = tracker.archive_run_output_files("output_set_run", tmp_path / "archive")

    assert report[key].copy_status == "unsupported_directory"


def test_archive_run_output_files_reports_existing_target_blockers(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "existing.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n6\n", encoding="utf-8")
    archive_root = tmp_path / "archive_existing"
    target = archive_root / "outputs" / "existing.csv"
    target.parent.mkdir(parents=True)
    target.write_text("different\n", encoding="utf-8")
    with tracker.start_run("producer_archive_existing", model="producer"):
        tracker.log_artifact(output_path, key="existing", direction="output")

    mismatch = tracker.archive_run_output_files(
        "producer_archive_existing", archive_root
    )
    no_preserve = tracker.archive_run_output_files(
        "producer_archive_existing", archive_root, preserve_existing=False
    )

    assert mismatch["existing"].copy_status == "destination_exists"
    assert mismatch["existing"].verification_status == "hash_mismatch"
    assert no_preserve["existing"].copy_status == "destination_exists"
    assert target.read_text(encoding="utf-8") == "different\n"


def test_archive_run_output_files_can_register_without_verification(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "unverified.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n7\n", encoding="utf-8")
    with tracker.start_run("producer_archive_unverified", model="producer"):
        tracker.log_artifact(output_path, key="unverified", direction="output")

    report = tracker.archive_run_output_files(
        "producer_archive_unverified", tmp_path / "archive_unverified", verify=False
    )

    assert report["unverified"].verification_status == "not_requested"
    assert report["unverified"].metadata_committed is True
    assert report.complete is True


def test_archive_run_output_files_trusts_preexisting_target_without_verification(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "trusted_existing.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n12\n", encoding="utf-8")
    archive_root = tmp_path / "archive_trusted_existing"
    target = archive_root / "outputs" / "trusted_existing.csv"
    target.parent.mkdir(parents=True)
    target.write_text("unverified but retained\n", encoding="utf-8")
    with tracker.start_run("producer_archive_trusted_existing", model="producer"):
        tracker.log_artifact(output_path, key="trusted_existing", direction="output")

    report = tracker.archive_run_output_files(
        "producer_archive_trusted_existing", archive_root, verify=False
    )

    assert report["trusted_existing"].copy_status == "preserved_existing"
    assert report["trusted_existing"].verification_status == "not_requested"
    assert report["trusted_existing"].metadata_committed is True


def test_archive_run_output_files_reports_missing_source_and_fast_hash(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    missing_path = tracker.run_dir / "outputs" / "missing.csv"
    fast_path = tracker.run_dir / "outputs" / "fast.csv"
    missing_path.parent.mkdir(parents=True, exist_ok=True)
    missing_path.write_text("value\n8\n", encoding="utf-8")
    fast_path.write_text("value\n9\n", encoding="utf-8")
    with tracker.start_run("producer_archive_blocked", model="producer"):
        tracker.log_artifact(missing_path, key="missing", direction="output")
        tracker.log_artifact(fast_path, key="fast", direction="output")
    missing_path.unlink()
    missing_report = tracker.archive_run_output_files(
        "producer_archive_blocked", tmp_path / "archive_blocked", keys=["missing"]
    )
    tracker.identity.hashing_strategy = "fast"
    fast_report = tracker.archive_run_output_files(
        "producer_archive_blocked", tmp_path / "archive_blocked", keys=["fast"]
    )

    assert missing_report["missing"].copy_status == "missing_source"
    assert fast_report["fast"].verification_status == "unverifiable_hash"


def test_archive_run_output_files_rejects_directory_output(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "directory_output"
    output_path.mkdir(parents=True)
    (output_path / "member.txt").write_text("value\n", encoding="utf-8")
    with tracker.start_run("producer_archive_directory", model="producer"):
        tracker.log_artifact(output_path, key="directory_output", direction="output")

    report = tracker.archive_run_output_files(
        "producer_archive_directory", tmp_path / "archive_directory", verify=False
    )

    assert report["directory_output"].copy_status == "unsupported_directory"


def test_archive_run_output_files_retry_repairs_failed_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "retry_metadata.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n10\n", encoding="utf-8")
    archive_root = tmp_path / "archive_retry_metadata"
    with tracker.start_run("producer_archive_retry_metadata", model="producer"):
        tracker.log_artifact(output_path, key="retry_metadata", direction="output")

    with monkeypatch.context() as patch:
        patch.setattr(
            tracker,
            "_set_artifact_recovery_roots_bulk",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bulk")),
        )
        patch.setattr(
            tracker,
            "set_artifact_recovery_roots",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("single")),
        )
        first = tracker.archive_run_output_files(
            "producer_archive_retry_metadata", archive_root
        )

    second = tracker.archive_run_output_files(
        "producer_archive_retry_metadata", archive_root
    )

    assert first["retry_metadata"].copy_status == "copied"
    assert first["retry_metadata"].metadata_committed is False
    assert second["retry_metadata"].copy_status == "preserved_existing"
    assert second["retry_metadata"].metadata_committed is True


def test_archive_run_output_files_reports_one_fallback_metadata_failure(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    paths = {
        key: tracker.run_dir / "outputs" / f"fallback_{key}.csv" for key in ("a", "b")
    }
    for index, path in enumerate(paths.values()):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"value\n{index}\n", encoding="utf-8")
    with tracker.start_run("producer_archive_fallback", model="producer"):
        for key, path in paths.items():
            tracker.log_artifact(path, key=key, direction="output")

    monkeypatch.setattr(
        tracker,
        "_set_artifact_recovery_roots_bulk",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bulk")),
    )
    original = tracker.set_artifact_recovery_roots

    def fail_one(artifact: Artifact, *args, **kwargs):
        if artifact.key == "b":
            raise RuntimeError("single b")
        return original(artifact, *args, **kwargs)

    monkeypatch.setattr(tracker, "set_artifact_recovery_roots", fail_one)
    report = tracker.archive_run_output_files(
        "producer_archive_fallback", tmp_path / "archive_fallback"
    )

    assert report["a"].metadata_committed is True
    assert report["b"].metadata_committed is False
    assert report["b"].verification_status == "verified"


def test_archive_run_output_files_uses_one_bulk_metadata_update(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    paths = {key: tracker.run_dir / "outputs" / f"bulk_{key}.csv" for key in ("a", "b")}
    for index, path in enumerate(paths.values()):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"value\n{index}\n", encoding="utf-8")
    with tracker.start_run("producer_archive_bulk_once", model="producer"):
        for key, path in paths.items():
            tracker.log_artifact(path, key=key, direction="output")

    calls = 0
    original = tracker._set_artifact_recovery_roots_bulk

    def count_bulk(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(tracker, "_set_artifact_recovery_roots_bulk", count_bulk)
    report = tracker.archive_run_output_files(
        "producer_archive_bulk_once", tmp_path / "archive_bulk_once"
    )

    assert calls == 1
    assert report.complete is True


def test_archive_run_output_files_empty_selection_is_complete_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    called = False

    def unexpected_registration(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("empty archive should not register outputs")

    monkeypatch.setattr(
        tracker, "register_run_output_recovery_copies", unexpected_registration
    )
    archive_root = tmp_path / "empty_archive"

    report = tracker.archive_run_output_files("empty_run", archive_root, keys=[])

    assert report.complete is True
    assert report.summary == "copied=0 verified=0 metadata_committed=0"
    assert called is False
    assert not archive_root.exists()


def test_archive_run_output_files_keeps_verified_status_for_structured_metadata_failure(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "structured_failure.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n13\n", encoding="utf-8")
    with tracker.start_run("producer_archive_structured_failure", model="producer"):
        artifact = tracker.log_artifact(
            output_path, key="structured_failure", direction="output"
        )

    registration = consist_materialize.ArtifactRecoveryCopyRegistration(
        artifact=artifact,
        key="structured_failure",
        artifact_id=str(artifact.id),
        recovery_root=tmp_path / "archive_structured_failure",
        expected_path=tmp_path
        / "archive_structured_failure"
        / "outputs"
        / "structured_failure.csv",
        status="failed",
        message="different persistence error wording",
        verification_succeeded=True,
    )
    monkeypatch.setattr(
        tracker,
        "register_run_output_recovery_copies",
        lambda *args, **kwargs: consist_materialize.RunOutputRecoveryCopiesRegistration(
            outputs={"structured_failure": registration}
        ),
    )

    report = tracker.archive_run_output_files(
        "producer_archive_structured_failure", tmp_path / "archive_structured_failure"
    )

    assert report["structured_failure"].verification_status == "verified"
    assert report["structured_failure"].metadata_committed is False


def test_archive_run_outputs_returns_archived_outputs_type(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "typed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")
    archive_root = tmp_path / "archive_typed"

    with tracker.start_run("producer_typed", model="producer"):
        tracker.log_artifact(output_path, key="typed", direction="output")

    result = tracker.archive_run_outputs("producer_typed", archive_root)

    assert isinstance(result, ArchivedOutputs)
    assert isinstance(result, Mapping)


def test_archive_run_outputs_outputs_attr_has_refreshed_recovery_roots(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "refreshed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_refreshed"

    with tracker.start_run("producer_refreshed", model="producer"):
        tracker.log_artifact(output_path, key="refreshed", direction="output")

    result = tracker.archive_run_outputs("producer_refreshed", archive_root)

    refreshed = result.outputs["refreshed"]
    assert isinstance(refreshed, Artifact)
    assert str(archive_root.resolve()) in refreshed.recovery_roots


def test_archive_run_outputs_outputs_attr_without_extra_get_run_outputs_call(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    """Footgun regression: archive should hand back refreshed artifacts directly.

    Before ArchivedOutputs, callers had to call get_run_outputs() again after
    archive_run_outputs() to get artifacts with recovery_roots set.  Verify the
    returned .outputs already reflects the new archive root.
    """
    output_path = tracker.run_dir / "outputs" / "stale.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n3\n", encoding="utf-8")
    archive_root = tmp_path / "archive_stale"

    with tracker.start_run("producer_stale", model="producer"):
        tracker.log_artifact(output_path, key="stale", direction="output")

    result = tracker.archive_run_outputs("producer_stale", archive_root)

    # .outputs must already carry recovery_roots — no second call needed
    assert str(archive_root.resolve()) in result.outputs["stale"].recovery_roots


def test_archive_run_outputs_outputs_attr_respects_keys_filter(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "fa.csv"
    b_path = tracker.run_dir / "outputs" / "fb.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    archive_root = tmp_path / "archive_filter"

    with tracker.start_run("producer_filter", model="producer"):
        tracker.log_artifact(a_path, key="fa", direction="output")
        tracker.log_artifact(b_path, key="fb", direction="output")

    result = tracker.archive_run_outputs("producer_filter", archive_root, keys=["fa"])

    assert set(result.keys()) == {"fa"}
    assert set(result.outputs.keys()) == {"fa"}
    assert "fb" not in result
    assert "fb" not in result.outputs


def test_archive_run_outputs_backward_compatible_dict_access(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "compat.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n4\n", encoding="utf-8")
    archive_root = tmp_path / "archive_compat"

    with tracker.start_run("producer_compat", model="producer"):
        tracker.log_artifact(output_path, key="compat", direction="output")

    result = tracker.archive_run_outputs("producer_compat", archive_root)

    expected_path = (archive_root / "outputs" / "compat.csv").resolve()
    assert result["compat"] == expected_path
    assert result == {"compat": expected_path}


def test_archive_run_outputs_outputs_attr_can_feed_downstream_run(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "handoff.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n7\n", encoding="utf-8")
    archive_root = tmp_path / "archive_handoff"

    with tracker.start_run("producer_handoff", model="producer"):
        tracker.log_artifact(output_path, key="handoff", direction="output")

    archive = tracker.archive_run_outputs("producer_handoff", archive_root)

    def consume_handoff(handoff: Path) -> dict[str, Path]:
        out = consist.output_path("handoff_copy", ext="csv")
        out.write_text(handoff.read_text(encoding="utf-8"), encoding="utf-8")
        return {"handoff_copy": out}

    result = tracker.run(
        name="consume_handoff",
        model="consumer",
        fn=consume_handoff,
        inputs={"handoff": archive.outputs["handoff"]},
        outputs=["handoff_copy"],
        execution_options=consist.ExecutionOptions(input_binding="paths"),
    )

    assert result.outputs["handoff_copy"].path.read_text(encoding="utf-8") == (
        "value\n7\n"
    )


def test_archive_current_run_outputs_returns_archived_outputs_type(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "current_typed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n5\n", encoding="utf-8")
    archive_root = tmp_path / "archive_current_typed"

    with tracker.start_run("producer_current_typed", model="producer"):
        tracker.log_artifact(output_path, key="current_typed", direction="output")
        result = tracker.archive_current_run_outputs(archive_root)

    assert isinstance(result, ArchivedOutputs)
    assert str(archive_root.resolve()) in result.outputs["current_typed"].recovery_roots


def test_api_archive_run_outputs_returns_archived_outputs_type(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    output_path = tracker.run_dir / "outputs" / "api_typed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n6\n", encoding="utf-8")
    archive_root = tmp_path / "archive_api_typed"

    with tracker.start_run("producer_api_typed", model="producer"):
        tracker.log_artifact(output_path, key="api_typed", direction="output")

    result = archive_run_outputs_api(
        "producer_api_typed", archive_root, tracker=tracker
    )

    assert isinstance(result, ArchivedOutputs)
    assert str(archive_root.resolve()) in result.outputs["api_typed"].recovery_roots


def test_consist_exports_archived_outputs_type() -> None:
    assert consist.ArchivedOutputs is ArchivedOutputs


def test_register_run_output_recovery_copies_reports_mixed_statuses_and_unknown_keys(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "bulk_a.csv"
    b_path = tracker.run_dir / "outputs" / "bulk_b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_bulk"

    with tracker.start_run("producer_adopt_bulk", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    expected_a = recovery_root / "outputs" / "bulk_a.csv"
    expected_a.parent.mkdir(parents=True, exist_ok=True)
    expected_a.write_text("value\n1\n", encoding="utf-8")

    result = register_run_output_recovery_copies_api(
        "producer_adopt_bulk",
        recovery_root,
        keys=["a", "b"],
        tracker=tracker,
    )

    assert result["a"].status == "registered"
    assert result["b"].status == "missing_copy"
    assert result.complete is False
    assert result.summary == (
        "registered=1 missing_copy=1 hash_mismatch=0 skipped_unmapped=0 "
        "blocked_by_container_policy=0 symlink_destination=0 "
        "unsupported_directory=0 unverifiable_hash=0 failed=0"
    )
    outputs = tracker.get_run_outputs("producer_adopt_bulk")
    assert outputs["a"].recovery_roots == [str(recovery_root.resolve())]
    assert "recovery_roots" not in outputs["b"].meta

    with pytest.raises(KeyError, match="Requested output keys were not found"):
        tracker.register_run_output_recovery_copies(
            "producer_adopt_bulk",
            recovery_root,
            keys=["missing"],
        )


def test_register_run_output_recovery_copies_bulk_updates_successes(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "bulk_update_a.csv"
    b_path = tracker.run_dir / "outputs" / "bulk_update_b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_bulk_update"

    with tracker.start_run("producer_adopt_bulk_update", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    expected_a = recovery_root / "outputs" / "bulk_update_a.csv"
    expected_b = recovery_root / "outputs" / "bulk_update_b.csv"
    expected_a.parent.mkdir(parents=True, exist_ok=True)
    expected_a.write_text("value\n1\n", encoding="utf-8")
    expected_b.write_text("value\n2\n", encoding="utf-8")

    def fail_per_artifact_update(*args, **kwargs):
        raise AssertionError("per-artifact update should not be used")

    monkeypatch.setattr(
        tracker,
        "set_artifact_recovery_roots",
        fail_per_artifact_update,
    )

    result = tracker.register_run_output_recovery_copies(
        "producer_adopt_bulk_update",
        recovery_root,
        verify=False,
    )

    assert result["a"].status == "registered"
    assert result["a"].metadata_updated is True
    assert result["b"].status == "registered"
    assert result["b"].metadata_updated is True
    outputs = tracker.get_run_outputs("producer_adopt_bulk_update")
    assert outputs["a"].recovery_roots == [str(recovery_root.resolve())]
    assert outputs["b"].recovery_roots == [str(recovery_root.resolve())]


def test_register_run_output_recovery_copies_falls_back_after_bulk_update_failure(
    monkeypatch: pytest.MonkeyPatch,
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "bulk_fallback_a.csv"
    b_path = tracker.run_dir / "outputs" / "bulk_fallback_b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_bulk_fallback"

    with tracker.start_run("producer_adopt_bulk_fallback", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    expected_a = recovery_root / "outputs" / "bulk_fallback_a.csv"
    expected_b = recovery_root / "outputs" / "bulk_fallback_b.csv"
    expected_a.parent.mkdir(parents=True, exist_ok=True)
    expected_a.write_text("value\n1\n", encoding="utf-8")
    expected_b.write_text("value\n2\n", encoding="utf-8")

    def fail_bulk_update(*args, **kwargs):
        raise RuntimeError("bulk update failed")

    monkeypatch.setattr(
        tracker,
        "_set_artifact_recovery_roots_bulk",
        fail_bulk_update,
    )

    result = tracker.register_run_output_recovery_copies(
        "producer_adopt_bulk_fallback",
        recovery_root,
        verify=False,
    )

    assert result["a"].status == "registered"
    assert result["a"].metadata_updated is True
    assert result["b"].status == "registered"
    assert result["b"].metadata_updated is True
    outputs = tracker.get_run_outputs("producer_adopt_bulk_fallback")
    assert outputs["a"].recovery_roots == [str(recovery_root.resolve())]
    assert outputs["b"].recovery_roots == [str(recovery_root.resolve())]


def test_register_run_output_recovery_copies_rejects_hashes_outside_selected_keys(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    a_path = tracker.run_dir / "outputs" / "selected_a.csv"
    b_path = tracker.run_dir / "outputs" / "selected_b.csv"
    a_path.parent.mkdir(parents=True, exist_ok=True)
    a_path.write_text("value\n1\n", encoding="utf-8")
    b_path.write_text("value\n2\n", encoding="utf-8")
    recovery_root = tmp_path / "external_archive_selected"

    with tracker.start_run("producer_adopt_selected", model="producer"):
        tracker.log_artifact(a_path, key="a", direction="output")
        tracker.log_artifact(b_path, key="b", direction="output")

    with pytest.raises(KeyError, match="content_hashes contained keys"):
        tracker.register_run_output_recovery_copies(
            "producer_adopt_selected",
            recovery_root,
            keys=["a"],
            content_hashes={"b": "unused"},
        )


def test_api_register_artifact_recovery_copy_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = Artifact(
        id=uuid.uuid4(),
        key="x",
        container_uri="./x.csv",
        driver="csv",
        meta={},
    )
    calls: dict[str, object] = {}
    expected = object()

    class _FakeTracker:
        def register_artifact_recovery_copy(
            self, artifact_arg, recovery_root, **kwargs
        ):
            calls["artifact"] = artifact_arg
            calls["recovery_root"] = recovery_root
            calls["kwargs"] = kwargs
            return expected

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = register_artifact_recovery_copy_api(
        artifact,
        "/tmp/recovery",
        verify=False,
        content_hash="abc",
        append=False,
    )

    assert result is expected
    assert calls == {
        "artifact": artifact,
        "recovery_root": "/tmp/recovery",
        "kwargs": {
            "verify": False,
            "content_hash": "abc",
            "append": False,
        },
    }
    assert (
        consist.register_artifact_recovery_copy is register_artifact_recovery_copy_api
    )


def test_api_register_run_output_recovery_copies_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}
    expected = object()

    class _FakeTracker:
        def register_run_output_recovery_copies(self, run_id, recovery_root, **kwargs):
            calls["run_id"] = run_id
            calls["recovery_root"] = recovery_root
            calls["kwargs"] = kwargs
            return expected

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = register_run_output_recovery_copies_api(
        "run_1",
        "/tmp/recovery",
        keys=("a",),
        verify=False,
        append=False,
        content_hashes={"a": "abc"},
    )

    assert result is expected
    assert calls == {
        "run_id": "run_1",
        "recovery_root": "/tmp/recovery",
        "kwargs": {
            "keys": ("a",),
            "verify": False,
            "append": False,
            "content_hashes": {"a": "abc"},
        },
    }
    assert (
        consist.register_run_output_recovery_copies
        is register_run_output_recovery_copies_api
    )


def test_api_archive_current_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def archive_current_run_outputs(self, archive_root, **kwargs):
            calls["archive_root"] = archive_root
            calls["kwargs"] = kwargs
            return {"x": Path("/tmp/archive/x.csv")}

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = archive_current_run_outputs_api(
        "/tmp/archive",
        keys=("x", "y"),
        mode="move",
        append=False,
    )

    assert result == {"x": Path("/tmp/archive/x.csv")}
    assert calls == {
        "archive_root": "/tmp/archive",
        "kwargs": {
            "keys": ("x", "y"),
            "mode": "move",
            "append": False,
        },
    }
    assert consist.archive_current_run_outputs is archive_current_run_outputs_api


def test_tracker_hydrate_run_outputs_allows_external_target_root_when_configured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        allow_external_paths=True,
    )
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(**kwargs):
        calls["allowed_base"] = kwargs["allowed_base"]
        return _hydrated_result({})

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    outside = tmp_path / "external"
    tracker.hydrate_run_outputs("run_1", target_root=outside)

    assert calls["allowed_base"] is None

    if tracker.engine:
        tracker.engine.dispose()


def test_run_materialize_outputs_delegates_to_tracker() -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "result"

    result = run.materialize_outputs(
        _FakeTracker(),
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("a",),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("a",),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }


def test_run_hydrate_outputs_delegates_to_tracker() -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated"

    result = run.hydrate_outputs(
        _FakeTracker(),
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("a",),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "hydrated"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("a",),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }


def test_api_materialize_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = materialize_run_outputs_api(
        "run_9",
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("x", "y"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "api-result"
    assert calls == {
        "run_id": "run_9",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("x", "y"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert consist.materialize_run_outputs is materialize_run_outputs_api
    assert consist.MaterializationResult is consist_materialize.MaterializationResult


def test_api_hydrate_run_outputs_delegates_with_default_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    result = hydrate_run_outputs_api(
        "run_9",
        target_root="/tmp/restored",
        source_root="/tmp/archive",
        keys=("x", "y"),
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_9",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": "/tmp/archive",
            "keys": ("x", "y"),
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
    assert consist.hydrate_run_outputs is hydrate_run_outputs_api


def test_tracker_hydrate_run_outputs_allows_configured_mount_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_root = tmp_path / "workspace"
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        mounts={"workspace": str(workspace_root)},
    )
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    def _fake_hydrate_core(**kwargs):
        calls["allowed_base"] = kwargs["allowed_base"]
        return _hydrated_result({})

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_core",
        _fake_hydrate_core,
    )

    tracker.hydrate_run_outputs(
        "run_1",
        target_root=workspace_root / "restored",
    )

    assert calls["allowed_base"] == (
        tracker.run_dir.resolve(),
        workspace_root.resolve(),
    )


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_tracker_materialize_run_outputs_rejects_scalar_string_keys(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)

    with pytest.raises(TypeError, match="keys must be a sequence"):
        tracker.materialize_run_outputs(
            "run_1", target_root=tracker.run_dir, keys=bad_keys
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_tracker_materialize_run_outputs_rejects_invalid_runtime_options(
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    monkeypatch.setattr(tracker, "get_run", lambda _run_id: run)
    kwargs = {"target_root": tracker.run_dir, field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        tracker.materialize_run_outputs("run_1", **kwargs)


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_run_materialize_outputs_rejects_scalar_string_keys_before_delegation(
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    with pytest.raises(TypeError, match="keys must be a sequence"):
        run.materialize_outputs(
            _FakeTracker(),
            target_root="/tmp/restored",
            keys=bad_keys,
        )


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_run_hydrate_outputs_rejects_scalar_string_keys_before_delegation(
    bad_keys: str | bytes,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid keys")

    with pytest.raises(TypeError, match="keys must be a sequence"):
        run.hydrate_outputs(
            _FakeTracker(),
            target_root="/tmp/restored",
            keys=bad_keys,
        )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_run_materialize_outputs_rejects_invalid_runtime_options_before_delegation(
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        run.materialize_outputs(_FakeTracker(), **kwargs)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_run_hydrate_outputs_rejects_invalid_runtime_options_before_delegation(
    field_name: str,
    field_value: str,
) -> None:
    run = Run(
        id="run_1",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            raise AssertionError("delegation should not happen for invalid options")

    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    with pytest.raises(ValueError, match=field_name):
        run.hydrate_outputs(_FakeTracker(), **kwargs)


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_api_materialize_run_outputs_passes_scalar_string_keys_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = materialize_run_outputs_api(
        "run_1",
        target_root="/tmp/restored",
        keys=bad_keys,
    )

    assert result == "api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": bad_keys,
            "preserve_existing": True,
            "on_missing": "warn",
            "db_fallback": "if_ingested",
        },
    }


@pytest.mark.parametrize("bad_keys", ["out", b"out"])
def test_api_hydrate_run_outputs_passes_scalar_string_keys_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    bad_keys: str | bytes,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )

    result = hydrate_run_outputs_api(
        "run_1",
        target_root="/tmp/restored",
        keys=bad_keys,
    )

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": bad_keys,
            "preserve_existing": True,
            "on_missing": "warn",
            "db_fallback": "if_ingested",
        },
    }


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_materialize_run_outputs_passes_invalid_runtime_options_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def materialize_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    result = materialize_run_outputs_api("run_1", **kwargs)

    assert result == "api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": None,
            "preserve_existing": True,
            "on_missing": "warn" if field_name != "on_missing" else field_value,
            "db_fallback": (
                "if_ingested" if field_name != "db_fallback" else field_value
            ),
        },
    }


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("on_missing", "ignore"),
        ("db_fallback", "always"),
    ],
)
def test_api_hydrate_run_outputs_passes_invalid_runtime_options_to_tracker(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
    field_value: str,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-api-result"

    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: _FakeTracker()
    )
    kwargs = {"target_root": "/tmp/restored", field_name: field_value}

    result = hydrate_run_outputs_api("run_1", **kwargs)

    assert result == "hydrated-api-result"
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "target_root": "/tmp/restored",
            "source_root": None,
            "keys": None,
            "preserve_existing": True,
            "on_missing": "warn" if field_name != "on_missing" else field_value,
            "db_fallback": (
                "if_ingested" if field_name != "db_fallback" else field_value
            ),
        },
    }


def test_api_hydrate_run_outputs_to_destinations_delegates_and_is_exported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class _FakeTracker:
        def hydrate_run_outputs_to_destinations(self, run_id: str, **kwargs):
            calls["run_id"] = run_id
            calls["kwargs"] = kwargs
            return "hydrated-destinations-result"

    fake_tracker = _FakeTracker()
    monkeypatch.setattr(
        "consist.api._resolve_tracker", lambda tracker=None: fake_tracker
    )

    destinations = {
        "persons": "/tmp/staged/persons.csv",
        "skims": "/tmp/staged/skims",
    }
    result = hydrate_run_outputs_to_destinations_api(
        "run_1",
        destinations_by_key=destinations,
        source_root="/tmp/archive",
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    assert result == "hydrated-destinations-result"
    assert (
        consist.hydrate_run_outputs_to_destinations
        is hydrate_run_outputs_to_destinations_api
    )
    assert calls == {
        "run_id": "run_1",
        "kwargs": {
            "destinations_by_key": destinations,
            "source_root": "/tmp/archive",
            "preserve_existing": False,
            "on_missing": "raise",
            "db_fallback": "never",
        },
    }
