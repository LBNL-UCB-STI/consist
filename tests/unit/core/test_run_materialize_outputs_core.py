from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import uuid

import pandas as pd
import pytest

from consist.core.fs import FileSystemManager
from consist.core.materialize import (
    PlannedMaterialization,
    build_run_output_materialize_plan,
    hydrate_run_outputs,
    materialize_planned_outputs,
)
from consist.models.artifact import Artifact
from consist.models.run import Run


class StubDb:
    def __init__(
        self, *, outputs_by_run: dict[str, list[Artifact]], runs: dict[str, Run]
    ):
        self.outputs_by_run = outputs_by_run
        self.runs = runs

    def get_output_artifacts_for_run(self, run_id: str) -> list[Artifact]:
        return list(self.outputs_by_run.get(run_id, []))

    def get_run(self, run_id: str) -> Run | None:
        return self.runs.get(run_id)


def _run(run_id: str, *, run_dir: Path, mounts: dict[str, str] | None = None) -> Run:
    return Run(
        id=run_id,
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={
            "_physical_run_dir": str(run_dir),
            "mounts": dict(mounts or {}),
        },
    )


def _artifact(
    key: str,
    uri: str,
    *,
    run_id: str | None = None,
    driver: str = "csv",
    meta: dict | None = None,
) -> Artifact:
    return Artifact(
        id=uuid.uuid4(),
        key=key,
        container_uri=uri,
        driver=driver,
        run_id=run_id,
        meta=dict(meta or {}),
    )


def _stub_tracker(
    *,
    run_dir: Path,
    outputs_by_run: dict[str, list[Artifact]],
    runs: dict[str, Run],
    mounts: dict[str, str] | None = None,
):
    return SimpleNamespace(
        db=StubDb(outputs_by_run=outputs_by_run, runs=runs),
        fs=FileSystemManager(run_dir, mounts),
        run_dir=run_dir,
        allow_external_paths=True,
        engine=None,
    )


def test_build_plan_detects_duplicate_output_keys(tmp_path: Path) -> None:
    run = _run("consumer", run_dir=tmp_path / "consumer")
    outputs = [
        _artifact("shared", "./outputs/a.csv", run_id="consumer"),
        _artifact("shared", "./outputs/b.csv", run_id="consumer"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": run},
    )

    with pytest.raises(ValueError, match="duplicate output keys"):
        build_run_output_materialize_plan(
            tracker,
            run,
            target_root=tmp_path / "restore",
            source_root=None,
            keys=None,
            preserve_existing=True,
            db_fallback="if_ingested",
        )


def test_build_plan_validates_requested_keys_strictly(tmp_path: Path) -> None:
    run = _run("consumer", run_dir=tmp_path / "consumer")
    outputs = [_artifact("present", "./outputs/a.csv", run_id="consumer")]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": run},
    )

    with pytest.raises(KeyError, match="missing"):
        build_run_output_materialize_plan(
            tracker,
            run,
            target_root=tmp_path / "restore",
            source_root=None,
            keys=["present", "missing"],
            preserve_existing=True,
            db_fallback="if_ingested",
        )


def test_build_plan_marks_absolute_and_file_uris_unmapped(tmp_path: Path) -> None:
    run = _run("consumer", run_dir=tmp_path / "consumer")
    outputs = [
        _artifact("absolute", "/tmp/data.csv", run_id="consumer"),
        _artifact("file_uri", "file:///tmp/data.csv", run_id="consumer"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": run},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        run,
        target_root=tmp_path / "restore",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert sorted(result.skipped_unmapped) == ["absolute", "file_uri"]


def test_build_plan_marks_missing_historical_metadata_unmapped(tmp_path: Path) -> None:
    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    missing_workspace_metadata = Run(
        id="producer_workspace",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    missing_mount_metadata = Run(
        id="producer_mount",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={"_physical_run_dir": str(tmp_path / "producer_mount")},
    )
    outputs = [
        _artifact("workspace", "./outputs/a.csv", run_id="producer_workspace"),
        _artifact("mounted", "outputs://beam/skims.csv", run_id="producer_mount"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={
            "consumer": selected_run,
            "producer_workspace": missing_workspace_metadata,
            "producer_mount": missing_mount_metadata,
        },
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restore",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert sorted(result.skipped_unmapped) == ["mounted", "workspace"]


def test_build_plan_dedupes_shared_container_outputs(tmp_path: Path) -> None:
    producer_dir = tmp_path / "producer"
    source_path = producer_dir / "outputs" / "shared.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("value\n1\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = _run("producer", run_dir=producer_dir)
    outputs = [
        _artifact("a", "./outputs/shared.csv", run_id="producer"),
        _artifact("b", "./outputs/shared.csv", run_id="producer"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": selected_run, "producer": producing_run},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restore",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert result.failed == []
    assert len(plan) == 1
    assert plan[0].keys == ("a", "b")
    assert (
        plan[0].destination
        == (tmp_path / "restore" / "outputs" / "shared.csv").resolve()
    )


def test_build_plan_reports_destination_collisions(tmp_path: Path) -> None:
    producer_a_dir = tmp_path / "producer_a"
    producer_b_dir = tmp_path / "producer_b"
    producer_a_dir.mkdir(parents=True, exist_ok=True)
    producer_b_dir.mkdir(parents=True, exist_ok=True)
    (producer_a_dir / "same.csv").write_text("a\n1\n", encoding="utf-8")
    (producer_b_dir / "same.csv").write_text("a\n2\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    run_a = _run("producer_a", run_dir=producer_a_dir)
    run_b = _run("producer_b", run_dir=producer_b_dir)
    outputs = [
        _artifact("left", "./same.csv", run_id="producer_a"),
        _artifact("right", "workspace://same.csv", run_id="producer_b"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={
            "consumer": selected_run,
            "producer_a": run_a,
            "producer_b": run_b,
        },
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restore",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert sorted(result.failed) == [
        (
            "left",
            f"destination collision at {(tmp_path / 'restore' / 'same.csv').resolve()}",
        ),
        (
            "right",
            f"destination collision at {(tmp_path / 'restore' / 'same.csv').resolve()}",
        ),
    ]


def test_build_plan_uses_mount_snapshot_and_source_root_override(
    tmp_path: Path,
) -> None:
    historical_mount = tmp_path / "historical_outputs"
    archive_root = tmp_path / "archive_outputs"
    current_mount = tmp_path / "current_outputs"
    archive_file = archive_root / "beam" / "skims.csv"
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    archive_file.write_text("value\n3\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = _run(
        "producer",
        run_dir=tmp_path / "producer",
        mounts={"outputs": str(historical_mount)},
    )
    output = _artifact("skims", "outputs://beam/skims.csv", run_id="producer")
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": [output]},
        runs={"consumer": selected_run, "producer": producing_run},
        mounts={"outputs": str(current_mount)},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restore",
        source_root=archive_root,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert result.failed == []
    assert len(plan) == 1
    assert plan[0].source_path == archive_file.resolve()
    assert plan[0].relative_path == Path("beam") / "skims.csv"


def test_build_plan_rejects_existing_symlink_even_with_preserve_existing(
    tmp_path: Path,
) -> None:
    producer_dir = tmp_path / "producer"
    source_path = producer_dir / "outputs" / "result.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("value\n1\n", encoding="utf-8")

    restore_root = tmp_path / "restore"
    symlink_target = tmp_path / "real.csv"
    symlink_target.write_text("value\n2\n", encoding="utf-8")
    symlink_path = restore_root / "outputs" / "result.csv"
    symlink_path.parent.mkdir(parents=True, exist_ok=True)
    symlink_path.symlink_to(symlink_target)

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = _run("producer", run_dir=producer_dir)
    outputs = [_artifact("result", "./outputs/result.csv", run_id="producer")]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": selected_run, "producer": producing_run},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=restore_root,
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert result.skipped_existing == []
    assert result.failed == [
        ("result", f"Symlink detected in destination path: {symlink_path}")
    ]


def test_build_plan_falls_back_to_artifact_mount_root(tmp_path: Path) -> None:
    mount_root = tmp_path / "archived_mount"
    source_path = mount_root / "reports" / "summary.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("value\n5\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = Run(
        id="producer",
        model_name="model",
        config_hash=None,
        git_hash=None,
        meta={"_physical_run_dir": str(tmp_path / "producer")},
    )
    output = _artifact(
        "summary",
        "outputs://reports/summary.csv",
        run_id="producer",
        meta={"mount_root": str(mount_root)},
    )
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": [output]},
        runs={"consumer": selected_run, "producer": producing_run},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restore",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert result.failed == []
    assert len(plan) == 1
    assert plan[0].source_path == source_path.resolve()


def test_materialize_planned_outputs_overwrites_existing_files(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n9\n", encoding="utf-8")
    destination = tmp_path / "restore" / "source.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("value\n0\n", encoding="utf-8")

    artifact = _artifact("out", "./source.csv")
    plan = [
        PlannedMaterialization(
            artifact=artifact,
            keys=("out",),
            source_kind="filesystem",
            source_path=source,
            destination=destination,
            relative_path=Path("source.csv"),
        )
    ]

    tracker = SimpleNamespace(engine=None)
    result = materialize_planned_outputs(
        plan,
        tracker=tracker,
        allowed_base=tmp_path,
        on_missing="raise",
        preserve_existing=False,
    )

    assert result.materialized_from_filesystem["out"] == str(destination.resolve())
    assert destination.read_text(encoding="utf-8") == "value\n9\n"


def test_build_plan_reports_structural_collision_even_when_destination_exists(
    tmp_path: Path,
) -> None:
    producer_a_dir = tmp_path / "producer_a"
    producer_b_dir = tmp_path / "producer_b"
    producer_a_dir.mkdir(parents=True, exist_ok=True)
    producer_b_dir.mkdir(parents=True, exist_ok=True)
    (producer_a_dir / "same.csv").write_text("a\n1\n", encoding="utf-8")
    (producer_b_dir / "same.csv").write_text("a\n2\n", encoding="utf-8")

    restore_root = tmp_path / "restore"
    existing_destination = restore_root / "same.csv"
    existing_destination.parent.mkdir(parents=True, exist_ok=True)
    existing_destination.write_text("a\n0\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    run_a = _run("producer_a", run_dir=producer_a_dir)
    run_b = _run("producer_b", run_dir=producer_b_dir)
    outputs = [
        _artifact("left", "./same.csv", run_id="producer_a"),
        _artifact("right", "workspace://same.csv", run_id="producer_b"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={
            "consumer": selected_run,
            "producer_a": run_a,
            "producer_b": run_b,
        },
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=restore_root,
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert result.skipped_existing == []
    assert sorted(result.failed) == [
        ("left", f"destination collision at {existing_destination}"),
        ("right", f"destination collision at {existing_destination}"),
    ]


def test_materialize_planned_outputs_enforces_allowed_base_in_warn_mode(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    artifact = _artifact("outside", "./source.csv")
    plan = [
        PlannedMaterialization(
            artifact=artifact,
            keys=("outside",),
            source_kind="filesystem",
            source_path=source,
            destination=tmp_path / "outside.csv",
            relative_path=Path("outside.csv"),
        )
    ]

    tracker = SimpleNamespace(engine=None)
    result = materialize_planned_outputs(
        plan,
        tracker=tracker,
        allowed_base=tmp_path / "sandbox",
        on_missing="warn",
        preserve_existing=True,
    )

    assert result.failed
    assert result.failed[0][0] == "outside"


def test_materialize_planned_outputs_captures_copy_failures_in_warn_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n1\n", encoding="utf-8")
    destination = tmp_path / "restore" / "source.csv"
    artifact = _artifact("copy_failure", "./source.csv")
    plan = [
        PlannedMaterialization(
            artifact=artifact,
            keys=("copy_failure",),
            source_kind="filesystem",
            source_path=source,
            destination=destination,
            relative_path=Path("source.csv"),
        )
    ]

    def _raise_copy_error(*_args, **_kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr("consist.core.materialize.shutil.copy2", _raise_copy_error)

    result = materialize_planned_outputs(
        plan,
        tracker=SimpleNamespace(engine=None),
        allowed_base=tmp_path,
        on_missing="warn",
        preserve_existing=True,
    )

    assert result.materialized == {}
    assert result.failed == [("copy_failure", "denied")]
    assert not destination.exists()


def test_materialize_run_outputs_db_fallback_restores_ingested_csv(
    tracker, run_dir: Path
) -> None:
    output_path = run_dir / "outputs" / "table.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"value": [1, 2]}).to_csv(output_path, index=False)

    with tracker.start_run("producer", model="producer", cache_mode="overwrite"):
        artifact = tracker.log_artifact(output_path, key="table", direction="output")

    tracker.ingest(artifact)
    output_path.unlink()

    result = tracker.materialize_run_outputs(
        "producer",
        target_root=run_dir / "restored",
        db_fallback="if_ingested",
    )

    restored_path = run_dir / "restored" / "outputs" / "table.csv"
    assert result.materialized_from_db == {"table": str(restored_path.resolve())}
    assert restored_path.exists()
    restored = pd.read_csv(restored_path)
    assert restored["value"].tolist() == [1, 2]


def test_materialize_run_outputs_allows_current_mount_root_as_target(
    tracker, run_dir: Path
) -> None:
    workspace_root = run_dir.parent / "workspace_mount"
    tracker.fs.mounts = {"workspace": str(workspace_root.resolve())}
    tracker.mounts = tracker.fs.mounts

    output_path = run_dir / "outputs" / "table.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"value": [1]}).to_csv(output_path, index=False)

    with tracker.start_run(
        "producer_mount_target", model="producer", cache_mode="overwrite"
    ):
        tracker.log_artifact(output_path, key="table", direction="output")

    result = tracker.materialize_run_outputs(
        "producer_mount_target",
        target_root=workspace_root,
    )

    restored_path = workspace_root / "outputs" / "table.csv"
    assert result.materialized_from_filesystem == {
        "table": str(restored_path.resolve())
    }
    assert restored_path.exists()


def test_materialize_run_outputs_db_fallback_empty_export_is_missing_source(
    tracker, run_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = run_dir / "outputs" / "empty.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"value": [1]}).to_csv(output_path, index=False)

    with tracker.start_run("producer_empty", model="producer", cache_mode="overwrite"):
        artifact = tracker.log_artifact(output_path, key="empty", direction="output")

    tracker.ingest(artifact)
    output_path.unlink()

    monkeypatch.setattr(
        "consist.core.materialize.pd.read_sql",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    result = tracker.materialize_run_outputs(
        "producer_empty",
        target_root=run_dir / "restored_empty",
        db_fallback="if_ingested",
        on_missing="warn",
    )

    restored_path = run_dir / "restored_empty" / "outputs" / "empty.csv"
    assert result.materialized == {}
    assert result.skipped_missing_source == ["empty"]
    assert not restored_path.exists()


def test_build_plan_reports_unsupported_ingested_driver_clearly(
    tmp_path: Path,
) -> None:
    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = _run("producer", run_dir=tmp_path / "producer")
    output = _artifact(
        "table_json",
        "./outputs/table.json",
        run_id="producer",
        driver="json",
        meta={"is_ingested": True},
    )
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": [output]},
        runs={"consumer": selected_run, "producer": producing_run},
    )

    plan, result = build_run_output_materialize_plan(
        tracker,
        selected_run,
        target_root=tmp_path / "restored_json",
        source_root=None,
        keys=None,
        preserve_existing=True,
        db_fallback="if_ingested",
    )

    assert plan == []
    assert result.materialized == {}
    assert result.failed == [
        (
            "table_json",
            "unsupported DB export for ingested driver 'json'",
        )
    ]


def test_hydrate_run_outputs_returns_detached_artifact_views(
    tracker, run_dir: Path
) -> None:
    output_path = run_dir / "outputs" / "table.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run(
        "producer_hydrate", model="producer", cache_mode="overwrite"
    ):
        tracker.log_artifact(output_path, key="table", direction="output")

    historical_artifact = tracker.get_run_outputs("producer_hydrate")["table"]

    hydrated = tracker.hydrate_run_outputs(
        "producer_hydrate",
        target_root=run_dir / "restored_hydrate",
        keys=["table"],
    )

    restored_path = (run_dir / "restored_hydrate" / "outputs" / "table.csv").resolve()
    result = hydrated["table"]
    assert result.status == "materialized_from_filesystem"
    assert result.resolvable is True
    assert result.path == restored_path
    assert result.artifact is not historical_artifact
    assert result.artifact.container_uri == historical_artifact.container_uri
    assert result.artifact.as_path() == restored_path
    assert hydrated.paths == {"table": restored_path}
    assert list(hydrated.resolvable) == ["table"]
    assert hydrated.complete is True
    assert historical_artifact.as_path() == output_path


def test_hydrate_run_outputs_warn_mode_returns_mixed_keyed_statuses(
    tmp_path: Path,
) -> None:
    producer_dir = tmp_path / "producer"
    existing_source = producer_dir / "outputs" / "good.csv"
    existing_source.parent.mkdir(parents=True, exist_ok=True)
    existing_source.write_text("value\n1\n", encoding="utf-8")

    selected_run = _run("consumer", run_dir=tmp_path / "consumer")
    producing_run = _run("producer", run_dir=producer_dir)
    outputs = [
        _artifact("good", "./outputs/good.csv", run_id="producer"),
        _artifact("missing", "./outputs/missing.csv", run_id="producer"),
        _artifact("absolute", "/tmp/data.csv", run_id="producer"),
    ]
    tracker = _stub_tracker(
        run_dir=tmp_path / "workspace",
        outputs_by_run={"consumer": outputs},
        runs={"consumer": selected_run, "producer": producing_run},
    )

    result = hydrate_run_outputs(
        tracker,
        selected_run,
        target_root=tmp_path / "restored",
        source_root=None,
        keys=None,
        allowed_base=tmp_path,
        preserve_existing=True,
        on_missing="warn",
        db_fallback="never",
    )

    assert result["good"].status == "materialized_from_filesystem"
    assert result["good"].resolvable is True
    assert result["missing"].status == "missing_source"
    assert result["missing"].resolvable is False
    assert result["absolute"].status == "skipped_unmapped"
    assert result["absolute"].path is None
    assert result.paths == {
        "good": (tmp_path / "restored" / "outputs" / "good.csv").resolve()
    }
    assert list(result.failed_keys) == []
    assert result.complete is False
