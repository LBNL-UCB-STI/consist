from __future__ import annotations

import json
from pathlib import Path
import uuid

from consist.core.materialize import (
    HydratedRunOutput,
    HydratedRunOutputsResult,
)
from consist.models.artifact import Artifact
from consist.types import (
    ExecutionOptions,
    OutputPolicyOptions,
    OutputSet,
    RunCompletionArchivePolicy,
)


def test_recovery_service_stage_inputs_normalizes_destination_paths(
    monkeypatch,
    tracker,
) -> None:
    service = tracker._recovery_service
    artifact = Artifact(
        id=uuid.uuid4(),
        key="input",
        container_uri="inputs://input.csv",
        driver="csv",
    )
    captured: dict[str, object] = {}

    def fake_stage_inputs(
        tracker_arg,
        inputs_by_key,
        destinations_by_key,
        **kwargs,
    ):
        captured.update(
            tracker=tracker_arg,
            inputs_by_key=inputs_by_key,
            destinations_by_key=destinations_by_key,
            kwargs=kwargs,
        )
        return "sentinel"

    monkeypatch.setattr(
        "consist.core.tracker_recovery.stage_inputs_core",
        fake_stage_inputs,
    )

    result = service.stage_inputs(
        {"input": artifact},
        destinations_by_key={"input": "relative/staged.csv"},
        overwrite=True,
    )

    assert result == "sentinel"
    assert captured["tracker"] is tracker
    assert captured["inputs_by_key"] == {"input": artifact}
    assert captured["destinations_by_key"] == {"input": Path("relative/staged.csv")}
    assert captured["kwargs"]["overwrite"] is True


def test_recovery_service_materialize_run_outputs_folds_tracker_results(
    monkeypatch,
    tracker,
    tmp_path: Path,
) -> None:
    service = tracker._recovery_service
    artifact = Artifact(
        id=uuid.uuid4(),
        key="result",
        container_uri="outputs://result.csv",
        driver="csv",
    )
    restored_path = tmp_path / "restored" / "result.csv"
    hydrated = HydratedRunOutputsResult(
        outputs={
            "result": HydratedRunOutput(
                key="result",
                artifact=artifact,
                path=restored_path,
                status="materialized_from_filesystem",
                resolvable=True,
            )
        }
    )
    captured: dict[str, object] = {}

    def fake_hydrate_run_outputs(run_id: str, **kwargs):
        captured.update(run_id=run_id, kwargs=kwargs)
        return hydrated

    monkeypatch.setattr(tracker, "hydrate_run_outputs", fake_hydrate_run_outputs)

    result = service.materialize_run_outputs(
        "prior-run",
        target_root=tmp_path / "restored",
        keys=["result"],
        preserve_existing=False,
    )

    assert captured["run_id"] == "prior-run"
    assert captured["kwargs"]["keys"] == ["result"]
    assert captured["kwargs"]["preserve_existing"] is False
    assert result.materialized_from_filesystem == {"result": str(restored_path)}


def test_recovery_service_hydrates_run_outputs_to_normalized_destinations(
    monkeypatch,
    tracker,
    tmp_path: Path,
) -> None:
    service = tracker._recovery_service
    run = type("Run", (), {"id": "prior-run"})()
    captured: dict[str, object] = {}

    monkeypatch.setattr(service, "get_run", lambda _run_id: run)

    def fake_hydrate_core(**kwargs):
        captured.update(kwargs)
        return HydratedRunOutputsResult(source_run_id="prior-run")

    monkeypatch.setattr(
        "consist.core.tracker.hydrate_run_outputs_to_destinations_core",
        fake_hydrate_core,
    )

    result = service.hydrate_run_outputs_to_destinations(
        "prior-run",
        destinations_by_key={
            "result": tracker.run_dir / "restored" / "." / "result.csv"
        },
    )

    assert result.source_run_id == "prior-run"
    assert captured["run"] is run
    assert captured["destinations_by_key"] == {
        "result": (tracker.run_dir / "restored" / "result.csv").resolve()
    }


def test_run_completion_archive_policy_archives_outputs_on_cache_miss(
    tracker,
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "archive_store"

    def step(ctx) -> dict[str, Path]:
        out_dir = tracker.run_dir / "outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        scalar_file = out_dir / "summary.csv"
        scalar_file.write_text("k,v\n1,2\n", encoding="utf-8")

        zarr_dir = out_dir / "bundle.zarr"
        zarr_dir.mkdir(parents=True, exist_ok=True)
        (zarr_dir / ".zgroup").write_text('{"zarr_format":2}\n', encoding="utf-8")
        tracker.log_output(
            zarr_dir, key="bundle", artifact_kind="directory", driver="zarr"
        )

        annual_dir = ctx.run_dir / "annual"
        annual_dir.mkdir(parents=True, exist_ok=True)
        (annual_dir / "year_2030.csv").write_text("year\n2030\n", encoding="utf-8")
        (annual_dir / "year_2040.csv").write_text("year\n2040\n", encoding="utf-8")

        return {"summary": scalar_file}

    result = tracker.run(
        fn=step,
        name="archive_miss_step",
        run_id="archive_miss_run",
        outputs=["summary", "bundle"],
        output_sets={"annual": OutputSet(root="annual", include="year_*.csv")},
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(
                archive_root=archive_root,
            )
        ),
    )

    expected_root = str(archive_root.resolve())
    assert result.cache_hit is False
    for key in ("summary", "bundle", "annual"):
        assert result.outputs[key].meta.get("recovery_roots") == [expected_root]

    # Verify archive files exist on disk
    assert (archive_root / "outputs" / "summary.csv").read_text(
        encoding="utf-8"
    ) == "k,v\n1,2\n"
    assert (archive_root / "outputs" / "bundle.zarr" / ".zgroup").exists()
    archived_annual_dir = Path(
        tracker.fs.resolve_historical_path(
            result.outputs["annual"].container_uri, archive_root
        )
    )
    assert (archived_annual_dir / "year_2030.csv").exists()
    assert (archived_annual_dir / "year_2040.csv").exists()

    # Verify tracker.last_run.outputs in-memory sync
    assert tracker.last_run is not None
    last_outputs_by_key = {art.key: art for art in tracker.last_run.outputs}
    for key in ("summary", "bundle", "annual"):
        assert last_outputs_by_key[key].meta.get("recovery_roots") == [expected_root]

    # Verify DB persistence
    db_outputs = tracker.get_artifacts_for_run("archive_miss_run").outputs
    for key in ("summary", "bundle", "annual"):
        assert db_outputs[key].meta.get("recovery_roots") == [expected_root]

    # Verify consist.json snapshot has recovery_roots
    snapshot = json.loads(
        (tracker.run_dir / "consist.json").read_text(encoding="utf-8")
    )
    snapshot_outputs = {art["key"]: art for art in snapshot["outputs"]}
    for key in ("summary", "bundle", "annual"):
        assert snapshot_outputs[key]["meta"].get("recovery_roots") == [expected_root]


def test_run_completion_archive_policy_on_cache_hit_backfills_new_archive_root(
    tracker,
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "backfill_archive"

    def step(ctx) -> dict[str, Path]:
        out_file = tracker.run_dir / "table.csv"
        out_file.write_text("col\n42\n", encoding="utf-8")
        annual_dir = ctx.run_dir / "annual_hit"
        annual_dir.mkdir(parents=True, exist_ok=True)
        (annual_dir / "part_1.csv").write_text("p\n1\n", encoding="utf-8")
        return {"table": out_file}

    first = tracker.run(
        fn=step,
        name="cache_hit_archive_step",
        run_id="cache_hit_archive_run_1",
        outputs=["table"],
        output_sets={"annual": OutputSet(root="annual_hit", include="part_*.csv")},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    assert first.cache_hit is False
    assert "recovery_roots" not in (first.outputs["table"].meta or {})

    second = tracker.run(
        fn=step,
        name="cache_hit_archive_step",
        run_id="cache_hit_archive_run_2",
        outputs=["table"],
        output_sets={"annual": OutputSet(root="annual_hit", include="part_*.csv")},
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(archive_root=archive_root)
        ),
    )
    expected_root = str(archive_root.resolve())
    assert second.cache_hit is True
    assert second.outputs["table"].meta.get("recovery_roots") == [expected_root]
    assert second.outputs["annual"].meta.get("recovery_roots") == [expected_root]
    assert (archive_root / "table.csv").read_text(encoding="utf-8") == "col\n42\n"
    archived_annual_hit = Path(
        tracker.fs.resolve_historical_path(
            second.outputs["annual"].container_uri, archive_root
        )
    )
    assert (archived_annual_hit / "part_1.csv").read_text(encoding="utf-8") == "p\n1\n"


def test_run_completion_archive_policy_skips_virtual_unbacked_artifacts(
    tracker,
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "virtual_skip_archive"

    def step(ctx) -> dict[str, Path]:
        backed = tracker.run_dir / "backed.csv"
        backed.write_text("a\n1\n", encoding="utf-8")
        virtual_missing_path = tracker.run_dir / "virtual_table.parquet"
        tracker.log_artifact(
            virtual_missing_path,
            key="virtual_ingested",
            direction="output",
            driver="parquet",
            content_hash="deadbeef",
            is_ingested=True,
        )
        return {"backed": backed}

    result = tracker.run(
        fn=step,
        name="virtual_skip_step",
        run_id="virtual_skip_run",
        outputs=["backed", "virtual_ingested"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(archive_root=archive_root)
        ),
    )

    expected_root = str(archive_root.resolve())
    assert result.outputs["backed"].meta.get("recovery_roots") == [expected_root]
    assert "recovery_roots" not in (result.outputs["virtual_ingested"].meta or {})
    assert (archive_root / "backed.csv").exists()


def test_run_completion_archive_policy_does_not_perturb_cache_identity(
    tracker,
    tmp_path: Path,
) -> None:
    archive_a = tmp_path / "archive_a"
    archive_b = tmp_path / "archive_b"

    def step(ctx) -> dict[str, Path]:
        out = ctx.run_dir / "id_check.csv"
        out.write_text("v\n1\n", encoding="utf-8")
        return {"out": out}

    run1 = tracker.run(
        fn=step,
        name="identity_step",
        run_id="identity_run_1",
        outputs=["out"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(archive_root=archive_a)
        ),
    )
    run2 = tracker.run(
        fn=step,
        name="identity_step",
        run_id="identity_run_2",
        outputs=["out"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(archive_root=archive_b)
        ),
    )

    assert run1.cache_hit is False
    assert run2.cache_hit is True
    assert run1.run.config_hash == run2.run.config_hash
    assert run1.run.input_hash == run2.run.input_hash
    assert run1.run.git_hash == run2.run.git_hash
    assert run2.outputs["out"].meta.get("recovery_roots") == [
        str(archive_a.resolve()),
        str(archive_b.resolve()),
    ]
