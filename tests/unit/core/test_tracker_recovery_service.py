from __future__ import annotations

import json
from pathlib import Path
import uuid

import pytest

from consist.core.materialize import (
    HydratedRunOutput,
    HydratedRunOutputsResult,
)
from consist.core.tracker import Tracker
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

    # Verify DB persistence (including OutputSet member and manifest artifacts)
    db_outputs = tracker.get_artifacts_for_run("archive_miss_run").outputs
    assert len(db_outputs) > 3
    for key, art in db_outputs.items():
        assert art.meta.get("recovery_roots") == [expected_root], key

    # Verify consist.json snapshot has recovery_roots across all outputs
    snapshot = json.loads(
        (tracker.run_dir / "consist.json").read_text(encoding="utf-8")
    )
    snapshot_outputs = {art["key"]: art for art in snapshot["outputs"]}
    for key, art in snapshot_outputs.items():
        assert art["meta"].get("recovery_roots") == [expected_root], key


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


def test_run_completion_archive_policy_cross_workspace_cache_hit_archives_file_backed_ingested_output(
    tracker,
    tmp_path: Path,
) -> None:
    archive_root = tmp_path / "cross_workspace_archive"
    second_run_dir = tmp_path / "second_workspace"

    def step(ctx) -> dict[str, Path]:
        backed_ingested = tracker.run_dir / "ingested_backed.csv"
        backed_ingested.write_text("col\n99\n", encoding="utf-8")
        tracker.log_artifact(
            backed_ingested,
            key="ingested_backed",
            direction="output",
            is_ingested=True,
        )
        return {}

    first = tracker.run(
        fn=step,
        name="cross_ws_ingested_step",
        run_id="cross_ws_run_1",
        outputs=["ingested_backed"],
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    assert first.cache_hit is False

    second_tracker = Tracker(run_dir=second_run_dir, db_path=tracker.db_path)
    second = second_tracker.run(
        fn=step,
        name="cross_ws_ingested_step",
        run_id="cross_ws_run_2",
        outputs=["ingested_backed"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(archive_root=archive_root)
        ),
    )

    expected_root = str(archive_root.resolve())
    assert second.cache_hit is True
    assert second.outputs["ingested_backed"].meta.get("recovery_roots") == [
        expected_root
    ]
    assert (archive_root / "ingested_backed.csv").read_text(
        encoding="utf-8"
    ) == "col\n99\n"


def test_run_completion_archive_policy_requires_database(tmp_path: Path) -> None:
    db_less_tracker = Tracker(run_dir=tmp_path / "no_db_runs", db_path=None)
    archive_root = tmp_path / "no_db_archive"

    def step(ctx) -> dict[str, Path]:
        out = db_less_tracker.run_dir / "out.csv"
        out.write_text("a\n1\n", encoding="utf-8")
        return {"out": out}

    with pytest.raises(
        RuntimeError,
        match="Cannot archive run outputs: tracker has no database configured",
    ):
        db_less_tracker.run(
            fn=step,
            name="no_db_step",
            run_id="no_db_run",
            outputs=["out"],
            execution_options=ExecutionOptions(inject_context="ctx"),
            output_policy=OutputPolicyOptions(
                run_completion_archive=RunCompletionArchivePolicy(
                    archive_root=archive_root
                )
            ),
        )


def test_run_completion_archive_policy_preserves_abs_path_on_miss_and_hydrated_hit(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    from consist.types import CacheOptions

    archive_miss = tmp_path / "archive_miss"
    archive_hit = tmp_path / "archive_hit"
    out_file = tracker.run_dir / "workspace_out" / "skims.omx"

    def step(ctx) -> dict[str, Path]:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_bytes(b"omx-bytes")
        return {"skims": out_file}

    with tracker.scenario(
        "sc_abs_path_miss",
        step_run_completion_archive=RunCompletionArchivePolicy(
            archive_root=archive_miss,
            mode="copy",
        ),
    ) as sc_miss:
        miss_res = sc_miss.run(
            fn=step,
            name="produce_skims",
            run_id="abs_path_miss_run",
            outputs=["skims"],
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        expected_abs = str(out_file.resolve())
        assert miss_res.cache_hit is False
        assert miss_res.outputs["skims"].abs_path == expected_abs
        assert sc_miss.coupler.require("skims").abs_path == expected_abs
        assert sc_miss.coupler.require("skims").path == out_file.resolve()

    # Remove workspace file so cache hit must hydrate it from archive_miss,
    # then archive to archive_hit via RunCompletionArchivePolicy(mode="copy").
    out_file.unlink()
    assert not out_file.exists()

    with tracker.scenario(
        "sc_abs_path_hit",
        step_run_completion_archive=RunCompletionArchivePolicy(
            archive_root=archive_hit,
            mode="copy",
        ),
    ) as sc_hit:
        hit_res = sc_hit.run(
            fn=step,
            name="produce_skims",
            run_id="abs_path_hit_run",
            outputs=["skims"],
            output_paths={"skims": out_file},
            cache_options=CacheOptions(cache_hydration="outputs-requested"),
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        expected_abs = str(out_file.resolve())
        assert hit_res.cache_hit is True
        assert out_file.exists()
        assert hit_res.outputs["skims"].abs_path == expected_abs
        assert sc_hit.coupler.require("skims").abs_path == expected_abs
        assert sc_hit.coupler.require("skims").path == out_file.resolve()


@pytest.mark.parametrize("scalar_declared_first", [True, False])
def test_archive_run_outputs_handles_nested_scalar_and_child_directory_inside_directory_artifact(
    tracker: Tracker,
    tmp_path: Path,
    scalar_declared_first: bool,
) -> None:
    archive_root = tmp_path / f"archive_nested_{scalar_declared_first}"
    beam_root = tracker.run_dir / "beam_outputs"
    iter_dir = beam_root / "ITERS" / "it.0"
    zarr_dir = beam_root / "skims.zarr"

    def step(ctx) -> None:
        iter_dir.mkdir(parents=True, exist_ok=True)
        zarr_dir.mkdir(parents=True, exist_ok=True)
        events_file = iter_dir / "0.events.csv.gz"
        events_file.write_bytes(b"events-payload")
        linkstats_file = iter_dir / "0.linkstats.csv.gz"
        linkstats_file.write_bytes(b"linkstats-payload")
        (zarr_dir / ".zgroup").write_text('{"zarr_format":2}\n', encoding="utf-8")
        (zarr_dir / "0.0").write_bytes(b"zarr-chunk")

        if scalar_declared_first:
            tracker.log_output(events_file, key="events")
            tracker.log_output(
                zarr_dir, key="skims_zarr", artifact_kind="directory", driver="zarr"
            )
            tracker.log_output(iter_dir, key="beam_iter_dir", artifact_kind="directory")
            tracker.log_output(
                beam_root, key="beam_output_root_dir", artifact_kind="directory"
            )
        else:
            tracker.log_output(
                beam_root, key="beam_output_root_dir", artifact_kind="directory"
            )
            tracker.log_output(iter_dir, key="beam_iter_dir", artifact_kind="directory")
            tracker.log_output(
                zarr_dir, key="skims_zarr", artifact_kind="directory", driver="zarr"
            )
            tracker.log_output(events_file, key="events")

    output_keys = (
        ["events", "skims_zarr", "beam_iter_dir", "beam_output_root_dir"]
        if scalar_declared_first
        else ["beam_output_root_dir", "beam_iter_dir", "skims_zarr", "events"]
    )
    res = tracker.run(
        fn=step,
        name=f"nested_dir_step_{scalar_declared_first}",
        run_id=f"nested_dir_run_{scalar_declared_first}",
        outputs=output_keys,
        execution_options=ExecutionOptions(inject_context="ctx"),
        output_policy=OutputPolicyOptions(
            run_completion_archive=RunCompletionArchivePolicy(
                archive_root=archive_root,
                mode="copy",
            )
        ),
    )

    expected_root = str(archive_root.resolve())
    for key in output_keys:
        assert res.outputs[key].meta.get("recovery_roots") == [expected_root]
        assert res.outputs[key].abs_path is not None

    assert (
        archive_root / "beam_outputs" / "ITERS" / "it.0" / "0.events.csv.gz"
    ).read_bytes() == b"events-payload"
    assert (
        archive_root / "beam_outputs" / "ITERS" / "it.0" / "0.linkstats.csv.gz"
    ).read_bytes() == b"linkstats-payload"
    assert (archive_root / "beam_outputs" / "skims.zarr" / "0.0").read_bytes() == (
        b"zarr-chunk"
    )

    # Also test direct archive_artifact(dir_artifact) when a nested scalar was archived first
    manual_archive = tmp_path / f"manual_archive_{scalar_declared_first}"
    tracker.archive_artifact(res.outputs["events"], manual_archive, mode="copy")
    tracker.archive_artifact(
        res.outputs["beam_output_root_dir"], manual_archive, mode="copy"
    )
    assert (
        manual_archive / "beam_outputs" / "ITERS" / "it.0" / "0.linkstats.csv.gz"
    ).read_bytes() == b"linkstats-payload"

    # Also verify mode="move" on archive_run_outputs with nested outputs
    move_archive = tmp_path / f"move_archive_{scalar_declared_first}"
    moved = tracker.archive_run_outputs(
        f"nested_dir_run_{scalar_declared_first}",
        move_archive,
        mode="move",
    )
    assert set(moved.keys()) == set(output_keys)
    assert not beam_root.exists()
    assert (
        move_archive / "beam_outputs" / "ITERS" / "it.0" / "0.events.csv.gz"
    ).read_bytes() == b"events-payload"
    assert (
        move_archive / "beam_outputs" / "ITERS" / "it.0" / "0.linkstats.csv.gz"
    ).read_bytes() == b"linkstats-payload"


def test_archive_directory_artifact_rollback_preserves_preexisting_files_and_recovery_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    dir_path = run_dir / "shared_dir"
    (dir_path / "nested").mkdir(parents=True)
    nested_scalar = dir_path / "nested" / "scalar.csv"
    nested_scalar.write_text("id\n1\n", encoding="utf-8")
    (dir_path / "other.txt").write_text("other-content", encoding="utf-8")

    with tracker.start_run("rollback_and_fallback_run", model="test"):
        scalar_art = tracker.log_output(nested_scalar, key="scalar")
        dir_art = tracker.log_output(dir_path, key="dir_out", artifact_kind="directory")

    # 1. Pre-populate archive destination with the nested scalar, then fail set_artifact_recovery_roots
    archive_a = tmp_path / "archive_a"
    tracker.archive_artifact(scalar_art, archive_a, mode="copy")
    archived_scalar = archive_a / "shared_dir" / "nested" / "scalar.csv"
    assert archived_scalar.exists()

    orig_set_roots = tracker.set_artifact_recovery_roots

    def fail_set_roots(*args, **kwargs):
        raise RuntimeError("simulated DB failure during recovery root registration")

    monkeypatch.setattr(tracker, "set_artifact_recovery_roots", fail_set_roots)
    with pytest.raises(RuntimeError, match="simulated DB failure"):
        tracker.archive_artifact(dir_art, archive_a, mode="copy")
    monkeypatch.setattr(tracker, "set_artifact_recovery_roots", orig_set_roots)

    # Pre-existing scalar must remain intact, while newly copied other.txt was rolled back
    assert archived_scalar.read_text(encoding="utf-8") == "id\n1\n"
    assert not (archive_a / "shared_dir" / "other.txt").exists()

    # 2. Succeed archiving to archive_a, corrupt primary source, and archive to archive_b via fallback
    tracker.archive_artifact(dir_art, archive_a, mode="copy")
    (dir_path / "other.txt").write_text("corrupted-in-workspace", encoding="utf-8")

    archive_b = tmp_path / "archive_b"
    refreshed_dir = tracker.get_run_outputs("rollback_and_fallback_run")["dir_out"]
    dest_b = tracker.archive_artifact(refreshed_dir, archive_b, mode="copy")
    assert (dest_b / "other.txt").read_text(encoding="utf-8") == "other-content"


def test_archive_shapefile_bundle_move_to_same_destination_sets_abs_path(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs"
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    shp_dir = run_dir / "shapes"
    shp_dir.mkdir(parents=True)
    for ext in (".shp", ".shx", ".dbf"):
        (shp_dir / f"zones{ext}").write_bytes(f"payload-{ext}".encode("utf-8"))

    with tracker.start_run("shp_same_dest_run", model="test"):
        shp_art = tracker.log_output(shp_dir / "zones.shp", key="zones")

    archive_root = tmp_path / "archive"
    bundle_dest = tracker.archive_artifact(shp_art, archive_root, mode="copy")
    # Remove original workspace files so recovery resolves from archive_root
    for ext in (".shp", ".shx", ".dbf"):
        (shp_dir / f"zones{ext}").unlink()

    shp_art.abs_path = None
    # Archiving with mode="move" when source already resolves to the archive bundle root
    tracker.archive_artifact(shp_art, archive_root, mode="move")
    assert shp_art.abs_path == str((bundle_dest / "zones.shp").resolve())
    assert (bundle_dest / "zones.shp").exists()


def test_archive_run_outputs_filters_virtual_ingested_and_syncs_last_consist_and_coupler(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True)
    db_path = str(tmp_path / "provenance.duckdb")
    tracker = Tracker(
        run_dir=workspace,
        db_path=db_path,
        mounts={"workspace": str(workspace)},
    )

    out_path = workspace / "outputs" / "metrics.csv"
    out_path.parent.mkdir(parents=True)

    with tracker.scenario("sync_scenario") as sc:

        def step_fn() -> None:
            out_path.write_text("id,val\n1,10\n", encoding="utf-8")
            # Also log a virtual unbacked ingested table row
            virtual_art = tracker.log_artifact(
                "db://ingested_summary",
                key="ingested_summary",
                direction="output",
                driver="duckdb",
                is_ingested=True,
            )
            virtual_art.abs_path = None

        res = sc.run(
            fn=step_fn,
            name="produce_and_ingest",
            run_id="run_produce_and_ingest",
            outputs=["metrics"],
            output_paths={"metrics": out_path},
        )
        archive_root = tmp_path / "archive_post_step"
        archived = tracker.archive_run_outputs(
            res.run.id,
            archive_root,
            mode="copy",
        )
        assert set(archived.keys()) == {"metrics"}
        expected_roots = [
            str(workspace.resolve()),
            str(archive_root.resolve()),
        ]
        assert archived.outputs["metrics"].meta["recovery_roots"] == expected_roots
        assert archived.outputs["metrics"].abs_path == str(out_path.resolve())

        # Verify in-memory sync onto ScenarioContext.coupler and tracker._last_consist
        coupler_art = sc.coupler.require("metrics")
        assert coupler_art.meta["recovery_roots"] == expected_roots
        assert coupler_art.abs_path == str(out_path.resolve())

        assert tracker._last_consist is not None
        last_metrics = next(
            a for a in tracker._last_consist.outputs if a.key == "metrics"
        )
        assert last_metrics.meta["recovery_roots"] == expected_roots
        assert last_metrics.abs_path == str(out_path.resolve())

    # Also test prepend_source_mount_root=False
    archive_root_no_prepend = tmp_path / "archive_no_prepend"
    with tracker.start_run("run_no_prepend", model="test"):
        out_b = workspace / "outputs" / "metrics_b.csv"
        out_b.write_text("id,val\n2,20\n", encoding="utf-8")
        tracker.log_output(out_b, key="metrics_b")

    archived_b = tracker.archive_run_outputs(
        "run_no_prepend",
        archive_root_no_prepend,
        mode="copy",
        prepend_source_mount_root=False,
    )
    assert archived_b.outputs["metrics_b"].meta["recovery_roots"] == [
        str(archive_root_no_prepend.resolve())
    ]
