from pathlib import Path

import pytest


def test_cache_validation_policy_eager_vs_lazy(tracker) -> None:
    """
    Cache validation policy controls whether cache hits require on-disk outputs.

    This test stays separate from hydration tests because it asserts *validation* logic
    only: whether a cache hit is accepted when cached output files are missing.

    Behavior summary:
    - eager: missing cached outputs invalidate the cache hit (run executes).
    - lazy: cached outputs are accepted without filesystem existence checks.
    """
    eager_path = tracker.run_dir / "outputs" / "cached_eager.txt"
    eager_path.parent.mkdir(parents=True, exist_ok=True)
    eager_path.write_text("cached output (eager)")

    # Seed a cached run with a real output file.
    with tracker.start_run("run_eager_seed", model="m_eager", cache_mode="overwrite"):
        tracker.log_artifact(eager_path, key="out", direction="output")

    # Delete the output to simulate a broken cache.
    eager_path.unlink()

    # Eager validation should reject the cache hit when outputs are missing.
    with tracker.start_run(
        "run_eager_check",
        model="m_eager",
        cache_mode="reuse",
        validate_cached_outputs="eager",
    ) as t:
        assert not t.is_cached

    run_eager = tracker.get_run("run_eager_check")
    assert run_eager is not None
    assert run_eager.meta.get("cache_hit") is not True

    lazy_path = tracker.run_dir / "outputs" / "cached_lazy.txt"
    lazy_path.write_text("cached output (lazy)")

    # Seed a second cached run; then remove its output to simulate missing bytes.
    with tracker.start_run("run_lazy_seed", model="m_lazy", cache_mode="overwrite"):
        tracker.log_artifact(lazy_path, key="out", direction="output")

    lazy_path.unlink()

    # Lazy validation should still allow a cache hit even with missing output files.
    with tracker.start_run(
        "run_lazy_check",
        model="m_lazy",
        cache_mode="reuse",
        validate_cached_outputs="lazy",
    ) as t:
        assert t.is_cached
        cached = t.cached_output("out")
        assert cached is not None
        assert cached.container_uri
        assert not Path(t.resolve_uri(cached.container_uri)).exists()

    run_lazy = tracker.get_run("run_lazy_check")
    assert run_lazy is not None
    assert run_lazy.meta.get("cache_hit") is True


def test_cache_validation_policy_default_is_lazy(tracker) -> None:
    """
    The default validation policy is lazy: missing cached outputs do not invalidate.

    This guards the performance-driven default so it does not regress to eager.
    """
    output_path = tracker.run_dir / "outputs" / "cached_default.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("cached output (default)")

    with tracker.start_run(
        "run_default_seed", model="m_default", cache_mode="overwrite"
    ):
        tracker.log_artifact(output_path, key="out", direction="output")

    output_path.unlink()

    with tracker.start_run(
        "run_default_check",
        model="m_default",
        cache_mode="reuse",
    ) as t:
        assert t.is_cached

    run_default = tracker.get_run("run_default_check")
    assert run_default is not None
    assert run_default.meta.get("cache_hit") is True


def test_cache_validation_policy_rejects_invalid_value(tracker) -> None:
    """
    Invalid validate_cached_outputs values should fail fast on run start.
    """
    output_path = tracker.run_dir / "outputs" / "cached_invalid.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("cached output (invalid)")

    with tracker.start_run(
        "run_invalid_seed", model="m_invalid", cache_mode="overwrite"
    ):
        tracker.log_artifact(output_path, key="out", direction="output")

    output_path.unlink()

    with pytest.raises(ValueError):
        with tracker.start_run(
            "run_invalid_check",
            model="m_invalid",
            cache_mode="reuse",
            validate_cached_outputs="not-a-policy",
        ):
            pass


def test_cache_validation_warning_includes_mount_hint(tmp_path, caplog) -> None:
    """
    Cache validation failures should include mount diagnostics hints.
    """
    from consist.core.tracker import Tracker

    caplog.set_level("WARNING")
    db_path = str(tmp_path / "provenance.db")
    old_mount = tmp_path / "old_outputs"
    new_mount = tmp_path / "new_outputs"
    old_mount.mkdir(parents=True, exist_ok=True)
    new_mount.mkdir(parents=True, exist_ok=True)

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_path)
    tracker.mounts["outputs"] = str(old_mount)

    output_path = old_mount / "cached.csv"
    output_path.write_text("value\n1\n", encoding="utf-8")

    with tracker.start_run("seed", model="model", cache_mode="overwrite"):
        tracker.log_artifact(output_path, key="out", direction="output")

    output_path.unlink()
    tracker.mounts["outputs"] = str(new_mount)

    with tracker.start_run(
        "check",
        model="model",
        cache_mode="reuse",
        validate_cached_outputs="eager",
    ) as t:
        assert not t.is_cached

    assert any("Cache Validation Failed" in record.message for record in caplog.records)
    assert any("Mount root mismatch" in record.message for record in caplog.records)
    if tracker.engine:
        tracker.engine.dispose()


@pytest.mark.parametrize("corruption", ["delete", "modify", "extra"])
def test_cache_validation_eager_rejects_corrupted_directory_output_when_dir_exists(
    tracker,
    corruption: str,
) -> None:
    """
    Eager validation must validate directory_manifest entries even when the
    top-level directory still exists on disk.
    """
    dir_path = tracker.run_dir / "outputs" / f"skims_{corruption}.zarr"
    (dir_path / "nested").mkdir(parents=True, exist_ok=True)
    member = dir_path / "nested" / "0.0"
    member.write_bytes(b"original-chunk")

    with tracker.start_run(
        f"seed_dir_{corruption}",
        model=f"model_dir_{corruption}",
        cache_mode="overwrite",
    ):
        tracker.log_output(dir_path, key="skims", artifact_kind="directory")

    assert dir_path.is_dir()
    if corruption == "delete":
        member.unlink()
    elif corruption == "modify":
        member.write_bytes(b"mutated-chunk")
    elif corruption == "extra":
        (dir_path / "nested" / "extra.bin").write_bytes(b"unexpected")

    with tracker.start_run(
        f"check_dir_{corruption}",
        model=f"model_dir_{corruption}",
        cache_mode="reuse",
        validate_cached_outputs="eager",
    ) as t:
        assert not t.is_cached


def test_cache_validation_eager_recovers_corrupted_primary_directory_from_recovery_root(
    tracker,
    tmp_path: Path,
) -> None:
    """
    Eager validation succeeds when the primary directory output in run_dir is
    corrupted if a valid archived directory tree exists in recovery_roots, and
    fails when both primary and recovery_roots copies are corrupted.
    """
    dir_path = tracker.run_dir / "outputs" / "skims_recovery.zarr"
    (dir_path / "nested").mkdir(parents=True, exist_ok=True)
    member = dir_path / "nested" / "0.0"
    member.write_bytes(b"original-chunk")

    with tracker.start_run(
        "seed_dir_recovery",
        model="model_dir_recovery",
        cache_mode="overwrite",
    ):
        tracker.log_output(dir_path, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("seed_dir_recovery", archive_root)

    # Corrupt primary directory in run_dir while archive_root remains valid.
    member.unlink()
    assert dir_path.is_dir()

    with tracker.start_run(
        "check_dir_recovery_valid_archive",
        model="model_dir_recovery",
        cache_mode="reuse",
        validate_cached_outputs="eager",
    ) as t:
        assert t.is_cached

    # Now corrupt the archived copy in recovery_roots as well.
    archived_member = (
        archive_root / "outputs" / "skims_recovery.zarr" / "nested" / "0.0"
    )
    archived_member.write_bytes(b"corrupted-archive")

    with tracker.start_run(
        "check_dir_recovery_corrupted_archive",
        model="model_dir_recovery",
        cache_mode="reuse",
        validate_cached_outputs="eager",
    ) as t:
        assert not t.is_cached


def test_directory_allow_extra_files_preserves_cache_and_input_validation_until_tracked_member_changes(
    tracker,
) -> None:
    from consist.types import CacheOptions, ExecutionOptions

    shared_dir = tracker.run_dir / "shared_work"
    (shared_dir / "sub").mkdir(parents=True, exist_ok=True)
    tracked_file = shared_dir / "sub" / "tracked.csv"
    tracked_file.write_text("v\n1\n", encoding="utf-8")

    producer_calls = 0

    def produce(ctx) -> None:
        nonlocal producer_calls
        producer_calls += 1
        (shared_dir / "sub").mkdir(parents=True, exist_ok=True)
        tracked_file.write_text("v\n1\n", encoding="utf-8")
        tracker.log_output(
            shared_dir,
            key="shared_dir",
            artifact_kind="directory",
            directory_allow_extra_files=True,
        )

    res1 = tracker.run(
        fn=produce,
        name="produce_shared_dir",
        run_id="produce_shared_1",
        outputs=["shared_dir"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(validate_cached_outputs="eager"),
    )
    assert res1.cache_hit is False
    assert producer_calls == 1

    # Add extra files from a later step into the shared directory
    extra_file = shared_dir / "later_step.csv"
    extra_file.write_text("extra\n42\n", encoding="utf-8")

    # Eager cached output validation should still succeed
    res2 = tracker.run(
        fn=produce,
        name="produce_shared_dir",
        run_id="produce_shared_2",
        outputs=["shared_dir"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(validate_cached_outputs="eager"),
    )
    assert res2.cache_hit is True
    assert producer_calls == 1

    # Downstream step consuming shared_dir with validate_materialized_inputs=True
    consumer_calls = 0

    def consume(ctx) -> dict[str, Path]:
        nonlocal consumer_calls
        consumer_calls += 1
        out = ctx.run_dir / "consumed.txt"
        out.write_text(tracked_file.read_text(encoding="utf-8"), encoding="utf-8")
        return {"consumed": out}

    c1 = tracker.run(
        fn=consume,
        name="consume_shared_dir",
        run_id="consume_shared_1",
        inputs={"shared_dir": res1.outputs["shared_dir"]},
        outputs=["consumed"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(
            validate_materialized_inputs=True,
            cache_hydration="inputs-missing",
        ),
    )
    assert c1.cache_hit is False
    assert consumer_calls == 1

    # Adding another extra file inside shared_dir keeps materialized input valid on cache hit
    (shared_dir / "sub" / "another_extra.txt").write_text("ok", encoding="utf-8")
    c2 = tracker.run(
        fn=consume,
        name="consume_shared_dir",
        run_id="consume_shared_2",
        inputs={"shared_dir": res1.outputs["shared_dir"]},
        outputs=["consumed"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(
            validate_materialized_inputs=True,
            cache_hydration="inputs-missing",
        ),
    )
    assert c2.cache_hit is True
    assert consumer_calls == 1

    # Modifying or deleting the tracked manifest member invalidates eager output validation
    tracked_file.write_text("v\ncorrupted\n", encoding="utf-8")
    res3 = tracker.run(
        fn=produce,
        name="produce_shared_dir",
        run_id="produce_shared_3",
        outputs=["shared_dir"],
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(validate_cached_outputs="eager"),
    )
    assert res3.cache_hit is False
    assert producer_calls == 2
