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
        assert cached.uri
        assert not Path(t.resolve_uri(cached.uri)).exists()

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
