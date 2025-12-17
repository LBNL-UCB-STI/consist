import logging
from pathlib import Path

import pandas as pd


def test_cache_hit_demotion_emits_guidance_warning_and_happy_path_no_warning(
    tracker, tmp_path, caplog
):
    """
    Targeted regression test for cache-hit demotion behavior in `Tracker.log_artifact`.

    - If a run starts as a cache hit but user code tries to log a *different* output path
      for an existing cached output key, Consist should demote the cache hit to an
      executing run and emit a best-practice guidance warning.
    - If user code re-logs by key with a placeholder/nonexistent path, Consist should
      return the cached artifact without demotion and without emitting that warning.
    """
    input_file = tmp_path / "input.csv"
    input_file.write_text("id,val\n1,100\n")
    config = {"param": 1}

    # Seed a cached run with a stable output.
    with tracker.start_run(
        "run_A",
        model="model",
        config=config,
        inputs=[str(input_file)],
    ) as t:
        out_path = t.run_dir / "out.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(out_path)
        produced = t.log_artifact(out_path, key="out", direction="output")

    # --- Demotion path: attempt to log a different existing path for the same output key.
    caplog.set_level(logging.WARNING)
    with tracker.start_run(
        "run_B",
        model="model",
        config=config,
        inputs=[str(input_file)],
    ) as t:
        assert t.is_cached

        different_path = tmp_path / "different.parquet"
        pd.DataFrame({"a": [2]}).to_parquet(different_path)

        new_out = t.log_artifact(different_path, key="out", direction="output")
        assert new_out.key == "out"
        assert not t.is_cached

    demotion_logs = [
        r.message
        for r in caplog.records
        if "Demoting cache hit to cache miss" in r.message
    ]
    assert demotion_logs, "Expected a demotion warning to be logged"
    assert any("Best practice:" in msg for msg in demotion_logs)

    # --- Happy path: re-log by key with a placeholder path (does not exist) => no demotion, no warning.
    caplog.clear()
    with tracker.start_run(
        "run_C",
        model="model",
        config=config,
        inputs=[str(input_file)],
    ) as t:
        assert t.is_cached

        placeholder = Path("does-not-exist.parquet")
        cached_before = t.cached_output("out")
        assert cached_before is not None
        returned = t.log_artifact(placeholder, key="out", direction="output")
        assert returned.id == cached_before.id
        assert t.is_cached

    assert not any(
        "Demoting cache hit to cache miss" in r.message for r in caplog.records
    )
