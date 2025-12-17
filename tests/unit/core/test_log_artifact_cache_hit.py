import pandas as pd


def test_log_artifact_raises_for_new_output_key_on_cache_hit(tracker, tmp_path):
    """
    Demonstrate the "new output key on cache hit" behavior.

    Scenario/task bodies still execute on cache hits. If the cached run never produced
    a given output key, attempting to log that output key should demote the cache hit
    to an executing run (so provenance remains truthful).
    """
    input_file = tmp_path / "input.csv"
    input_file.write_text("id,val\n1,100\n")

    with tracker.start_run(
        "run_A_cache_hit_new_output",
        model="test_model",
        config={"param": 1},
        inputs=[str(input_file)],
    ) as t:
        out_path = t.run_dir / "out.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(out_path)
        t.log_artifact(out_path, key="out", direction="output")

    with tracker.start_run(
        "run_B_cache_hit_new_output",
        model="test_model",
        config={"param": 1},
        inputs=[str(input_file)],
    ) as t:
        assert t.is_cached
        new_out = t.log_artifact("new.parquet", key="new", direction="output")
        assert new_out.key == "new"
        assert not t.is_cached
