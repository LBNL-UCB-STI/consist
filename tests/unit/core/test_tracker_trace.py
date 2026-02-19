import pytest


def test_tracker_trace_logs_output_paths_on_exception(tracker, tmp_path):
    output_path = tmp_path / "out.txt"

    with pytest.raises(RuntimeError, match="boom"):
        with tracker.trace(
            name="trace_failure",
            output_paths={"out": output_path},
        ):
            output_path.write_text("data")
            raise RuntimeError("boom")

    record = tracker.last_run
    assert record is not None
    assert any(artifact.key == "out" for artifact in record.outputs)


def test_tracker_trace_accepts_identity_kwargs(tracker, tmp_path):
    identity_dep = tmp_path / "identity_dep.txt"
    identity_dep.write_text("dep=true\n")

    with tracker.trace(
        name="trace_identity_kwargs",
        identity_inputs=[identity_dep],
        cache_version=7,
        cache_epoch=3,
        code_identity="repo_git",
        code_identity_extra_deps=[str(identity_dep)],
    ) as t:
        out_path = t.run_dir / "out.txt"
        out_path.write_text("ok")
        t.log_artifact(out_path, key="out", direction="output")

    run = tracker.last_run.run
    assert run.meta["cache_version"] == 7
    assert run.meta["cache_epoch"] == 3
    assert run.meta["code_identity"] == "repo_git"
    assert run.meta["code_identity_extra_deps"] == [str(identity_dep)]
    assert isinstance(run.meta.get("consist_hash_inputs"), dict)
