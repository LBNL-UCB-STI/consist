from contextlib import contextmanager
from pathlib import Path

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


def test_tracker_trace_propagates_start_run_optional_kwargs(tracker, monkeypatch):
    captured_start_kwargs: dict[str, object] = {}
    original_start_run = tracker.start_run

    @contextmanager
    def _spy_start_run(*args, **kwargs):
        captured_start_kwargs.update(kwargs)
        with original_start_run(*args, **kwargs) as active_tracker:
            yield active_tracker

    monkeypatch.setattr(tracker, "start_run", _spy_start_run)

    observed_run_dir = None
    with tracker.trace(
        name="trace_start_kwargs",
        model="trace_start_kwargs_model",
        cache_mode="overwrite",
        cache_hydration="outputs-all",
        cache_version=5,
        facet_schema_version="trace-facet-v1",
        facet_index=False,
    ) as t:
        observed_run_dir = t.run_artifact_dir()

    assert observed_run_dir is not None
    assert captured_start_kwargs["cache_version"] == 5
    assert captured_start_kwargs["cache_hydration"] == "outputs-all"
    assert captured_start_kwargs["facet_schema_version"] == "trace-facet-v1"
    assert captured_start_kwargs["facet_index"] is False

    assert "_consist_code_identity_callable" not in captured_start_kwargs
    assert "materialize_cached_output_paths" not in captured_start_kwargs
    assert (
        Path(str(captured_start_kwargs["materialize_cached_outputs_dir"]))
        == observed_run_dir
    )
