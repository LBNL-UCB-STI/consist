from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys

import pytest

from consist.core.identity import CodeIdentityUnavailableError


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


def test_tracker_trace_distinguishes_positional_and_dependency_roles(tracker, tmp_path):
    first = tmp_path / "first.txt"
    first.write_text("first\n", encoding="utf-8")
    second = tmp_path / "second.txt"
    second.write_text("second\n", encoding="utf-8")
    dependency = tmp_path / "dependency.txt"
    dependency.write_text("dependency\n", encoding="utf-8")

    with tracker.trace(
        name="trace_input_roles",
        inputs=[first, second],
        depends_on=[dependency],
    ):
        pass

    run = tracker.last_run.run
    assert [
        (binding["kind"], binding["role"])
        for binding in run.meta["input_binding"]["bindings"]
    ] == [
        ("positional", 0),
        ("positional", 1),
        ("dependency", 0),
    ]

    with tracker.trace(
        name="trace_input_roles",
        inputs=[second, first],
        depends_on=[dependency],
        cache_mode="overwrite",
    ):
        pass

    reversed_run = tracker.last_run.run
    assert run.input_hash != reversed_run.input_hash
    assert [
        binding["artifact_id"] for binding in run.meta["input_binding"]["bindings"]
    ] != [
        binding["artifact_id"]
        for binding in reversed_run.meta["input_binding"]["bindings"]
    ]


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


def test_tracker_trace_without_callable_identity_fails_before_cache_lookup(tmp_path):
    runner = """
import json
import sys
from pathlib import Path

from consist.core.identity import CodeIdentityUnavailableError
from consist.core.tracker import Tracker

root = Path(sys.argv[1])
tracker = Tracker(
    run_dir=root / 'runs',
    db_path=root / 'provenance.duckdb',
    project_root=root,
)

def fail_lookup(*args, **kwargs):
    raise AssertionError('cache lookup must not start')

tracker.find_matching_run = fail_lookup
try:
    with tracker.trace('trace_without_identity'):
        pass
except CodeIdentityUnavailableError as exc:
    print(json.dumps({'error': type(exc).__name__, 'active': tracker.current_consist is not None}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", runner, str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {
        "error": "CodeIdentityUnavailableError",
        "active": False,
    }


def test_scenario_trace_without_callable_identity_fails_before_cache_lookup(
    tracker, monkeypatch
):
    with tracker.scenario("scenario_trace_without_identity") as scenario:

        def unavailable_code_version():
            raise CodeIdentityUnavailableError(
                mode="repo_git", reason="repository identity unavailable"
            )

        def fail_lookup(*args, **kwargs):
            pytest.fail("cache lookup must not start")

        monkeypatch.setattr(
            tracker.identity, "get_code_version", unavailable_code_version
        )
        monkeypatch.setattr(tracker, "find_matching_run", fail_lookup)

        with pytest.raises(CodeIdentityUnavailableError, match="Code identity"):
            with scenario.trace("trace_without_identity"):
                pass
