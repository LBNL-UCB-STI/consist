from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Dict

import pytest

from consist.core.tracker_config import TrackerConfig
from consist.core.orchestration import (
    BatchRunSpec,
    PythonCallableTarget,
    ExecutionSpec,
    ExecutionAttempt,
    RunSpecResult,
    BatchResult,
    resolve_callable,
    execute_worker_run,
)

# -------------------------------------------------------------------------
# Test Callables (must be top-level for importability & pickling)
# -------------------------------------------------------------------------


def sample_target_fn(config: Dict[str, Any], val: int = 1) -> Dict[str, Any]:
    """Simple test function that returns a dict."""
    return {"result": val + config.get("add", 0)}


def sample_scalar_fn(config: Dict[str, Any]) -> int:
    """Simple test function returning a scalar."""
    return config.get("value", 42)


def sample_failing_fn(config: Dict[str, Any]) -> None:
    """Test function that raises an exception."""
    raise ValueError("Intentional error in sample_failing_fn")


def sample_conditional_failing_fn(config: Dict[str, Any]) -> int:
    """Test function that conditionally raises an error based on config."""
    if config.get("fail"):
        raise ValueError("Sweep failure")
    return config.get("value", 0)


# -------------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------------


def test_models_serialization_and_pickling():
    """Verify that all orchestration models support JSON and pickle serialization."""
    target = PythonCallableTarget(
        callable_ref="tests.unit.core.test_orchestration:sample_target_fn"
    )

    spec = BatchRunSpec(
        run_id="run-1",
        model="test_model",
        config={"add": 10},
        tags=["test"],
        facets={"case": 1},
        parent_run_id="parent-1",
        expected_outputs=["result_file"],
    )

    exec_spec = ExecutionSpec(
        run_id="exec-1",
        parent_run_id="parent-1",
        target=target,
        run_spec=spec,
    )

    attempt = ExecutionAttempt(
        attempt_id="exec-1-attempt-1",
        run_id="run-1",
        attempt_number=1,
        status="running",
        started_at=123.45,
    )

    run_result = RunSpecResult(
        spec_id="run-1",
        run_id="real-run-123",
        persisted_run_id="real-run-123",
        status="success",
        cache_hit=True,
        scalar_payload={"foo": "bar"},
        artifact_keys=["result_file"],
        attempts=[attempt],
    )

    batch_result = BatchResult(
        total_count=1,
        success_count=1,
        failure_count=0,
        cache_hit_count=1,
        child_run_ids=["real-run-123"],
        failed_specs=[],
        results=[run_result],
    )

    # Test JSON round-trip
    dumped_json = batch_result.model_dump_json()
    loaded_batch = BatchResult.model_validate_json(dumped_json)
    assert loaded_batch.total_count == 1
    assert loaded_batch.results[0].spec_id == "run-1"
    assert loaded_batch.results[0].attempts[0].status == "running"

    # Test pickle round-trip
    pickled = pickle.dumps(batch_result)
    unpickled = pickle.loads(pickled)
    assert unpickled.total_count == 1
    assert unpickled.results[0].spec_id == "run-1"
    assert unpickled.results[0].attempts[0].status == "running"

    # Test pickle round-trip for exec_spec
    pickled_exec = pickle.dumps(exec_spec)
    unpickled_exec = pickle.loads(pickled_exec)
    assert unpickled_exec.run_id == "exec-1"


def test_resolve_callable_string():
    """Verify string reference resolution."""
    # Successful resolution
    resolved = resolve_callable("tests.unit.core.test_orchestration:sample_target_fn")
    assert resolved.__name__ == sample_target_fn.__name__
    assert resolved({"add": 5}) == {"result": 6}

    # Invalid formats
    with pytest.raises(ValueError, match="Expected format 'module:function'"):
        resolve_callable("tests.unit.core.test_orchestration.sample_target_fn")

    # Non-existent module
    with pytest.raises(ValueError, match="Could not import module"):
        resolve_callable("nonexistent_module_xyz:some_func")

    # Non-existent attribute
    with pytest.raises(ValueError, match="has no attribute"):
        resolve_callable("tests.unit.core.test_orchestration:nonexistent_func")

    # Non-callable attribute
    with pytest.raises(ValueError, match="is not callable"):
        resolve_callable("tests.unit.core.test_orchestration:__doc__")


def test_resolve_callable_rejects_direct_callables():
    """Process execution requires importable string refs, not direct callables."""
    with pytest.raises(ValueError, match="requires fn to be an importable"):
        resolve_callable(sample_target_fn)

    def local_func(x):
        return x

    with pytest.raises(ValueError, match="requires fn to be an importable"):
        resolve_callable(local_func)

    def nested_func():
        pass

    with pytest.raises(ValueError, match="requires fn to be an importable"):
        resolve_callable(nested_func)


def test_execute_worker_run_success(run_dir: Path, tmp_path: Path):
    """Test a successful run execution through execute_worker_run."""
    db_path = tmp_path / "test_worker_prov.db"
    tracker_config = TrackerConfig(
        run_dir=run_dir,
        db_path=db_path,
    )

    # Build the spec
    target = PythonCallableTarget(
        callable_ref="tests.unit.core.test_orchestration:sample_scalar_fn"
    )
    run_spec = BatchRunSpec(
        run_id="run-scalar-1",
        model="scalar_model",
        config={"value": 100},
        tags=["unit-test"],
    )
    exec_spec = ExecutionSpec(
        run_id="exec-scalar-1",
        target=target,
        run_spec=run_spec,
    )

    # Run the worker execution
    result = execute_worker_run(tracker_config, exec_spec)

    # Assert successful results
    assert result.status == "success"
    assert result.spec_id == "run-scalar-1"
    assert result.persisted_run_id == "run-scalar-1"
    assert result.cache_hit is False
    assert result.scalar_payload == 100
    assert len(result.attempts) == 1
    assert result.attempts[0].status == "succeeded"
    assert result.attempts[0].exit_code == 0

    # Run again to verify cache hit behavior
    result_cached = execute_worker_run(tracker_config, exec_spec)
    assert result_cached.status == "success"
    assert result_cached.persisted_run_id == "run-scalar-1"
    assert result_cached.cache_hit is True
    # Cache hit returns standard scalar_payload as None (since function is skipped)
    assert result_cached.scalar_payload is None


def test_execute_worker_run_failure(run_dir: Path, tmp_path: Path):
    """Test a failing run execution through execute_worker_run."""
    db_path = tmp_path / "test_worker_prov_fail.db"
    tracker_config = TrackerConfig(
        run_dir=run_dir,
        db_path=db_path,
    )

    target = PythonCallableTarget(
        callable_ref="tests.unit.core.test_orchestration:sample_failing_fn"
    )
    run_spec = BatchRunSpec(
        run_id="run-fail-1",
        model="fail_model",
    )
    exec_spec = ExecutionSpec(
        run_id="exec-fail-1",
        target=target,
        run_spec=run_spec,
    )

    result = execute_worker_run(tracker_config, exec_spec)

    # Assert failed results
    assert result.status == "failed"
    assert result.persisted_run_id == "run-fail-1"
    assert result.error_message == "Intentional error in sample_failing_fn"
    assert result.error_traceback is not None
    assert len(result.attempts) == 1
    assert result.attempts[0].status == "failed"
    assert result.attempts[0].error_class == "ValueError"
    assert result.attempts[0].error_message == "Intentional error in sample_failing_fn"


def test_execute_worker_run_invalid_callable_fails_early(run_dir: Path, tmp_path: Path):
    """Test that invalid callable reference is captured early before running."""
    db_path = tmp_path / "test_worker_prov_invalid.db"
    tracker_config = TrackerConfig(
        run_dir=run_dir,
        db_path=db_path,
    )

    target = PythonCallableTarget(callable_ref="nonexistent_module_abc:func")
    run_spec = BatchRunSpec(run_id="run-invalid-1", model="invalid_model")
    exec_spec = ExecutionSpec(
        run_id="exec-invalid-1",
        target=target,
        run_spec=run_spec,
    )

    result = execute_worker_run(tracker_config, exec_spec)

    assert result.status == "failed"
    assert result.persisted_run_id is None
    assert result.error_message is not None
    assert "Could not import module" in result.error_message
    assert len(result.attempts) == 1
    assert result.attempts[0].status == "failed"
    assert result.attempts[0].error_class == "ValueError"


def test_batch_result_retry_rows():
    """Verify that BatchResult aggregates failed specs and generates retry rows."""
    failed_spec = BatchRunSpec(
        run_id="run-fail",
        model="test_model",
        config={"x": 5},
    )

    batch_result = BatchResult(
        total_count=2,
        success_count=1,
        failure_count=1,
        cache_hit_count=0,
        failed_specs=[failed_spec],
    )

    retry_rows = batch_result.retry_rows()
    assert retry_rows == [{"x": 5}]


def test_scenario_map_runs_threads(tracker):
    """Test map_runs with unsupported backend raises NotImplementedError."""
    rows = [{"value": 10}, {"value": 20}, {"value": 30}]

    with tracker.scenario("test_thread_sweep") as sc:
        with pytest.raises(NotImplementedError):
            sc.map_runs(
                rows=rows,
                fn="tests.unit.core.test_orchestration:sample_scalar_fn",
                name_template="case-{value}",
                backend="threads",
            )


def test_scenario_map_runs_processes_rejects_direct_callable(tracker):
    rows = [{"value": 10}]

    with tracker.scenario("test_process_sweep_rejects_direct_callable") as sc:
        with pytest.raises(ValueError, match="requires fn to be an importable"):
            sc.map_runs(
                rows=rows,
                fn=sample_scalar_fn,
                backend="processes",
            )


def test_scenario_map_runs_processes(tracker):
    """Test map_runs with ProcessPoolExecutor."""
    rows = [{"value": 5}, {"value": 15}]

    with tracker.scenario("test_process_sweep") as sc:
        res = sc.map_runs(
            rows=rows,
            fn="tests.unit.core.test_orchestration:sample_scalar_fn",
            name_template="case-{value}",
            backend="processes",
        )

    assert res.total_count == 2
    assert res.success_count == 2
    assert res.failure_count == 0
    assert res.child_run_ids == [result.persisted_run_id for result in res.results]
    assert res.results[0].scalar_payload == 5
    assert res.results[1].scalar_payload == 15


def test_scenario_map_runs_failures(tracker):
    """Test map_runs with a failure (fail-slow default)."""
    rows = [
        {"value": 1, "fail": False},
        {"value": 2, "fail": True},
        {"value": 3, "fail": False},
    ]

    with tracker.scenario("test_fail_slow") as sc:
        res = sc.map_runs(
            rows=rows,
            fn="tests.unit.core.test_orchestration:sample_conditional_failing_fn",
            backend="processes",
        )

    assert res.total_count == 3
    assert res.success_count == 2
    assert res.failure_count == 1
    assert set(res.child_run_ids) == {
        result.persisted_run_id
        for result in res.results
        if result.persisted_run_id is not None
    }
    assert len(res.failed_specs) == 1
    assert res.failed_specs[0].config["fail"] is True
    assert res.retry_rows() == [{"value": 2, "fail": True}]


def test_scenario_map_runs_cancel_pending_on_failure(tracker):
    """Test map_runs can stop collecting after first failure."""
    rows = [
        {"value": 1, "fail": False},
        {"value": 2, "fail": True},
        {"value": 3, "fail": False},
    ]

    with tracker.scenario("test_cancel_pending_on_failure") as sc:
        res = sc.map_runs(
            rows=rows,
            fn="tests.unit.core.test_orchestration:sample_conditional_failing_fn",
            backend="processes",
            cancel_pending_on_failure=True,
        )

    assert res.total_count == 3
    assert res.failure_count >= 1


def test_scenario_map_runs_cache_hits_processes(tracker):
    """Test that fanned-out parallel runs resolve cache hits correctly on a second execution."""
    rows = [{"value": 100}, {"value": 200}, {"value": 300}]

    # First execution - all should run successfully with 0 cache hits
    with tracker.scenario("test_cache_sweep_1") as sc1:
        res1 = sc1.map_runs(
            rows=rows,
            fn="tests.unit.core.test_orchestration:sample_scalar_fn",
            name_template="case-{value}",
            backend="processes",
        )

    assert res1.total_count == 3
    assert res1.success_count == 3
    assert res1.cache_hit_count == 0

    # Second execution - all should run successfully and return 100% cache hits
    with tracker.scenario("test_cache_sweep_2") as sc2:
        res2 = sc2.map_runs(
            rows=rows,
            fn="tests.unit.core.test_orchestration:sample_scalar_fn",
            name_template="case-{value}",
            backend="processes",
        )

    assert res2.total_count == 3
    assert res2.success_count == 3
    assert res2.cache_hit_count == 3
