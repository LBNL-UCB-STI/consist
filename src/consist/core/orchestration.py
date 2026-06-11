from __future__ import annotations

import functools
import importlib
import pickle
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# -------------------------------------------------------------------------
# Execution Targets
# -------------------------------------------------------------------------


class ExecutionTarget(BaseModel):
    """Base model for all execution targets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)


class PythonCallableTarget(ExecutionTarget):
    """Target representing a local Python callable."""

    target_type: str = "python_callable"
    callable_ref: str


# -------------------------------------------------------------------------
# Specifications
# -------------------------------------------------------------------------


class BatchRunSpec(BaseModel):
    """
    Data-only specification for a single run in a parallel sweep or batch job.
    """

    run_id: str
    model: str
    config: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    facets: Dict[str, Any] = Field(default_factory=dict)
    parent_run_id: Optional[str] = None
    inputs: Optional[Dict[str, Any]] = None
    expected_outputs: Optional[List[str]] = None


class ExecutionSpec(BaseModel):
    """
    Backend-ready request wrapping a run spec and its execution target.
    """

    run_id: str
    parent_run_id: Optional[str] = None
    target: PythonCallableTarget
    run_spec: BatchRunSpec


# -------------------------------------------------------------------------
# Results and Attempts
# -------------------------------------------------------------------------


class ExecutionAttempt(BaseModel):
    """
    Records a single attempt of executing a run on a backend.
    """

    attempt_id: str
    run_id: str
    attempt_number: int
    status: str  # e.g., "created", "submitted", "running", "succeeded", "failed"
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    exit_code: Optional[int] = None
    error_class: Optional[str] = None
    error_message: Optional[str] = None


class RunSpecResult(BaseModel):
    """
    The outcome of running a single BatchRunSpec.
    """

    spec_id: str
    run_id: str
    status: str  # "success" | "failed"
    cache_hit: bool = False
    scalar_payload: Optional[Any] = None
    artifact_keys: List[str] = Field(default_factory=list)
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    attempts: List[ExecutionAttempt] = Field(default_factory=list)


class BatchResult(BaseModel):
    """
    Aggregation of results from a batch execution/sweep.
    """

    total_count: int
    success_count: int
    failure_count: int
    cache_hit_count: int
    child_run_ids: List[str] = Field(default_factory=list)
    failed_specs: List[BatchRunSpec] = Field(default_factory=list)
    results: List[RunSpecResult] = Field(default_factory=list)

    def retry_rows(self) -> List[Dict[str, Any]]:
        """
        Extract the configuration rows of failed runs to facilitate retrying.
        """
        return [spec.config for spec in self.failed_specs]


# -------------------------------------------------------------------------
# Utilities and Worker Execution
# -------------------------------------------------------------------------


_CALLABLE_REF_FORMAT = "'module:function'"


def _parse_callable_ref(ref: str) -> tuple[str, str]:
    if ":" not in ref:
        raise ValueError(
            f"Invalid callable reference format: {ref!r}. "
            f"Expected format {_CALLABLE_REF_FORMAT}."
        )
    module_name, attr_path = ref.split(":", 1)
    if not module_name or not attr_path:
        raise ValueError(
            f"Invalid callable reference format: {ref!r}. "
            f"Expected format {_CALLABLE_REF_FORMAT}."
        )
    return module_name, attr_path


def _import_callable_ref(module_name: str, attr_path: str) -> Callable[..., Any]:
    try:
        mod = importlib.import_module(module_name)
    except ImportError as e:
        raise ValueError(f"Could not import module {module_name!r}: {e}") from e

    obj: Any = mod
    for attr_name in attr_path.split("."):
        if not hasattr(obj, attr_name):
            raise ValueError(
                f"Module {module_name!r} has no attribute {attr_path!r}"
            )
        obj = getattr(obj, attr_name)

    if not callable(obj):
        raise ValueError(
            f"Attribute {attr_path!r} in module {module_name!r} is not callable"
        )
    return obj


def resolve_callable(ref: str) -> Callable[..., Any]:
    """
    Resolve an importable ``"module:function"`` reference.

    Direct callables are intentionally rejected for the process backend. Spawned
    workers need importable references that can be reconstructed in a fresh
    interpreter.

    Returns the resolved callable, or raises ValueError if invalid/not pickle-safe.
    """
    if not isinstance(ref, str):
        raise ValueError(
            "backend='processes' requires fn to be an importable "
            f"{_CALLABLE_REF_FORMAT} string. Direct callables, lambdas, and "
            "notebook-local functions are not supported yet. Move the function "
            f"into an importable module and pass {_CALLABLE_REF_FORMAT}."
        )

    module_name, attr_path = _parse_callable_ref(ref)
    func = _import_callable_ref(module_name, attr_path)

    try:
        pickle.dumps(func)
    except Exception as e:
        raise ValueError(f"Resolved callable {ref!r} is not pickle-safe: {e}") from e

    return func


def execute_worker_run(
    tracker_config: Any,  # TrackerConfig
    exec_spec: ExecutionSpec,
) -> RunSpecResult:
    """
    Bootstraps a Tracker, executes the requested spec via the resolved callable,
    captures outcomes, and returns a detailed RunSpecResult.
    """
    from consist.core.tracker import Tracker
    from consist.core.tracker_config import TrackerConfig

    run_spec = exec_spec.run_spec
    start_time = time.time()

    # 1. Resolve callable
    try:
        func = resolve_callable(exec_spec.target.callable_ref)
    except Exception as e:
        return RunSpecResult(
            spec_id=run_spec.run_id,
            run_id=run_spec.run_id,
            status="failed",
            cache_hit=False,
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            attempts=[
                ExecutionAttempt(
                    attempt_id=f"{exec_spec.run_id}-attempt-0",
                    run_id=run_spec.run_id,
                    attempt_number=1,
                    status="failed",
                    started_at=start_time,
                    ended_at=time.time(),
                    error_class=e.__class__.__name__,
                    error_message=str(e),
                )
            ],
        )

    # 2. Reconstruct Tracker
    try:
        if isinstance(tracker_config, dict):
            config_obj = TrackerConfig(**tracker_config)
        else:
            config_obj = tracker_config
        tracker = Tracker.from_config(config_obj)
    except Exception as e:
        return RunSpecResult(
            spec_id=run_spec.run_id,
            run_id=run_spec.run_id,
            status="failed",
            cache_hit=False,
            error_message=f"Tracker bootstrap failed: {e}",
            error_traceback=traceback.format_exc(),
            attempts=[
                ExecutionAttempt(
                    attempt_id=f"{exec_spec.run_id}-attempt-0",
                    run_id=run_spec.run_id,
                    attempt_number=1,
                    status="failed",
                    started_at=start_time,
                    ended_at=time.time(),
                    error_class=e.__class__.__name__,
                    error_message=f"Tracker bootstrap failed: {e}",
                )
            ],
        )

    # 3. Execute with Run wrapping to capture scalar return values
    attempt_id = f"{exec_spec.run_id}-attempt-1"
    attempt = ExecutionAttempt(
        attempt_id=attempt_id,
        run_id=run_spec.run_id,
        attempt_number=1,
        status="running",
        started_at=start_time,
    )

    returned_value = None

    @functools.wraps(func)
    def run_wrapper(*args, **kwargs):
        nonlocal returned_value
        returned_value = func(*args, **kwargs)
        return returned_value

    try:
        run_res = tracker.run(
            fn=run_wrapper,
            run_id=run_spec.run_id,
            model=run_spec.model,
            config=run_spec.config,
            tags=run_spec.tags,
            facet=run_spec.facets,
            parent_run_id=run_spec.parent_run_id,
            inputs=run_spec.inputs,
            outputs=run_spec.expected_outputs,
        )

        attempt.status = "succeeded"
        attempt.ended_at = time.time()
        attempt.exit_code = 0

        # Capture small scalar payloads
        scalar_payload = None
        if not run_res.cache_hit:
            # Check if it is a standard JSON-serializable/small type
            if isinstance(returned_value, (int, float, str, bool, type(None))):
                scalar_payload = returned_value
            elif isinstance(returned_value, (list, dict)):
                # Simple check for size/safety
                try:
                    import json

                    dumped = json.dumps(returned_value)
                    if len(dumped) < 65536:  # limit to 64KB
                        scalar_payload = returned_value
                except Exception:
                    pass

        return RunSpecResult(
            spec_id=run_spec.run_id,
            run_id=run_res.run.id,
            status="success",
            cache_hit=run_res.cache_hit,
            scalar_payload=scalar_payload,
            artifact_keys=list(run_res.outputs.keys()),
            attempts=[attempt],
        )
    except Exception as e:
        attempt.status = "failed"
        attempt.ended_at = time.time()
        attempt.error_class = e.__class__.__name__
        attempt.error_message = str(e)

        return RunSpecResult(
            spec_id=run_spec.run_id,
            run_id=run_spec.run_id,
            status="failed",
            cache_hit=False,
            error_message=str(e),
            error_traceback=traceback.format_exc(),
            attempts=[attempt],
        )
