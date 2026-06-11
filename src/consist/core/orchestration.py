from __future__ import annotations

import functools
import importlib
import pickle
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Union

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
    callable_ref: Union[str, Callable[..., Any]]


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


def resolve_callable(ref: Union[str, Callable[..., Any]]) -> Callable[..., Any]:
    """
    Resolves a string reference or directly validates a Python callable.
    Returns the resolved callable, or raises ValueError if invalid/not pickle-safe.
    """
    if isinstance(ref, str):
        # Format: "module.submodule:func_name" or "module:func_name"
        if ":" not in ref:
            raise ValueError(
                f"Invalid callable reference format: '{ref}'. Expected format 'module:function'."
            )
        module_name, func_name = ref.split(":", 1)
        try:
            mod = importlib.import_module(module_name)
        except ImportError as e:
            raise ValueError(f"Could not import module '{module_name}': {e}") from e

        if not hasattr(mod, func_name):
            raise ValueError(f"Module '{module_name}' has no attribute '{func_name}'")

        func = getattr(mod, func_name)
        if not callable(func):
            raise ValueError(
                f"Attribute '{func_name}' in module '{module_name}' is not callable"
            )

        # Verify pickle-safety
        try:
            pickle.dumps(func)
        except Exception as e:
            raise ValueError(
                f"Resolved callable '{ref}' is not pickle-safe: {e}"
            ) from e

        return func

    elif callable(ref):
        # Direct callable object validation
        # Verify pickle-safety
        try:
            pickle.dumps(ref)
        except Exception as e:
            raise ValueError(f"Callable {ref} is not pickle-safe: {e}") from e

        module_name = getattr(ref, "__module__", None)
        qualname = getattr(ref, "__qualname__", None)
        if not module_name or not qualname or module_name == "__main__":
            # Non-importable but pickle-safe (e.g. from dynamic context/notebooks using serializers)
            return ref

        # If it claims to be importable, attempt to verify it
        try:
            mod = importlib.import_module(module_name)
            parts = qualname.split(".")
            obj = mod
            for part in parts:
                obj = getattr(obj, part)
            if obj is not ref:
                raise ValueError(
                    "Imported object does not match the original callable."
                )
        except Exception:
            # We don't raise here if pickling succeeds, but log/verify
            pass

        return ref
    else:
        raise ValueError(
            f"Callable reference must be a string or callable, got {type(ref)}"
        )


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
