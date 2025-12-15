import functools
import inspect
import uuid
import weakref
from pathlib import Path
from typing import Any, Callable, Optional, List, Union

from consist.models.artifact import Artifact
from consist.types import ArtifactRef

# Use TYPE_CHECKING to avoid circular imports if needed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def create_task_decorator(
    tracker: "Tracker",
    cache_mode: str = "reuse",
    depends_on: Optional[list[ArtifactRef]] = None,
    capture_dir: Optional[Union[str, Path]] = None,
    capture_pattern: str = "*",
    **run_kwargs: Any,
) -> Callable:
    """
    Factory that creates the decorator for tracker.task().
    Separated from Tracker to keep the core class clean.
    """

    def decorator(func: Callable) -> Callable:
        """
        Factory that creates a decorator for a Consist tracker task.

        Parameters
        ----------
        func : Callable
            The function to be wrapped by the decorator.

        Returns
        -------
        Callable
            A decorator that returns a wrapped function.
        """

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # 1. Inspect Signature to build Config & Inputs
            sig = inspect.signature(func)
            try:
                bound = sig.bind(*args, **kwargs)
            except TypeError:
                bound = None

            config: dict[str, Any] = {}
            inputs: List[Any] = []
            resolved_args: Optional[inspect.BoundArguments] = None
            if bound:
                bound.apply_defaults()
                resolved_args = bound
                for k, v in bound.arguments.items():
                    if isinstance(v, Artifact):
                        v._tracker = weakref.ref(tracker)
                        inputs.append(v)
                        resolved_args.arguments[k] = v.path
                    elif (
                        isinstance(v, list)
                        and v
                        and all(isinstance(item, Artifact) for item in v)
                    ):
                        for item in v:
                            item._tracker = weakref.ref(tracker)
                        inputs.extend(v)
                        resolved_args.arguments[k] = [item.path for item in v]
                    elif isinstance(v, Path):
                        path_artifact = tracker.artifacts.create_artifact(
                            path=str(v), run_id=None, key=k, direction="input"
                        )
                        inputs.append(path_artifact)
                        resolved_args.arguments[k] = v
                    else:
                        config[k] = v

            # 2. Process Manual Dependencies
            if depends_on:
                for dep in depends_on:
                    if isinstance(dep, Artifact):
                        dep._tracker = weakref.ref(tracker)
                        inputs.append(dep)
                    elif isinstance(dep, (str, Path)):
                        # Simple path resolution logic
                        if any(c in str(dep) for c in "*?["):
                            pass  # skip globs for explicit inputs usually
                        else:
                            inputs.append(str(Path(dep).resolve()))

            model_name = func.__name__
            run_id = f"{model_name}_{uuid.uuid4().hex[:8]}"

            # 3. Execution Wrapper
            with tracker.start_run(
                run_id=run_id,
                model=model_name,
                config=config,
                inputs=inputs,
                cache_mode=cache_mode,
                **run_kwargs,
            ):
                # Cache Hit Handling
                if tracker.is_cached:
                    cache_source_id = None
                    if tracker.current_consist and tracker.current_consist.cached_run:
                        cache_source_id = tracker.current_consist.cached_run.id
                    tracker.log_meta(cache_hit=True)
                    if cache_source_id:
                        tracker.log_meta(cache_source=cache_source_id)
                    outputs = tracker.current_consist.outputs
                    for art in outputs or []:
                        art._tracker = weakref.ref(tracker)
                    if capture_dir:
                        return outputs
                    if not outputs:
                        return None
                    if len(outputs) == 1:
                        return outputs[0]
                    return outputs

                # Execution & Capture
                if capture_dir:
                    with tracker.capture_outputs(
                        capture_dir, pattern=capture_pattern
                    ) as cap:
                        if resolved_args:
                            result = func(*resolved_args.args, **resolved_args.kwargs)
                        else:
                            result = func(*args, **kwargs)
                    if result is not None:
                        raise ValueError("Task with capture_dir must return None")
                    return cap.artifacts
                else:
                    if resolved_args:
                        result = func(*resolved_args.args, **resolved_args.kwargs)
                    else:
                        result = func(*args, **kwargs)

                    # Return Value Handling
                    if isinstance(result, Artifact):
                        if result not in tracker.current_consist.outputs:
                            result = tracker.log_artifact(
                                result, key=result.key, direction="output"
                            )
                        result._tracker = weakref.ref(tracker)
                        return result
                    elif isinstance(result, dict) and all(
                        isinstance(v, Artifact) for v in result.values()
                    ):
                        logged_results = {}
                        for k, v in result.items():
                            if v not in tracker.current_consist.outputs:
                                logged = tracker.log_artifact(
                                    v, key=v.key or k, direction="output"
                                )
                            else:
                                logged = v
                            logged._tracker = weakref.ref(tracker)
                            logged_results[k] = logged
                        return logged_results
                    elif isinstance(result, (str, Path)):
                        return tracker.log_artifact(Path(result), key=Path(result).stem)
                    elif isinstance(result, dict):
                        return {
                            k: tracker.log_artifact(v, key=k) for k, v in result.items()
                        }
                    elif result is None:
                        return None
                    else:
                        raise TypeError(
                            f"Task returned unsupported type {type(result)}"
                        )

        return wrapper

    return decorator
