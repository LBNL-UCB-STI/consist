import functools
import inspect
import uuid
from pathlib import Path
from typing import Any, Callable, Optional, List, Union

from consist.models.artifact import Artifact
# Use TYPE_CHECKING to avoid circular imports if needed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


def create_task_decorator(
        tracker: "Tracker",
        cache_mode: str = "reuse",
        depends_on: Optional[List[Union[str, Path, Artifact]]] = None,
        capture_dir: Optional[Union[str, Path]] = None,
        capture_pattern: str = "*",
        **run_kwargs: Any,
) -> Callable:
    """
    Factory that creates the decorator for tracker.task().
    Separated from Tracker to keep the core class clean.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # 1. Inspect Signature to build Config & Inputs
            sig = inspect.signature(func)
            try:
                bound = sig.bind(*args, **kwargs)
            except TypeError:
                bound = None

            config = {}
            inputs = []
            if bound:
                bound.apply_defaults()
                for k, v in bound.arguments.items():
                    if isinstance(v, Artifact):
                        inputs.append(v)
                        config[k] = v.uri
                    elif isinstance(v, list) and v and isinstance(v[0], Artifact):
                        inputs.extend(v)
                        config[k] = [a.uri for a in v]
                    else:
                        config[k] = v

            # 2. Process Manual Dependencies
            if depends_on:
                for dep in depends_on:
                    if isinstance(dep, Artifact):
                        inputs.append(dep)
                        config[f"dep_{dep.key}"] = dep.uri
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
                    outputs = tracker.current_consist.outputs
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
                        result = func(*args, **kwargs)
                    if result is not None:
                        raise ValueError("Task with capture_dir must return None")
                    return cap.artifacts
                else:
                    result = func(*args, **kwargs)

                    # Return Value Handling
                    if isinstance(result, (str, Path)):
                        return tracker.log_artifact(
                            Path(result), key=Path(result).stem
                        )
                    elif isinstance(result, dict):
                        return {
                            k: tracker.log_artifact(v, key=k)
                            for k, v in result.items()
                        }
                    elif result is None:
                        return None
                    else:
                        raise TypeError(
                            f"Task returned unsupported type {type(result)}"
                        )

        return wrapper

    return decorator