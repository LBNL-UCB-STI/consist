from dataclasses import dataclass
from typing import Callable, List, Optional


@dataclass(frozen=True)
class StepDefinition:
    outputs: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None


def define_step(
    *,
    outputs: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> Callable[[Callable], Callable]:
    """
    Attach metadata to a function without changing its execution behavior.

    This is used by Tracker.run/ScenarioContext.run to infer defaults.
    """

    def decorator(func: Callable) -> Callable:
        setattr(
            func,
            "__consist_step__",
            StepDefinition(outputs=outputs, tags=tags, description=description),
        )
        return func

    return decorator


def require_runtime_kwargs(*names: str) -> Callable[[Callable], Callable]:
    """
    Declare required runtime kwargs for a Consist-executed function.

    Tracker.run/ScenarioContext.run will raise a ValueError if any of the
    declared names are missing from runtime_kwargs.
    """
    if not names:
        raise ValueError("require_runtime_kwargs requires at least one name.")
    for name in names:
        if not isinstance(name, str) or not name:
            raise ValueError("require_runtime_kwargs expects non-empty string names.")

    def decorator(func: Callable) -> Callable:
        setattr(func, "__consist_runtime_required__", tuple(names))
        return func

    return decorator
