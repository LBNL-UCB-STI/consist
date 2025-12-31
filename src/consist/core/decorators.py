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
        func.__consist_step__ = StepDefinition(
            outputs=outputs, tags=tags, description=description
        )
        return func

    return decorator
