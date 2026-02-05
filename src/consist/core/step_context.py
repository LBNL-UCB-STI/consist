from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class StepContext:
    """
    Context passed to callable metadata resolvers.

    When metadata fields in @define_step are callables, they receive
    this context as their single argument.
    """

    func_name: str
    model: Optional[str] = None
    year: Optional[int] = None
    iteration: Optional[int] = None
    phase: Optional[str] = None
    stage: Optional[str] = None
    settings: Optional[Any] = None
    workspace: Optional[Path] = None
    state: Optional[Any] = None
    runtime_kwargs: Optional[Dict[str, Any]] = None

    def __getitem__(self, key: str) -> Any:
        """Allow dict-style access for convenience."""
        value = getattr(self, key, None)
        if value is not None:
            return value
        if self.runtime_kwargs and key in self.runtime_kwargs:
            return self.runtime_kwargs[key]
        return None

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-style get with default."""
        value = self[key]
        return default if value is None else value


def resolve_metadata(value: Any, ctx: StepContext) -> Any:
    """
    Resolve potentially-callable metadata value.

    If value is a callable (but not a class), invoke it with ctx.
    Otherwise return value unchanged.
    """
    if callable(value) and not isinstance(value, type):
        return value(ctx)
    return value


class _DefaultFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return ""


def format_step_name(template: str, ctx: StepContext) -> str:
    """Format step name, omitting missing fields rather than erroring."""
    values = _DefaultFormatDict(
        {
            "func_name": ctx.func_name,
            "model": ctx.model or ctx.func_name,
            "year": "" if ctx.year is None else ctx.year,
            "iteration": "" if ctx.iteration is None else ctx.iteration,
            "stage": ctx.stage or "",
            "phase": ctx.phase or "",
        }
    )
    return template.format_map(values)
