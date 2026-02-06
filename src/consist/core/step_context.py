from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional, TypeVar, cast, overload
import warnings

_DefaultT = TypeVar("_DefaultT")
_ResolvedT = TypeVar("_ResolvedT")


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
    consist_settings: Optional[object] = None
    consist_workspace: Optional[Path] = None
    consist_state: Optional[object] = None
    runtime_settings: Optional[object] = None
    runtime_workspace: Optional[object] = None
    runtime_state: Optional[object] = None
    runtime_kwargs: Mapping[str, object] = field(default_factory=dict)

    @property
    def settings(self) -> Optional[object]:
        """
        Deprecated compatibility alias.

        Prefer ``runtime_settings`` for workflow/runtime config and
        ``consist_settings`` for Consist internal settings.
        """
        warnings.warn(
            "StepContext.settings is deprecated; use StepContext.runtime_settings "
            "or StepContext.consist_settings.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.runtime_settings is not None:
            return self.runtime_settings
        return self.consist_settings

    @property
    def workspace(self) -> Optional[object]:
        """
        Deprecated compatibility alias.

        Prefer ``runtime_workspace`` for workflow/runtime workspace and
        ``consist_workspace`` for Consist internal workspace.
        """
        warnings.warn(
            "StepContext.workspace is deprecated; use StepContext.runtime_workspace "
            "or StepContext.consist_workspace.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.runtime_workspace is not None:
            return self.runtime_workspace
        return self.consist_workspace

    @property
    def state(self) -> Optional[object]:
        """
        Deprecated compatibility alias.

        Prefer ``runtime_state`` for workflow/runtime state and
        ``consist_state`` for Consist internal state.
        """
        warnings.warn(
            "StepContext.state is deprecated; use StepContext.runtime_state "
            "or StepContext.consist_state.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self.runtime_state is not None:
            return self.runtime_state
        return self.consist_state

    def __getitem__(self, key: str) -> Optional[object]:
        """Allow dict-style access for convenience."""
        runtime_value = self.get_runtime(key, default=None)
        if runtime_value is not None:
            return runtime_value
        return cast(Optional[object], getattr(self, key, None))

    def get(
        self, key: str, default: _DefaultT | None = None
    ) -> object | _DefaultT | None:
        """Dict-style get with default."""
        value = self.get_runtime(key, default=None)
        if value is None:
            value = cast(Optional[object], getattr(self, key, None))
        return default if value is None else value

    def get_runtime(
        self, name: str, default: _DefaultT | None = None
    ) -> object | _DefaultT | None:
        """Fetch workflow runtime values with explicit precedence."""
        runtime_attr = getattr(self, f"runtime_{name}", None)
        if runtime_attr is not None:
            return cast(object, runtime_attr)
        if name in self.runtime_kwargs:
            return self.runtime_kwargs[name]
        return default

    def require_runtime(self, name: str) -> object:
        """Fetch a required runtime value or raise a clear error."""
        sentinel = object()
        value = self.get_runtime(name, default=sentinel)
        if value is sentinel:
            raise ValueError(
                f"Missing runtime value {name!r} in StepContext runtime fields/kwargs."
            )
        return value


@overload
def resolve_metadata(
    value: Callable[[StepContext], _ResolvedT], ctx: StepContext
) -> _ResolvedT: ...


@overload
def resolve_metadata(value: _ResolvedT, ctx: StepContext) -> _ResolvedT: ...


def resolve_metadata(value: object, ctx: StepContext) -> object:
    """
    Resolve potentially-callable metadata value.

    If value is a callable (but not a class), invoke it with ctx.
    Otherwise return value unchanged.
    """
    if callable(value) and not isinstance(value, type):
        return cast(Callable[[StepContext], object], value)(ctx)
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
