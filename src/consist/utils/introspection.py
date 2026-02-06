from __future__ import annotations

import warnings
from typing import Any, Callable, Dict, Iterable, List, Optional

from consist.core.step_context import StepContext, resolve_metadata


def _step_name(step: Callable[..., Any]) -> str:
    return getattr(step, "__name__", step.__class__.__name__)


def _normalize_outputs(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple) or isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        return [value]
    try:
        return list(value)
    except TypeError:
        return []


def collect_step_schema(
    steps: Iterable[Callable[..., Any]],
    settings: Optional[Any] = None,
    extra_keys: Optional[Dict[str, str]] = None,
    warn_unresolvable: bool = True,
) -> Dict[str, str]:
    """
    Extract outputs and descriptions from @define_step decorated functions.

    Args:
        steps: Iterable of decorated step functions
        settings: Settings object for resolving callable outputs
        extra_keys: Additional keys to include (e.g., init keys, static inputs)
        warn_unresolvable: Warn when callable outputs can't be resolved

    Returns:
        Dict mapping output keys to descriptions
    """
    schema: Dict[str, str] = dict(extra_keys or {})

    for step in steps:
        defn = getattr(step, "__consist_step__", None)
        if not defn:
            continue

        step_name = _step_name(step)
        schema_outputs = getattr(defn, "schema_outputs", None)
        outputs_value = (
            schema_outputs
            if schema_outputs is not None
            else getattr(defn, "outputs", None)
        )

        if callable(outputs_value):
            if settings is not None:
                try:
                    ctx = StepContext(
                        func_name=step_name,
                        runtime_settings=settings,
                        runtime_kwargs={"settings": settings},
                    )
                    outputs = _normalize_outputs(resolve_metadata(outputs_value, ctx))
                except Exception as exc:
                    if warn_unresolvable:
                        warnings.warn(
                            "Cannot resolve callable outputs for "
                            f"{step_name}: {exc}. "
                            "Consider adding schema_outputs to @define_step.",
                            UserWarning,
                            stacklevel=2,
                        )
                    outputs = []
            else:
                if warn_unresolvable:
                    warnings.warn(
                        "Callable outputs for "
                        f"{step_name} require settings. "
                        "Consider adding schema_outputs to @define_step.",
                        UserWarning,
                        stacklevel=2,
                    )
                outputs = []
        else:
            outputs = _normalize_outputs(outputs_value)

        description_value = getattr(defn, "description", None)
        if callable(description_value) and settings is not None:
            try:
                ctx = StepContext(
                    func_name=step_name,
                    runtime_settings=settings,
                    runtime_kwargs={"settings": settings},
                )
                description_value = resolve_metadata(description_value, ctx)
            except Exception:
                description_value = None
        description = description_value or f"Output from {step_name}"
        for key in outputs:
            schema.setdefault(key, description)

    return schema
