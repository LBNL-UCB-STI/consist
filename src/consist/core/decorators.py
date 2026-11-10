from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    TYPE_CHECKING,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    Dict,
)

from consist.core.step_context import StepContext
from consist.types import IdentityInputs, InputBindingMode, OutputPathRef, OutputSet

if TYPE_CHECKING:
    from consist.core.config_canonicalization import ConfigAdapter

T = TypeVar("T")
MetaValue = Union[T, Callable[[StepContext], T]]
DeclaredOutputsLike = Union[Sequence[str], Mapping[str, OutputPathRef]]


@dataclass(frozen=True)
class StepDefinition:
    """Metadata attached to functions by @define_step."""

    # Identity & naming
    model: Optional[MetaValue[str]] = None
    name_template: Optional[MetaValue[str]] = None
    description: Optional[MetaValue[str]] = None

    # Outputs
    outputs: Optional[MetaValue[DeclaredOutputsLike]] = None
    schema_outputs: Optional[MetaValue[List[str]]] = None
    output_paths: Optional[MetaValue[Mapping[str, Any]]] = None
    output_sets: Optional[MetaValue[Mapping[str, OutputSet]]] = None

    # Inputs
    inputs: Optional[MetaValue[Union[Mapping[str, Any], Iterable[Any]]]] = None
    input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None
    optional_input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None

    # Config & facets
    config: Optional[MetaValue[Dict[str, Any]]] = None
    adapter: Optional[MetaValue["ConfigAdapter"]] = None
    identity_inputs: Optional[MetaValue[IdentityInputs]] = None
    facet: Optional[MetaValue[Any]] = None
    facet_index: Optional[MetaValue[bool]] = None

    # Caching
    cache_mode: Optional[MetaValue[str]] = None
    cache_hydration: Optional[MetaValue[str]] = None
    cache_version: Optional[MetaValue[int]] = None
    validate_cached_outputs: Optional[MetaValue[str]] = None

    # Runtime
    input_binding: Optional[MetaValue[InputBindingMode]] = None
    load_inputs: Optional[MetaValue[bool]] = None

    # Metadata
    tags: Optional[MetaValue[List[str]]] = None
    facet_from: Optional[MetaValue[List[str]]] = None
    facet_schema_version: Optional[MetaValue[Union[str, int]]] = None

    # Forward compatibility
    extra: Optional[Mapping[str, Any]] = None


def define_step(
    *,
    model: Optional[MetaValue[str]] = None,
    name_template: Optional[MetaValue[str]] = None,
    outputs: Optional[MetaValue[DeclaredOutputsLike]] = None,
    schema_outputs: Optional[MetaValue[List[str]]] = None,
    output_paths: Optional[MetaValue[Mapping[str, Any]]] = None,
    output_sets: Optional[MetaValue[Mapping[str, OutputSet]]] = None,
    inputs: Optional[MetaValue[Union[Mapping[str, Any], Iterable[Any]]]] = None,
    input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None,
    optional_input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None,
    config: Optional[MetaValue[Dict[str, Any]]] = None,
    adapter: Optional[MetaValue["ConfigAdapter"]] = None,
    identity_inputs: Optional[MetaValue[IdentityInputs]] = None,
    facet: Optional[MetaValue[Any]] = None,
    facet_index: Optional[MetaValue[bool]] = None,
    cache_mode: Optional[MetaValue[str]] = None,
    cache_hydration: Optional[MetaValue[str]] = None,
    cache_version: Optional[MetaValue[int]] = None,
    validate_cached_outputs: Optional[MetaValue[str]] = None,
    input_binding: Optional[MetaValue[InputBindingMode]] = None,
    load_inputs: Optional[MetaValue[bool]] = None,
    tags: Optional[MetaValue[List[str]]] = None,
    facet_from: Optional[MetaValue[List[str]]] = None,
    facet_schema_version: Optional[MetaValue[Union[str, int]]] = None,
    description: Optional[MetaValue[str]] = None,
    **extra: Any,
) -> Callable[[Callable], Callable]:
    """
    Attach metadata to a function without changing its execution behavior.

    This is used by Tracker.run/ScenarioContext.run to infer defaults. Callable
    values are resolved at runtime with a StepContext.

    ``output_sets`` may be supplied here as reusable defaults for steps that
    write one logical output as many files. Each ``OutputSet`` needs only
    ``root`` and ``include``; ``expected_members`` and ``expected_count`` are
    optional validation checks.
    """
    removed_kwargs = [name for name in ("config_plan", "hash_inputs") if name in extra]
    if removed_kwargs:
        if len(removed_kwargs) == 1:
            raise TypeError(f"unexpected keyword argument '{removed_kwargs[0]}'")
        joined = "', '".join(sorted(removed_kwargs))
        raise TypeError(f"unexpected keyword arguments '{joined}'")

    def decorator(func: Callable) -> Callable:
        func.__dict__["__consist_step__"] = StepDefinition(
            model=model,
            name_template=name_template,
            description=description,
            outputs=outputs,
            schema_outputs=schema_outputs,
            output_paths=output_paths,
            output_sets=output_sets,
            inputs=inputs,
            input_keys=input_keys,
            optional_input_keys=optional_input_keys,
            config=config,
            adapter=adapter,
            identity_inputs=identity_inputs,
            facet=facet,
            facet_index=facet_index,
            cache_mode=cache_mode,
            cache_hydration=cache_hydration,
            cache_version=cache_version,
            validate_cached_outputs=validate_cached_outputs,
            input_binding=input_binding,
            load_inputs=load_inputs,
            tags=tags,
            facet_from=facet_from,
            facet_schema_version=facet_schema_version,
            extra=extra or None,
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
        func.__dict__["__consist_runtime_required__"] = tuple(names)
        return func

    return decorator


def get_callable_name(fn: Optional[Any]) -> Optional[str]:
    """Return ``fn.__name__`` if ``fn`` is a named callable, else ``None``."""
    if fn is None:
        return None
    try:
        name = fn.__name__
    except AttributeError:
        return None
    return name if isinstance(name, str) and name else None


def get_step_definition(fn: Optional[Any]) -> StepDefinition:
    """Return the ``StepDefinition`` attached to ``fn``, or an empty ``StepDefinition()``."""
    if fn is None:
        return StepDefinition()
    try:
        candidate = fn.__consist_step__
    except AttributeError:
        return StepDefinition()
    return candidate if isinstance(candidate, StepDefinition) else StepDefinition()


def get_required_runtime_kwargs(fn: Optional[Any]) -> tuple[str, ...]:
    """Return the tuple of required runtime kwarg names attached to ``fn``."""
    if fn is None:
        return ()
    try:
        req = fn.__consist_runtime_required__
    except AttributeError:
        return ()
    return req if isinstance(req, tuple) else ()
