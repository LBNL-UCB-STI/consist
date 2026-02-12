from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    TYPE_CHECKING,
    Iterable,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
    Dict,
)

from consist.core.step_context import StepContext

if TYPE_CHECKING:
    from consist.core.config_canonicalization import ConfigPlan

T = TypeVar("T")
MetaValue = Union[T, Callable[[StepContext], T]]


@dataclass(frozen=True)
class StepDefinition:
    """Metadata attached to functions by @define_step."""

    # Identity & naming
    model: Optional[MetaValue[str]] = None
    name_template: Optional[MetaValue[str]] = None
    description: Optional[MetaValue[str]] = None

    # Outputs
    outputs: Optional[MetaValue[List[str]]] = None
    schema_outputs: Optional[MetaValue[List[str]]] = None
    output_paths: Optional[MetaValue[Mapping[str, Any]]] = None

    # Inputs
    inputs: Optional[MetaValue[Union[Mapping[str, Any], Iterable[Any]]]] = None
    input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None
    optional_input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None

    # Config & facets
    config: Optional[MetaValue[Dict[str, Any]]] = None
    config_plan: Optional[MetaValue["ConfigPlan"]] = None
    facet: Optional[MetaValue[Any]] = None
    facet_index: Optional[MetaValue[bool]] = None

    # Caching
    cache_mode: Optional[MetaValue[str]] = None
    cache_hydration: Optional[MetaValue[str]] = None
    cache_version: Optional[MetaValue[int]] = None
    validate_cached_outputs: Optional[MetaValue[str]] = None

    # Runtime
    load_inputs: Optional[MetaValue[bool]] = None
    hash_inputs: Optional[MetaValue[Any]] = None

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
    outputs: Optional[MetaValue[List[str]]] = None,
    schema_outputs: Optional[MetaValue[List[str]]] = None,
    output_paths: Optional[MetaValue[Mapping[str, Any]]] = None,
    inputs: Optional[MetaValue[Union[Mapping[str, Any], Iterable[Any]]]] = None,
    input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None,
    optional_input_keys: Optional[MetaValue[Union[Iterable[str], str]]] = None,
    config: Optional[MetaValue[Dict[str, Any]]] = None,
    config_plan: Optional[MetaValue["ConfigPlan"]] = None,
    facet: Optional[MetaValue[Any]] = None,
    facet_index: Optional[MetaValue[bool]] = None,
    cache_mode: Optional[MetaValue[str]] = None,
    cache_hydration: Optional[MetaValue[str]] = None,
    cache_version: Optional[MetaValue[int]] = None,
    validate_cached_outputs: Optional[MetaValue[str]] = None,
    load_inputs: Optional[MetaValue[bool]] = None,
    hash_inputs: Optional[MetaValue[Any]] = None,
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
    """

    def decorator(func: Callable) -> Callable:
        setattr(
            func,
            "__consist_step__",
            StepDefinition(
                model=model,
                name_template=name_template,
                description=description,
                outputs=outputs,
                schema_outputs=schema_outputs,
                output_paths=output_paths,
                inputs=inputs,
                input_keys=input_keys,
                optional_input_keys=optional_input_keys,
                config=config,
                config_plan=config_plan,
                facet=facet,
                facet_index=facet_index,
                cache_mode=cache_mode,
                cache_hydration=cache_hydration,
                cache_version=cache_version,
                validate_cached_outputs=validate_cached_outputs,
                load_inputs=load_inputs,
                hash_inputs=hash_inputs,
                tags=tags,
                facet_from=facet_from,
                facet_schema_version=facet_schema_version,
                extra=extra or None,
            ),
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
