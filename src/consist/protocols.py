"""
Layer-1 structural protocols and active ``ArtifactResolver`` context hooks.

This module is a strict Layer-1 leaf:
- Zero imports from ``consist.models.*`` or ``consist.core.*``.
- Every ``@runtime_checkable`` protocol imports ``Protocol, runtime_checkable``
  from ``typing_extensions`` so Python 3.11 ``isinstance()`` never evaluates
  ``@property`` descriptors such as ``.path``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Sequence,
)
from typing_extensions import Protocol, runtime_checkable

from consist.types import ArtifactRef, OutputPathRef, OutputSet


@runtime_checkable
class ArtifactLike(Protocol):
    @property
    def path(self) -> Path: ...

    @property
    def uri(self) -> str: ...


@runtime_checkable
class ArtifactRecordLike(ArtifactLike, Protocol):
    """
    Structural type for persisted Consist artifact records.

    ``ArtifactLike`` is the narrow path/URI contract used by lightweight
    integrations. This richer protocol captures the stable fields expected by
    loaders, identity helpers, and schema-aware workflows.
    """

    id: Any
    key: str
    digest: Any
    member: Optional[str]
    meta: Mapping[str, Any]

    @property
    def driver(self) -> str: ...

    @property
    def abs_path(self) -> Optional[str]: ...


@runtime_checkable
class ArtifactResolver(Protocol):
    """Runtime resolution and loading service backing ``Artifact`` convenience methods."""

    def resolve_uri(self, uri: str) -> str: ...

    def resolve_artifact_path(self, artifact: Any) -> Path: ...

    def ensure_local_artifact_path(
        self,
        artifact: Any,
        *,
        target_root: Optional[Path | str] = None,
        source_root: Optional[Path | str] = None,
        on_missing: Literal["warn", "raise"] = "warn",
    ) -> Optional[Path]: ...

    def load_artifact_df(self, artifact: Any, **kwargs: Any) -> Any: ...

    def artifact_accessibility(self, artifact: Any) -> str: ...

    def artifact_recovery_roots(self, artifact: Any) -> list[str]: ...

    def artifact_abs_path_override(
        self, artifact: Any
    ) -> Optional[tuple[Optional[str], Optional[str]]]: ...


_ACTIVE_RESOLVER_STACK: ContextVar[Sequence[ArtifactResolver]] = ContextVar(
    "consist_active_resolver_stack", default=()
)
_DEFAULT_RESOLVER: ContextVar[Optional[ArtifactResolver]] = ContextVar(
    "consist_default_resolver", default=None
)


def get_active_resolver() -> Optional[ArtifactResolver]:
    """Return the innermost active ``ArtifactResolver`` or ``None``."""
    stack = _ACTIVE_RESOLVER_STACK.get()
    return stack[-1] if stack else None


def push_active_resolver(resolver: ArtifactResolver) -> None:
    """Push an ``ArtifactResolver`` onto the active context stack."""
    stack = _ACTIVE_RESOLVER_STACK.get()
    _ACTIVE_RESOLVER_STACK.set((*stack, resolver))


def pop_active_resolver() -> Optional[ArtifactResolver]:
    """Pop and return the top ``ArtifactResolver`` from the active context stack."""
    stack = _ACTIVE_RESOLVER_STACK.get()
    if not stack:
        return None
    resolver = stack[-1]
    _ACTIVE_RESOLVER_STACK.set(stack[:-1])
    return resolver


def get_default_resolver() -> Optional[ArtifactResolver]:
    """Return the default ``ArtifactResolver`` for the current context, if set."""
    return _DEFAULT_RESOLVER.get()


def set_default_resolver(
    resolver: Optional[ArtifactResolver],
) -> Optional[ArtifactResolver]:
    """Set the default ``ArtifactResolver`` for the current context and return the previous one."""
    previous = _DEFAULT_RESOLVER.get()
    _DEFAULT_RESOLVER.set(resolver)
    return previous


def resolve_active_or_default_resolver() -> Optional[ArtifactResolver]:
    """Return the active ``ArtifactResolver``, falling back to the default resolver."""
    active = get_active_resolver()
    if active is not None:
        return active
    return get_default_resolver()


@contextmanager
def bind_active_resolver(resolver: ArtifactResolver) -> Iterator[ArtifactResolver]:
    """Temporarily push ``resolver`` onto the active resolver stack."""
    push_active_resolver(resolver)
    try:
        yield resolver
    finally:
        pop_active_resolver()


@runtime_checkable
class RunLike(Protocol):
    id: str
    model_name: str
    status: str
    config_hash: Optional[str]
    input_hash: Optional[str]
    git_hash: Optional[str]
    signature: Optional[str]
    meta: Mapping[str, Any]


@runtime_checkable
class RunResultLike(Protocol):
    outputs: Mapping[str, ArtifactLike]
    cache_hit: bool


@runtime_checkable
class RunIdentifiedResultLike(RunResultLike, Protocol):
    run: RunLike


@runtime_checkable
class ScenarioLike(Protocol):
    def run(
        self,
        fn: Optional[Callable[..., object]] = None,
        name: Optional[str] = None,
        *,
        outputs: Optional[list[str]] = None,
        output_paths: Optional[Mapping[str, OutputPathRef]] = None,
        output_sets: Optional[Mapping[str, OutputSet]] = None,
        runtime_kwargs: Optional[Mapping[str, object]] = None,
        inject_context: bool | str = False,
        **kwargs: object,
    ) -> RunResultLike: ...

    def trace(self, name: str, **kwargs: object) -> object: ...


@runtime_checkable
class TrackerLike(Protocol):
    def log_output(
        self, path: ArtifactRef, key: Optional[str] = None, **metadata: object
    ) -> ArtifactLike: ...

    def log_input(
        self, path: ArtifactRef, key: Optional[str] = None, **metadata: object
    ) -> ArtifactLike: ...

    def log_artifacts(
        self, outputs: Mapping[str, ArtifactRef], **metadata: object
    ) -> Mapping[str, ArtifactLike]: ...

    def scenario(self, name: str, **kwargs: object) -> ScenarioLike: ...
