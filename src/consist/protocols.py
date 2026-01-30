from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class ArtifactLike(Protocol):
    @property
    def path(self) -> Path: ...

    @property
    def container_uri(self) -> str: ...


@runtime_checkable
class RunResultLike(Protocol):
    outputs: Dict[str, ArtifactLike]
    cache_hit: bool


@runtime_checkable
class ScenarioLike(Protocol):
    def run(
        self,
        fn: Optional[Callable[..., Any]] = None,
        name: Optional[str] = None,
        *,
        outputs: Optional[list[str]] = None,
        output_paths: Optional[Mapping[str, Any]] = None,
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str = False,
        **kwargs: Any,
    ) -> RunResultLike: ...

    def trace(self, name: str, **kwargs: Any) -> Any: ...


@runtime_checkable
class TrackerLike(Protocol):
    def log_output(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> Optional[ArtifactLike]: ...

    def log_input(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> Optional[ArtifactLike]: ...

    def log_artifacts(
        self, outputs: Mapping[str, Any], **metadata: Any
    ) -> Mapping[str, ArtifactLike]: ...

    def scenario(self, name: str, **kwargs: Any) -> ScenarioLike: ...
