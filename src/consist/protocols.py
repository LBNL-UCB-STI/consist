from __future__ import annotations

from pathlib import Path
from typing import (
    Callable,
    Mapping,
    Optional,
    Protocol,
    TYPE_CHECKING,
    runtime_checkable,
)

from consist.types import ArtifactRef

if TYPE_CHECKING:
    from consist.models.run import Run


@runtime_checkable
class ArtifactLike(Protocol):
    @property
    def path(self) -> Path: ...

    @property
    def container_uri(self) -> str: ...


@runtime_checkable
class RunResultLike(Protocol):
    outputs: Mapping[str, ArtifactLike]
    cache_hit: bool


@runtime_checkable
class RunIdentifiedResultLike(RunResultLike, Protocol):
    run: "Run"


@runtime_checkable
class ScenarioLike(Protocol):
    def run(
        self,
        fn: Optional[Callable[..., object]] = None,
        name: Optional[str] = None,
        *,
        outputs: Optional[list[str]] = None,
        output_paths: Optional[Mapping[str, ArtifactRef]] = None,
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
