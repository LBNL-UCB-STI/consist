from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
    Union,
    runtime_checkable,
)

from consist.types import (
    ArtifactRef,
    OutputArtifactSpec,
    OutputPathRef,
    OutputSet,
)

if TYPE_CHECKING:
    import pandas as pd

    from consist.artifact import Artifact
    from consist.location import Location
    from consist.models.run import Run
    from consist.store.ledger import StoreLedger

_RowModelT = TypeVar("_RowModelT")


@runtime_checkable
class ParameterizedSQLProtocol(Protocol):
    """Structural protocol for parameterized DuckDB SQL query objects."""

    @property
    def sql(self) -> str: ...

    @property
    def params(self) -> Sequence[Any]: ...


@runtime_checkable
class ArtifactLike(Protocol):
    @property
    def path(self) -> Path: ...

    @property
    def primary_location(self) -> Optional["Location"]: ...

    @property
    def member(self) -> Optional[str]: ...


@runtime_checkable
class ArtifactRecordLike(ArtifactLike, Protocol):
    """
    Structural type for persisted Consist artifact records.

    ``ArtifactLike`` is the narrow path/location contract used by lightweight
    integrations. This richer protocol captures the stable fields expected by
    loaders, identity helpers, and schema-aware workflows.
    """

    @property
    def id(self) -> Any: ...

    @property
    def key(self) -> str: ...

    @property
    def hash(self) -> str: ...

    @property
    def meta(self) -> Mapping[str, Any]: ...

    @property
    def driver(self) -> str: ...


@runtime_checkable
class LedgerProviderLike(Protocol):
    """Protocol for persistence services exposing a single `StoreLedger`."""

    @property
    def ledger(self) -> Optional["StoreLedger"]: ...

    def query_df(
        self,
        sql: Union[str, ParameterizedSQLProtocol],
        params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,
    ) -> "pd.DataFrame": ...

    def query_rows(
        self,
        sql: Union[str, ParameterizedSQLProtocol],
        params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,
        *,
        model: type[_RowModelT],
    ) -> list[_RowModelT]: ...


@runtime_checkable
class RunResultLike(Protocol):
    outputs: Mapping[str, "Artifact"]
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
        outputs: Optional[
            Union[
                list[str],
                Mapping[str, Union[OutputPathRef, OutputArtifactSpec, OutputSet]],
            ]
        ] = None,
        output_paths: Optional[Mapping[str, OutputPathRef]] = None,
        output_sets: Optional[Mapping[str, OutputSet]] = None,
        runtime_kwargs: Optional[Mapping[str, object]] = None,
        inject_context: bool | str = False,
        **kwargs: object,
    ) -> RunResultLike: ...

    def trace(self, name: str, **kwargs: object) -> object: ...


@runtime_checkable
class TrackerLike(Protocol):
    @property
    def ledger(self) -> Optional["StoreLedger"]: ...

    def log_output(
        self, path: ArtifactRef, key: Optional[str] = None, **metadata: object
    ) -> "Artifact": ...

    def log_input(
        self, path: ArtifactRef, key: Optional[str] = None, **metadata: object
    ) -> "Artifact": ...

    def log_artifacts(
        self, outputs: Mapping[str, ArtifactRef], **metadata: object
    ) -> Mapping[str, "Artifact"]: ...

    def scenario(self, name: str, **kwargs: object) -> ScenarioLike: ...
