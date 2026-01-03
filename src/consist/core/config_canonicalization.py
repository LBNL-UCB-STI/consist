from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Iterable,
    Literal,
    NamedTuple,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Union,
)

from sqlmodel import SQLModel

from consist.core.identity import IdentityManager
from consist.models.run import Run


class CanonicalConfig(NamedTuple):
    root_dirs: list[Path]
    primary_config: Optional[Path]
    config_files: list[Path]
    external_files: list[Path]
    content_hash: str


class ArtifactSpec(NamedTuple):
    path: Path
    key: str
    direction: Literal["input", "output"]
    meta: dict[str, Any]


class IngestSpec(NamedTuple):
    table_name: str
    schema: type[SQLModel]
    rows: Iterable[dict[str, Any]]
    source: Optional[str] = None


class CanonicalizationResult(NamedTuple):
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]


@dataclass
class ConfigContribution:
    identity_hash: str
    adapter_version: Optional[str]
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    facet: Optional[dict[str, Any]] = None
    facet_schema_name: Optional[str] = None
    facet_schema_version: Optional[Union[str, int]] = None
    meta: Optional[dict[str, Any]] = None


class ConfigAdapter(Protocol):
    model_name: str

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity: IdentityManager,
        strict: bool = False,
    ) -> CanonicalConfig: ...

    def canonicalize(
        self,
        config: CanonicalConfig,
        *,
        run: Run,
        tracker: "Tracker",
        strict: bool = False,
    ) -> CanonicalizationResult: ...


if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker


def compute_config_pack_hash(
    *,
    root_dirs: list[Path],
    identity: IdentityManager,
) -> str:
    digest_map: dict[str, str] = {}
    for idx, root_dir in enumerate(root_dirs):
        label = f"config_dir_{idx}:{root_dir}"
        digest_map[label] = identity.digest_path(root_dir)
    return identity.canonical_json_sha256({"config_dirs": digest_map})
