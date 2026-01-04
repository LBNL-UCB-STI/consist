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
    """
    Canonical representation of a discovered configuration.

    Attributes
    ----------
    root_dirs : list[Path]
        Ordered configuration root directories.
    primary_config : Optional[Path]
        Primary config file (if any), such as a settings file.
    config_files : list[Path]
        Config files considered active for the run.
    external_files : list[Path]
        Referenced config files outside the root dirs.
    content_hash : str
        Deterministic hash of the config directory contents.
    """

    root_dirs: list[Path]
    primary_config: Optional[Path]
    config_files: list[Path]
    external_files: list[Path]
    content_hash: str


class ArtifactSpec(NamedTuple):
    """
    Specification for an artifact to be logged.

    Attributes
    ----------
    path : Path
        Filesystem path to the artifact.
    key : str
        Stable artifact key.
    direction : Literal["input", "output"]
        Artifact direction for provenance.
    meta : dict[str, Any]
        Additional metadata to attach to the artifact.
    """

    path: Path
    key: str
    direction: Literal["input", "output"]
    meta: dict[str, Any]


class IngestSpec(NamedTuple):
    """
    Specification for a table ingestion.

    Attributes
    ----------
    table_name : str
        Destination table name.
    schema : type[SQLModel]
        SQLModel schema describing the table.
    rows : Iterable[dict[str, Any]]
        Iterable of row dicts to ingest.
    source : Optional[str]
        Optional artifact key that provided the rows.
    """

    table_name: str
    schema: type[SQLModel]
    rows: Iterable[dict[str, Any]]
    source: Optional[str] = None


class CanonicalizationResult(NamedTuple):
    """
    Output of adapter canonicalization.

    Attributes
    ----------
    artifacts : list[ArtifactSpec]
        Artifacts discovered for logging.
    ingestables : list[IngestSpec]
        Table ingestion specs for queryable config slices.
    """

    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]


@dataclass
class ConfigContribution:
    """
    Structured summary of artifacts and ingestion for a config run.

    Attributes
    ----------
    identity_hash : str
        Hash that identifies the canonical config state.
    adapter_version : Optional[str]
        Adapter version used to generate the contribution.
    artifacts : list[ArtifactSpec]
        Artifact specs to log.
    ingestables : list[IngestSpec]
        Ingest specs to apply.
    facet : Optional[dict[str, Any]]
        Optional facet data stored alongside the run.
    facet_schema_name : Optional[str]
        Optional facet schema name.
    facet_schema_version : Optional[Union[str, int]]
        Optional facet schema version.
    meta : Optional[dict[str, Any]]
        Optional metadata for the contribution.
    """

    identity_hash: str
    adapter_version: Optional[str]
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    facet: Optional[dict[str, Any]] = None
    facet_schema_name: Optional[str] = None
    facet_schema_version: Optional[Union[str, int]] = None
    meta: Optional[dict[str, Any]] = None


class ConfigAdapter(Protocol):
    """
    Protocol for model-specific config canonicalization adapters.
    """

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
    """
    Compute a deterministic hash for a set of config directories.

    Parameters
    ----------
    root_dirs : list[Path]
        Ordered config directories.
    identity : IdentityManager
        Identity helper used for path digesting and hashing.

    Returns
    -------
    str
        Deterministic hash of config directory content.
    """
    digest_map: dict[str, str] = {}
    for idx, root_dir in enumerate(root_dirs):
        label = f"config_dir_{idx}:{root_dir}"
        digest_map[label] = identity.digest_path(root_dir)
    return identity.canonical_json_sha256({"config_dirs": digest_map})
