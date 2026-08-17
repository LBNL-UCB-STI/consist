from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    cast,
    Iterable,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    TYPE_CHECKING,
    Union,
    runtime_checkable,
)

from sqlmodel import SQLModel

from consist.core.identity import IdentityManager
from consist.models.run import Run
from consist.types import IdentityInputs

RowFactory = Callable[[str], Iterable[dict[str, Any]]]
RowSource = Union[Iterable[dict[str, Any]], RowFactory]
ConfigReferenceStatus = Literal[
    "resolved",
    "missing_required",
    "missing_optional",
    "missing_ignored",
]
ConfigReferenceIdentityPolicy = Literal[
    "content_hash",
    "directory_hash",
    "fingerprint_manifest",
    "path_alias",
    "delegated_to_artifacts",
    "scalar_value",
    "ignored",
    "output_or_runtime_ignored",
]


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
    driver: Optional[str] = None


@dataclass(frozen=True)
class ConfigAdapterOptions:
    """
    Shared adapter options for config canonicalization.

    Attributes
    ----------
    strict : bool
        If True, adapters should error on missing references.
    bundle : bool
        Whether to bundle configs when the adapter supports it.
    ingest : bool
        Whether to ingest queryable slices after canonicalization.
    allow_heuristic_refs : Optional[bool]
        Whether adapters should scan heuristic keys for references. ``None``
        means use the adapter's constructor default.
    """

    strict: bool = False
    bundle: bool = True
    ingest: bool = True
    allow_heuristic_refs: Optional[bool] = None
    path_aliases: Optional[Mapping[str, Union[str, Path]]] = None


@dataclass(frozen=True)
class ConfigPathAlias:
    alias: str
    path: Union[str, Path]
    role: Optional[str] = None

    def to_meta_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "path": _json_safe(self.path),
            "role": self.role,
        }


@dataclass(frozen=True)
class ConfigReference:
    config_key: Optional[str]
    raw_value: str
    canonical_value: Optional[str]
    status: ConfigReferenceStatus
    required: bool
    role: Optional[str] = None
    identity_policy: ConfigReferenceIdentityPolicy = "content_hash"
    reason: Optional[str] = None
    hash: Optional[str] = None
    delegated_artifact_keys: tuple[str, ...] = ()

    def to_meta_dict(self) -> dict[str, Any]:
        data = {
            "config_key": self.config_key,
            "raw_value": self.raw_value,
            "canonical_value": self.canonical_value,
            "status": self.status,
            "required": self.required,
            "role": self.role,
            "identity_policy": self.identity_policy,
            "reason": self.reason,
            "hash": self.hash,
            "delegated_artifact_keys": list(self.delegated_artifact_keys),
        }
        return {key: value for key, value in data.items() if value not in (None, [])}


def _freeze_snapshot_metadata(value: Any) -> Any:
    """
    Recursively freeze runtime observation metadata.

    Parameters
    ----------
    value : Any
        Adapter-provided metadata value to preserve in a snapshot.

    Returns
    -------
    Any
        A recursively immutable equivalent: mappings become mapping proxies and
        lists or tuples become tuples. Scalar values are returned unchanged.
    """
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_snapshot_metadata(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_snapshot_metadata(item) for item in value)
    return value


@dataclass(frozen=True)
class CanonicalizationArtifactMember:
    """
    Represent one exact artifact selected under a config reference.

    Parameters
    ----------
    role : str
        Adapter-defined semantic role for the selected artifact.
    resolved_path : pathlib.Path
        Local path observed during canonicalization. This is not a portable
        identity or a final staged/container execution path.
    artifact_key : str
        Exact persisted key of the emitted adapter artifact.
    metadata : Mapping[str, Any]
        Immutable, adapter-defined selection facts. Nested mappings and
        sequences are frozen when the observation is created.
    """

    role: str
    resolved_path: Path
    artifact_key: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Freeze nested metadata after dataclass construction.

        Notes
        -----
        ``CanonicalizationArtifactMember`` is frozen, but a mutable mapping
        supplied by an adapter would otherwise remain mutable through the
        ``metadata`` attribute.
        """
        object.__setattr__(self, "metadata", _freeze_snapshot_metadata(self.metadata))


@dataclass(frozen=True)
class CanonicalizationReference:
    """
    Runtime facts observed for one canonicalized configuration reference.

    Parameters
    ----------
    reference : ConfigReference
        Portable identity facts for the configuration reference.
    resolved_path : Optional[Path]
        Local path observed during canonicalization, if one was resolved. This
        is not an execution path and must not be used as portable identity.
    artifact_keys : tuple[str, ...]
        Exact artifact keys logged for this reference. A reference can map to
        zero, one, or many artifacts.
    artifact_members : tuple[CanonicalizationArtifactMember, ...]
        Exact member-level observations for artifacts whose semantic role and
        local observed path must remain associated with this reference.
    """

    reference: ConfigReference
    resolved_path: Optional[Path]
    artifact_keys: tuple[str, ...] = ()
    artifact_members: tuple[CanonicalizationArtifactMember, ...] = ()


@dataclass(frozen=True)
class CanonicalizationSnapshot:
    """
    Immutable runtime view of one adapter canonicalization result.

    Parameters
    ----------
    adapter_name : str
        Name of the adapter that produced the snapshot.
    adapter_version : Optional[str]
        Adapter version, when declared by the adapter.
    identity_hash : str
        Existing portable canonical identity hash.
    references : tuple[CanonicalizationReference, ...]
        Ordered reference observations from the same canonicalization pass.
    """

    adapter_name: str
    adapter_version: Optional[str]
    identity_hash: str
    references: tuple[CanonicalizationReference, ...] = ()


@dataclass(frozen=True)
class DirectoryIdentity:
    canonical_value: str
    role: Optional[str]
    identity_policy: str
    hash_strategy: Optional[str] = None
    hash: Optional[str] = None

    def to_meta_dict(self) -> dict[str, Any]:
        data = {
            "canonical_value": self.canonical_value,
            "role": self.role,
            "identity_policy": self.identity_policy,
            "hash_strategy": self.hash_strategy,
            "hash": self.hash,
        }
        return {key: value for key, value in data.items() if value is not None}


@dataclass(frozen=True)
class MaterializationRequirement:
    canonical_value: str
    required: bool
    role: Optional[str] = None
    reason: Optional[str] = None

    def to_meta_dict(self) -> dict[str, Any]:
        data = {
            "canonical_value": self.canonical_value,
            "required": self.required,
            "role": self.role,
            "reason": self.reason,
        }
        return {key: value for key, value in data.items() if value is not None}


@dataclass(frozen=True)
class CanonicalConfigIdentity:
    adapter_name: str
    adapter_version: Optional[str]
    primary_config: Optional[str]
    identity_hash: str
    identity_schema_version: int = 1
    scalar_hash: Optional[str] = None
    reference_hash: Optional[str] = None
    directory_hash: Optional[str] = None
    scalars: Mapping[str, Any] = field(default_factory=dict)
    references: tuple[ConfigReference, ...] = ()
    directories: tuple[DirectoryIdentity, ...] = ()
    materialization_requirements: tuple[MaterializationRequirement, ...] = ()
    diagnostics: tuple[ConfigDiagnostic, ...] = ()

    def to_meta_dict(self) -> dict[str, Any]:
        data = {
            "identity_schema_version": self.identity_schema_version,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "primary_config": self.primary_config,
            "identity_hash": self.identity_hash,
            "scalar_hash": self.scalar_hash,
            "reference_hash": self.reference_hash,
            "directory_hash": self.directory_hash,
            "scalars": _json_safe(dict(self.scalars)),
            "references": [ref.to_meta_dict() for ref in self.references],
            "directories": [item.to_meta_dict() for item in self.directories],
            "materialization_requirements": [
                item.to_meta_dict() for item in self.materialization_requirements
            ],
            "diagnostics": [
                {
                    "message": diagnostic.message,
                    "table_name": diagnostic.table_name,
                    "source_path": _json_safe(diagnostic.source_path),
                    "artifact_key": diagnostic.artifact_key,
                    "exception_type": diagnostic.exception_type,
                }
                for diagnostic in self.diagnostics
            ],
        }
        scalars = data["scalars"]
        if isinstance(scalars, Mapping):
            files = scalars.get("files")
            if files not in (None, [], ()):
                data["files"] = _json_safe(files)
            options = scalars.get("options")
            if options not in (None, {}, []):
                data["options"] = _json_safe(options)
        return {
            key: value for key, value in data.items() if value not in (None, {}, [], ())
        }


def canonical_identity_from_config(
    *,
    adapter_name: str,
    adapter_version: Optional[str],
    config: CanonicalConfig,
    identity_hash: Optional[str] = None,
) -> CanonicalConfigIdentity:
    return CanonicalConfigIdentity(
        adapter_name=adapter_name,
        adapter_version=adapter_version,
        primary_config=(
            config.primary_config.as_posix()
            if config.primary_config is not None
            else None
        ),
        identity_hash=identity_hash or config.content_hash,
    )


@dataclass(frozen=True)
class ConfigDiagnostic:
    """
    Structured validation diagnostics for config preparation.

    Attributes
    ----------
    message : str
        Human-readable diagnostic message.
    table_name : Optional[str]
        Optional ingestable table name tied to the diagnostic.
    source_path : Optional[Path]
        Optional file path tied to the diagnostic.
    artifact_key : Optional[str]
        Optional artifact key tied to the diagnostic.
    exception_type : Optional[str]
        Exception type if the diagnostic originated from an exception.
    """

    message: str
    table_name: Optional[str] = None
    source_path: Optional[Path] = None
    artifact_key: Optional[str] = None
    exception_type: Optional[str] = None


@dataclass(frozen=True)
class ConfigDiagnostics:
    """
    Validation diagnostics for a config plan.

    Attributes
    ----------
    warnings : tuple[ConfigDiagnostic, ...]
        Non-fatal warnings discovered during validation.
    errors : tuple[ConfigDiagnostic, ...]
        Errors discovered during validation.
    """

    warnings: tuple[ConfigDiagnostic, ...] = ()
    errors: tuple[ConfigDiagnostic, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class IngestSpec:
    """
    Specification for a table ingestion.

    Attributes
    ----------
    table_name : str
        Destination table name.
    schema : type[SQLModel]
        SQLModel schema describing the table.
    rows : Optional[RowSource]
        Iterable of row dicts to ingest, or a factory that accepts a run_id.
    source_path : Optional[Path]
        Optional path to regenerate rows at apply-time.
    source : Optional[str]
        Optional artifact key that provided the rows.
    content_hash : Optional[str]
        Optional content hash used for ingest de-duplication.
    dedupe_on_hash : bool
        Whether to skip ingestion when content_hash already exists.
    """

    table_name: str
    schema: type[SQLModel]
    rows: Optional[RowSource]
    source_path: Optional[Path] = None
    source: Optional[str] = None
    content_hash: Optional[str] = None
    dedupe_on_hash: bool = False

    def materialize_rows(self, run_id: str) -> Iterable[dict[str, Any]]:
        """
        Resolve rows, calling the factory if needed.
        """
        rows = self.rows
        if rows is None:
            return []
        if callable(rows):
            row_factory = cast(RowFactory, rows)
            return row_factory(run_id)
        return rows


class CanonicalizationResult(NamedTuple):
    """
    Output of adapter canonicalization.

    Attributes
    ----------
    artifacts : list[ArtifactSpec]
        Artifacts discovered for logging.
    ingestables : list[IngestSpec]
        Table ingestion specs for queryable config slices.
    identity : CanonicalConfigIdentity
        Structured adapter identity manifest.
    canonicalization : Optional[CanonicalizationSnapshot]
        Immutable runtime reference observations from the same canonicalization
        pass.
    """

    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    identity: CanonicalConfigIdentity
    canonicalization: Optional[CanonicalizationSnapshot] = None


class _IngestableDataFrameMixin:
    ingestables: list[IngestSpec]

    def to_df(
        self, table_names: Union[str, Iterable[str]], *, run_id: Optional[str] = None
    ):
        """
        Return rows from matching table(s) as a Pandas DataFrame.
        """
        return _ingestable_df(self.ingestables, table_names, run_id=run_id)

    def constants_df(self, *, run_id: Optional[str] = None):
        """
        Return constants rows as a Pandas DataFrame.
        """
        return self.to_df(["activitysim_constants_cache"], run_id=run_id)

    def coefficients_df(self, *, run_id: Optional[str] = None):
        """
        Return coefficients rows as a Pandas DataFrame.
        """
        return self.to_df(["activitysim_coefficients_cache"], run_id=run_id)

    def probabilities_df(self, *, run_id: Optional[str] = None):
        """
        Return probabilities rows as a Pandas DataFrame.
        """
        return self.to_df(["activitysim_probabilities_cache"], run_id=run_id)

    def probabilities_entries_df(self, *, run_id: Optional[str] = None):
        """
        Return probability entry rows as a Pandas DataFrame.
        """
        return self.to_df(["activitysim_probabilities_entries_cache"], run_id=run_id)

    def probabilities_meta_entries_df(self, *, run_id: Optional[str] = None):
        """
        Return probability metadata entry rows as a Pandas DataFrame.
        """
        return self.to_df(
            ["activitysim_probabilities_meta_entries_cache"], run_id=run_id
        )


@dataclass
class ConfigContribution(_IngestableDataFrameMixin):
    """
    Structured summary of artifacts and ingestion for a config run.

    Attributes
    ----------
    identity : CanonicalConfigIdentity
        Structured adapter identity manifest.
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
    canonicalization : Optional[CanonicalizationSnapshot]
        Immutable runtime reference observations preserved from the config plan.
    """

    identity: CanonicalConfigIdentity
    adapter_version: Optional[str]
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    facet: Optional[dict[str, Any]] = None
    facet_schema_name: Optional[str] = None
    facet_schema_version: Optional[Union[str, int]] = None
    meta: Optional[dict[str, Any]] = None
    canonicalization: Optional[CanonicalizationSnapshot] = None

    @property
    def identity_hash(self) -> str:
        return self.identity.identity_hash


@dataclass(frozen=True)
class ConfigPlan(_IngestableDataFrameMixin):
    """
    Pre-run plan for config canonicalization.

    Attributes
    ----------
    adapter_name : str
        Adapter name used to create the plan.
    adapter_version : Optional[str]
        Adapter version, if available.
    canonical : CanonicalConfig
        Discovered canonical config metadata.
    artifacts : list[ArtifactSpec]
        Artifact specs to log at apply time.
    ingestables : list[IngestSpec]
        Ingest specs to apply at apply time.
    identity : CanonicalConfigIdentity
        Structured adapter identity manifest.
    facet : Optional[dict[str, Any]]
        Optional facet data derived from the config plan.
    facet_schema_name : Optional[str]
        Optional facet schema name.
    facet_schema_version : Optional[Union[str, int]]
        Optional facet schema version.
    facet_index : Optional[bool]
        Optional flag controlling KV facet indexing.
    meta : Optional[dict[str, Any]]
        Optional metadata for the plan.
    diagnostics : Optional[ConfigDiagnostics]
        Optional diagnostics produced by validation.
    adapter : Optional[ConfigAdapter]
        Adapter instance for run-scoped artifacts, if available.
    canonicalization : Optional[CanonicalizationSnapshot]
        Immutable runtime reference observations produced by the adapter.
    """

    adapter_name: str
    adapter_version: Optional[str]
    canonical: CanonicalConfig
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    identity: CanonicalConfigIdentity
    facet: Optional[dict[str, Any]] = None
    facet_schema_name: Optional[str] = None
    facet_schema_version: Optional[Union[str, int]] = None
    facet_index: Optional[bool] = None
    meta: Optional[dict[str, Any]] = None
    diagnostics: Optional[ConfigDiagnostics] = None
    adapter: Optional["ConfigAdapter"] = None
    canonicalization: Optional[CanonicalizationSnapshot] = None

    @property
    def identity_hash(self) -> str:
        return self.identity.identity_hash

    @property
    def signature(self) -> str:
        return self.identity_hash


def _resolve_canonicalization_snapshot(
    *,
    identity: CanonicalConfigIdentity,
    artifacts: Sequence[ArtifactSpec],
    snapshot: Optional[CanonicalizationSnapshot],
) -> CanonicalizationSnapshot:
    """
    Validate or synthesize the runtime view for a canonicalization result.

    Parameters
    ----------
    identity : CanonicalConfigIdentity
        Portable identity produced by the adapter canonicalization pass.
    artifacts : Sequence[ArtifactSpec]
        Artifact specs emitted by that same pass, including any apply-time
        bundle artifact when validation occurs during plan application.
    snapshot : CanonicalizationSnapshot or None
        Adapter-provided runtime observations. ``None`` is valid only when the
        canonical identity has no references.

    Returns
    -------
    CanonicalizationSnapshot
        The validated adapter snapshot, or an empty snapshot aligned with an
        identity that contains no references.

    Raises
    ------
    ValueError
        If snapshot metadata, reference ordering, member keys, or member paths
        disagree with the canonical identity and emitted artifact specs.

    Notes
    -----
    This is the boundary that prevents runtime observations from becoming an
    independently invented configuration identity. It validates observations
    against the canonicalization result but does not claim they are final
    execution paths.
    """
    if snapshot is None:
        if identity.references:
            raise ValueError(
                "ConfigAdapter.canonicalize() must provide a "
                "CanonicalizationSnapshot when identity.references is non-empty."
            )
        return CanonicalizationSnapshot(
            adapter_name=identity.adapter_name,
            adapter_version=identity.adapter_version,
            identity_hash=identity.identity_hash,
        )

    if (
        snapshot.adapter_name != identity.adapter_name
        or snapshot.adapter_version != identity.adapter_version
        or snapshot.identity_hash != identity.identity_hash
    ):
        raise ValueError(
            "CanonicalizationSnapshot adapter metadata must match the canonical "
            "config identity."
        )

    references = tuple(item.reference for item in snapshot.references)
    if references != identity.references:
        raise ValueError(
            "CanonicalizationSnapshot references must exactly match the ordered "
            "canonical config identity references."
        )

    artifacts_by_key = {artifact.key: artifact for artifact in artifacts}
    artifact_keys = set(artifacts_by_key)
    unlisted_member_keys = sorted(
        {
            member.artifact_key
            for item in snapshot.references
            for member in item.artifact_members
            if member.artifact_key not in item.artifact_keys
        }
    )
    if unlisted_member_keys:
        raise ValueError(
            "CanonicalizationSnapshot member artifact keys are not listed on "
            f"its parent reference: {unlisted_member_keys}."
        )
    missing_keys = sorted(
        {
            key
            for item in snapshot.references
            for key in item.artifact_keys
            if key not in artifact_keys
        }
    )
    if missing_keys:
        raise ValueError(
            "CanonicalizationSnapshot references unknown artifact keys: "
            f"{missing_keys}."
        )
    path_mismatches = sorted(
        {
            member.artifact_key
            for item in snapshot.references
            for member in item.artifact_members
            if member.resolved_path
            != artifacts_by_key[member.artifact_key].path.resolve()
        }
    )
    if path_mismatches:
        raise ValueError(
            "CanonicalizationSnapshot member paths do not match emitted "
            f"artifacts: {path_mismatches}."
        )
    return snapshot


class ConfigAdapter(Protocol):
    """
    Define the contract for model-specific config canonicalization adapters.

    Attributes
    ----------
    model_name : str
        Stable model namespace used in persisted identity metadata.

    Notes
    -----
    Implementations must return a ``CanonicalizationSnapshot`` whenever their
    identity contains configuration references. The snapshot must describe the
    same ordered reference sequence and emitted artifacts as the identity; it
    is validated before plans or contributions become runtime context.
    """

    model_name: str

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity: IdentityManager,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalConfig:
        """
        Discover source configuration before canonicalization.

        Parameters
        ----------
        root_dirs : list[pathlib.Path]
            Ordered config roots selected by the caller.
        identity : IdentityManager
            Helper used to normalize and hash discovered config content.
        strict : bool, default False
            Whether adapter-specific missing configuration should raise.
        options : ConfigAdapterOptions, optional
            Structured overrides for the adapter discovery pass.

        Returns
        -------
        CanonicalConfig
            Discovered config files, roots, primary config, and content hash.
        """
        ...

    def canonicalize(
        self,
        config: CanonicalConfig,
        *,
        run: Optional[Run] = None,
        tracker: Optional["Tracker"] = None,
        strict: bool = False,
        plan_only: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalizationResult:
        """
        Produce artifacts, identity, ingestion, and runtime observations.

        Parameters
        ----------
        config : CanonicalConfig
            Result previously returned by :meth:`discover`.
        run : Run, optional
            Active run for run-scoped work; may be absent in plan-only mode.
        tracker : Tracker, optional
            Tracker available for adapter-specific identity operations.
        strict : bool, default False
            Whether unresolved required references should raise.
        plan_only : bool, default False
            Whether to avoid run-scoped side effects while producing specs.
        options : ConfigAdapterOptions, optional
            Structured overrides for the canonicalization pass.

        Returns
        -------
        CanonicalizationResult
            Identity-aligned artifacts, ingest specs, and runtime snapshot.
        """
        ...

    def build_facet(
        self, config: CanonicalConfig, *, facet_spec: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """
        Extract optional facet values from discovered config.

        Parameters
        ----------
        config : CanonicalConfig
            Adapter-discovered configuration metadata.
        facet_spec : dict[str, Any]
            Adapter-specific facet selection request.

        Returns
        -------
        dict[str, Any] or None
            Normalizable facet data, or ``None`` when the adapter does not
            implement facets.
        """
        return None


@runtime_checkable
class SupportsRunWithConfigOverrides(Protocol):
    """
    Define optional override-driven config execution support.

    Implementations stage a derived config, run a supplied callable, and retain
    enough identity metadata to distinguish the override run from its base.
    """

    def run_with_config_overrides(
        self,
        *,
        tracker: "Tracker",
        base_run_id: Optional[str] = None,
        base_config_dirs: Optional[Sequence[Path]] = None,
        base_primary_config: Optional[Path] = None,
        overrides: Any,
        output_dir: Path,
        fn: Callable[..., Any],
        name: str,
        model: Optional[str] = None,
        config: Optional[dict[str, Any]] = None,
        outputs: Optional[list[str]] = None,
        execution_options: Any = None,
        strict: bool = True,
        identity_inputs: IdentityInputs = None,
        resolved_config_identity: Literal["auto", "off"] = "auto",
        identity_label: str = "activitysim_config",
        override_runtime_kwargs: Optional[Mapping[str, Any]] = None,
        **run_kwargs: Any,
    ) -> Any:
        """
        Execute a callable with a staged config derived from one base config.

        Parameters
        ----------
        tracker : Tracker
            Tracker that owns the base lookup and resulting run.
        base_run_id, base_config_dirs, base_primary_config
            Alternative selectors for the source config; implementations define
            their exact precedence and validation.
        overrides : Any
            Adapter-defined override payload.
        output_dir : pathlib.Path
            Empty directory where the derived configuration is staged.
        fn : callable
            Callable to execute with the derived configuration.
        name : str
            Name for the resulting Consist run.
        **run_kwargs
            Additional run invocation options accepted by the implementation.

        Returns
        -------
        Any
            Implementation-defined run result.
        """
        ...


def _ingestable_df(
    ingestables: Iterable[IngestSpec],
    table_names: str | Iterable[str],
    *,
    run_id: Optional[str] = None,
):
    """
    Collect rows from matching ingestables and return a Pandas DataFrame.
    """
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("Pandas is required to build DataFrames.") from exc
    table_names = [table_names] if isinstance(table_names, str) else list(table_names)
    rows: list[dict[str, Any]] = []
    for spec in ingestables:
        if spec.table_name not in table_names:
            continue
        resolved_run_id = run_id or "plan"
        rows.extend(list(spec.materialize_rows(resolved_run_id)))
    return pd.DataFrame(rows)


def validate_config_plan(
    plan: ConfigPlan, *, run_id: Optional[str] = None
) -> ConfigDiagnostics:
    """
    Validate a config plan without ingesting data.

    Parameters
    ----------
    plan : ConfigPlan
        Config plan to validate.
    run_id : Optional[str], optional
        Run identifier to use when materializing row factories.

    Returns
    -------
    ConfigDiagnostics
        Structured diagnostics describing warnings and errors.
    """
    errors: list[ConfigDiagnostic] = []
    warning_list: list[ConfigDiagnostic] = []
    resolved_run_id = run_id or "validation"

    for artifact in plan.artifacts:
        if not artifact.path.exists():
            errors.append(
                ConfigDiagnostic(
                    message=f"Missing artifact path: {artifact.path}",
                    source_path=artifact.path,
                    artifact_key=artifact.key,
                )
            )

    for spec in plan.ingestables:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            try:
                rows = spec.materialize_rows(resolved_run_id)
                for _ in rows:
                    pass
            except Exception as exc:
                errors.append(
                    ConfigDiagnostic(
                        message=str(exc),
                        table_name=spec.table_name,
                        source_path=spec.source_path,
                        exception_type=exc.__class__.__name__,
                    )
                )
            for warning in captured:
                warning_list.append(
                    ConfigDiagnostic(
                        message=str(warning.message),
                        table_name=spec.table_name,
                        source_path=spec.source_path,
                        exception_type=warning.category.__name__,
                    )
                )

    return ConfigDiagnostics(warnings=tuple(warning_list), errors=tuple(errors))


__all__ = [
    "CanonicalConfig",
    "CanonicalConfigIdentity",
    "ConfigPathAlias",
    "ConfigReference",
    "CanonicalizationReference",
    "CanonicalizationArtifactMember",
    "CanonicalizationSnapshot",
    "ConfigReferenceStatus",
    "ConfigReferenceIdentityPolicy",
    "DirectoryIdentity",
    "MaterializationRequirement",
    "ArtifactSpec",
    "ConfigAdapterOptions",
    "ConfigDiagnostic",
    "ConfigDiagnostics",
    "IngestSpec",
    "CanonicalizationResult",
    "ConfigContribution",
    "ConfigPlan",
    "ConfigAdapter",
    "SupportsRunWithConfigOverrides",
    "RowFactory",
    "RowSource",
    "canonical_identity_from_config",
    "compute_config_pack_hash",
    "validate_config_plan",
]


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
        digest_map[label] = identity.digest_path(
            root_dir,
            hashing_strategy_override="full",
        )
    return identity.canonical_json_sha256({"config_dirs": digest_map})
