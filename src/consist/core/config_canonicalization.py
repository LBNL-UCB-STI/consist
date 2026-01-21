from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
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

RowFactory = Callable[[str], Iterable[dict[str, Any]]]
RowSource = Union[Iterable[dict[str, Any]], RowFactory]


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
    allow_heuristic_refs : bool
        Whether adapters should scan heuristic keys for references.
    """

    strict: bool = False
    bundle: bool = True
    ingest: bool = True
    allow_heuristic_refs: bool = True


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
        if self.rows is None:
            return []
        if callable(self.rows):
            return self.rows(run_id)
        return self.rows


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
    """

    adapter_name: str
    adapter_version: Optional[str]
    canonical: CanonicalConfig
    artifacts: list[ArtifactSpec]
    ingestables: list[IngestSpec]
    facet: Optional[dict[str, Any]] = None
    facet_schema_name: Optional[str] = None
    facet_schema_version: Optional[Union[str, int]] = None
    facet_index: Optional[bool] = None
    meta: Optional[dict[str, Any]] = None
    diagnostics: Optional[ConfigDiagnostics] = None
    adapter: Optional["ConfigAdapter"] = None

    @property
    def identity_hash(self) -> str:
        return self.canonical.content_hash

    @property
    def signature(self) -> str:
        return self.identity_hash


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
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalConfig: ...

    def canonicalize(
        self,
        config: CanonicalConfig,
        *,
        run: Optional[Run] = None,
        tracker: Optional["Tracker"] = None,
        strict: bool = False,
        plan_only: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalizationResult: ...

    def build_facet(
        self, config: CanonicalConfig, *, facet_spec: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """
        Optional: extract facet values from config.
        """
        return None


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
    "ArtifactSpec",
    "ConfigAdapterOptions",
    "ConfigDiagnostic",
    "ConfigDiagnostics",
    "IngestSpec",
    "CanonicalizationResult",
    "ConfigContribution",
    "ConfigPlan",
    "ConfigAdapter",
    "RowFactory",
    "RowSource",
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
        digest_map[label] = identity.digest_path(root_dir)
    return identity.canonical_json_sha256({"config_dirs": digest_map})
