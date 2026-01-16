from __future__ import annotations

import csv
import hashlib
import gzip
import json
import logging
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    IO,
    Any,
    Dict,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Set,
    TYPE_CHECKING,
    Union,
)

from sqlalchemy.sql import Executable
from sqlmodel import SQLModel, col

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfig,
    CanonicalizationResult,
    ConfigAdapterOptions,
    IngestSpec,
    compute_config_pack_hash,
)
from consist.core.identity import IdentityManager
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimCoefficientTemplateRefsCache,
    ActivitySimConstantsCache,
    ActivitySimProbabilitiesCache,
    ActivitySimProbabilitiesEntriesCache,
    ActivitySimProbabilitiesMetaEntriesCache,
    ActivitySimConfigIngestRunLink,
)
from consist.models.run import Run

if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


_REFERENCE_KEYS = {
    "SPEC",
    "SAMPLE_SPEC",
    "COEFFICIENTS",
    "INTERACTION_COEFFICIENTS",
    "COEFFICIENT_TEMPLATE",
    "LOGSUM_SETTINGS",
    "PROBS_SPEC",
}

_HEURISTIC_SUFFIXES = ("_FILE", "_PATH", "_SPEC", "_SETTINGS")

_COEFFICIENT_SUFFIXES = (
    "_coefficients.csv",
    "_coeffs.csv",
)

_COEFFICIENT_TEMPLATE_SUFFIXES = ("_coefficients_template.csv",)

_PROBABILITY_SUFFIXES = ("_probs.csv",)

_SETTINGS_ALLOWLIST = {
    "sample_rate",
    "chunk_size",
    "households_sample_size",
    "trace_hh_id",
}

_MODEL_ALIAS_MAP = {
    "compute_accessibility": "accessibility",
    "atwork_subtour_scheduling": "tour_scheduling_atwork",
    "atwork_subtour_mode_choice": "tour_mode_choice",
    "write_tables": "summarize",
}

_PROBABILITIES_META_KEYS = {
    "depart_range_start",
    "depart_range_end",
    "tour_hour",
    "trip_num",
    "vehicle_year",
}


@dataclass(frozen=True)
class _FileHandler:
    predicate: Any
    table_name: str
    schema: type
    row_fn: Any
    row_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class _IngestSpecBuilder:
    plan_only: bool
    run_id: Optional[str]

    def build_cache_specs(
        self,
        *,
        table_name: str,
        schema: type,
        row_fn: Any,
        source_path: Path,
        source_key: str,
        content_hash: str,
        strict: bool,
        row_kwargs: Optional[dict[str, Any]] = None,
        link: bool = True,
    ) -> list[IngestSpec]:
        if self.plan_only:

            def rows(
                run_id: str,
                p: Path = source_path,
                s: bool = strict,
                kwargs: dict[str, Any] = row_kwargs or {},
                h: str = content_hash,
            ) -> Iterable[dict[str, Any]]:
                return _with_content_hash(row_fn(p, run_id, strict=s, **kwargs), h)
        else:
            if self.run_id is None:
                raise RuntimeError("ActivitySim canonicalize requires run.")
            rows = _with_content_hash(
                row_fn(source_path, self.run_id, strict=strict, **(row_kwargs or {})),
                content_hash,
            )
        specs = [
            IngestSpec(
                table_name=table_name,
                schema=schema,
                rows=rows,
                source_path=source_path,
                source=source_key,
                content_hash=content_hash,
                dedupe_on_hash=True,
            )
        ]
        if link:
            specs.append(
                self.link_spec(
                    table_names=[table_name],
                    content_hash=content_hash,
                    source_path=source_path,
                    source_key=source_key,
                )
            )
        return specs

    def build_constants_specs(
        self,
        *,
        rows_factory: Any,
        source_path: Path,
        source_key: str,
        content_hash: str,
    ) -> list[IngestSpec]:
        if self.plan_only:

            def rows(
                run_id: str,
                rows_fn: Any = rows_factory,
                h: str = content_hash,
            ) -> Iterable[dict[str, Any]]:
                return _with_content_hash(rows_fn(run_id), h)
        else:
            if self.run_id is None:
                raise RuntimeError("ActivitySim canonicalize requires run.")
            rows = _with_content_hash(rows_factory(self.run_id), content_hash)
        return [
            IngestSpec(
                table_name="activitysim_constants_cache",
                schema=ActivitySimConstantsCache,
                rows=rows,
                source_path=source_path,
                source=source_key,
                content_hash=content_hash,
                dedupe_on_hash=True,
            ),
            self.link_spec(
                table_names=["activitysim_constants_cache"],
                content_hash=content_hash,
                source_path=source_path,
                source_key=source_key,
            ),
        ]

    def link_spec(
        self,
        *,
        table_names: list[str],
        content_hash: str,
        source_path: Path,
        source_key: str,
    ) -> IngestSpec:
        if self.plan_only:

            def rows(
                run_id: str,
                t: list[str] = table_names,
                h: str = content_hash,
                f: str = source_path.name,
            ) -> list[dict[str, Any]]:
                return [_run_link_row(run_id, table_name, h, f) for table_name in t]
        else:
            if self.run_id is None:
                raise RuntimeError("ActivitySim canonicalize requires run.")
            rows = [
                _run_link_row(self.run_id, table_name, content_hash, source_path.name)
                for table_name in table_names
            ]
        return IngestSpec(
            table_name="activitysim_config_ingest_run_link",
            schema=ActivitySimConfigIngestRunLink,
            rows=rows,
            source_path=source_path,
            source=source_key,
        )


_CSV_HANDLERS: list[_FileHandler]


@dataclass
class ActivitySimConfigAdapter:
    """
    Canonicalize ActivitySim config directories into Consist artifacts and tables.

    Attributes
    ----------
    model_name : str
        Model namespace used for Consist metadata.
    adapter_version : str
        Adapter version string stored in run metadata.
    allow_heuristic_refs : bool
        Whether to scan heuristic keys (e.g., *_FILE) for CSV references.
    bundle_configs : bool
        Whether to create a config bundle tarball artifact.
    bundle_cache_dir : Optional[Path]
        Optional shared bundle cache directory; defaults to `<run_dir>/config_bundles`.
    check_probabilities : bool
        Whether to warn when probability rows do not sum to 1.0.
    """

    model_name: str = "activitysim"
    adapter_version: str = "0.1"
    allow_heuristic_refs: bool = True
    bundle_configs: bool = True
    bundle_cache_dir: Optional[Path] = None
    check_probabilities: bool = False

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity: IdentityManager,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalConfig:
        """
        Discover ActivitySim config files and compute a content hash.

        Parameters
        ----------
        root_dirs : list[Path]
            Ordered config directories.
        identity : IdentityManager
            Identity helper used to hash config directories.
        strict : bool, default False
            If True, raise when `settings.yaml` is missing.

        Returns
        -------
        CanonicalConfig
            Canonical config metadata for downstream canonicalization.

        Raises
        ------
        ImportError
            If PyYAML is not available.
        FileNotFoundError
            If strict mode is enabled and `settings.yaml` is missing.
        """
        if yaml is None:
            raise ImportError("PyYAML is required for ActivitySim canonicalization.")
        resolved_strict = options.strict if options is not None else strict
        settings_path = _resolve_yaml_path("settings.yaml", root_dirs)
        if settings_path is None:
            subdir_candidates = _find_settings_yaml_subdirs(root_dirs)
            if subdir_candidates:
                candidates = ", ".join(str(path) for path in subdir_candidates)
                raise FileNotFoundError(
                    "settings.yaml not found in config_dirs. "
                    "Pass the specific config subdirectories in order. "
                    f"Found settings.yaml under: {candidates}"
                )
            if resolved_strict:
                raise FileNotFoundError("settings.yaml not found in config_dirs.")
            logging.warning(
                "[Consist][ActivitySim] settings.yaml not found in config_dirs=%s",
                [str(p) for p in root_dirs],
            )
        settings = (
            _load_effective_settings(root_dirs, settings_path=settings_path)
            if settings_path
            else {}
        )
        models = settings.get("models", [])
        config_files = _resolve_model_yamls(models, root_dirs)
        if settings_path:
            config_files.insert(0, settings_path)

        content_hash = compute_config_pack_hash(root_dirs=root_dirs, identity=identity)

        return CanonicalConfig(
            root_dirs=root_dirs,
            primary_config=settings_path,
            config_files=config_files,
            external_files=[],
            content_hash=content_hash,
        )

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
        Convert a discovered config into artifacts and ingestable tables.

        Parameters
        ----------
        config : CanonicalConfig
            Discovered config metadata for this run.
        run : Optional[Run]
            Active run used for artifact/ingest attribution.
        tracker : Optional[Tracker]
            Tracker instance used for artifact logging and ingest.
        strict : bool, default False
            If True, raise on missing referenced files.
        plan_only : bool, default False
            If True, return specs without run-scoped artifacts or row materialization.

        Returns
        -------
        CanonicalizationResult
            Artifact specs and ingest specs to apply to the run.

        Raises
        ------
        ImportError
            If PyYAML is not available.
        FileNotFoundError
            If strict mode is enabled and referenced files are missing.
        """
        if yaml is None:
            raise ImportError("PyYAML is required for ActivitySim canonicalization.")
        resolved_strict = options.strict if options is not None else strict
        allow_heuristics = (
            options.allow_heuristic_refs
            if options is not None
            else self.allow_heuristic_refs
        )
        bundle_configs = options.bundle if options is not None else self.bundle_configs

        settings = (
            _load_effective_settings(
                config.root_dirs, settings_path=config.primary_config
            )
            if config.primary_config
            else {}
        )
        inherit_settings = bool(settings.get("inherit_settings", False))

        artifacts: list[ArtifactSpec] = []
        ingestables: list[IngestSpec] = []
        artifacts_by_path: dict[Path, ArtifactSpec] = {}

        def add_artifact(path: Path, *, role: str) -> None:
            key = _artifact_key_for_path(path, config.root_dirs)
            spec = ArtifactSpec(
                path=path,
                key=key,
                direction="input",
                meta={"config_role": role},
            )
            artifacts_by_path[path] = spec

        for yaml_path in config.config_files:
            add_artifact(yaml_path, role="yaml")

        referenced = _collect_referenced_csvs(
            config.config_files,
            config.root_dirs,
            inherit_settings=inherit_settings,
            allow_heuristics=allow_heuristics,
            strict=resolved_strict,
        )
        for csv_path in referenced:
            add_artifact(csv_path, role="csv")

        if bundle_configs and not plan_only:
            if run is None or tracker is None:
                raise RuntimeError("ActivitySim bundling requires run and tracker.")
            bundle_path = _bundle_configs(
                config_dirs=config.root_dirs,
                run=run,
                tracker=tracker,
                content_hash=config.content_hash,
                cache_dir=self.bundle_cache_dir,
            )
            add_artifact(bundle_path, role="bundle")
            artifacts_by_path[bundle_path].meta["content_hash"] = config.content_hash

        artifacts = list(artifacts_by_path.values())

        builder = _IngestSpecBuilder(
            plan_only=plan_only, run_id=run.id if run else None
        )

        if config.primary_config:
            source_key = _artifact_key_for_path(config.primary_config, config.root_dirs)
            file_hash = config.content_hash

            def rows_factory(
                run_id: str,
                cfg: CanonicalConfig = config,
                s: dict[str, Any] = settings,
                inherit: bool = inherit_settings,
            ) -> Iterable[dict[str, Any]]:
                return _build_constants_rows(
                    run_id=run_id,
                    config=cfg,
                    settings=s,
                    inherit_settings=inherit,
                )

            ingestables.extend(
                builder.build_constants_specs(
                    rows_factory=rows_factory,
                    source_path=config.primary_config,
                    source_key=source_key,
                    content_hash=file_hash,
                )
            )

        for csv_path in referenced:
            file_name = csv_path.name
            key = _artifact_key_for_path(csv_path, config.root_dirs)
            file_hash = _digest_path_with_name(csv_path, tracker)
            if _is_probabilities_file(file_name):
                ingestables.extend(
                    self._build_probabilities_ingest_specs(
                        builder=builder,
                        csv_path=csv_path,
                        source_key=key,
                        content_hash=file_hash,
                        strict=resolved_strict,
                    )
                )
                continue

            for handler in _CSV_HANDLERS:
                if handler.predicate(file_name):
                    ingestables.extend(
                        builder.build_cache_specs(
                            table_name=handler.table_name,
                            schema=handler.schema,
                            row_fn=handler.row_fn,
                            source_path=csv_path,
                            source_key=key,
                            content_hash=file_hash,
                            strict=resolved_strict,
                            row_kwargs=handler.row_kwargs,
                        )
                    )
                    break

        return CanonicalizationResult(artifacts=artifacts, ingestables=ingestables)

    def _build_probabilities_ingest_specs(
        self,
        *,
        builder: _IngestSpecBuilder,
        csv_path: Path,
        source_key: str,
        content_hash: str,
        strict: bool,
    ) -> list[IngestSpec]:
        specs: list[IngestSpec] = []
        specs.extend(
            builder.build_cache_specs(
                table_name="activitysim_probabilities_cache",
                schema=ActivitySimProbabilitiesCache,
                row_fn=_iter_probabilities_rows,
                source_path=csv_path,
                source_key=source_key,
                content_hash=content_hash,
                strict=strict,
                row_kwargs={"check_probabilities": self.check_probabilities},
                link=False,
            )
        )
        specs.extend(
            builder.build_cache_specs(
                table_name="activitysim_probabilities_entries_cache",
                schema=ActivitySimProbabilitiesEntriesCache,
                row_fn=_iter_probabilities_entries,
                source_path=csv_path,
                source_key=source_key,
                content_hash=content_hash,
                strict=strict,
                link=False,
            )
        )
        specs.extend(
            builder.build_cache_specs(
                table_name="activitysim_probabilities_meta_entries_cache",
                schema=ActivitySimProbabilitiesMetaEntriesCache,
                row_fn=_iter_probabilities_meta_entries,
                source_path=csv_path,
                source_key=source_key,
                content_hash=content_hash,
                strict=strict,
                link=False,
            )
        )
        specs.append(
            builder.link_spec(
                table_names=[
                    "activitysim_probabilities_cache",
                    "activitysim_probabilities_entries_cache",
                    "activitysim_probabilities_meta_entries_cache",
                ],
                content_hash=content_hash,
                source_path=csv_path,
                source_key=source_key,
            )
        )
        return specs

    def bundle_artifact(
        self, config: CanonicalConfig, *, run: Run, tracker: "Tracker"
    ) -> Optional[ArtifactSpec]:
        """
        Build a bundle artifact spec for a run-scoped config archive.
        """
        if not self.bundle_configs:
            return None
        bundle_path = _bundle_configs(
            config_dirs=config.root_dirs,
            run=run,
            tracker=tracker,
            content_hash=config.content_hash,
            cache_dir=self.bundle_cache_dir,
        )
        spec = ArtifactSpec(
            path=bundle_path,
            key=_artifact_key_for_path(bundle_path, config.root_dirs),
            direction="input",
            meta={"config_role": "bundle", "content_hash": config.content_hash},
        )
        return spec

    def materialize(
        self,
        base_bundle: Path,
        overrides: "ConfigOverrides",
        *,
        output_dir: Path,
        identity: IdentityManager,
        strict: bool = True,
    ) -> CanonicalConfig:
        """
        Unpack a base config bundle, apply overrides, and return a new CanonicalConfig.

        Parameters
        ----------
        base_bundle : Path
            Path to the base config bundle tarball.
        overrides : ConfigOverrides
            Structured overrides applied to YAML constants/settings and coefficients.
        output_dir : Path
            Empty directory to stage the materialized config.
        identity : IdentityManager
            Identity helper used to compute deterministic content hashes.
        strict : bool, default True
            If True, error on missing override targets or missing `settings.yaml`.

        Returns
        -------
        CanonicalConfig
            Canonical config metadata for the staged output directory.

        Raises
        ------
        ImportError
            If PyYAML is not available.
        ValueError
            If `output_dir` is non-empty or overrides are invalid.
        FileNotFoundError
            If override targets do not exist.
        KeyError
            If override keys are missing in the target config.
        """
        if yaml is None:
            raise ImportError("PyYAML is required for ActivitySim canonicalization.")
        if output_dir.exists() and any(output_dir.iterdir()):
            raise ValueError(f"output_dir must be empty: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        staged_root_dirs = _unpack_bundle(
            bundle_path=base_bundle, output_dir=output_dir
        )
        base_bundle_hash = compute_config_pack_hash(
            root_dirs=staged_root_dirs, identity=identity
        )

        for (file_name, key), value in overrides.constants.items():
            _apply_yaml_override(
                file_name=file_name,
                key=key,
                value=value,
                config_dirs=staged_root_dirs,
            )
        for (
            file_name,
            coefficient_name,
            segment,
        ), value in overrides.coefficients.items():
            _apply_coefficients_override(
                file_name=file_name,
                coefficient_name=coefficient_name,
                segment=segment,
                value=value,
                config_dirs=staged_root_dirs,
            )

        content_hash = identity.canonical_json_sha256(
            {
                "base_bundle_hash": base_bundle_hash,
                "overrides": overrides.to_canonical_dict(),
                "adapter_version": self.adapter_version,
            }
        )

        discovered = self.discover(staged_root_dirs, identity=identity, strict=strict)
        return CanonicalConfig(
            root_dirs=discovered.root_dirs,
            primary_config=discovered.primary_config,
            config_files=discovered.config_files,
            external_files=discovered.external_files,
            content_hash=content_hash,
        )

    def build_facet(
        self, config: CanonicalConfig, *, facet_spec: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Build a facet dict from adapter-specific facet specs.

        Supported keys in facet_spec:
        - yaml: { "file.yaml": ["CONSTANTS.foo", {"key": "CONSTANTS.bar", "alias": "bar"}] }
        - coefficients: { "file.csv": ["coef_name", {"key": "coef", "alias": "alias"}] }
        """
        if yaml is None:
            raise ImportError("PyYAML is required for ActivitySim canonicalization.")
        settings = (
            _load_effective_settings(
                config.root_dirs, settings_path=config.primary_config
            )
            if config.primary_config
            else {}
        )
        inherit_settings = bool(settings.get("inherit_settings", False))
        facet: dict[str, Any] = {}

        for file_name, entries in (facet_spec.get("yaml") or {}).items():
            data = _load_effective_yaml(file_name, config.root_dirs, inherit_settings)
            constants = data.get("CONSTANTS", {}) if isinstance(data, dict) else {}
            for key, alias in _parse_facet_entries(entries):
                const_key = (
                    key[len("CONSTANTS.") :] if key.startswith("CONSTANTS.") else key
                )
                value = _get_nested_value(constants, const_key.split("."))
                facet[alias or key] = value

        for file_name, entries in (facet_spec.get("coefficients") or {}).items():
            csv_path = _resolve_csv_reference(file_name, config.root_dirs)[0]
            for key, alias in _parse_facet_entries(entries):
                value = _read_coefficient_value(csv_path, key) if csv_path else None
                facet[alias or key] = value

        return facet

    @staticmethod
    def _normalize_where(where: Optional[Union[Iterable[Any], Any]]) -> list[Any]:
        if where is None:
            return []
        if isinstance(where, (list, tuple, set)):
            return list(where)
        return [where]

    @staticmethod
    def _resolve_value_column(table: type[SQLModel], value_column: str) -> Any:
        if not hasattr(table, value_column):
            raise ValueError(
                f"Unknown value_column {value_column!r} for {table.__name__}."
            )
        return getattr(table, value_column)

    def run_link_query(
        self,
        table: type[SQLModel],
        *,
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        table_name: Optional[str] = None,
    ) -> Executable:
        """
        Build a run-linked query for a config cache table.
        """
        from consist.api import config_run_query

        return config_run_query(
            table,
            link_table=ActivitySimConfigIngestRunLink,
            table_name=table_name,
            columns=columns,
            where=where,
            join_on=join_on,
        )

    def run_link_rows(
        self,
        table: type[SQLModel],
        *,
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        table_name: Optional[str] = None,
        tracker: Optional["Tracker"] = None,
    ) -> list:
        """
        Execute a run-linked query for a config cache table.
        """
        from consist.api import config_run_rows

        return config_run_rows(
            table,
            link_table=ActivitySimConfigIngestRunLink,
            table_name=table_name,
            columns=columns,
            where=where,
            join_on=join_on,
            tracker=tracker,
        )

    def constants_query(
        self,
        *,
        key: Optional[str] = None,
        file_name: Optional[str] = None,
        value_column: str = "value_num",
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
    ) -> Executable:
        """
        Build a query for ActivitySim constants joined to runs.

        Defaults to selecting (run_id, value_column) when columns is not provided.
        """
        clauses: list[Any] = []
        if key is not None:
            clauses.append(ActivitySimConstantsCache.key == key)
        if file_name is not None:
            clauses.append(ActivitySimConstantsCache.file_name == file_name)
        clauses.extend(self._normalize_where(where))

        if columns is None:
            value_attr = self._resolve_value_column(
                ActivitySimConstantsCache, value_column
            )
            columns = [ActivitySimConfigIngestRunLink.run_id, value_attr]

        return self.run_link_query(
            ActivitySimConstantsCache,
            columns=columns,
            where=clauses,
            join_on=join_on,
        )

    def constants_rows(
        self,
        *,
        key: Optional[str] = None,
        file_name: Optional[str] = None,
        value_column: str = "value_num",
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        tracker: Optional["Tracker"] = None,
    ) -> list:
        """
        Fetch ActivitySim constants joined to runs.

        Defaults to selecting (run_id, value_column) when columns is not provided.
        """
        if columns is None:
            value_attr = self._resolve_value_column(
                ActivitySimConstantsCache, value_column
            )
            columns = [ActivitySimConfigIngestRunLink.run_id, value_attr]

        clauses: list[Any] = []
        if key is not None:
            clauses.append(ActivitySimConstantsCache.key == key)
        if file_name is not None:
            clauses.append(ActivitySimConstantsCache.file_name == file_name)
        clauses.extend(self._normalize_where(where))

        return self.run_link_rows(
            ActivitySimConstantsCache,
            columns=columns,
            where=clauses,
            join_on=join_on,
            tracker=tracker,
        )

    def constants_by_run(
        self,
        *,
        key: Optional[str] = None,
        file_name: Optional[str] = None,
        value_column: str = "value_num",
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        collapse: Literal["first", "last", "list", "error"] = "list",
        tracker: Optional["Tracker"] = None,
    ) -> dict[str, Any]:
        """
        Return constants grouped by run_id.
        """
        rows = self.constants_rows(
            key=key,
            file_name=file_name,
            value_column=value_column,
            where=where,
            join_on=join_on,
            tracker=tracker,
        )
        return self._collapse_run_values(rows, collapse=collapse)

    def coefficients_query(
        self,
        *,
        coefficient: Optional[str] = None,
        file_name: Optional[str] = None,
        segment: Optional[str] = None,
        value_column: str = "value_num",
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
    ) -> Executable:
        """
        Build a query for ActivitySim coefficients joined to runs.

        Defaults to selecting (run_id, value_column) when columns is not provided.
        """
        clauses: list[Any] = []
        if coefficient is not None:
            clauses.append(ActivitySimCoefficientsCache.coefficient_name == coefficient)
        if file_name is not None:
            clauses.append(ActivitySimCoefficientsCache.file_name == file_name)
        if segment is not None:
            clauses.append(ActivitySimCoefficientsCache.segment == segment)
        clauses.extend(self._normalize_where(where))

        if columns is None:
            value_attr = self._resolve_value_column(
                ActivitySimCoefficientsCache, value_column
            )
            columns = [ActivitySimConfigIngestRunLink.run_id, value_attr]

        return self.run_link_query(
            ActivitySimCoefficientsCache,
            columns=columns,
            where=clauses,
            join_on=join_on,
        )

    def coefficients_rows(
        self,
        *,
        coefficient: Optional[str] = None,
        file_name: Optional[str] = None,
        segment: Optional[str] = None,
        value_column: str = "value_num",
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        tracker: Optional["Tracker"] = None,
    ) -> list:
        """
        Fetch ActivitySim coefficients joined to runs.

        Defaults to selecting (run_id, value_column) when columns is not provided.
        """
        if columns is None:
            value_attr = self._resolve_value_column(
                ActivitySimCoefficientsCache, value_column
            )
            columns = [ActivitySimConfigIngestRunLink.run_id, value_attr]

        clauses: list[Any] = []
        if coefficient is not None:
            clauses.append(ActivitySimCoefficientsCache.coefficient_name == coefficient)
        if file_name is not None:
            clauses.append(ActivitySimCoefficientsCache.file_name == file_name)
        if segment is not None:
            clauses.append(ActivitySimCoefficientsCache.segment == segment)
        clauses.extend(self._normalize_where(where))

        return self.run_link_rows(
            ActivitySimCoefficientsCache,
            columns=columns,
            where=clauses,
            join_on=join_on,
            tracker=tracker,
        )

    def coefficients_by_run(
        self,
        *,
        coefficient: Optional[str] = None,
        file_name: Optional[str] = None,
        segment: Optional[str] = None,
        value_column: str = "value_num",
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        collapse: Literal["first", "last", "list", "error"] = "list",
        tracker: Optional["Tracker"] = None,
    ) -> dict[str, Any]:
        """
        Return coefficients grouped by run_id.
        """
        rows = self.coefficients_rows(
            coefficient=coefficient,
            file_name=file_name,
            segment=segment,
            value_column=value_column,
            where=where,
            join_on=join_on,
            tracker=tracker,
        )
        return self._collapse_run_values(rows, collapse=collapse)

    def coefficients_combinations(
        self,
        *,
        coefficients: Union[str, Iterable[str]],
        file_name: Optional[str] = None,
        segment: Optional[str] = None,
        value_column: str = "value_num",
        where: Optional[Union[Iterable[Any], Any]] = None,
        join_on: Optional[Iterable[str]] = None,
        require_all: bool = True,
        missing_value: Any = None,
        collapse: Literal["first", "last", "list", "error"] = "error",
        tracker: Optional["Tracker"] = None,
    ) -> dict[tuple[Any, ...], list[str]]:
        """
        Group runs by unique combinations of coefficient values.

        Returns a mapping of (coeff_values...) -> [run_id, ...] following the order
        of the `coefficients` argument.
        """
        if isinstance(coefficients, str):
            coeff_list = [coefficients]
        else:
            coeff_list = list(coefficients)
        if not coeff_list:
            raise ValueError("coefficients_combinations requires at least one name.")

        if collapse not in {"first", "last", "list", "error"}:
            raise ValueError(
                "collapse must be one of: 'first', 'last', 'list', 'error'"
            )

        value_attr = self._resolve_value_column(
            ActivitySimCoefficientsCache, value_column
        )
        clauses: list[Any] = [
            col(ActivitySimCoefficientsCache.coefficient_name).in_(coeff_list)
        ]
        if file_name is not None:
            clauses.append(ActivitySimCoefficientsCache.file_name == file_name)
        if segment is not None:
            clauses.append(ActivitySimCoefficientsCache.segment == segment)
        clauses.extend(self._normalize_where(where))

        rows = self.run_link_rows(
            ActivitySimCoefficientsCache,
            columns=[
                ActivitySimConfigIngestRunLink.run_id,
                ActivitySimCoefficientsCache.coefficient_name,
                value_attr,
            ],
            where=clauses,
            join_on=join_on,
            tracker=tracker,
        )

        by_run: dict[str, dict[str, list[Any]]] = {}
        for row in rows:
            if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
                raise ValueError(
                    "Expected rows to include (run_id, coefficient, value)."
                )
            if len(row) < 3:
                raise ValueError(
                    "Expected rows to include (run_id, coefficient, value)."
                )
            run_id = row[0]
            coeff = row[1]
            value = row[2]
            by_run.setdefault(run_id, {}).setdefault(coeff, []).append(value)

        combos: dict[tuple[Any, ...], list[str]] = {}
        for run_id, values_by_coeff in by_run.items():
            if require_all and any(c not in values_by_coeff for c in coeff_list):
                continue

            combo_values: list[Any] = []
            for coeff in coeff_list:
                values = values_by_coeff.get(coeff, [])
                if not values:
                    combo_values.append(missing_value)
                    continue
                if len(values) > 1:
                    if collapse == "error":
                        raise ValueError(
                            "Multiple values per run_id; set collapse or refine filters."
                        )
                    if collapse == "first":
                        combo_values.append(values[0])
                        continue
                    if collapse == "last":
                        combo_values.append(values[-1])
                        continue
                    combo_values.append(tuple(values))
                    continue
                combo_values.append(values[0])

            combo = tuple(combo_values)
            combos.setdefault(combo, []).append(run_id)

        for run_ids in combos.values():
            run_ids.sort()

        return combos

    @staticmethod
    def _collapse_run_values(
        rows: Iterable[Any],
        *,
        collapse: Literal["first", "last", "list", "error"],
    ) -> dict[str, Any]:
        by_run: dict[str, list[Any]] = {}
        for row in rows:
            if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
                raise ValueError(
                    "Expected rows to include (run_id, value) for collapse."
                )
            if len(row) < 2:
                raise ValueError(
                    "Expected rows to include (run_id, value) for collapse."
                )
            run_id = row[0]
            value = row[1]
            by_run.setdefault(run_id, []).append(value)

        if collapse == "list":
            return by_run
        if collapse == "first":
            return {run_id: values[0] for run_id, values in by_run.items() if values}
        if collapse == "last":
            return {run_id: values[-1] for run_id, values in by_run.items() if values}
        if collapse == "error":
            collisions = {
                run_id: values for run_id, values in by_run.items() if len(values) > 1
            }
            if collisions:
                raise ValueError(
                    "Multiple values per run_id; use collapse='list' or refine filters."
                )
            return {run_id: values[0] for run_id, values in by_run.items() if values}
        raise ValueError("collapse must be one of: 'first', 'last', 'list', 'error'")


@dataclass
class ConfigOverrides:
    """
    Structured overrides for calibration runs.

    Attributes
    ----------
    constants : dict[tuple[str, str], Any]
        Mapping of (file_name, key) to new value. Keys may be dotted.
    coefficients : dict[tuple[str, str, str], float]
        Mapping of (file_name, coefficient_name, segment) to new value.
        Use empty segment for direct coefficients.
    """

    constants: dict[tuple[str, str], Any] = field(default_factory=dict)
    coefficients: dict[tuple[str, str, str], float] = field(default_factory=dict)

    def to_canonical_dict(self) -> dict[str, Any]:
        """
        Serialize overrides into a deterministic dictionary.

        Returns
        -------
        dict[str, Any]
            Canonical, sorted representation suitable for hashing.
        """
        constants = [
            {"file": file_name, "key": key, "value": value}
            for (file_name, key), value in sorted(self.constants.items())
        ]
        coefficients = [
            {
                "file": file_name,
                "coefficient": coefficient_name,
                "segment": segment,
                "value": value,
            }
            for (file_name, coefficient_name, segment), value in sorted(
                self.coefficients.items()
            )
        ]
        return {"constants": constants, "coefficients": coefficients}


def _ensure_yaml() -> None:
    if yaml is None:
        raise ImportError("PyYAML is required for ActivitySim canonicalization.")


def _load_yaml(path: Path) -> Dict[str, Any]:
    _ensure_yaml()
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if isinstance(data, dict):
        return data
    return {}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_yaml_path(
    name: str, config_dirs: Sequence[Path], base_dir: Optional[Path] = None
) -> Optional[Path]:
    candidate = Path(name)
    if candidate.suffix == "":
        candidate = candidate.with_suffix(".yaml")
    if candidate.is_absolute():
        return candidate if candidate.exists() else None
    if base_dir:
        local = base_dir / candidate
        if local.exists():
            return local
    for config_dir in config_dirs:
        path = config_dir / candidate
        if path.exists():
            return path
    return None


def _find_settings_yaml_subdirs(config_dirs: Sequence[Path]) -> list[Path]:
    candidates: list[Path] = []
    for config_dir in config_dirs:
        if not config_dir.is_dir():
            continue
        for path in config_dir.rglob("settings.yaml"):
            if path.is_file():
                candidates.append(path.parent)
    return candidates


def _load_yaml_with_includes(
    path: Path, config_dirs: Sequence[Path], visited: Set[Path]
) -> Dict[str, Any]:
    if path in visited:
        return {}
    visited.add(path)
    data = _load_yaml(path)
    include_settings = data.pop("include_settings", None)
    merged: Dict[str, Any] = {}
    if include_settings:
        if isinstance(include_settings, str):
            include_settings = [include_settings]
        for include_name in include_settings:
            include_path = _resolve_yaml_path(
                str(include_name), config_dirs, base_dir=path.parent
            )
            if include_path is None:
                logging.warning(
                    "[Consist][ActivitySim] include_settings file %s not found.",
                    include_name,
                )
                continue
            include_data = _load_yaml_with_includes(include_path, config_dirs, visited)
            merged = _deep_merge(merged, include_data)
    merged = _deep_merge(merged, data)
    return merged


def _load_effective_settings(
    config_dirs: Sequence[Path], *, settings_path: Optional[Path]
) -> Dict[str, Any]:
    if settings_path is None:
        return {}
    primary = _load_yaml_with_includes(settings_path, config_dirs, set())
    inherit_settings = bool(primary.get("inherit_settings", False))
    if not inherit_settings:
        return primary
    settings_paths = []
    for config_dir in config_dirs:
        candidate = config_dir / "settings.yaml"
        if candidate.exists():
            settings_paths.append(candidate)
    merged: Dict[str, Any] = {}
    for path in reversed(settings_paths):
        merged = _deep_merge(merged, _load_yaml_with_includes(path, config_dirs, set()))
    return merged


def _load_effective_yaml(
    file_name: str, config_dirs: Sequence[Path], inherit_settings: bool
) -> Dict[str, Any]:
    candidate = Path(file_name)
    if candidate.suffix == "":
        candidate = candidate.with_suffix(".yaml")
    if not inherit_settings:
        path = _resolve_yaml_path(str(candidate), config_dirs)
        if path is None:
            return {}
        return _load_yaml_with_includes(path, config_dirs, set())
    paths = []
    for config_dir in config_dirs:
        path = config_dir / candidate
        if path.exists():
            paths.append(path)
    merged: Dict[str, Any] = {}
    for path in reversed(paths):
        merged = _deep_merge(merged, _load_yaml_with_includes(path, config_dirs, set()))
    return merged


def _resolve_model_yamls(models: Any, config_dirs: Sequence[Path]) -> list[Path]:
    if not isinstance(models, list):
        logging.warning(
            "[Consist][ActivitySim] Expected list for settings.models, got %s.",
            type(models),
        )
        return []
    active: list[Path] = []
    seen: Set[Path] = set()
    unmatched: list[str] = []
    for model in models:
        if not isinstance(model, str):
            continue
        candidates = [model]
        alias = _MODEL_ALIAS_MAP.get(model)
        if alias:
            candidates.append(alias)
        if model.endswith("_simulate"):
            candidates.append(model[: -len("_simulate")])
        if model.endswith("_model"):
            candidates.append(model[: -len("_model")])
        if model.endswith("_settings"):
            candidates.append(model[: -len("_settings")])
        path = None
        for candidate in candidates:
            path = _resolve_yaml_path(candidate, config_dirs)
            if path:
                break
        if path is None:
            unmatched.append(model)
            continue
        if path not in seen:
            active.append(path)
            seen.add(path)
    if unmatched:
        yaml_files: Set[str] = set()
        for config_dir in config_dirs:
            yaml_files.update(p.name for p in config_dir.glob("*.yaml") if p.is_file())
        logging.warning(
            "[Consist][ActivitySim] Unmatched model YAMLs for %s. Available YAMLs: %s",
            unmatched,
            sorted(yaml_files),
        )
    return active


def _artifact_key_for_path(path: Path, config_dirs: Sequence[Path]) -> str:
    resolved = path.resolve()
    for config_dir in config_dirs:
        config_dir = config_dir.resolve()
        if resolved.is_relative_to(config_dir):
            rel = resolved.relative_to(config_dir).as_posix()
            return f"config:{config_dir.name}/{rel}"
    return f"config:{resolved.name}"


def _collect_reference_values(data: Any, *, allow_heuristics: bool) -> Set[str]:
    refs: Set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned and cleaned != ".":
                refs.add(cleaned)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    cleaned = item.strip()
                    if cleaned and cleaned != ".":
                        refs.add(cleaned)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str):
                    key_upper = key.upper()
                    if key_upper in _REFERENCE_KEYS:
                        collect(value)
                    elif allow_heuristics and key_upper.endswith(_HEURISTIC_SUFFIXES):
                        collect(value)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return refs


def _normalize_csv_reference(name: str) -> list[Path]:
    if not name:
        return []
    if name.endswith(".csv.gz"):
        return [Path(name)]
    candidate = Path(name)
    if candidate.name in {"", "."}:
        return []
    if candidate.suffix not in {"", ".csv"}:
        return []
    if candidate.suffix == "":
        candidate = candidate.with_suffix(".csv")
    candidates = [candidate]
    candidates.append(candidate.with_suffix(".csv.gz"))
    return candidates


def _resolve_csv_reference(
    name: str, config_dirs: Sequence[Path]
) -> tuple[Optional[Path], bool]:
    candidates = _normalize_csv_reference(name)
    if not candidates:
        return None, False
    for candidate in candidates:
        if candidate.is_absolute():
            if candidate.exists():
                return candidate, True
            continue
        for config_dir in config_dirs:
            path = config_dir / candidate
            if path.exists():
                return path, True
    return None, True


def _collect_referenced_csvs(
    yaml_paths: Sequence[Path],
    config_dirs: Sequence[Path],
    *,
    inherit_settings: bool,
    allow_heuristics: bool,
    strict: bool,
) -> list[Path]:
    referenced: list[Path] = []
    seen: Set[Path] = set()
    for path in yaml_paths:
        data = _load_effective_yaml(path.name, config_dirs, inherit_settings)
        for ref in sorted(
            _collect_reference_values(data, allow_heuristics=allow_heuristics)
        ):
            csv_path, attempted = _resolve_csv_reference(ref, config_dirs)
            if csv_path is None:
                if not attempted:
                    continue
                message = f"[Consist][ActivitySim] Missing referenced CSV {ref} in {path.name}"
                if strict:
                    raise FileNotFoundError(message)
                logging.warning(message)
                continue
            if csv_path not in seen:
                referenced.append(csv_path)
                seen.add(csv_path)
    return referenced


def _bundle_configs(
    *,
    config_dirs: Sequence[Path],
    run: Run,
    tracker: "Tracker",
    content_hash: str,
    cache_dir: Optional[Path],
) -> Path:
    if cache_dir is None:
        cache_dir = tracker.run_dir / "config_bundles"
    cache_dir = cache_dir.resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = (
        cache_dir / f"{run.model_name}_config_bundle_{content_hash[:10]}.tar.gz"
    )
    if bundle_path.exists():
        return bundle_path
    with tarfile.open(bundle_path, "w:gz") as archive:
        for config_dir in config_dirs:
            base_name = config_dir.name
            for file_path in config_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                rel = file_path.relative_to(config_dir)
                archive.add(file_path, arcname=str(Path(base_name) / rel))
    return bundle_path


def _unpack_bundle(*, bundle_path: Path, output_dir: Path) -> list[Path]:
    top_level: list[str] = []
    seen: Set[str] = set()
    with tarfile.open(bundle_path, "r:gz") as archive:
        for member in archive.getmembers():
            parts = Path(member.name).parts
            if not parts:
                continue
            top = parts[0]
            if top not in seen:
                seen.add(top)
                top_level.append(top)
        archive.extractall(output_dir, filter="data")

    staged_root_dirs = [
        (output_dir / name) for name in top_level if (output_dir / name).is_dir()
    ]
    if not staged_root_dirs:
        staged_root_dirs = sorted(
            [path for path in output_dir.iterdir() if path.is_dir()],
            key=lambda path: path.name,
        )
    if not staged_root_dirs:
        raise ValueError(f"No config directories found in bundle {bundle_path}")
    return staged_root_dirs


def _build_constants_rows(
    *,
    run_id: str,
    config: CanonicalConfig,
    settings: Dict[str, Any],
    inherit_settings: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for yaml_path in config.config_files:
        # Attribute constants to the active YAML file after inheritance resolution.
        data = _load_effective_yaml(yaml_path.name, config.root_dirs, inherit_settings)
        constants = data.get("CONSTANTS", {})
        if isinstance(constants, dict):
            flattened = _flatten_constants(constants)
            for key, value in flattened.items():
                rows.append(
                    _format_constant_row(
                        run_id=run_id,
                        file_name=yaml_path.name,
                        key=f"CONSTANTS.{key}",
                        value=value,
                    )
                )
    if config.primary_config:
        for key in sorted(_SETTINGS_ALLOWLIST):
            if key in settings:
                rows.append(
                    _format_constant_row(
                        run_id=run_id,
                        file_name=config.primary_config.name,
                        key=key,
                        value=settings[key],
                    )
                )
    return rows


def _apply_yaml_override(
    *,
    file_name: str,
    key: str,
    value: Any,
    config_dirs: Sequence[Path],
) -> None:
    path = _resolve_yaml_path(file_name, config_dirs)
    if path is None:
        raise FileNotFoundError(f"YAML file not found for override: {file_name}")
    data = _load_yaml(path)
    if not isinstance(data, dict):
        data = {}
    keys = key.split(".") if key else []
    if not keys:
        raise ValueError("Override key must be non-empty.")
    if not _has_nested_key(data, keys):
        raise KeyError(f"Override key not found: {key} in {file_name}")
    _set_nested_value(data, keys, value)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)


def _has_nested_key(data: Dict[str, Any], keys: list[str]) -> bool:
    cursor: Any = data
    for key in keys:
        if not isinstance(cursor, dict) or key not in cursor:
            return False
        cursor = cursor[key]
    return True


def _set_nested_value(data: Dict[str, Any], keys: list[str], value: Any) -> None:
    cursor = data
    for key in keys[:-1]:
        if key not in cursor or not isinstance(cursor[key], dict):
            cursor[key] = {}
        cursor = cursor[key]
    cursor[keys[-1]] = value


def _get_nested_value(data: Dict[str, Any], keys: list[str]) -> Any:
    cursor: Any = data
    for key in keys:
        if not isinstance(cursor, dict) or key not in cursor:
            return None
        cursor = cursor[key]
    return cursor


def _parse_facet_entries(entries: Any) -> list[tuple[str, Optional[str]]]:
    parsed: list[tuple[str, Optional[str]]] = []
    if not isinstance(entries, list):
        return parsed
    for item in entries:
        if isinstance(item, str):
            parsed.append((item, None))
        elif isinstance(item, dict):
            key = item.get("key")
            if not key:
                continue
            parsed.append((str(key), item.get("alias")))
    return parsed


def _flatten_constants(constants: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, sub_value in value.items():
                new_prefix = f"{prefix}.{key}" if prefix else str(key)
                walk(new_prefix, sub_value)
        elif isinstance(value, list):
            flattened[prefix] = value
        else:
            flattened[prefix] = value

    walk("", constants)
    return flattened


def _format_constant_row(
    *, run_id: str, file_name: str, key: str, value: Any
) -> dict[str, Any]:
    value_type = "json"
    value_str: Optional[str] = None
    value_num: Optional[float] = None
    value_bool: Optional[bool] = None
    value_json: Optional[Dict[str, Any]] = None

    if value is None:
        value_type = "null"
    elif isinstance(value, bool):
        value_type = "bool"
        value_bool = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        value_type = "num"
        value_num = float(value)
    elif isinstance(value, str):
        value_type = "str"
        value_str = value
    else:
        value_type = "json"
        value_json = json.loads(json.dumps(value))

    return {
        "run_id": run_id,
        "file_name": file_name,
        "key": key,
        "value_type": value_type,
        "value_str": value_str,
        "value_num": value_num,
        "value_bool": value_bool,
        "value_json": value_json,
    }


def _is_coefficients_file(file_name: str) -> bool:
    name = file_name[:-3] if file_name.endswith(".csv.gz") else file_name
    return name.endswith(_COEFFICIENT_SUFFIXES)


def _is_coefficients_template_file(file_name: str) -> bool:
    name = file_name[:-3] if file_name.endswith(".csv.gz") else file_name
    return name.endswith(_COEFFICIENT_TEMPLATE_SUFFIXES)


def _is_probabilities_file(file_name: str) -> bool:
    name = file_name[:-3] if file_name.endswith(".csv.gz") else file_name
    return name.endswith(_PROBABILITY_SUFFIXES)


def _validate_csv_headers(
    reader: csv.DictReader, path: Path, *, strict: bool = False
) -> bool:
    """
    Validate that a CSV reader has fieldnames.

    Parameters
    ----------
    reader : csv.DictReader
        The CSV reader to validate.
    path : Path
        Path to the CSV file (for error messages).
    strict : bool, default False
        If True, raise ValueError when headers are missing.
        If False, log warning and return False.

    Returns
    -------
    bool
        True if headers are present, False otherwise.
    """
    if reader.fieldnames is None:
        if strict:
            raise ValueError(f"[Consist][ActivitySim] CSV has no headers: {path}")
        logging.warning(
            f"[Consist][ActivitySim] CSV has no headers, skipping ingestion: {path}"
        )
        return False
    return True


def _iter_coefficients_rows(
    path: Path, run_id: str, *, strict: bool = False
) -> Iterable[dict[str, Any]]:
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if not _validate_csv_headers(reader, path, strict=strict):
            return
        assert reader.fieldnames is not None
        fieldnames = [name or "" for name in reader.fieldnames]
        coeff_key = (
            "coefficient_name" if "coefficient_name" in fieldnames else "coefficient"
        )
        value_key = "value" if "value" in fieldnames else None
        constrain_key = "constrain" if "constrain" in fieldnames else None
        dims_keys = [
            name
            for name in fieldnames
            if name
            and name
            not in {
                coeff_key,
                value_key,
                constrain_key,
                "description",
                "comment",
                "notes",
            }
        ]
        extra_keys = {
            "description",
            "comment",
            "notes",
            coeff_key,
            value_key,
            constrain_key,
        }
        for row in reader:
            if not row:
                continue
            coef_name = (row.get(coeff_key) or "").strip()
            if not coef_name or coef_name.startswith("#"):
                continue
            constrain = (
                (row.get(constrain_key) or "").strip() if constrain_key else None
            )
            is_constrained = None
            if constrain:
                is_constrained = constrain.upper() in {"T", "TRUE", "1", "Y", "YES"}
            if value_key:
                raw = (row.get(value_key) or "").strip()
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "coefficient_name": coef_name,
                    "segment": "",
                    "source_type": "direct",
                    "value_raw": raw,
                    "value_num": _parse_float(raw),
                    "dims_json": None,
                    "constrain": constrain or None,
                    "is_constrained": is_constrained,
                }
                continue
            if coeff_key == "coefficient" and dims_keys:
                dims = {
                    key: _coerce_scalar(row.get(key) or "")
                    for key in dims_keys
                    if row.get(key) not in {None, ""}
                }
                raw = coef_name
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "coefficient_name": "coefficient",
                    "segment": "",
                    "source_type": "dimensional",
                    "value_raw": raw,
                    "value_num": _parse_float(raw),
                    "dims_json": json.dumps(dims, sort_keys=True) if dims else None,
                    "constrain": constrain or None,
                    "is_constrained": is_constrained,
                }
                continue

            for column, raw_value in row.items():
                if column in extra_keys:
                    continue
                raw = (raw_value or "").strip()
                if not raw or raw.startswith("#"):
                    continue
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "coefficient_name": coef_name,
                    "segment": column,
                    "source_type": "template",
                    "value_raw": raw,
                    "value_num": _parse_float(raw),
                    "dims_json": None,
                    "constrain": constrain or None,
                    "is_constrained": is_constrained,
                }


def _iter_probabilities_rows(
    path: Path,
    run_id: str,
    *,
    strict: bool = False,
    check_probabilities: bool = False,
) -> Iterable[dict[str, Any]]:
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if not _validate_csv_headers(reader, path, strict=strict):
            return
        for idx, row in enumerate(reader):
            dims: Dict[str, Any] = {}
            probs: Dict[str, Any] = {}
            for key, value in row.items():
                if value is None or value == "":
                    continue
                numeric = _parse_float(value)
                if numeric is not None:
                    if _is_probabilities_meta_key(key):
                        dims[key] = numeric
                    else:
                        probs[key] = numeric
                else:
                    dims[key] = _coerce_scalar(value)
            if check_probabilities and probs:
                total = sum(probs.values())
                if abs(total - 1.0) > 1e-6:
                    logging.warning(
                        "[Consist][ActivitySim] Probabilities sum != 1.0 in %s row %s (sum=%s).",
                        path.name,
                        idx,
                        total,
                    )
            yield {
                "run_id": run_id,
                "file_name": path.name,
                "row_index": idx,
                "dims": dims,
                "probs": probs,
            }


def _iter_probabilities_entries(
    path: Path, run_id: str, *, strict: bool = False
) -> Iterable[dict[str, Any]]:
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if not _validate_csv_headers(reader, path, strict=strict):
            return
        for idx, row in enumerate(reader):
            if not row:
                continue
            for key, value in row.items():
                if value is None or value == "":
                    continue
                numeric = _parse_float(value)
                if numeric is None:
                    continue
                if _is_probabilities_meta_key(key):
                    continue
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "row_index": idx,
                    "key": key,
                    "value_num": numeric,
                }


def _iter_probabilities_meta_entries(
    path: Path, run_id: str, *, strict: bool = False
) -> Iterable[dict[str, Any]]:
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if not _validate_csv_headers(reader, path, strict=strict):
            return
        for idx, row in enumerate(reader):
            if not row:
                continue
            for key, value in row.items():
                if value is None or value == "":
                    continue
                numeric = _parse_float(value)
                if numeric is None:
                    continue
                if not _is_probabilities_meta_key(key):
                    continue
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "row_index": idx,
                    "key": key,
                    "value_num": numeric,
                }


def _is_probabilities_meta_key(key: str) -> bool:
    return key in _PROBABILITIES_META_KEYS


def _digest_path(path: Path, tracker: Optional["Tracker"]) -> str:
    if tracker is not None:
        return tracker.identity.digest_path(path)
    return _file_sha256(path)


def _digest_path_with_name(path: Path, tracker: Optional["Tracker"]) -> str:
    content_hash = _digest_path(path, tracker)
    digest = hashlib.sha256()
    digest.update(path.name.encode("utf-8"))
    digest.update(b":")
    digest.update(content_hash.encode("utf-8"))
    return digest.hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _with_content_hash(
    rows: Iterable[dict[str, Any]], content_hash: str
) -> Iterable[dict[str, Any]]:
    for row in rows:
        updated = dict(row)
        updated.pop("run_id", None)
        updated["content_hash"] = content_hash
        yield updated


def _run_link_row(
    run_id: str, table_name: str, content_hash: str, file_name: str
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "table_name": table_name,
        "content_hash": content_hash,
        "file_name": file_name,
    }


def _iter_coefficients_template_refs(
    path: Path, run_id: str, *, strict: bool = False
) -> Iterable[dict[str, Any]]:
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            if strict:
                raise ValueError(f"[Consist][ActivitySim] CSV has no headers: {path}")
            logging.warning(
                f"[Consist][ActivitySim] CSV has no headers, skipping ingestion: {path}"
            )
            return
        fieldnames = [name or "" for name in reader.fieldnames]
        coeff_key = (
            "coefficient_name" if "coefficient_name" in fieldnames else "coefficient"
        )
        extra_keys = {
            "description",
            "comment",
            "notes",
            coeff_key,
        }
        for row in reader:
            if not row:
                continue
            coef_name = (row.get(coeff_key) or "").strip()
            if not coef_name or coef_name.startswith("#"):
                continue
            for column, raw_value in row.items():
                if column in extra_keys:
                    continue
                raw = (raw_value or "").strip()
                if not raw or raw.startswith("#"):
                    continue
                yield {
                    "run_id": run_id,
                    "file_name": path.name,
                    "coefficient_name": coef_name,
                    "segment": column,
                    "referenced_coefficient": raw,
                }


def _read_coefficient_value(
    path: Optional[Path], coefficient_name: str
) -> Optional[str]:
    if path is None:
        return None
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return None
        fieldnames = [name or "" for name in reader.fieldnames]
        coeff_key = (
            "coefficient_name"
            if "coefficient_name" in fieldnames
            else "coefficient"
            if "coefficient" in fieldnames
            else None
        )
        if coeff_key is None:
            return None
        value_key = "value" if "value" in fieldnames else None
        for row in reader:
            name = (row.get(coeff_key) or "").strip()
            if name != coefficient_name:
                continue
            if value_key:
                return (row.get(value_key) or "").strip() or None
            return None
    return None


def _parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_scalar(value: str) -> Any:
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    num = _parse_float(value)
    if num is not None:
        if isinstance(num, float) and num.is_integer():
            return int(num)
        return num
    return value


def _apply_coefficients_override(
    *,
    file_name: str,
    coefficient_name: str,
    segment: str,
    value: float,
    config_dirs: Sequence[Path],
) -> None:
    csv_path, _ = _resolve_csv_reference(file_name, config_dirs)
    if csv_path is None:
        raise FileNotFoundError(f"CSV file not found for override: {file_name}")
    with _open_csv(csv_path) as handle:
        reader = csv.DictReader(handle)
        _validate_csv_headers(reader, csv_path, strict=True)
        assert reader.fieldnames is not None
        fieldnames = [name or "" for name in reader.fieldnames]
        rows = list(reader)

    coeff_key = (
        "coefficient_name"
        if "coefficient_name" in fieldnames
        else "coefficient"
        if "coefficient" in fieldnames
        else None
    )
    if coeff_key is None:
        raise ValueError(f"Missing coefficient column in {csv_path}")
    value_key = "value" if "value" in fieldnames else None

    if value_key and segment:
        raise ValueError(
            f"Segment override provided for direct coefficients in {csv_path}"
        )
    if value_key is None and not segment:
        raise ValueError(
            f"Template coefficients require a segment column in {csv_path}"
        )
    if value_key is None and segment not in fieldnames:
        raise KeyError(f"Segment column '{segment}' not found in {csv_path}")

    updated = 0
    for row in rows:
        coef = (row.get(coeff_key) or "").strip()
        if coef != coefficient_name:
            continue
        if value_key:
            row[value_key] = str(value)
        else:
            row[segment] = str(value)
        updated += 1

    if updated == 0:
        raise KeyError(f"Coefficient '{coefficient_name}' not found in {csv_path}")

    _write_csv(csv_path, fieldnames, rows)


def _write_csv(
    path: Path, fieldnames: Sequence[str], rows: list[dict[str, Any]]
) -> None:
    if path.suffix == ".gz":
        handle = gzip.open(path, "wt", encoding="utf-8-sig", newline="")
    else:
        handle = path.open("w", encoding="utf-8-sig", newline="")
    with handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _open_csv(path: Path) -> IO[str]:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    return path.open("r", encoding="utf-8-sig", newline="")


_CSV_HANDLERS = [
    _FileHandler(
        predicate=_is_coefficients_file,
        table_name="activitysim_coefficients_cache",
        schema=ActivitySimCoefficientsCache,
        row_fn=_iter_coefficients_rows,
    ),
    _FileHandler(
        predicate=_is_coefficients_template_file,
        table_name="activitysim_coefficients_template_refs_cache",
        schema=ActivitySimCoefficientTemplateRefsCache,
        row_fn=_iter_coefficients_template_refs,
    ),
]
