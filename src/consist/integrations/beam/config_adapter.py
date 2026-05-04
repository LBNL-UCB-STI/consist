from __future__ import annotations

import importlib
import json
import logging
import re
from dataclasses import dataclass, field, replace
import shutil
from pathlib import Path
from typing import (
    Any,
    cast,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
)

from sqlmodel import SQLModel

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfig,
    CanonicalConfigIdentity,
    CanonicalizationResult,
    ConfigAdapterOptions,
    ConfigReference,
    ConfigReferenceIdentityPolicy,
    DirectoryIdentity,
    IngestSpec,
    _canonical_json_sha256,
)
from consist.core.config_canonicalization import ConfigPlan
from consist.core.identity import IdentityManager
from consist.integrations._config_adapter_shared import (
    OverrideRunHooks as _shared_OverrideRunHooks,
    artifact_key_for_path as _shared_artifact_key_for_path,
    build_override_runtime_kwargs as _shared_build_override_runtime_kwargs,
    callable_accepts_kwarg as _shared_callable_accepts_kwarg,
    digest_path as _shared_digest_path,
    file_sha256 as _shared_file_sha256,
    get_nested_value as _shared_get_nested_value,
    resolve_base_config_dirs as _shared_resolve_base_config_dirs,
    resolve_override_base_selector as _shared_resolve_override_base_selector,
    run_with_config_overrides_orchestrated as _shared_run_with_config_overrides_orchestrated,
)
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from consist.models.run import Run

if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker
    from consist.models.run import RunResult
    from consist.types import ExecutionOptions, IdentityInputs

try:
    _pyhocon = importlib.import_module("pyhocon")
    _pyhocon_config_tree = importlib.import_module("pyhocon.config_tree")
    _pyhocon_config_parser = importlib.import_module("pyhocon.config_parser")
    ConfigFactory = _pyhocon.ConfigFactory
    HOCONConverter = _pyhocon.HOCONConverter
    ConfigParser = _pyhocon_config_parser.ConfigParser
    NonExistentKey = _pyhocon_config_tree.NonExistentKey
except ImportError:  # pragma: no cover
    ConfigFactory = None
    HOCONConverter = None
    ConfigParser = None
    NonExistentKey = None


_INCLUDE_RE = re.compile(r"^\s*include\s+(?:\"([^\"]+)\"|file\(\"([^\"]+)\"\))")

_DEFAULT_OVERRIDE_RUNTIME_KWARGS: dict[str, str] = {
    "config_dir": "selected_root_dir",
}


@dataclass(frozen=True)
class BeamIngestSpec:
    key: str
    schema: type[SQLModel]
    table_name: Optional[str] = None
    dedupe_on_hash: bool = True
    extensions: tuple[str, ...] = (".csv", ".csv.gz", ".parquet")


@dataclass(frozen=True)
class BeamConfigOverrides:
    values: dict[str, Any]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {"values": self.values}


@dataclass(frozen=True)
class BeamReferencePolicy:
    identity_policy: str
    role: Optional[str] = None
    required: bool = True
    reason: Optional[str] = None
    delegated_artifact_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class _BeamPathCandidate:
    config_key: str
    raw_value: str
    index: int = 0


@dataclass
class BeamConfigAdapter:
    model_name: str = "beam"
    adapter_version: str = "0.1"
    root_dirs: Optional[list[Path]] = None
    primary_config: Optional[Path] = None
    resolve_substitutions: bool = True
    env_overrides: Optional[dict[str, str]] = None
    ingest_specs: list[BeamIngestSpec] = field(default_factory=list)
    path_aliases: Optional[dict[str, str | Path]] = None
    allow_heuristic_refs: bool = False
    reference_policies: dict[str, BeamReferencePolicy | dict[str, Any]] = field(
        default_factory=dict
    )

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity: IdentityManager,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> CanonicalConfig:
        if ConfigFactory is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        config_path = self._resolve_primary_config(root_dirs)
        config_files = self._collect_conf_files(config_path)
        config_tree = _load_config_tree(
            config_path,
            resolve=self.resolve_substitutions,
            env_overrides=self.env_overrides,
        )
        content_hash = identity.canonical_json_sha256(
            {
                "primary_config": str(config_path.name),
                "config": identity.normalize_json(config_tree),
            }
        )
        external = [p for p in config_files if not _is_in_roots(p, root_dirs)]
        return CanonicalConfig(
            root_dirs=root_dirs,
            primary_config=config_path,
            config_files=config_files,
            external_files=external,
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
        resolved_strict = options.strict if options is not None else strict
        if ConfigFactory is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        if run is None and not plan_only:
            raise RuntimeError("Beam canonicalize requires run.")

        artifacts_by_path: dict[Path, ArtifactSpec] = {}
        for conf_path in config.config_files:
            _add_artifact(
                artifacts_by_path,
                conf_path,
                config.root_dirs,
                role="conf",
            )

        if config.primary_config is None:
            raise ValueError("Beam canonicalize requires a primary_config.")
        config_tree = _load_config_tree(
            config.primary_config,
            resolve=self.resolve_substitutions,
            env_overrides=self.env_overrides,
        )
        path_aliases = _merge_path_aliases(
            self.path_aliases,
            options.path_aliases if options is not None else None,
        )
        reference_identity = _build_beam_config_identity(
            adapter_name=self.model_name,
            adapter_version=self.adapter_version,
            primary_config=config.primary_config,
            config_tree=config_tree,
            root_dirs=config.root_dirs,
            path_aliases=path_aliases,
            reference_policies=self.reference_policies,
            allow_heuristic_refs=(
                options.allow_heuristic_refs
                if options is not None and options.allow_heuristic_refs is not None
                else self.allow_heuristic_refs
            ),
            tracker=tracker,
        )
        canonical_config_tree = _canonicalize_config_tree_paths(
            config_tree,
            references=reference_identity.references,
        )
        for ref in reference_identity.references:
            if ref.status.startswith("missing"):
                if ref.status != "missing_ignored":
                    logging.warning(
                        "[Consist][BEAM] Missing referenced path for config key %s "
                        "(status=%s, policy=%s, canonical=%s): %s. "
                        "If this is a run-local or machine-local path, pass "
                        "ConfigAdapterOptions(path_aliases=...).",
                        ref.config_key or "<unknown>",
                        ref.status,
                        ref.identity_policy,
                        ref.canonical_value,
                        ref.raw_value,
                    )
                if resolved_strict and ref.status == "missing_required":
                    raise FileNotFoundError(
                        f"{ref.config_key or '<unknown>'}: {ref.raw_value}"
                    )
                continue
            if ref.identity_policy in {"ignored", "output_or_runtime_ignored"}:
                continue
            resolved = _resolve_reference(ref.raw_value, config.root_dirs)
            if resolved is None or not resolved.exists():
                continue
            _add_artifact(
                artifacts_by_path,
                resolved,
                config.root_dirs,
                role="ref",
                meta={
                    "config_reference": ref.raw_value,
                    "config_reference_key": ref.config_key,
                    "config_reference_policy": ref.identity_policy,
                },
            )

        rows = _iter_config_rows(
            canonical_config_tree,
            content_hash=reference_identity.identity_hash,
        )
        if plan_only:
            # Return a fresh iterator each time the plan is materialized.
            def rows_factory(
                run_id: str,
                tree: dict[str, Any] = canonical_config_tree,
                h: str = reference_identity.identity_hash,
            ) -> Iterable[dict[str, Any]]:
                return _iter_config_rows(tree, content_hash=h)
        else:
            rows_factory = rows
        source_key = _artifact_key_for_path(config.primary_config, config.root_dirs)
        ingestables = [
            IngestSpec(
                table_name="beam_config_cache",
                schema=BeamConfigCache,
                rows=rows_factory,
                source_path=config.primary_config,
                source=source_key,
                content_hash=reference_identity.identity_hash,
                dedupe_on_hash=True,
            ),
            _run_link_spec(
                table_name="beam_config_cache",
                content_hash=reference_identity.identity_hash,
                config_name=config.primary_config.name,
                run_id=run.id if run else None,
                plan_only=plan_only,
                source_path=config.primary_config,
                source_key=source_key,
            ),
        ]

        tabular_specs = _build_tabular_ingest_specs(
            config_tree=config_tree,
            root_dirs=config.root_dirs,
            ingest_specs=self.ingest_specs,
            artifacts_by_path=artifacts_by_path,
            tracker=tracker,
            strict=resolved_strict,
        )
        ingestables.extend(tabular_specs)

        artifacts = list(artifacts_by_path.values())

        return CanonicalizationResult(
            artifacts=artifacts,
            ingestables=ingestables,
            identity=reference_identity,
        )

    def materialize(
        self,
        base_root_dirs: list[Path],
        overrides: BeamConfigOverrides,
        *,
        output_dir: Path,
        identity: IdentityManager,
        strict: bool = True,
    ) -> CanonicalConfig:
        if ConfigFactory is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        if HOCONConverter is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        if output_dir.exists() and any(output_dir.iterdir()):
            raise ValueError(f"output_dir must be empty: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        staged_root_dirs: list[Path] = []
        for root_dir in base_root_dirs:
            staged_root = output_dir / root_dir.name
            shutil.copytree(root_dir, staged_root)
            staged_root_dirs.append(staged_root)

        primary_path = _map_primary_config(
            primary_config=self.primary_config,
            base_root_dirs=base_root_dirs,
            staged_root_dirs=staged_root_dirs,
        )

        config = ConfigFactory.parse_file(str(primary_path), resolve=False)
        for key, value in overrides.values.items():
            if strict and config.get(key, NonExistentKey) is NonExistentKey:
                raise KeyError(f"Override key not found: {key}")
            config.put(key, value)

        primary_path.write_text(HOCONConverter.to_hocon(config), encoding="utf-8")

        materialized_adapter = BeamConfigAdapter(
            primary_config=primary_path,
            resolve_substitutions=self.resolve_substitutions,
            env_overrides=self.env_overrides,
            ingest_specs=self.ingest_specs,
            path_aliases=self.path_aliases,
            reference_policies=self.reference_policies,
        )
        return materialized_adapter.discover(
            staged_root_dirs, identity=identity, strict=strict
        )

    def materialize_from_plan(
        self,
        plan: "ConfigPlan",
        overrides: BeamConfigOverrides,
        *,
        output_dir: Path,
        identity: IdentityManager,
        strict: bool = True,
    ) -> CanonicalConfig:
        config_dirs = (plan.meta or {}).get("config_dirs")
        if not config_dirs:
            raise ValueError("Config plan missing config_dirs metadata.")
        return self.materialize(
            [Path(path) for path in config_dirs],
            overrides,
            output_dir=output_dir,
            identity=identity,
            strict=strict,
        )

    def materialize_from_run(
        self,
        tracker: "Tracker",
        run_id: str,
        overrides: BeamConfigOverrides,
        output_dir: Path,
        strict: bool = True,
    ) -> CanonicalConfig:
        run = tracker.get_run(run_id)
        run_adapter: str | None = None
        if run is not None and isinstance(run.meta, dict):
            adapter_value = run.meta.get("config_adapter")
            if isinstance(adapter_value, str) and adapter_value:
                run_adapter = adapter_value
        if run_adapter is not None and run_adapter != self.model_name:
            raise ValueError(
                "Base run is not a BEAM config run: "
                f"run_id={run_id!r}, config_adapter={run_adapter!r}."
            )

        root_dirs, primary_config = self._resolve_base_config_from_run(tracker, run_id)
        run_adapter_obj = replace(
            self, root_dirs=list(root_dirs), primary_config=primary_config
        )
        return run_adapter_obj.materialize(
            list(root_dirs),
            overrides,
            output_dir=output_dir,
            identity=tracker.identity,
            strict=strict,
        )

    def run_with_config_overrides(
        self,
        *,
        tracker: "Tracker",
        base_run_id: str | None = None,
        base_config_dirs: Sequence[Path] | None = None,
        base_primary_config: Path | None = None,
        overrides: BeamConfigOverrides,
        output_dir: Path,
        fn: Any,
        name: str,
        model: str | None = None,
        config: dict[str, Any] | None = None,
        outputs: list[str] | None = None,
        execution_options: "ExecutionOptions" | None = None,
        strict: bool = True,
        identity_inputs: "IdentityInputs" = None,
        resolved_config_identity: Literal["auto", "off"] = "auto",
        identity_label: str = "beam_config",
        override_runtime_kwargs: Mapping[str, Any] | None = None,
        **run_kwargs: Any,
    ) -> "RunResult":
        def _selector_identity(
            base_selector: dict[str, Any], resolved_strict: bool
        ) -> dict[str, Any]:
            if base_selector["kind"] == "run_id":
                return {
                    "selector": "base_run_id",
                    "base_run_id": base_selector["run_id"],
                }

            resolved_dirs = self._resolve_base_config_dirs(base_selector["config_dirs"])
            resolved_primary = self._resolve_base_primary_config_for_dirs(
                base_config_dirs=resolved_dirs,
                base_primary_config=base_primary_config,
            )
            base_adapter = replace(
                self,
                root_dirs=list(resolved_dirs),
                primary_config=resolved_primary,
            )
            discovered_base = base_adapter.discover(
                list(resolved_dirs),
                identity=tracker.identity,
                strict=resolved_strict,
            )
            return {
                "selector": "base_config_dirs",
                "base_config_hash": discovered_base.content_hash,
                "base_primary_config": str(resolved_primary),
            }

        def _materialize(
            base_selector: dict[str, Any],
            staged_output_dir: Path,
            resolved_strict: bool,
        ) -> CanonicalConfig:
            if base_selector["kind"] == "run_id":
                return self.materialize_from_run(
                    tracker=tracker,
                    run_id=base_selector["run_id"],
                    overrides=overrides,
                    output_dir=staged_output_dir,
                    strict=resolved_strict,
                )

            resolved_dirs = self._resolve_base_config_dirs(base_selector["config_dirs"])
            resolved_primary = self._resolve_base_primary_config_for_dirs(
                base_config_dirs=resolved_dirs,
                base_primary_config=base_primary_config,
            )
            base_adapter = replace(
                self,
                root_dirs=list(resolved_dirs),
                primary_config=resolved_primary,
            )
            return base_adapter.materialize(
                list(resolved_dirs),
                overrides,
                output_dir=staged_output_dir,
                identity=tracker.identity,
                strict=resolved_strict,
            )

        def _select_root(materialized: CanonicalConfig) -> Path:
            if not materialized.root_dirs:
                raise ValueError("Materialized BEAM config has no root directories.")
            return materialized.root_dirs[0]

        return _shared_run_with_config_overrides_orchestrated(
            tracker=tracker,
            base_run_id=base_run_id,
            base_config_dirs=base_config_dirs,
            base_primary_config=base_primary_config,
            overrides=overrides,
            output_dir=output_dir,
            fn=fn,
            name=name,
            model=model,
            config=config,
            outputs=outputs,
            execution_options=execution_options,
            strict=strict,
            identity_inputs=identity_inputs,
            resolved_config_identity=resolved_config_identity,
            identity_label=identity_label,
            override_runtime_kwargs=override_runtime_kwargs,
            run_kwargs=run_kwargs,
            adapter_version=self.adapter_version,
            hooks=_shared_OverrideRunHooks(
                resolve_base_selector=self._resolve_override_base_selector,
                selector_identity=_selector_identity,
                materialize=_materialize,
                select_root=_select_root,
                build_run_adapter=lambda materialized: replace(
                    self,
                    root_dirs=list(materialized.root_dirs),
                    primary_config=materialized.primary_config,
                ),
                build_override_runtime_kwargs=self._build_override_runtime_kwargs,
            ),
        )

    def _resolve_base_config_from_run(
        self, tracker: "Tracker", run_id: str
    ) -> tuple[list[Path], Path]:
        artifacts = tracker.get_artifacts_for_run(run_id)
        conf_artifacts = [
            artifact
            for artifact in artifacts.inputs.values()
            if isinstance(artifact.meta, dict)
            and artifact.meta.get("config_role") == "conf"
        ]
        if not conf_artifacts:
            raise FileNotFoundError(
                "No BEAM config artifacts found for "
                f"run_id={run_id!r}. Expected input artifacts with "
                "meta['config_role'] == 'conf'."
            )

        ordered_conf = sorted(
            conf_artifacts,
            key=lambda artifact: (
                artifact.key,
                artifact.created_at.isoformat() if artifact.created_at else "",
                str(artifact.id),
            ),
        )

        root_dirs: list[Path] = []
        resolved_conf_paths: list[Path] = []
        for artifact in ordered_conf:
            resolved = Path(tracker.resolve_uri(artifact.container_uri))
            if not resolved.exists():
                raise FileNotFoundError(
                    "Config artifact exists in metadata but file is missing for "
                    f"run_id={run_id!r}, key={artifact.key!r}: {resolved!s}"
                )
            resolved_conf_paths.append(resolved)
            root = _infer_root_dir_from_config_artifact(artifact.key, resolved)
            if root not in root_dirs:
                root_dirs.append(root)

        primary_config = _select_primary_config_from_artifacts(
            resolved_conf_paths,
            primary_hint=self.primary_config,
        )
        return root_dirs, primary_config

    @staticmethod
    def _resolve_override_base_selector(
        *,
        base_run_id: str | None,
        base_config_dirs: Sequence[Path] | None,
        base_primary_config: Path | None,
    ) -> dict[str, Any]:
        return _shared_resolve_override_base_selector(
            base_run_id=base_run_id,
            base_config_dirs=base_config_dirs,
            base_primary_config=base_primary_config,
        )

    @staticmethod
    def _resolve_base_config_dirs(base_config_dirs: Sequence[Path]) -> list[Path]:
        return _shared_resolve_base_config_dirs(base_config_dirs)

    def _resolve_base_primary_config_for_dirs(
        self,
        *,
        base_config_dirs: Sequence[Path],
        base_primary_config: Path | None,
    ) -> Path:
        primary_hint = (
            base_primary_config
            if base_primary_config is not None
            else self.primary_config
        )
        if primary_hint is None:
            raise ValueError(
                "base_primary_config is required when using base_config_dirs unless "
                "adapter.primary_config is already set."
            )
        primary_path = Path(primary_hint)
        if primary_path.is_absolute():
            if not primary_path.exists():
                raise FileNotFoundError(f"BEAM config not found: {primary_path!s}")
            resolved = primary_path.resolve()
            if any(
                resolved.is_relative_to(root_dir.resolve())
                for root_dir in base_config_dirs
            ):
                return resolved
            raise ValueError(
                "base_primary_config must be located under base_config_dirs when "
                "an absolute path is provided."
            )
        for root_dir in base_config_dirs:
            candidate = (root_dir / primary_path).resolve()
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            "base_primary_config was not found under base_config_dirs: "
            f"{primary_hint!s}"
        )

    @staticmethod
    def _build_override_runtime_kwargs(
        *,
        fn: Any,
        selected_root: Path,
        root_dirs: Sequence[Path],
        explicit_mapping: Mapping[str, Any] | None,
        existing_runtime_kwargs: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        return _shared_build_override_runtime_kwargs(
            fn=fn,
            selected_root=selected_root,
            root_dirs=root_dirs,
            explicit_mapping=explicit_mapping,
            existing_runtime_kwargs=existing_runtime_kwargs,
            default_mapping=_DEFAULT_OVERRIDE_RUNTIME_KWARGS,
        )

    @staticmethod
    def _callable_accepts_kwarg(fn: Any, runtime_key: str) -> bool:
        return _shared_callable_accepts_kwarg(fn, runtime_key)

    def build_facet(
        self, config: CanonicalConfig, *, facet_spec: dict[str, Any]
    ) -> dict[str, Any]:
        if config.primary_config is None:
            raise ValueError("Beam canonicalize requires a primary_config.")
        config_tree = _load_config_tree(
            config.primary_config,
            resolve=self.resolve_substitutions,
            env_overrides=self.env_overrides,
        )
        facet: dict[str, Any] = {}
        for key, alias in _parse_facet_entries(facet_spec.get("keys") or []):
            value = _get_nested_value(config_tree, key.split("."))
            facet[alias or key] = value
        return facet

    def _resolve_primary_config(self, root_dirs: list[Path]) -> Path:
        if self.primary_config is None:
            raise ValueError("primary_config is required for BEAM configs.")
        config_path = Path(self.primary_config)
        if not config_path.is_absolute():
            for root in root_dirs:
                candidate = root / config_path
                if candidate.exists():
                    config_path = candidate
                    break
        if not config_path.exists():
            raise FileNotFoundError(f"BEAM config not found: {config_path}")
        return config_path.resolve()

    def _collect_conf_files(self, root: Path) -> list[Path]:
        seen: set[Path] = set()
        ordered: list[Path] = []
        queue = [root]
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            ordered.append(current)
            for include in _scan_includes(current):
                queue.append(include)
        return ordered


def _scan_includes(path: Path) -> list[Path]:
    includes: list[Path] = []
    if not path.exists():
        return includes
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _INCLUDE_RE.match(line)
        if not match:
            continue
        rel = match.group(1) or match.group(2)
        if rel:
            include_path = (path.parent / rel).resolve()
            includes.append(include_path)
    return includes


def _load_config_tree(
    path: Path, *, resolve: bool, env_overrides: Optional[dict[str, str]] = None
) -> dict[str, Any]:
    if ConfigFactory is None:
        raise ImportError("pyhocon is required for BEAM canonicalization.")
    if HOCONConverter is None:
        raise ImportError("pyhocon is required for BEAM canonicalization.")
    if resolve and env_overrides:
        if ConfigParser is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        if NonExistentKey is None:
            raise ImportError("pyhocon is required for BEAM canonicalization.")
        config = ConfigFactory.parse_file(str(path), resolve=False)
        inserted_keys: list[str] = []
        for key, value in env_overrides.items():
            if not key:
                continue
            if config.get(key, NonExistentKey) is not NonExistentKey:
                continue
            config.put(key, value)
            inserted_keys.append(key)
        ConfigParser.resolve_substitutions(config, accept_unresolved=False)
        for key in inserted_keys:
            _remove_config_key(config, key)
        return json.loads(HOCONConverter.to_json(config))

    config = ConfigFactory.parse_file(str(path), resolve=resolve)
    return json.loads(HOCONConverter.to_json(config))


def _remove_config_key(config: Any, key: str) -> None:
    parts = [part for part in key.split(".") if part]
    if not parts:
        return
    cursor: Any = config
    parents: list[tuple[dict[str, Any], str]] = []
    for part in parts[:-1]:
        if not isinstance(cursor, dict):
            return
        child = cursor.get(part)
        if not isinstance(child, dict):
            return
        parents.append((cursor, part))
        cursor = child

    if not isinstance(cursor, dict):
        return
    cursor.pop(parts[-1], None)
    for parent, part in reversed(parents):
        child = parent.get(part)
        if isinstance(child, dict) and not child:
            parent.pop(part, None)
        else:
            break


def _iter_config_rows(
    config_tree: dict[str, Any], *, content_hash: str
) -> Iterable[dict[str, Any]]:
    def walk(node: Any, prefix: str) -> Iterable[dict[str, Any]]:
        if isinstance(node, dict):
            for key, value in node.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                yield from walk(value, next_prefix)
            return
        value_type, value_str, value_num, value_bool, value_json_str = _normalize_value(
            node
        )
        yield {
            "content_hash": content_hash,
            "key": prefix,
            "value_type": value_type,
            "value_str": value_str,
            "value_num": value_num,
            "value_bool": value_bool,
            "value_json_str": value_json_str,
        }

    return walk(config_tree, "")


def _normalize_value(
    value: Any,
) -> tuple[str, Optional[str], Optional[float], Optional[bool], Optional[str]]:
    if value is None:
        return "null", None, None, None, None
    if isinstance(value, bool):
        return "bool", None, None, value, None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "num", None, float(value), None, None
    if isinstance(value, str):
        return "str", value, None, None, None
    return "json", None, None, None, json.dumps(value, sort_keys=True)


def _is_path_like_config_value(value: str) -> bool:
    candidate = value.strip()
    if not candidate:
        return False
    if candidate in {"csv", "csv.gz", "xml", "xml.gz", "parquet", "omx", "h5"}:
        return False
    if candidate.startswith(("http://", "https://", "tcp://")):
        return False
    return "/" in candidate or candidate.endswith(
        (
            ".csv",
            ".csv.gz",
            ".xml",
            ".xml.gz",
            ".gz",
            ".parquet",
            ".zip",
            ".omx",
            ".h5",
        )
    )


def _collect_path_candidates(
    config_tree: dict[str, Any],
    *,
    root_dirs: Sequence[Path],
    reference_policies: Mapping[str, BeamReferencePolicy | Mapping[str, Any]],
    allow_heuristic_refs: bool,
) -> list[_BeamPathCandidate]:
    candidates: list[_BeamPathCandidate] = []
    explicit_policy_keys = set(reference_policies)

    def walk(node: Any, prefix: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                walk(value, next_prefix)
            return
        if isinstance(node, list):
            for index, item in enumerate(node):
                walk(item, f"{prefix}[{index}]")
            return
        if isinstance(node, str) and (
            prefix in explicit_policy_keys
            or _is_path_like_config_value(node)
            or _resolves_under_config_root(node, root_dirs)
            or (allow_heuristic_refs and _is_path_like_config_key(prefix))
        ):
            candidates.append(
                _BeamPathCandidate(
                    config_key=prefix,
                    raw_value=node.strip(),
                    index=len(candidates),
                )
            )

    walk(config_tree, "")
    return candidates


def _resolves_under_config_root(value: str, root_dirs: Sequence[Path]) -> bool:
    raw = value.strip()
    if not raw:
        return False
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        resolved = candidate.resolve()
        return resolved.exists() and any(
            resolved == root.resolve() or resolved.is_relative_to(root.resolve())
            for root in root_dirs
        )
    return any((root / candidate).resolve().exists() for root in root_dirs)


def _merge_path_aliases(
    base: Optional[Mapping[str, str | Path]],
    override: Optional[Mapping[str, str | Path]],
) -> dict[str, Path]:
    merged: dict[str, Path] = {}
    for source in (base, override):
        if not source:
            continue
        for alias, path in source.items():
            if alias:
                merged[str(alias)] = Path(path).expanduser().resolve()
    return merged


def _normalize_reference_policy(
    value: BeamReferencePolicy | Mapping[str, Any] | None,
) -> BeamReferencePolicy | None:
    if value is None:
        return None
    if isinstance(value, BeamReferencePolicy):
        return value
    return BeamReferencePolicy(
        identity_policy=str(value.get("identity_policy", "content_hash")),
        role=value.get("role"),
        required=bool(value.get("required", True)),
        reason=value.get("reason"),
        delegated_artifact_keys=tuple(value.get("delegated_artifact_keys", ())),
    )


def _reference_policy_options(
    value: BeamReferencePolicy | Mapping[str, Any],
) -> dict[str, Any]:
    policy = _normalize_reference_policy(value)
    if policy is None:
        return {}
    data = {
        "identity_policy": policy.identity_policy,
        "role": policy.role,
        "required": policy.required,
        "reason": policy.reason,
        "delegated_artifact_keys": list(policy.delegated_artifact_keys),
    }
    return {key: item for key, item in data.items() if item not in (None, [], ())}


def _policy_for_candidate(
    candidate: _BeamPathCandidate,
    policies: Mapping[str, BeamReferencePolicy | Mapping[str, Any]],
) -> BeamReferencePolicy:
    explicit = _normalize_reference_policy(policies.get(candidate.config_key))
    if explicit is not None:
        return explicit

    key_lower = candidate.config_key.lower()
    if key_lower.endswith("inputdirectory"):
        return BeamReferencePolicy(
            identity_policy="path_alias",
            role="beam_input_root",
            required=True,
        )
    if "output" in key_lower or _is_runtime_output_config_key(candidate.config_key):
        return BeamReferencePolicy(
            identity_policy="output_or_runtime_ignored",
            role="runtime_output",
            required=False,
            reason="runtime_output_path",
        )
    return BeamReferencePolicy(identity_policy="content_hash", required=True)


def _is_path_like_config_key(config_key: str) -> bool:
    compact_key = re.sub(r"[^a-z0-9]", "", config_key.lower())
    return any(
        compact_key.endswith(suffix)
        for suffix in ("directory", "dir", "filepath", "filename", "path", "file")
    )


def _is_runtime_output_config_key(config_key: str) -> bool:
    compact_key = re.sub(r"[^a-z0-9]", "", config_key.lower())
    return compact_key.endswith("filebasename")


def _canonical_path_value(
    *,
    raw_value: str,
    resolved: Optional[Path],
    root_dirs: Sequence[Path],
    path_aliases: Mapping[str, Path],
) -> str:
    path = resolved or Path(raw_value)
    path = path.expanduser()
    if path.is_absolute():
        resolved_path = path.resolve()
        alias_matches = sorted(
            (
                (alias, root)
                for alias, root in path_aliases.items()
                if resolved_path == root or resolved_path.is_relative_to(root)
            ),
            key=lambda item: len(item[1].parts),
            reverse=True,
        )
        if alias_matches:
            alias, root = alias_matches[0]
            rel = resolved_path.relative_to(root).as_posix()
            return alias if not rel else f"{alias}/{rel}"
        for root in root_dirs:
            root_resolved = root.resolve()
            if resolved_path == root_resolved or resolved_path.is_relative_to(
                root_resolved
            ):
                rel = resolved_path.relative_to(root_resolved).as_posix()
                return (
                    f"config:{root_resolved.name}/{rel}"
                    if rel
                    else f"config:{root_resolved.name}"
                )
        return raw_value

    if resolved is not None:
        return _canonical_path_value(
            raw_value=raw_value,
            resolved=resolved.resolve(),
            root_dirs=root_dirs,
            path_aliases=path_aliases,
        )
    return raw_value


def _canonicalize_config_tree_paths(
    node: Any,
    *,
    references: Sequence[ConfigReference],
) -> Any:
    by_key = {
        ref.config_key: ref
        for ref in references
        if ref.config_key and ref.canonical_value is not None
    }

    def walk(value: Any, prefix: str = "") -> Any:
        if isinstance(value, dict):
            return {
                key: walk(item, f"{prefix}.{key}" if prefix else str(key))
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                walk(item, f"{prefix}[{index}]") for index, item in enumerate(value)
            ]
        if isinstance(value, str):
            ref = by_key.get(prefix)
            if ref is not None:
                if ref.identity_policy in {"ignored", "output_or_runtime_ignored"}:
                    return {"identity_policy": ref.identity_policy}
                return ref.canonical_value
        return value

    return walk(node)


def _build_beam_config_identity(
    *,
    adapter_name: str,
    adapter_version: Optional[str],
    primary_config: Optional[Path],
    config_tree: dict[str, Any],
    root_dirs: Sequence[Path],
    path_aliases: Mapping[str, Path],
    reference_policies: Mapping[str, BeamReferencePolicy | Mapping[str, Any]],
    allow_heuristic_refs: bool,
    tracker: Optional["Tracker"],
) -> CanonicalConfigIdentity:
    references: list[ConfigReference] = []
    directories: list[DirectoryIdentity] = []
    for candidate in _collect_path_candidates(
        config_tree,
        root_dirs=root_dirs,
        reference_policies=reference_policies,
        allow_heuristic_refs=allow_heuristic_refs,
    ):
        policy = _policy_for_candidate(candidate, reference_policies)
        resolved = _resolve_reference(candidate.raw_value, root_dirs)
        exists = bool(resolved is not None and resolved.exists())
        canonical_value = _canonical_path_value(
            raw_value=candidate.raw_value,
            resolved=resolved,
            root_dirs=root_dirs,
            path_aliases=path_aliases,
        )
        required = policy.required
        identity_policy = policy.identity_policy
        reason = policy.reason
        delegated_artifact_keys = policy.delegated_artifact_keys
        if exists:
            status = "resolved"
        elif identity_policy in {"ignored", "output_or_runtime_ignored"}:
            status = "missing_ignored"
            required = False
        elif required:
            status = "missing_required"
        else:
            status = "missing_optional"

        digest: Optional[str] = None
        if exists and resolved is not None:
            if identity_policy == "content_hash" and resolved.is_file():
                digest = _digest_path(resolved, tracker)
            elif identity_policy == "content_hash" and resolved.is_dir():
                identity_policy = "delegated_to_artifacts"
                reason = reason or "directory_content_delegated_to_artifact_inputs"
                if not delegated_artifact_keys:
                    delegated_artifact_keys = (
                        _artifact_key_for_path(resolved, root_dirs),
                    )
            elif identity_policy in {"path_alias", "delegated_to_artifacts"}:
                if not delegated_artifact_keys:
                    delegated_artifact_keys = (
                        _artifact_key_for_path(resolved, root_dirs),
                    )
        if exists and resolved is not None and resolved.is_dir():
            directories.append(
                DirectoryIdentity(
                    canonical_value=canonical_value,
                    role=policy.role,
                    identity_policy=identity_policy,
                    hash_strategy=getattr(
                        getattr(tracker, "identity", None), "hashing_strategy", None
                    ),
                    hash=digest,
                )
            )

        references.append(
            ConfigReference(
                config_key=candidate.config_key,
                raw_value=candidate.raw_value,
                canonical_value=canonical_value,
                status=status,
                required=required,
                role=policy.role,
                identity_policy=cast(ConfigReferenceIdentityPolicy, identity_policy),
                reason=reason,
                hash=digest,
                delegated_artifact_keys=delegated_artifact_keys,
            )
        )

    references = sorted(
        references,
        key=lambda ref: (ref.config_key or "", ref.raw_value, ref.identity_policy),
    )
    canonical_tree = _canonicalize_config_tree_paths(
        config_tree,
        references=references,
    )
    reference_payload = [
        {
            "config_key": ref.config_key,
            "canonical_value": ref.canonical_value
            if ref.canonical_value is not None
            else ref.raw_value,
            "status": ref.status,
            "required": ref.required,
            "role": ref.role,
            "identity_policy": ref.identity_policy,
            "hash": ref.hash,
            "delegated_artifact_keys": list(ref.delegated_artifact_keys),
            "reason": ref.reason,
        }
        for ref in references
        if ref.identity_policy not in {"ignored", "output_or_runtime_ignored"}
    ]
    scalar_hash = _canonical_json_sha256({"config": canonical_tree})
    reference_hash = _canonical_json_sha256({"references": reference_payload})
    directory_hash = _canonical_json_sha256(
        {"directories": [item.to_meta_dict() for item in directories]}
    )
    options_payload = {
        "allow_heuristic_refs": allow_heuristic_refs,
        "path_aliases": {
            alias: path.as_posix() for alias, path in sorted(path_aliases.items())
        },
        "reference_policies": {
            key: _reference_policy_options(value)
            for key, value in sorted(reference_policies.items())
        },
    }
    primary_config_key = (
        _artifact_key_for_path(primary_config, root_dirs)
        if primary_config is not None
        else None
    )
    identity_hash = _canonical_json_sha256(
        {
            "identity_schema_version": 1,
            "adapter_name": adapter_name,
            "adapter_version": adapter_version,
            "primary_config": primary_config_key,
            "scalar_hash": scalar_hash,
            "reference_hash": reference_hash,
            "directory_hash": directory_hash,
        }
    )
    return CanonicalConfigIdentity(
        adapter_name=adapter_name,
        adapter_version=adapter_version,
        primary_config=primary_config_key,
        identity_hash=identity_hash,
        scalar_hash=scalar_hash,
        reference_hash=reference_hash,
        directory_hash=directory_hash,
        scalars={"options": options_payload},
        references=tuple(references),
        directories=tuple(directories),
    )


def _resolve_reference(value: str, root_dirs: Sequence[Path]) -> Optional[Path]:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    for root in root_dirs:
        resolved = (root / candidate).resolve()
        if resolved.exists():
            return resolved
    return (root_dirs[0] / candidate).resolve() if root_dirs else None


def _artifact_key_for_path(path: Path, config_dirs: Sequence[Path]) -> str:
    return _shared_artifact_key_for_path(path, config_dirs)


def _run_link_spec(
    *,
    table_name: str,
    content_hash: str,
    config_name: str,
    run_id: Optional[str],
    plan_only: bool,
    source_path: Path,
    source_key: str,
) -> IngestSpec:
    if plan_only:

        def rows(
            run_id: str,
            t: str = table_name,
            h: str = content_hash,
            c: str = config_name,
        ) -> list[dict[str, Any]]:
            return [
                {"run_id": run_id, "table_name": t, "content_hash": h, "config_name": c}
            ]
    else:
        if run_id is None:
            raise RuntimeError("Beam canonicalize requires run for run link.")
        rows = [
            {
                "run_id": run_id,
                "table_name": table_name,
                "content_hash": content_hash,
                "config_name": config_name,
            }
        ]
    return IngestSpec(
        table_name="beam_config_ingest_run_link",
        schema=BeamConfigIngestRunLink,
        rows=rows,
        source_path=source_path,
        source=source_key,
    )


def _parse_facet_entries(entries: Iterable[Any]) -> list[tuple[str, Optional[str]]]:
    parsed: list[tuple[str, Optional[str]]] = []
    for entry in entries:
        if isinstance(entry, str):
            parsed.append((entry, None))
        elif isinstance(entry, dict):
            parsed.append((entry.get("key"), entry.get("alias")))
    return [(key, alias) for key, alias in parsed if key]


def _get_nested_value(data: Any, keys: Sequence[str]) -> Any:
    return _shared_get_nested_value(data, keys)


def _add_artifact(
    artifacts: dict[Path, ArtifactSpec],
    path: Path,
    config_dirs: Sequence[Path],
    *,
    role: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    if path in artifacts:
        return
    data = {"config_role": role}
    if meta:
        data.update(meta)
    artifacts[path] = ArtifactSpec(
        path=path,
        key=_artifact_key_for_path(path, config_dirs),
        direction="input",
        meta=data,
    )


def _parse_config_key_root_and_rel(key: str) -> tuple[str, Path] | None:
    if not key.startswith("config:"):
        return None
    suffix = key[len("config:") :]
    if "/" not in suffix:
        return None
    root_name, rel = suffix.split("/", 1)
    if not root_name or not rel:
        return None
    return root_name, Path(rel)


def _infer_root_dir_from_config_artifact(key: str, resolved_path: Path) -> Path:
    parsed = _parse_config_key_root_and_rel(key)
    if parsed is None:
        return resolved_path.parent.resolve()

    root_name, rel_path = parsed
    depth = len(rel_path.parts)
    candidate = resolved_path
    for _ in range(depth):
        candidate = candidate.parent
    candidate = candidate.resolve()
    if candidate.name == root_name:
        return candidate

    for parent in resolved_path.parents:
        if parent.name == root_name:
            return parent.resolve()
    return candidate


def _select_primary_config_from_artifacts(
    conf_paths: Sequence[Path], *, primary_hint: Path | None
) -> Path:
    if not conf_paths:
        raise ValueError("Cannot resolve primary config from an empty artifact list.")

    if primary_hint is None:
        return conf_paths[0]

    hint = Path(primary_hint)
    hint_name = hint.name
    hint_posix = hint.as_posix()

    for path in conf_paths:
        if path.name == hint_name:
            return path
    for path in conf_paths:
        if path.as_posix().endswith(hint_posix):
            return path
    return conf_paths[0]


def _is_in_roots(path: Path, root_dirs: Sequence[Path]) -> bool:
    resolved = path.resolve()
    for root in root_dirs:
        if resolved.is_relative_to(root.resolve()):
            return True
    return False


def _map_primary_config(
    *,
    primary_config: Optional[Path],
    base_root_dirs: Sequence[Path],
    staged_root_dirs: Sequence[Path],
) -> Path:
    if primary_config is None:
        raise ValueError("primary_config is required for BEAM configs.")
    config_path = Path(primary_config)
    if config_path.is_absolute():
        resolved = config_path.resolve()
        for base_root, staged_root in zip(base_root_dirs, staged_root_dirs):
            base_root = base_root.resolve()
            if resolved.is_relative_to(base_root):
                relative = resolved.relative_to(base_root)
                return (staged_root / relative).resolve()
        raise FileNotFoundError(
            f"BEAM config not found under base roots: {config_path}"
        )
    for staged_root in staged_root_dirs:
        candidate = (staged_root / config_path).resolve()
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"BEAM config not found in staged roots: {config_path}")


def _build_tabular_ingest_specs(
    *,
    config_tree: dict[str, Any],
    root_dirs: Sequence[Path],
    ingest_specs: list[BeamIngestSpec],
    artifacts_by_path: dict[Path, ArtifactSpec],
    tracker: Optional["Tracker"],
    strict: bool,
) -> list[IngestSpec]:
    if not ingest_specs:
        return []
    specs: list[IngestSpec] = []
    seen: set[tuple[str, Path]] = set()
    for spec in ingest_specs:
        if not hasattr(spec.schema, "model_fields"):
            raise ValueError("BeamIngestSpec schema must be a SQLModel.")
        if spec.dedupe_on_hash and "content_hash" not in spec.schema.model_fields:
            raise ValueError(
                f"BeamIngestSpec schema {spec.schema.__name__} must include "
                "content_hash for dedupe_on_hash."
            )
        table_name = spec.table_name or str(spec.schema.__tablename__)
        if spec.key == "*":
            raise ValueError("BeamIngestSpec key='*' is not supported.")
        value = _get_nested_value(config_tree, spec.key.split("."))
        candidates = _coerce_to_path_values(value)
        for candidate in candidates:
            resolved = _resolve_reference(candidate, root_dirs)
            if resolved is None or not resolved.exists():
                logging.warning(
                    "[Consist][BEAM] Missing referenced path: %s", candidate
                )
                if strict:
                    raise FileNotFoundError(candidate)
                continue
            if not _has_extension(resolved, spec.extensions):
                continue
            key = _artifact_key_for_path(resolved, root_dirs)
            if resolved not in artifacts_by_path:
                _add_artifact(artifacts_by_path, resolved, root_dirs, role="ref")
            content_hash = _digest_path(resolved, tracker)
            dedupe_key = (table_name, resolved)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            specs.append(
                IngestSpec(
                    table_name=table_name,
                    schema=spec.schema,
                    rows=_tabular_rows_factory(resolved, content_hash),
                    source_path=resolved,
                    source=key,
                    content_hash=content_hash,
                    dedupe_on_hash=spec.dedupe_on_hash,
                )
            )
    return specs


def _coerce_to_path_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def _has_extension(path: Path, extensions: Sequence[str]) -> bool:
    suffixes = "".join(path.suffixes).lower()
    return any(suffixes.endswith(ext) for ext in extensions)


def _tabular_rows_factory(path: Path, content_hash: str):
    def rows(_: str, p: Path = path, h: str = content_hash):
        return _iter_tabular_rows(p, h)

    return rows


def _iter_tabular_rows(path: Path, content_hash: str) -> Iterable[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError("pandas is required for tabular config ingestion.") from exc

    if path.suffixes[-1] == ".parquet":
        df = pd.read_parquet(path)
        df["content_hash"] = content_hash
        yield from df.to_dict(orient="records")
        return

    for chunk in pd.read_csv(path, chunksize=50000):
        chunk["content_hash"] = content_hash
        yield from chunk.to_dict(orient="records")


def _digest_path(path: Path, tracker: Optional["Tracker"]) -> str:
    return _shared_digest_path(path, tracker)


def _file_sha256(path: Path) -> str:
    return _shared_file_sha256(path)
