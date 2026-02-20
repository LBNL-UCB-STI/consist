from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import logging
import os
import re
from dataclasses import dataclass, field, replace
import shutil
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, TYPE_CHECKING

from sqlmodel import SQLModel

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfig,
    CanonicalizationResult,
    ConfigAdapterOptions,
    IngestSpec,
)
from consist.core.config_canonicalization import ConfigPlan
from consist.core.identity import IdentityManager
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from consist.models.run import Run

if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker
    from consist.models.run import RunResult
    from consist.types import ExecutionOptions

try:
    _pyhocon = importlib.import_module("pyhocon")
    _pyhocon_config_tree = importlib.import_module("pyhocon.config_tree")
    ConfigFactory = _pyhocon.ConfigFactory
    HOCONConverter = _pyhocon.HOCONConverter
    NonExistentKey = _pyhocon_config_tree.NonExistentKey
except ImportError:  # pragma: no cover
    ConfigFactory = None
    HOCONConverter = None
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


@dataclass
class BeamConfigAdapter:
    model_name: str = "beam"
    adapter_version: str = "0.1"
    root_dirs: Optional[list[Path]] = None
    primary_config: Optional[Path] = None
    resolve_substitutions: bool = True
    env_overrides: Optional[dict[str, str]] = None
    ingest_specs: list[BeamIngestSpec] = field(default_factory=list)

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
        referenced_paths = _collect_path_references(config_tree)
        for value in referenced_paths:
            resolved = _resolve_reference(value, config.root_dirs)
            if resolved is None or not resolved.exists():
                logging.warning("[Consist][BEAM] Missing referenced path: %s", value)
                if resolved_strict:
                    raise FileNotFoundError(value)
                continue
            _add_artifact(
                artifacts_by_path,
                resolved,
                config.root_dirs,
                role="ref",
                meta={"config_reference": value},
            )

        rows = _iter_config_rows(config_tree, content_hash=config.content_hash)
        if plan_only:
            # Return a fresh iterator each time the plan is materialized.
            def rows_factory(
                run_id: str,
                tree: dict[str, Any] = config_tree,
                h: str = config.content_hash,
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
                content_hash=config.content_hash,
                dedupe_on_hash=True,
            ),
            _run_link_spec(
                table_name="beam_config_cache",
                content_hash=config.content_hash,
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

        return CanonicalizationResult(artifacts=artifacts, ingestables=ingestables)

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
        base_run_id: str,
        overrides: BeamConfigOverrides,
        output_dir: Path,
        fn: Any,
        name: str,
        model: str | None = None,
        config: dict[str, Any] | None = None,
        outputs: list[str] | None = None,
        execution_options: "ExecutionOptions" | None = None,
        strict: bool = True,
        identity_label: str = "beam_config",
        override_runtime_kwargs: Mapping[str, Any] | None = None,
        **run_kwargs: Any,
    ) -> "RunResult":
        for forbidden in ("adapter", "identity_inputs", "config_plan", "hash_inputs"):
            if forbidden in run_kwargs:
                raise ValueError(
                    "run_with_config_overrides does not accept "
                    f"{forbidden}= in run kwargs."
                )
        runtime_kwargs = run_kwargs.pop("runtime_kwargs", None)
        if (
            runtime_kwargs is not None
            and execution_options is not None
            and execution_options.runtime_kwargs is not None
        ):
            raise ValueError(
                "run_with_config_overrides received runtime kwargs in both "
                "runtime_kwargs= and execution_options.runtime_kwargs=. "
                "Use exactly one runtime kwargs source, and use "
                "override_runtime_kwargs=... to map adapter-derived override "
                "roots (for example selected_root_dir) into callable kwargs."
            )
        if not identity_label or not identity_label.strip():
            raise ValueError("identity_label must be a non-empty string.")
        if output_dir.exists() and not output_dir.is_dir():
            raise ValueError(f"output_dir must be a directory path: {output_dir!s}")

        override_key = tracker.identity.canonical_json_sha256(
            {
                "base_run_id": base_run_id,
                "overrides": overrides.to_canonical_dict(),
                "adapter_version": self.adapter_version,
            }
        )
        staged_output_dir = output_dir / f"override_{override_key[:16]}"
        if staged_output_dir.exists():
            shutil.rmtree(staged_output_dir)

        materialized = self.materialize_from_run(
            tracker=tracker,
            run_id=base_run_id,
            overrides=overrides,
            output_dir=staged_output_dir,
            strict=strict,
        )
        if not materialized.root_dirs:
            raise ValueError("Materialized BEAM config has no root directories.")
        selected_root = materialized.root_dirs[0]
        run_adapter = replace(
            self,
            root_dirs=list(materialized.root_dirs),
            primary_config=materialized.primary_config,
        )

        existing_runtime_kwargs = (
            runtime_kwargs
            if runtime_kwargs is not None
            else (execution_options.runtime_kwargs if execution_options else None)
        )
        injected_runtime_kwargs = self._build_override_runtime_kwargs(
            fn=fn,
            selected_root=selected_root,
            root_dirs=materialized.root_dirs,
            explicit_mapping=override_runtime_kwargs,
            existing_runtime_kwargs=existing_runtime_kwargs,
        )
        merged_runtime_kwargs: dict[str, Any] = {}
        if existing_runtime_kwargs is not None:
            merged_runtime_kwargs.update(existing_runtime_kwargs)
        merged_runtime_kwargs.update(injected_runtime_kwargs)
        resolved_execution_options = execution_options
        if runtime_kwargs is not None or injected_runtime_kwargs:
            if execution_options is None:
                from consist.types import ExecutionOptions

                resolved_execution_options = ExecutionOptions(
                    runtime_kwargs=merged_runtime_kwargs
                )
            else:
                resolved_execution_options = replace(
                    execution_options,
                    runtime_kwargs=merged_runtime_kwargs,
                )

        return tracker.run(
            fn=fn,
            name=name,
            model=model,
            config=config,
            adapter=run_adapter,
            identity_inputs=[(identity_label.strip(), selected_root)],
            outputs=outputs,
            execution_options=resolved_execution_options,
            **run_kwargs,
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
    def _build_override_runtime_kwargs(
        *,
        fn: Any,
        selected_root: Path,
        root_dirs: Sequence[Path],
        explicit_mapping: Mapping[str, Any] | None,
        existing_runtime_kwargs: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        mapping = (
            dict(explicit_mapping)
            if explicit_mapping is not None
            else dict(_DEFAULT_OVERRIDE_RUNTIME_KWARGS)
        )
        existing = dict(existing_runtime_kwargs or {})
        resolved_sources: dict[str, Any] = {
            "selected_root_dir": selected_root,
            "root_dirs": list(root_dirs),
        }
        injected: dict[str, Any] = {}
        for runtime_key, source in mapping.items():
            if runtime_key in existing:
                continue
            if not BeamConfigAdapter._callable_accepts_kwarg(fn, runtime_key):
                continue
            if isinstance(source, str) and source in resolved_sources:
                injected[runtime_key] = resolved_sources[source]
            else:
                injected[runtime_key] = source
        return injected

    @staticmethod
    def _callable_accepts_kwarg(fn: Any, runtime_key: str) -> bool:
        try:
            signature = inspect.signature(fn)
        except (TypeError, ValueError):
            return False
        if runtime_key in signature.parameters:
            return True
        return any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )

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
    original_env: dict[str, Optional[str]] = {}
    if env_overrides:
        for key, value in env_overrides.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value
    try:
        config = ConfigFactory.parse_file(str(path), resolve=resolve)
        return json.loads(HOCONConverter.to_json(config))
    finally:
        if env_overrides:
            for key, original in original_env.items():
                if original is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original


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


def _collect_path_references(config_tree: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    ignore_tokens = {
        "csv",
        "csv.gz",
        "xml",
        "xml.gz",
        "parquet",
        "omx",
        "h5",
    }

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            candidate = node.strip()
            if not candidate:
                return
            if candidate in ignore_tokens:
                return
            if candidate.startswith(("http://", "https://", "tcp://")):
                return
            if "/" in candidate or candidate.endswith(
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
            ):
                refs.add(candidate)

    walk(config_tree)
    return refs


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
    resolved = path.resolve()
    for config_dir in config_dirs:
        config_dir = config_dir.resolve()
        if resolved.is_relative_to(config_dir):
            rel = resolved.relative_to(config_dir).as_posix()
            return f"config:{config_dir.name}/{rel}"
    return f"config:{resolved.name}"


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
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


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
    if tracker is not None:
        return tracker.identity.digest_path(path)
    return _file_sha256(path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
