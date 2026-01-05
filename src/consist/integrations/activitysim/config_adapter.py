from __future__ import annotations

import csv
import gzip
import json
import logging
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Set, TYPE_CHECKING

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfig,
    CanonicalizationResult,
    IngestSpec,
    compute_config_pack_hash,
)
from consist.core.identity import IdentityManager
from consist.models.activitysim import (
    ActivitySimCoefficients,
    ActivitySimConstants,
    ActivitySimProbabilities,
)
from consist.models.run import Run

if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker

try:
    import yaml  # type: ignore[import-not-found]
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
    "_coefficients_template.csv",
)

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
    """

    model_name: str = "activitysim"
    adapter_version: str = "0.1"
    allow_heuristic_refs: bool = True
    bundle_configs: bool = True
    bundle_cache_dir: Optional[Path] = None

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity: IdentityManager,
        strict: bool = False,
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
            if strict:
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
        run: Run,
        tracker: "Tracker",
        strict: bool = False,
    ) -> CanonicalizationResult:
        """
        Convert a discovered config into artifacts and ingestable tables.

        Parameters
        ----------
        config : CanonicalConfig
            Discovered config metadata for this run.
        run : Run
            Active run used for artifact/ingest attribution.
        tracker : Tracker
            Tracker instance used for artifact logging and ingest.
        strict : bool, default False
            If True, raise on missing referenced files.

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
            allow_heuristics=self.allow_heuristic_refs,
            strict=strict,
        )
        for csv_path in referenced:
            add_artifact(csv_path, role="csv")

        if self.bundle_configs:
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

        constants_rows = _build_constants_rows(
            run_id=run.id,
            config=config,
            settings=settings,
            inherit_settings=inherit_settings,
        )
        if constants_rows and config.primary_config:
            ingestables.append(
                IngestSpec(
                    table_name="activitysim_constants",
                    schema=ActivitySimConstants,
                    rows=constants_rows,
                    source=_artifact_key_for_path(
                        config.primary_config, config.root_dirs
                    ),
                )
            )

        for csv_path in referenced:
            file_name = csv_path.name
            key = _artifact_key_for_path(csv_path, config.root_dirs)
            if _is_coefficients_file(file_name):
                ingestables.append(
                    IngestSpec(
                        table_name="activitysim_coefficients",
                        schema=ActivitySimCoefficients,
                        rows=_iter_coefficients_rows(csv_path, run.id, strict=strict),
                        source=key,
                    )
                )
            elif _is_probabilities_file(file_name):
                ingestables.append(
                    IngestSpec(
                        table_name="activitysim_probabilities",
                        schema=ActivitySimProbabilities,
                        rows=_iter_probabilities_rows(csv_path, run.id, strict=strict),
                        source=key,
                    )
                )

        return CanonicalizationResult(artifacts=artifacts, ingestables=ingestables)

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
        archive.extractall(output_dir)

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


def _flatten_constants(constants: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, sub_value in value.items():
                new_prefix = f"{prefix}.{key}" if prefix else str(key)
                walk(new_prefix, sub_value)
        elif isinstance(value, list):
            for idx, sub_value in enumerate(value):
                new_prefix = f"{prefix}.{idx}" if prefix else str(idx)
                walk(new_prefix, sub_value)
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


def _is_probabilities_file(file_name: str) -> bool:
    name = file_name[:-3] if file_name.endswith(".csv.gz") else file_name
    return name.endswith(_PROBABILITY_SUFFIXES)


def _iter_coefficients_rows(
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
            return iter([])
        fieldnames = [name or "" for name in reader.fieldnames]
        coeff_key = (
            "coefficient_name" if "coefficient_name" in fieldnames else "coefficient"
        )
        value_key = "value" if "value" in fieldnames else None
        constrain_key = "constrain" if "constrain" in fieldnames else None
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
                    "constrain": constrain or None,
                    "is_constrained": is_constrained,
                }


def _iter_probabilities_rows(
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
            return iter([])
        for idx, row in enumerate(reader):
            dims: Dict[str, Any] = {}
            probs: Dict[str, Any] = {}
            for key, value in row.items():
                if value is None or value == "":
                    continue
                numeric = _parse_float(value)
                if numeric is not None:
                    probs[key] = numeric
                else:
                    dims[key] = _coerce_scalar(value)
            yield {
                "run_id": run_id,
                "file_name": path.name,
                "row_index": idx,
                "dims": dims,
                "probs": probs,
            }


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
        if reader.fieldnames is None:
            raise ValueError(f"CSV missing headers: {csv_path}")
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


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    return path.open("r", encoding="utf-8-sig", newline="")
