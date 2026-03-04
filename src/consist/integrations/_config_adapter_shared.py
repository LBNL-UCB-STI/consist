"""Shared helpers for config-adapter override execution flows.

This module is intended for adapter authors implementing
``run_with_config_overrides``-style APIs. Keep adapter-specific behavior in
callbacks and use the shared orchestration to preserve consistent run semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import inspect
from pathlib import Path
import shutil
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

if TYPE_CHECKING:  # pragma: no cover
    from consist.core.tracker import Tracker


class OverridesLike(Protocol):
    """Minimal protocol required by the shared override orchestration."""

    def to_canonical_dict(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class OverrideRunHooks:
    """Adapter-specific callbacks used by override-run orchestration.

    Adapter authors implementing a new config adapter should wire these hooks
    instead of copying the override execution control flow.

    Contract
    --------
    - ``resolve_base_selector`` returns a dict with ``kind`` set to either
      ``"run_id"`` or ``"config_dirs"`` and enough data for downstream hooks.
    - ``selector_identity`` returns a deterministic payload used for override-key
      hashing. It should include all fields that materially affect the staged
      config result.
    - ``materialize`` returns an object with at least ``root_dirs``.
    - ``select_root`` picks the root used for runtime-kwarg injection and
      optional ``resolved_config_identity`` hashing.
    - ``build_run_adapter`` returns the adapter instance to pass to
      ``tracker.run`` and must expose ``model_name`` for metadata.
    - ``build_override_runtime_kwargs`` should only inject kwargs the callable
      can accept and must avoid overriding caller-provided runtime kwargs.
    """

    resolve_base_selector: Callable[..., dict[str, Any]]
    selector_identity: Callable[[dict[str, Any], bool], dict[str, Any]]
    materialize: Callable[[dict[str, Any], Path, bool], Any]
    select_root: Callable[[Any], Path]
    build_run_adapter: Callable[[Any], Any]
    build_override_runtime_kwargs: Callable[..., dict[str, Any]]


def resolve_override_base_selector(
    *,
    base_run_id: str | None,
    base_config_dirs: Sequence[Path] | None,
    base_primary_config: Path | None,
) -> dict[str, Any]:
    """Validate and normalize the base-selector inputs for override runs."""
    if base_run_id is not None and base_config_dirs is not None:
        raise ValueError(
            "run_with_config_overrides requires exactly one base selector. "
            "Provide either base_run_id or base_config_dirs, not both."
        )
    if base_run_id is None and base_config_dirs is None:
        raise ValueError(
            "run_with_config_overrides requires a base selector. "
            "Provide either base_run_id or base_config_dirs."
        )
    if base_run_id is not None:
        if base_primary_config is not None:
            raise ValueError(
                "base_primary_config can only be used with base_config_dirs."
            )
        resolved_run_id = str(base_run_id).strip()
        if not resolved_run_id:
            raise ValueError("base_run_id must be a non-empty string when provided.")
        return {"kind": "run_id", "run_id": resolved_run_id}
    if base_config_dirs is None or len(base_config_dirs) == 0:
        raise ValueError(
            "base_config_dirs must contain at least one directory when provided."
        )
    return {"kind": "config_dirs", "config_dirs": list(base_config_dirs)}


def resolve_base_config_dirs(base_config_dirs: Sequence[Path]) -> list[Path]:
    """Resolve and validate base config directories.

    Returned paths are absolute and guaranteed to exist as directories.
    """
    resolved_dirs: list[Path] = []
    for path_like in base_config_dirs:
        root_dir = Path(path_like).resolve()
        if not root_dir.exists():
            raise FileNotFoundError(
                f"Base config directory does not exist: {root_dir!s}"
            )
        if not root_dir.is_dir():
            raise ValueError(
                f"base_config_dirs entries must be directories. Got: {root_dir!s}"
            )
        resolved_dirs.append(root_dir)
    if not resolved_dirs:
        raise ValueError(
            "base_config_dirs must contain at least one directory when provided."
        )
    return resolved_dirs


def run_with_config_overrides_orchestrated(
    *,
    tracker: "Tracker",
    base_run_id: str | None,
    base_config_dirs: Sequence[Path] | None,
    base_primary_config: Path | None,
    overrides: OverridesLike,
    output_dir: Path,
    fn: Any,
    name: str,
    model: str | None,
    config: dict[str, Any] | None,
    outputs: list[str] | None,
    execution_options: Any | None,
    strict: bool,
    identity_inputs: Any,
    resolved_config_identity: Literal["auto", "off"],
    identity_label: str,
    override_runtime_kwargs: Mapping[str, Any] | None,
    run_kwargs: Mapping[str, Any],
    adapter_version: str,
    hooks: OverrideRunHooks,
) -> Any:
    """Run the shared config-override orchestration.

    This centralizes the common logic used by ActivitySim/BEAM adapters:
    selector validation, deterministic staging directory creation, materializing
    overridden configs, runtime-kwarg injection, ``tracker.run`` execution, and
    resolved identity metadata persistence.

    Adapter-author usage
    --------------------
    Pass adapter-specific behavior through ``hooks`` rather than branching here.
    For a new adapter, most work should be implementing:
    1) base selector identity payload,
    2) materialization strategy,
    3) root selection,
    4) run-adapter construction.
    """
    resolved_run_kwargs = dict(run_kwargs)
    for forbidden in ("adapter", "config_plan", "hash_inputs"):
        if forbidden in resolved_run_kwargs:
            raise ValueError(
                f"run_with_config_overrides does not accept {forbidden}= in run kwargs."
            )
    runtime_kwargs = resolved_run_kwargs.pop("runtime_kwargs", None)
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
    if resolved_config_identity not in {"auto", "off"}:
        raise ValueError(
            "resolved_config_identity must be either 'auto' or 'off'. "
            f"Got {resolved_config_identity!r}."
        )
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"output_dir must be a directory path: {output_dir!s}")

    base_selector = hooks.resolve_base_selector(
        base_run_id=base_run_id,
        base_config_dirs=base_config_dirs,
        base_primary_config=base_primary_config,
    )
    selector_identity = hooks.selector_identity(base_selector, strict)

    override_key = tracker.identity.canonical_json_sha256(
        {
            "base_selector": selector_identity,
            "overrides": overrides.to_canonical_dict(),
            "adapter_version": adapter_version,
        }
    )
    staged_output_dir = output_dir / f"override_{override_key[:16]}"
    if staged_output_dir.exists():
        shutil.rmtree(staged_output_dir)

    materialized = hooks.materialize(base_selector, staged_output_dir, strict)
    selected_root = hooks.select_root(materialized)
    run_adapter = hooks.build_run_adapter(materialized)

    existing_runtime_kwargs = (
        runtime_kwargs
        if runtime_kwargs is not None
        else (execution_options.runtime_kwargs if execution_options else None)
    )
    injected_runtime_kwargs = hooks.build_override_runtime_kwargs(
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

    auto_identity_label = identity_label.strip()
    merged_identity_inputs: list[Any] = list(identity_inputs or [])
    auto_identity_path: Path | None = None
    if resolved_config_identity == "auto":
        auto_identity_path = selected_root
        merged_identity_inputs.append((auto_identity_label, selected_root))
    resolved_identity_inputs = merged_identity_inputs or None

    run_result = tracker.run(
        fn=fn,
        name=name,
        model=model,
        config=config,
        adapter=run_adapter,
        identity_inputs=resolved_identity_inputs,
        outputs=outputs,
        execution_options=resolved_execution_options,
        **resolved_run_kwargs,
    )
    auto_digest: str | None = None
    if resolved_config_identity == "auto":
        consist_hash_inputs = (
            run_result.run.meta.get("consist_hash_inputs")
            if isinstance(run_result.run.meta, dict)
            else None
        )
        if isinstance(consist_hash_inputs, dict):
            resolved_digest = consist_hash_inputs.get(auto_identity_label)
            if isinstance(resolved_digest, str):
                auto_digest = resolved_digest

    if run_result.run.meta is None:
        run_result.run.meta = {}
    run_result.run.meta["resolved_config_identity"] = {
        "mode": resolved_config_identity,
        "adapter": run_adapter.model_name,
        "label": auto_identity_label,
        "path": str(auto_identity_path) if auto_identity_path is not None else None,
        "digest": auto_digest,
    }
    tracker._flush_run_snapshot(run_result.run)
    tracker._sync_run_to_db(run_result.run)
    return run_result


def callable_accepts_kwarg(fn: Any, runtime_key: str) -> bool:
    """Return True when ``fn`` can accept the given kwarg name."""
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


def build_override_runtime_kwargs(
    *,
    fn: Any,
    selected_root: Path,
    root_dirs: Sequence[Path],
    explicit_mapping: Mapping[str, Any] | None,
    existing_runtime_kwargs: Mapping[str, Any] | None,
    default_mapping: Mapping[str, Any],
) -> dict[str, Any]:
    """Build adapter-derived runtime kwargs for override runs.

    The returned mapping only includes keys that:
    - are not already provided by the caller, and
    - are accepted by the target callable.
    """
    mapping = (
        dict(explicit_mapping)
        if explicit_mapping is not None
        else dict(default_mapping)
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
        if not callable_accepts_kwarg(fn, runtime_key):
            continue
        if isinstance(source, str) and source in resolved_sources:
            injected[runtime_key] = resolved_sources[source]
        else:
            injected[runtime_key] = source
    return injected


def artifact_key_for_path(path: Path, config_dirs: Sequence[Path]) -> str:
    """Create a stable config artifact key relative to known config roots."""
    resolved = path.resolve()
    for config_dir in config_dirs:
        config_dir = config_dir.resolve()
        if resolved.is_relative_to(config_dir):
            rel = resolved.relative_to(config_dir).as_posix()
            return f"config:{config_dir.name}/{rel}"
    return f"config:{resolved.name}"


def get_nested_value(data: Any, keys: Sequence[str]) -> Any:
    """Fetch a nested dictionary value, returning ``None`` on missing path."""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def digest_path(path: Path, tracker: Optional["Tracker"]) -> str:
    """Digest a path with tracker identity when available, else SHA-256."""
    if tracker is not None:
        return tracker.identity.digest_path(path)
    return file_sha256(path)


def file_sha256(path: Path) -> str:
    """Compute SHA-256 digest for a file path."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
