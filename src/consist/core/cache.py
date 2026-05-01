import hashlib
import os
import logging
import shutil
import tempfile
import weakref
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Dict,
    Literal,
    Optional,
    Protocol,
    Sequence,
    TYPE_CHECKING,
    cast,
    runtime_checkable,
)

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifacts, ConsistRecord
from consist.core.error_messages import format_problem_cause_fix

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


@runtime_checkable
class CacheDb(Protocol):
    def get_run(self, run_id: str) -> Optional[Run]: ...


@runtime_checkable
class CacheFs(Protocol):
    def resolve_historical_path(
        self, uri: str, original_run_dir: Optional[str]
    ) -> str: ...

    def normalize_recovery_roots(
        self,
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]] | None,
    ) -> list[str]: ...

    def get_remappable_relative_path(self, uri: str) -> Path | None: ...

    def get_historical_root(
        self,
        uri: str,
        *,
        original_run_dir: Optional[str],
        mounts_snapshot: Optional[Dict[str, str]] = None,
        artifact_mount_root: Optional[str] = None,
    ) -> Path | None: ...


@runtime_checkable
class CacheIdentity(Protocol):
    def compute_file_checksum(self, file_path: str | Path) -> str: ...


@runtime_checkable
class CacheMaterializationRootContext(Protocol):
    run_dir: Path
    mounts: Dict[str, str]
    allow_external_paths: bool


@runtime_checkable
class CacheHydrationContext(Protocol):
    run_dir: Path
    allow_external_paths: bool
    current_consist: Optional[ConsistRecord]
    db: Optional[CacheDb]
    fs: CacheFs
    mounts: Dict[str, str]

    def resolve_uri(self, uri: str) -> str: ...

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts: ...

    def materialize_run_outputs(
        self,
        run_id: str,
        *,
        target_root: str | Path,
        source_root: str | Path | None = None,
        keys: Sequence[str] | None = None,
        preserve_existing: bool = True,
        on_missing: Literal["warn", "raise"] = "warn",
        db_fallback: Literal["never", "if_ingested"] = "if_ingested",
    ) -> Any: ...


@runtime_checkable
class CacheValidationContext(Protocol):
    """
    Protocol describing the tracker surface required for cache validation.

    This keeps cache validation logic reusable across tracker-like objects.
    """

    current_consist: Optional[ConsistRecord]
    db: Optional[CacheDb]
    mounts: Dict[str, str]

    def resolve_uri(self, uri: str) -> str: ...

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts: ...


@runtime_checkable
class CacheMaterializationContext(Protocol):
    """
    Protocol describing the tracker surface required for input materialization.

    This keeps input materialization logic reusable across tracker-like objects.
    """

    current_consist: Optional[ConsistRecord]
    db: Optional[CacheDb]
    fs: CacheFs
    identity: CacheIdentity
    run_dir: Path
    allow_external_paths: bool
    mounts: Dict[str, str]

    def resolve_uri(self, uri: str) -> str: ...


@dataclass(frozen=True, slots=True)
class ActiveRunCacheOptions:
    """
    Active-run cache/materialization controls for Tracker.

    Stored on the Tracker (not in run.meta) because they affect runtime
    behavior and should reset automatically at the end of the run.

    ``materialize_cached_outputs_source_root`` remains the per-run override for
    archive/mirror recovery. Artifact-level ``recovery_roots`` are consulted
    separately during cache-hit output hydration and eager cache validation.
    """

    cache_mode: str = "reuse"
    cache_hydration: str = "metadata"  # "metadata" | "inputs-missing" | "outputs-requested" | "outputs-all"
    materialize_cached_output_paths: Optional[Dict[str, Path]] = None
    materialize_cached_outputs_dir: Optional[Path] = None
    materialize_cached_outputs_source_root: Optional[Path] = None
    validate_cached_outputs: str = "lazy"  # "eager" | "lazy"
    requested_input_paths: Optional[Dict[str, Path]] = None
    requested_input_materialization: Optional[str] = None
    requested_input_materialization_mode: Optional[str] = None
    requested_input_validate_content_hash: str = "if-present"


def _can_delegate_run_output_materialization(tracker: CacheHydrationContext) -> bool:
    return callable(getattr(tracker, "materialize_run_outputs", None))


def _allowed_materialization_roots(
    tracker: CacheMaterializationRootContext,
    *,
    target_run: Run | None = None,
) -> tuple[Path, ...] | None:
    from consist.core.materialize import build_allowed_materialization_roots

    allow_external_paths = bool(getattr(tracker, "allow_external_paths", False))
    if (
        target_run is not None
        and isinstance(target_run.meta, dict)
        and "allow_external_paths" in target_run.meta
    ):
        allow_external_paths = bool(target_run.meta["allow_external_paths"])

    return build_allowed_materialization_roots(
        run_dir=tracker.run_dir,
        mounts=getattr(tracker, "mounts", {}),
        allow_external_paths=allow_external_paths,
    )


def _warn_on_partial_materialization_result(result: Any, *, policy: str) -> None:
    missing = list(getattr(result, "skipped_missing_source", []) or [])
    failed = list(getattr(result, "failed", []) or [])
    if not missing and not failed:
        return

    summary = getattr(result, "summary", None)
    detail = str(summary) if summary else f"missing={len(missing)} failed={len(failed)}"
    logging.warning(
        "[Consist] Cached output materialization completed with partial results "
        "(policy=%s): %s",
        policy,
        detail,
    )


def _should_fallback_to_legacy_output_materialization(exc: Exception) -> bool:
    message = str(exc)
    partial_impl_markers = (
        "build_run_output_materialize_plan",
        "materialize_planned_outputs",
        "MaterializationResult",
    )
    return (
        isinstance(exc, AttributeError)
        and any(marker in message for marker in partial_impl_markers)
    ) or (
        isinstance(exc, RuntimeError)
        and "tracker has no database configured" in message
    )


def _materialize_cached_outputs_via_run_api(
    *,
    tracker: CacheHydrationContext,
    cached_run: Run,
    target_run: Run,
    active_options: ActiveRunCacheOptions,
    outputs_by_key: dict[str, Artifact],
) -> Optional[dict[str, str]]:
    requested_paths = active_options.materialize_cached_output_paths or {}
    requested_keys = list(requested_paths)
    hydrated_keys = set(outputs_by_key)
    missing_keys = sorted(set(requested_keys) - hydrated_keys)
    if missing_keys:
        logging.warning(
            "[Consist] Requested cached output materialization for missing keys: %s",
            missing_keys,
        )

    allowed_roots = _allowed_materialization_roots(tracker, target_run=target_run)

    if active_options.cache_hydration == "outputs-all":
        if not active_options.materialize_cached_outputs_dir:
            raise ValueError(
                "cache_hydration='outputs-all' requires materialize_cached_outputs_dir"
            )
        result = tracker.materialize_run_outputs(
            cached_run.id,
            target_root=Path(active_options.materialize_cached_outputs_dir).resolve(),
            source_root=active_options.materialize_cached_outputs_source_root,
            preserve_existing=True,
            on_missing="raise",
            db_fallback="if_ingested",
        )
        _warn_on_partial_materialization_result(result, policy="outputs-all")
        return dict(getattr(result, "materialized", {}) or {})

    existing_keys = [key for key in requested_keys if key in hydrated_keys]
    if not existing_keys:
        return {}

    from consist.core.materialize import materialize_artifacts_from_sources

    staging_parent = tracker.run_dir.resolve()
    staging_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="consist-cache-hit-",
        dir=str(staging_parent),
    ) as tmp_dir:
        staged_root = Path(tmp_dir)
        result = tracker.materialize_run_outputs(
            cached_run.id,
            target_root=staged_root,
            source_root=active_options.materialize_cached_outputs_source_root,
            keys=existing_keys,
            preserve_existing=False,
            on_missing="warn",
            db_fallback="if_ingested",
        )
        _warn_on_partial_materialization_result(result, policy="outputs-requested")

        staged_items: list[tuple[Artifact, Path, Path]] = []
        for key, staged_path in dict(getattr(result, "materialized", {}) or {}).items():
            artifact = outputs_by_key.get(key)
            destination = requested_paths.get(key)
            if artifact is None or destination is None:
                continue
            staged_items.append((artifact, Path(staged_path), Path(destination)))

        if not staged_items:
            return {}

        return materialize_artifacts_from_sources(
            items=staged_items,
            allowed_base=allowed_roots,
            on_missing="warn",
        )


def parse_materialize_cached_outputs_kwargs(
    kwargs: Dict[str, Any],
) -> tuple[str, Optional[Dict[str, Path]], Optional[Path], Optional[Path], str]:
    """
    Parse and validate cached-output materialization options from run kwargs.

    This keeps `Tracker.begin_run(...)` focused on orchestration by moving the
    parsing/validation of these cache-related knobs into `core.cache`.
    """
    retired_keys = [
        "materialize_cached_outputs",
        "materialize_cached_inputs",
        "resolve_cached_output_paths",
    ]
    used_retired = [key for key in retired_keys if key in kwargs]
    if used_retired:
        raise ValueError(
            "Retired cache options used: "
            + ", ".join(sorted(used_retired))
            + ". Use cache_hydration instead."
        )

    cache_hydration = str(kwargs.pop("cache_hydration", "metadata")).lower()
    materialize_cached_output_paths_raw = kwargs.pop(
        "materialize_cached_output_paths", None
    )
    materialize_cached_outputs_dir_raw = kwargs.pop(
        "materialize_cached_outputs_dir", None
    )
    materialize_cached_outputs_source_root_raw = kwargs.pop(
        "materialize_cached_outputs_source_root", None
    )
    validate_cached_outputs = str(kwargs.pop("validate_cached_outputs", "lazy")).lower()

    if cache_hydration not in {
        "metadata",
        "inputs-missing",
        "outputs-requested",
        "outputs-all",
    }:
        raise ValueError(
            "cache_hydration must be one of: "
            "'metadata', 'inputs-missing', 'outputs-requested', 'outputs-all'"
        )

    if validate_cached_outputs not in {"eager", "lazy"}:
        raise ValueError("validate_cached_outputs must be one of: 'eager', 'lazy'")

    if cache_hydration == "outputs-requested":
        if materialize_cached_output_paths_raw is None:
            raise ValueError(
                "cache_hydration='outputs-requested' requires materialize_cached_output_paths"
            )
        if materialize_cached_outputs_dir_raw is not None:
            raise ValueError(
                "cache_hydration='outputs-requested' does not use materialize_cached_outputs_dir"
            )
    elif cache_hydration == "outputs-all":
        if materialize_cached_outputs_dir_raw is None:
            raise ValueError(
                "cache_hydration='outputs-all' requires materialize_cached_outputs_dir"
            )
        if materialize_cached_output_paths_raw is not None:
            raise ValueError(
                "cache_hydration='outputs-all' does not use materialize_cached_output_paths"
            )
    else:
        if (
            materialize_cached_output_paths_raw is not None
            or materialize_cached_outputs_dir_raw is not None
            or materialize_cached_outputs_source_root_raw is not None
        ):
            raise ValueError(
                "cache_hydration does not accept materialize_cached_output_paths or "
                "materialize_cached_outputs_dir or "
                "materialize_cached_outputs_source_root in metadata/inputs-missing "
                "modes."
            )

    materialize_cached_output_paths: Optional[Dict[str, Path]] = None
    if materialize_cached_output_paths_raw is not None:
        materialize_cached_output_paths = {
            str(k): Path(v)
            for k, v in dict(materialize_cached_output_paths_raw).items()
        }

    materialize_cached_outputs_dir: Optional[Path] = (
        Path(materialize_cached_outputs_dir_raw)
        if materialize_cached_outputs_dir_raw is not None
        else None
    )
    materialize_cached_outputs_source_root: Optional[Path] = (
        Path(materialize_cached_outputs_source_root_raw)
        if materialize_cached_outputs_source_root_raw is not None
        else None
    )

    return (
        cache_hydration,
        materialize_cached_output_paths,
        materialize_cached_outputs_dir,
        materialize_cached_outputs_source_root,
        validate_cached_outputs,
    )


def hydrate_cache_hit_outputs(
    *,
    tracker: CacheHydrationContext,
    run: Run,
    cached_run: Run,
    options: Optional[ActiveRunCacheOptions] = None,
    link_outputs: bool = True,
) -> RunArtifacts:
    """
    Hydrate cached outputs into the active run and optionally materialize bytes to disk.

    Notes
    -----
    - Hydration means: populate `tracker.current_consist.cached_run` and
      `tracker.current_consist.outputs` with Artifact objects from `cached_run`.
    - Materialization means: make the files exist on disk at requested destinations,
      using DB recovery where possible (see `consist.core.materialize`).
    """
    record = tracker.current_consist
    if record is None:
        raise RuntimeError("Cannot hydrate cache hit: no active run.")

    record.cached_run = cached_run
    target_run = record.run

    cached_items = tracker.get_artifacts_for_run(cached_run.id)

    scenario_hint = (
        f", scenario='{cached_run.parent_run_id}'"
        if getattr(cached_run, "parent_run_id", None)
        else ""
    )
    logging.info(
        "✅ [Consist] Cache HIT for step '%s': matched cached run '%s'%s "
        "(signature=config+inputs+code, inputs=%d, outputs=%d).",
        run.id,
        cached_run.id,
        scenario_hint,
        len(cached_items.inputs),
        len(cached_items.outputs),
    )

    active_options = options or ActiveRunCacheOptions()

    for art in cached_items.outputs.values():
        # Deep-clone artifacts to prevent metadata sharing between cached run and active run.
        # This ensures mutations to artifact.meta in active run don't affect cached run.
        cloned = deepcopy(art)
        # Attach tracker ref so Artifact.path can lazily resolve URIs on demand.
        cloned._tracker = weakref.ref(tracker)
        record.outputs.append(cloned)

    # Ensure the new (cached) run has DB links to its hydrated outputs.
    if link_outputs:
        db = getattr(tracker, "db", None)
        if db and getattr(db, "engine", None) is not None:
            try:
                db.link_artifacts_to_run_bulk(
                    artifact_ids=[a.id for a in record.outputs],
                    run_id=run.id,
                    direction="output",
                )
            except Exception as e:
                logging.warning(
                    "[Consist] Failed to link cached outputs to run id=%s: %s",
                    run.id,
                    e,
                )

    # Optional physical materialization of cached outputs (copy-bytes-on-disk).
    # Core Consist defaults to "never" (artifact hydration only). Integrations and
    # advanced workflows can opt in when callers expect host paths to exist.
    try:
        if active_options.cache_hydration in {"outputs-requested", "outputs-all"}:
            materialized: dict[str, str] = {}
            should_use_legacy = not _can_delegate_run_output_materialization(tracker)
            if not should_use_legacy:
                try:
                    materialized = (
                        _materialize_cached_outputs_via_run_api(
                            tracker=tracker,
                            cached_run=cached_run,
                            target_run=target_run,
                            active_options=active_options,
                            outputs_by_key={
                                artifact.key: artifact for artifact in record.outputs
                            },
                        )
                        or {}
                    )
                except Exception as exc:
                    if not _should_fallback_to_legacy_output_materialization(exc):
                        raise
                    if (
                        active_options.materialize_cached_outputs_source_root
                        is not None
                    ):
                        raise RuntimeError(
                            "cache-hydration source-root override requires the "
                            "run-scoped output materialization path."
                        ) from exc
                    should_use_legacy = True

            if (
                should_use_legacy
                and active_options.materialize_cached_outputs_source_root is not None
            ):
                raise RuntimeError(
                    "cache-hydration source-root override requires the run-scoped "
                    "output materialization path."
                )

            if should_use_legacy:
                from consist.core.materialize import (
                    build_materialize_items_for_keys,
                    find_existing_recovery_source_path,
                    materialize_artifacts_from_sources,
                )

                items: list[tuple[Artifact, Path, Path]] = []
                on_missing = "warn"
                db = getattr(tracker, "db", None)
                run_cache: dict[str, Optional[Run]] = {}

                if active_options.cache_hydration == "outputs-requested":
                    destinations = active_options.materialize_cached_output_paths or {}
                    requested_items = build_materialize_items_for_keys(
                        record.outputs,
                        destinations_by_key=destinations,
                    )
                    unresolved_requested: list[tuple[str, str]] = []
                    for art, dest in requested_items:
                        historical_run = cached_run
                        if db and art.run_id:
                            run_id = str(art.run_id)
                            if run_id not in run_cache:
                                run_cache[run_id] = db.get_run(run_id)
                            run: Run | None = run_cache.get(run_id)
                            if run is not None:
                                historical_run = run
                        _, source, _ = find_existing_recovery_source_path(
                            cast("Tracker", tracker),
                            artifact=art,
                            run=historical_run,
                            source_root=active_options.materialize_cached_outputs_source_root,
                        )
                        if source is not None:
                            items.append((art, source, Path(dest)))
                        else:
                            unresolved_requested.append(
                                (
                                    art.key,
                                    f"[Consist] Cannot materialize cached input {art.key!r}: source path missing.",
                                )
                            )
                    requested_keys = set(destinations.keys())
                    hydrated_keys = {a.key for a in record.outputs}
                    missing_keys = requested_keys - hydrated_keys
                    if missing_keys:
                        logging.warning(
                            "[Consist] Requested cached output materialization for missing keys: %s",
                            sorted(missing_keys),
                        )
                    for _, message in unresolved_requested:
                        logging.warning(message)
                else:  # outputs-all
                    on_missing = "raise"
                    if not active_options.materialize_cached_outputs_dir:
                        raise ValueError(
                            "cache_hydration='outputs-all' requires materialize_cached_outputs_dir"
                        )
                    out_dir = Path(
                        active_options.materialize_cached_outputs_dir
                    ).resolve()
                    for art in record.outputs:
                        historical_run = cached_run
                        if db and art.run_id:
                            run_id = str(art.run_id)
                            if run_id not in run_cache:
                                run_cache[run_id] = db.get_run(run_id)
                            run: Run | None = run_cache.get(run_id)
                            if run is not None:
                                historical_run = run
                        relative_path, source, _ = find_existing_recovery_source_path(
                            cast("Tracker", tracker),
                            artifact=art,
                            run=historical_run,
                            source_root=active_options.materialize_cached_outputs_source_root,
                        )
                        if source is None or relative_path is None:
                            raise FileNotFoundError(
                                "[Consist] Cannot materialize cached input "
                                f"{art.key!r}: source path missing."
                            )
                        items.append((art, source, out_dir / relative_path))

                allowed_base = _allowed_materialization_roots(
                    tracker, target_run=target_run
                )
                materialized = materialize_artifacts_from_sources(
                    items=items, allowed_base=allowed_base, on_missing=on_missing
                )

            if materialized:
                target_run.meta["materialized_outputs"] = materialized
    except Exception as e:
        if active_options.materialize_cached_outputs_source_root is not None:
            raise
        if active_options.cache_hydration == "outputs-all":
            raise
        logging.warning(
            "[Consist] Failed to materialize cached outputs (policy=%s): %s",
            active_options.cache_hydration,
            e,
        )

    target_run.meta["cache_hit"] = True
    target_run.meta["cache_source"] = cached_run.id
    target_run.meta["declared_outputs"] = list(cached_items.outputs.keys())

    return cached_items


def validate_cached_run_outputs(
    *,
    tracker: CacheValidationContext,
    run: Run,
    options: Optional[ActiveRunCacheOptions],
) -> bool:
    """
    Validate that cached outputs for a run still exist on disk (or are ingested).

    Parameters
    ----------
    tracker : CacheValidationContext
        Tracker-like context that provides DB access, mounts, and URI resolution.
    run : Run
        Cached run whose outputs should be validated.
    options : Optional[ActiveRunCacheOptions]
        Active cache options controlling validation policy.

    Returns
    -------
    bool
        True if all required outputs are present or validation is disabled.
    """
    if not getattr(tracker, "db", None):
        return True

    policy = "eager"
    if options:
        policy = str(options.validate_cached_outputs or "eager").lower()

    if policy != "eager":
        return True

    run_artifacts = tracker.get_artifacts_for_run(run.id)
    db = tracker.db

    for art in run_artifacts.outputs.values():
        resolved_path = tracker.resolve_uri(art.container_uri)
        if not Path(resolved_path).exists() and not art.meta.get("is_ingested", False):
            from consist.core.materialize import find_existing_recovery_source_path
            from consist.tools.mount_diagnostics import (
                build_mount_resolution_hint,
                format_missing_artifact_mount_help,
            )

            owning_run = run
            if db is not None and art.run_id and str(art.run_id) != run.id:
                producing_run = db.get_run(str(art.run_id))
                if producing_run is not None:
                    owning_run = producing_run

            _, recovery_source, _ = find_existing_recovery_source_path(
                cast("Tracker", tracker),
                artifact=art,
                run=owning_run,
                source_root=None,
            )
            if recovery_source is not None:
                continue

            hint = build_mount_resolution_hint(
                art.container_uri, artifact_meta=art.meta, mounts=tracker.mounts
            )
            help_text = (
                "\n"
                + format_missing_artifact_mount_help(hint, resolved_path=resolved_path)
                if hint
                else f"\nResolved path: {resolved_path}"
            )
            logging.warning(
                "⚠️ Cache Validation Failed. Missing: %s%s",
                art.container_uri,
                help_text,
            )
            return False
    return True


def materialize_missing_inputs(
    *, tracker: CacheMaterializationContext, options: Optional[ActiveRunCacheOptions]
) -> None:
    """
    Copy cached-input artifacts into the current run_dir when missing.

    This is intended for cache-miss runs that rely on cached outputs from
    previous runs in a different run_dir.

    Parameters
    ----------
    tracker : CacheMaterializationContext
        Tracker-like context that provides DB access, filesystem helpers,
        and URI resolution.
    options : Optional[ActiveRunCacheOptions]
        Active cache options that determine whether to materialize inputs.
    """
    active_options = options or ActiveRunCacheOptions()
    if active_options.cache_hydration != "inputs-missing":
        return
    if not tracker.current_consist:
        return
    db = tracker.db
    if db is None:
        return

    items: list[tuple[Artifact, Path, Path]] = []
    db_items: list[tuple[Artifact, Path]] = []

    for artifact in tracker.current_consist.inputs:
        if not artifact.run_id:
            continue

        destination = Path(tracker.resolve_uri(artifact.container_uri))
        if destination.exists():
            continue

        run_id = str(artifact.run_id)
        run = db.get_run(run_id)
        original_run_dir = (
            run.meta.get("_physical_run_dir") if run and run.meta else None
        )
        if not original_run_dir:
            continue

        source = Path(
            tracker.fs.resolve_historical_path(artifact.container_uri, original_run_dir)
        )
        if source is not None and source.exists():
            items.append((artifact, source, destination))
            continue

        if artifact.meta.get("is_ingested", False):
            db_items.append((artifact, destination))

    materialized: dict[str, str] = {}

    if items:
        from consist.core.materialize import materialize_artifacts_from_sources

        materialized.update(
            materialize_artifacts_from_sources(
                items,
                allowed_base=_allowed_materialization_roots(
                    tracker, target_run=tracker.current_consist.run
                ),
                on_missing="warn",
            )
        )
        for artifact, _, destination in items:
            artifact.abs_path = str(destination)

    if db_items:
        from consist.core.materialize import materialize_ingested_artifact_from_db

        for artifact, destination in db_items:
            materialized_path = materialize_ingested_artifact_from_db(
                artifact=artifact,
                tracker=cast("Tracker", tracker),
                destination=destination,
            )
            materialized[artifact.key] = materialized_path
            artifact.abs_path = materialized_path

    if materialized:
        tracker.current_consist.run.meta["materialized_inputs"] = materialized


def materialize_requested_inputs(
    *,
    tracker: CacheMaterializationContext,
    options: Optional[ActiveRunCacheOptions],
) -> dict[str, str]:
    """
    Stage explicitly requested input paths for the active run.

    This helper is used after cache-hit hydration and after cache-miss input
    materialization so Python execution sees concrete local paths when
    ``ExecutionOptions.input_materialization='requested'`` is enabled.
    """
    active_options = options or ActiveRunCacheOptions()
    if active_options.requested_input_materialization != "requested":
        return {}
    if not tracker.current_consist:
        return {}

    requested_mode = active_options.requested_input_materialization_mode or "copy"
    requested_paths = active_options.requested_input_paths or {}
    if not requested_paths:
        return {}

    run = tracker.current_consist.run
    inputs_by_key: dict[str, list[Artifact]] = {}
    for artifact in tracker.current_consist.inputs:
        inputs_by_key.setdefault(artifact.key, []).append(artifact)

    allowed_base = _allowed_materialization_roots(tracker, target_run=run)
    staged: dict[str, str] = {}
    run_dir_cache: dict[str, Optional[str]] = {}

    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _dir_hash(path: Path) -> str:
        digest = hashlib.sha256()
        base = Path(path).resolve()
        for root, dirnames, filenames in os.walk(base):
            dirnames.sort()
            filenames.sort()
            rel_root = Path(root).resolve().relative_to(base).as_posix()
            digest.update(f"dir:{rel_root}".encode())
            for dirname in dirnames:
                digest.update(
                    f"subdir:{Path(root, dirname).resolve().relative_to(base).as_posix()}".encode()
                )
            for filename in filenames:
                file_path = Path(root, filename)
                rel_file = file_path.resolve().relative_to(base).as_posix()
                digest.update(f"file:{rel_file}:{file_path.stat().st_size}".encode())
                digest.update(_file_hash(file_path).encode())
        return digest.hexdigest()

    def _content_signature(path: Path) -> tuple[str, str] | None:
        if path.is_dir():
            return ("dir", _dir_hash(path))
        if path.is_file():
            return ("file", _file_hash(path))
        return None

    def _artifact_hash_matches_path(artifact: Artifact, path: Path) -> bool:
        if not artifact.hash:
            return False
        return tracker.identity.compute_file_checksum(path) == artifact.hash

    def _paths_match(source: Path, destination: Path) -> bool:
        if source.resolve() == destination.resolve():
            return True
        source_sig = _content_signature(source)
        destination_sig = _content_signature(destination)
        return source_sig is not None and source_sig == destination_sig

    def _remove_existing(path: Path) -> None:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()

    def _resolve_source(artifact: Artifact) -> Optional[Path]:
        resolved_path = Path(tracker.resolve_uri(artifact.container_uri))
        if resolved_path.exists():
            return resolved_path

        if artifact.abs_path:
            abs_path = Path(artifact.abs_path)
            if abs_path.exists():
                return abs_path

        if tracker.db is not None and artifact.run_id:
            run_id = str(artifact.run_id)
            if run_id not in run_dir_cache:
                prior_run = tracker.db.get_run(run_id)
                run_dir_cache[run_id] = (
                    prior_run.meta.get("_physical_run_dir")
                    if prior_run and prior_run.meta
                    else None
                )
            original_run_dir = run_dir_cache.get(run_id)
            if original_run_dir:
                historical = Path(
                    tracker.fs.resolve_historical_path(
                        artifact.container_uri,
                        original_run_dir,
                    )
                )
                if historical.exists():
                    return historical

        return None

    for key, destination in requested_paths.items():
        matches = inputs_by_key.get(key, [])
        if not matches:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        f"Requested input materialization key {key!r} was not "
                        "present in the resolved run inputs."
                    ),
                    cause=(
                        "Requested staging destinations must correspond to a "
                        "resolved input artifact key."
                    ),
                    fix=(
                        "Choose keys from the resolved inputs mapping, or remove "
                        "the entry from execution_options.input_paths."
                    ),
                )
            )
        if len(matches) > 1:
            raise ValueError(
                format_problem_cause_fix(
                    problem=(
                        f"Requested input materialization key {key!r} is ambiguous."
                    ),
                    cause="Multiple resolved input artifacts share the same key.",
                    fix=(
                        "Use unique input keys for the run, or stage a different key."
                    ),
                )
            )

        artifact = matches[0]
        destination_path = Path(destination).resolve()

        from consist.core.materialize import (
            _ensure_destination_not_symlink,
            materialize_artifacts_from_sources,
            materialize_ingested_artifact_from_db,
            validate_allowed_materialization_destination,
        )

        validate_allowed_materialization_destination(destination_path, allowed_base)
        _ensure_destination_not_symlink(destination_path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        source_path = _resolve_source(artifact)
        if destination_path.exists():
            if destination_path.is_symlink():
                raise ValueError(
                    f"Symlink detected in destination path: {destination_path}"
                )
            if (
                source_path is None
                and artifact.hash
                and _artifact_hash_matches_path(artifact, destination_path)
            ):
                artifact.abs_path = str(destination_path)
                staged[key] = str(destination_path)
                continue
            if source_path is not None and _paths_match(source_path, destination_path):
                artifact.abs_path = str(destination_path)
                staged[key] = str(destination_path)
                continue
            if requested_mode != "copy":
                raise ValueError(
                    format_problem_cause_fix(
                        problem=(
                            "requested_input_materialization_mode must be 'copy'."
                        ),
                        cause="Only copy-based requested input staging is supported.",
                        fix=(
                            "Use execution_options=ExecutionOptions("
                            "input_materialization_mode='copy')."
                        ),
                    )
                )
            _remove_existing(destination_path)

        if source_path is None:
            if artifact.meta.get("is_ingested", False):
                staged_path = Path(
                    materialize_ingested_artifact_from_db(
                        artifact=artifact,
                        tracker=cast("Tracker", tracker),
                        destination=destination_path,
                        overwrite=False,
                    )
                ).resolve()
                if (
                    active_options.requested_input_validate_content_hash == "if-present"
                    and artifact.hash
                    and not _artifact_hash_matches_path(artifact, staged_path)
                ):
                    raise ValueError(
                        format_problem_cause_fix(
                            problem=(
                                f"Hash mismatch after staging ingested input {key!r}."
                            ),
                            cause=(
                                "The reconstructed bytes do not match the artifact hash."
                            ),
                            fix=(
                                "Verify the source data in DuckDB or disable hash "
                                "validation for this staging request."
                            ),
                        )
                    )
                artifact.abs_path = str(staged_path)
                staged[key] = str(staged_path)
                continue
            raise FileNotFoundError(
                f"[Consist] Cannot stage input {artifact.key!r}: source path missing."
            )

        if source_path.resolve() == destination_path:
            artifact.abs_path = str(destination_path)
            staged[key] = str(destination_path)
            continue

        materialize_artifacts_from_sources(
            items=[(artifact, source_path, destination_path)],
            allowed_base=allowed_base,
            on_missing="raise",
        )

        if (
            active_options.requested_input_validate_content_hash == "if-present"
            and artifact.hash
            and not _artifact_hash_matches_path(artifact, destination_path)
        ):
            raise ValueError(
                format_problem_cause_fix(
                    problem=f"Hash mismatch after staging input {key!r}.",
                    cause="The staged file bytes do not match the artifact hash.",
                    fix=(
                        "Verify the source artifact or disable hash validation for "
                        "this staging request."
                    ),
                )
            )

        artifact.abs_path = str(destination_path)
        staged[key] = str(destination_path)

    return staged
