import logging
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


@runtime_checkable
class CacheHydrationContext(Protocol):
    run_dir: Path
    current_consist: Optional[ConsistRecord]
    db: Optional[CacheDb]
    fs: CacheFs

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
    run_dir: Path

    def resolve_uri(self, uri: str) -> str: ...


@dataclass(frozen=True, slots=True)
class ActiveRunCacheOptions:
    """
    Active-run cache/materialization controls for Tracker.

    Stored on the Tracker (not in run.meta) because they affect runtime behavior and
    should reset automatically at the end of the run.
    """

    cache_mode: str = "reuse"
    cache_hydration: str = "metadata"  # "metadata" | "inputs-missing" | "outputs-requested" | "outputs-all"
    materialize_cached_output_paths: Optional[Dict[str, Path]] = None
    materialize_cached_outputs_dir: Optional[Path] = None
    materialize_cached_outputs_source_root: Optional[Path] = None
    validate_cached_outputs: str = "lazy"  # "eager" | "lazy"


def _can_delegate_run_output_materialization(tracker: CacheHydrationContext) -> bool:
    return callable(getattr(tracker, "materialize_run_outputs", None))


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

    allow_external_paths = bool(getattr(tracker, "allow_external_paths", False))
    if isinstance(target_run.meta, dict) and "allow_external_paths" in target_run.meta:
        allow_external_paths = bool(target_run.meta["allow_external_paths"])
    allowed_base = None if allow_external_paths else tracker.run_dir

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

    with tempfile.TemporaryDirectory(prefix="consist-cache-hit-") as tmp_dir:
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
            allowed_base=allowed_base,
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
                    materialize_artifacts_from_sources,
                )

                items: list[tuple[Artifact, Path, Path]] = []
                on_missing = "warn"
                db = getattr(tracker, "db", None)
                run_dir_cache: dict[str, Optional[str]] = {}
                fallback_run_dir = None
                if cached_run.meta:
                    fallback_run_dir = cached_run.meta.get("_physical_run_dir")

                if active_options.cache_hydration == "outputs-requested":
                    destinations = active_options.materialize_cached_output_paths or {}
                    requested_items = build_materialize_items_for_keys(
                        record.outputs,
                        destinations_by_key=destinations,
                    )
                    for art, dest in requested_items:
                        original_run_dir = fallback_run_dir
                        if db and art.run_id:
                            run_id = str(art.run_id)
                            if run_id not in run_dir_cache:
                                run = db.get_run(run_id)
                                run_dir_cache[run_id] = (
                                    run.meta.get("_physical_run_dir")
                                    if run and run.meta
                                    else None
                                )
                            original_run_dir = run_dir_cache.get(run_id)
                        source = Path(
                            tracker.fs.resolve_historical_path(
                                art.container_uri, original_run_dir
                            )
                        )
                        items.append((art, source, Path(dest)))
                    requested_keys = set(destinations.keys())
                    hydrated_keys = {a.key for a in record.outputs}
                    missing_keys = requested_keys - hydrated_keys
                    if missing_keys:
                        logging.warning(
                            "[Consist] Requested cached output materialization for missing keys: %s",
                            sorted(missing_keys),
                        )
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
                        original_run_dir = fallback_run_dir
                        if db and art.run_id:
                            run_id = str(art.run_id)
                            if run_id not in run_dir_cache:
                                run = db.get_run(run_id)
                                run_dir_cache[run_id] = (
                                    run.meta.get("_physical_run_dir")
                                    if run and run.meta
                                    else None
                                )
                            original_run_dir = run_dir_cache.get(run_id)
                        source = Path(
                            tracker.fs.resolve_historical_path(
                                art.container_uri, original_run_dir
                            )
                        )
                        items.append((art, source, out_dir / source.name))

                allow_external_paths = bool(
                    getattr(tracker, "allow_external_paths", False)
                )
                if (
                    isinstance(target_run.meta, dict)
                    and "allow_external_paths" in target_run.meta
                ):
                    allow_external_paths = bool(target_run.meta["allow_external_paths"])
                allowed_base = None if allow_external_paths else tracker.run_dir
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

    for art in run_artifacts.outputs.values():
        resolved_path = tracker.resolve_uri(art.container_uri)
        if not Path(resolved_path).exists() and not art.meta.get("is_ingested", False):
            from consist.tools.mount_diagnostics import (
                build_mount_resolution_hint,
                format_missing_artifact_mount_help,
            )

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
    run_dir_cache: dict[str, Optional[str]] = {}

    for artifact in tracker.current_consist.inputs:
        if not artifact.run_id:
            continue

        destination = Path(tracker.resolve_uri(artifact.container_uri))
        if destination.exists():
            continue

        run_id = str(artifact.run_id)
        if run_id not in run_dir_cache:
            run = db.get_run(run_id)
            run_dir_cache[run_id] = (
                run.meta.get("_physical_run_dir") if run and run.meta else None
            )

        original_run_dir = run_dir_cache.get(run_id)
        if not original_run_dir:
            continue

        source = Path(
            tracker.fs.resolve_historical_path(artifact.container_uri, original_run_dir)
        )
        if source.exists():
            items.append((artifact, source, destination))
            continue

        if artifact.meta.get("is_ingested", False):
            db_items.append((artifact, destination))

    materialized: dict[str, str] = {}

    if items:
        from consist.core.materialize import materialize_artifacts_from_sources

        materialized.update(
            materialize_artifacts_from_sources(
                items, allowed_base=tracker.run_dir, on_missing="warn"
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
