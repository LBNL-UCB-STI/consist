import logging
import weakref
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, runtime_checkable, TYPE_CHECKING, cast

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifacts, ConsistRecord

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


@runtime_checkable
class CacheHydrationContext(Protocol):
    run_dir: Path
    current_consist: Optional[ConsistRecord]
    db: object
    fs: object

    def resolve_uri(self, uri: str) -> str: ...

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts: ...


@runtime_checkable
class CacheValidationContext(Protocol):
    """
    Protocol describing the tracker surface required for cache validation.

    This keeps cache validation logic reusable across tracker-like objects.
    """

    current_consist: Optional[ConsistRecord]
    db: object
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
    db: object
    fs: object

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
    validate_cached_outputs: str = "lazy"  # "eager" | "lazy"


def parse_materialize_cached_outputs_kwargs(
    kwargs: Dict[str, Any],
) -> tuple[str, Optional[Dict[str, Path]], Optional[Path], str]:
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
        ):
            raise ValueError(
                "cache_hydration does not accept materialize_cached_output_paths or "
                "materialize_cached_outputs_dir in metadata/inputs-missing modes."
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

    return (
        cache_hydration,
        materialize_cached_output_paths,
        materialize_cached_outputs_dir,
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
        # Attach tracker ref so Artifact.path can lazily resolve URIs on demand.
        art._tracker = weakref.ref(tracker)
        record.outputs.append(art)

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
                        tracker.fs.resolve_historical_path(art.uri, original_run_dir)
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
                out_dir = Path(active_options.materialize_cached_outputs_dir).resolve()
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
                        tracker.fs.resolve_historical_path(art.uri, original_run_dir)
                    )
                    items.append((art, source, out_dir / source.name))

            materialized = materialize_artifacts_from_sources(
                items=items, on_missing=on_missing
            )
            if materialized:
                run.meta["materialized_outputs"] = materialized
    except Exception as e:
        if active_options.cache_hydration == "outputs-all":
            raise
        logging.warning(
            "[Consist] Failed to materialize cached outputs (policy=%s): %s",
            active_options.cache_hydration,
            e,
        )

    run.meta["cache_hit"] = True
    run.meta["cache_source"] = cached_run.id
    run.meta["declared_outputs"] = list(cached_items.outputs.keys())

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
        resolved_path = tracker.resolve_uri(art.uri)
        if not Path(resolved_path).exists() and not art.meta.get("is_ingested", False):
            from consist.tools.mount_diagnostics import (
                build_mount_resolution_hint,
                format_missing_artifact_mount_help,
            )

            hint = build_mount_resolution_hint(
                art.uri, artifact_meta=art.meta, mounts=tracker.mounts
            )
            help_text = (
                "\n"
                + format_missing_artifact_mount_help(hint, resolved_path=resolved_path)
                if hint
                else f"\nResolved path: {resolved_path}"
            )
            logging.warning(
                "⚠️ Cache Validation Failed. Missing: %s%s", art.uri, help_text
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
    if not getattr(tracker, "db", None):
        return

    items: list[tuple[Artifact, Path, Path]] = []
    db_items: list[tuple[Artifact, Path]] = []
    run_dir_cache: dict[str, Optional[str]] = {}

    for artifact in tracker.current_consist.inputs:
        if not artifact.run_id:
            continue

        destination = Path(tracker.resolve_uri(artifact.uri))
        if destination.exists():
            continue

        run_id = str(artifact.run_id)
        if run_id not in run_dir_cache:
            run = tracker.db.get_run(run_id)
            run_dir_cache[run_id] = (
                run.meta.get("_physical_run_dir") if run and run.meta else None
            )

        original_run_dir = run_dir_cache.get(run_id)
        if not original_run_dir:
            continue

        source = Path(
            tracker.fs.resolve_historical_path(artifact.uri, original_run_dir)
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
            materialize_artifacts_from_sources(items, on_missing="warn")
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
