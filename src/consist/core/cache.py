import logging
import weakref
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifacts, ConsistRecord


@runtime_checkable
class CacheHydrationContext(Protocol):
    run_dir: Path
    current_consist: Optional[ConsistRecord]
    db: object
    fs: object

    def resolve_uri(self, uri: str) -> str: ...

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts: ...


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
