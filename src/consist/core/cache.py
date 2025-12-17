import logging
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
    materialize_cached_outputs: str = "never"  # "never" | "requested" | "all"
    materialize_cached_output_paths: Optional[Dict[str, Path]] = None
    materialize_cached_outputs_dir: Optional[Path] = None


def parse_materialize_cached_outputs_kwargs(
    kwargs: Dict[str, Any],
) -> tuple[str, Optional[Dict[str, Path]], Optional[Path]]:
    """
    Parse and validate cached-output materialization options from run kwargs.

    This keeps `Tracker.begin_run(...)` focused on orchestration by moving the
    parsing/validation of these cache-related knobs into `core.cache`.
    """
    materialize_cached_outputs = str(
        kwargs.pop("materialize_cached_outputs", "never")
    ).lower()
    materialize_cached_output_paths_raw = kwargs.pop(
        "materialize_cached_output_paths", None
    )
    materialize_cached_outputs_dir_raw = kwargs.pop(
        "materialize_cached_outputs_dir", None
    )

    if materialize_cached_outputs not in {"never", "requested", "all"}:
        raise ValueError(
            "materialize_cached_outputs must be one of: 'never', 'requested', 'all'"
        )
    if (
        materialize_cached_outputs == "requested"
        and materialize_cached_output_paths_raw is None
    ):
        raise ValueError(
            "materialize_cached_outputs='requested' requires materialize_cached_output_paths"
        )
    if (
        materialize_cached_outputs == "all"
        and materialize_cached_outputs_dir_raw is None
    ):
        raise ValueError(
            "materialize_cached_outputs='all' requires materialize_cached_outputs_dir"
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
        materialize_cached_outputs,
        materialize_cached_output_paths,
        materialize_cached_outputs_dir,
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

    for art in cached_items.outputs.values():
        art.abs_path = tracker.resolve_uri(art.uri)
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
    active_options = options or ActiveRunCacheOptions()

    try:
        policy = str(active_options.materialize_cached_outputs or "never")
        if policy in {"requested", "all"}:
            from consist.core.materialize import (
                build_materialize_items_for_keys,
                materialize_artifacts,
            )

            items: list[tuple[Artifact, Path]] = []
            on_missing = "warn"

            if policy == "requested":
                destinations = active_options.materialize_cached_output_paths or {}
                items = build_materialize_items_for_keys(
                    record.outputs,
                    destinations_by_key=destinations,
                )
                requested_keys = set(destinations.keys())
                hydrated_keys = {a.key for a in record.outputs}
                missing_keys = requested_keys - hydrated_keys
                if missing_keys:
                    logging.warning(
                        "[Consist] Requested cached output materialization for missing keys: %s",
                        sorted(missing_keys),
                    )
            else:  # policy == "all"
                on_missing = "raise"
                if not active_options.materialize_cached_outputs_dir:
                    raise ValueError(
                        "materialize_cached_outputs='all' requires materialize_cached_outputs_dir"
                    )
                out_dir = Path(active_options.materialize_cached_outputs_dir).resolve()
                items = [
                    (a, out_dir / Path(tracker.resolve_uri(a.uri)).name)
                    for a in record.outputs
                ]

            materialized = materialize_artifacts(
                tracker=tracker, items=items, on_missing=on_missing
            )
            if materialized:
                run.meta["materialized_outputs"] = materialized
    except Exception as e:
        if str(active_options.materialize_cached_outputs) == "all":
            raise
        logging.warning(
            "[Consist] Failed to materialize cached outputs (policy=%s): %s",
            active_options.materialize_cached_outputs,
            e,
        )

    run.meta["cache_hit"] = True
    run.meta["cache_source"] = cached_run.id
    run.meta["declared_outputs"] = list(cached_items.outputs.keys())

    return cached_items
