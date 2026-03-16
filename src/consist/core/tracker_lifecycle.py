"""Internal run lifecycle coordination for :class:`consist.core.tracker.Tracker`.

This module owns imperative run lifecycle sequencing (`begin_run` / `end_run`) and
its ordering-sensitive side effects (identity derivation, cache hydration decisions,
state mutation, and persistence/event boundaries). It is intentionally internal and
keeps public API behavior in ``Tracker`` stable.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from datetime import datetime, timezone
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union, cast

from pydantic import BaseModel

from consist.core.cache import (
    ActiveRunCacheOptions,
    CacheHydrationContext,
    CacheMaterializationContext,
    CacheValidationContext,
    hydrate_cache_hit_outputs,
    materialize_missing_inputs,
    parse_materialize_cached_outputs_kwargs,
    validate_cached_run_outputs,
)
from consist.core.context import pop_tracker, push_tracker
from consist.core.error_messages import format_problem_cause_fix
from consist.core.validation import (
    validate_config_structure,
    validate_run_meta,
    validate_run_strings,
)
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord, Run
from consist.types import (
    ArtifactRef,
    CodeIdentityMode,
    FacetLike,
    HashInputs,
    HasFacetSchemaVersion,
)

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

UTC = timezone.utc


def _normalize_identity_inputs(hash_inputs: HashInputs) -> Optional[list[Any]]:
    if hash_inputs is None:
        return None
    if isinstance(hash_inputs, (str, Path)):
        raise ValueError(
            format_problem_cause_fix(
                problem="identity_inputs/hash_inputs must be a list of paths.",
                cause=(
                    "A single string/path was provided, which is ambiguous for "
                    "identity input parsing."
                ),
                fix=(
                    "Use the recommended path: identity_inputs=[Path(...)] or "
                    "identity_inputs=[('label', Path(...))]."
                ),
            )
        )
    try:
        items = list(hash_inputs)
    except TypeError as exc:
        raise TypeError(
            format_problem_cause_fix(
                problem="identity_inputs/hash_inputs must be iterable.",
                cause="The provided value cannot be iterated as identity input items.",
                fix=(
                    "Pass a list of paths or (label, path) tuples, for example "
                    "identity_inputs=[Path('config.yaml')]."
                ),
            )
        ) from exc

    for item in items:
        if isinstance(item, tuple):
            if len(item) != 2:
                raise TypeError(
                    format_problem_cause_fix(
                        problem=(
                            "identity_inputs tuple items must be (label, path) pairs."
                        ),
                        cause=f"Received tuple with {len(item)} elements: {item!r}.",
                        fix=(
                            "Use ('my_label', Path('file_or_dir')) for tuple-style "
                            "identity inputs."
                        ),
                    )
                )
            label, path_value = item
            if not isinstance(label, str) or not isinstance(path_value, (str, Path)):
                raise TypeError(
                    format_problem_cause_fix(
                        problem=(
                            "identity_inputs tuple items must be (str, path-like)."
                        ),
                        cause=f"Received tuple item with invalid types: {item!r}.",
                        fix=(
                            "Ensure tuple values are ('label', str|Path) when using "
                            "identity_inputs."
                        ),
                    )
                )
            continue
        if not isinstance(item, (str, Path)):
            raise TypeError(
                format_problem_cause_fix(
                    problem=(
                        "identity_inputs entries must be paths or (label, path) tuples."
                    ),
                    cause=f"Received unsupported identity input item: {item!r}.",
                    fix=(
                        "Pass each identity input as str/Path, or as a "
                        "(label, str|Path) tuple."
                    ),
                )
            )
    return items


class RunLifecycleCoordinator:
    """Coordinate imperative run lifecycle sequencing for ``Tracker``."""

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def begin_run(
        self,
        run_id: str,
        model: str,
        config: Union[Dict[str, Any], BaseModel, None] = None,
        inputs: Optional[list[ArtifactRef]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        cache_mode: str = "reuse",
        *,
        artifact_dir: Optional[Union[str, Path]] = None,
        allow_external_paths: Optional[bool] = None,
        facet: Optional[FacetLike] = None,
        facet_from: Optional[List[str]] = None,
        hash_inputs: HashInputs = None,
        code_identity: Optional[CodeIdentityMode] = None,
        code_identity_extra_deps: Optional[List[str]] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: bool = True,
        **kwargs: Any,
    ) -> Run:
        """Start an imperative run and initialize cache/lifecycle state.

        Parameters
        ----------
        run_id : str
            Unique run identifier.
        model : str
            Model/component name.
        config : dict[str, Any] | BaseModel | None, optional
            Run configuration used for config hashing and snapshot persistence.
        inputs : list[ArtifactRef] | None, optional
            Inputs to log immediately as run inputs.
        tags : list[str] | None, optional
            Run tags.
        description : str | None, optional
            Human-readable run description.
        cache_mode : str, default "reuse"
            Cache operation mode for this run.
        artifact_dir : str | Path | None, optional
            Per-run artifact directory override.
        allow_external_paths : bool | None, optional
            Override external-path policy for this run.
        facet : FacetLike | None, optional
            Optional run facet payload.
        facet_from : list[str] | None, optional
            Config keys to derive into run facet payload.
        hash_inputs : HashInputs, optional
            Additional hash-only identity inputs.
        code_identity : CodeIdentityMode | None, optional
            Code identity mode override for cache signatures.
        code_identity_extra_deps : list[str] | None, optional
            Extra dependency paths folded into code identity hashing.
        facet_schema_version : str | int | None, optional
            Optional facet schema version.
        facet_index : bool, default True
            Whether to index persisted facet key/value pairs.
        **kwargs : Any
            Remaining run metadata and cache-related keyword options.

        Returns
        -------
        Run
            Initialized run record.

        Notes
        -----
        Ordering in this method is intentional:
        1. Normalize and validate identity-relevant inputs.
        2. Create and bind active run state.
        3. Perform cache lookup/hydration side effects.
        4. Persist and emit run-start hooks.
        """
        tracker = self._tracker
        tracker._ensure_write_provenance()
        timing_enabled = os.getenv("CONSIST_CACHE_TIMING", "").lower() in {
            "1",
            "true",
            "yes",
        }
        debug_cache = os.getenv("CONSIST_CACHE_DEBUG", "").lower() in {
            "1",
            "true",
            "yes",
        }

        def _log_timing(label: str, start: float) -> None:
            elapsed = time.perf_counter() - start
            if timing_enabled:
                logging.info("[Consist][timing] %s %.3fs", label, elapsed)
            if debug_cache:
                logging.info("[Consist][cache] %s %.3fs", label, elapsed)

        if tracker.current_consist is not None:
            raise RuntimeError(
                f"Cannot begin_run: A run is already active (id={tracker.current_consist.run.id}). "
                "Call end_run() first."
            )

        validate_run_strings(model_name=model, description=description, tags=tags)

        (
            cache_hydration,
            materialize_cached_output_paths,
            materialize_cached_outputs_dir,
            materialize_cached_outputs_source_root,
            validate_cached_outputs,
        ) = parse_materialize_cached_outputs_kwargs(kwargs)

        raw_config_model: Optional[BaseModel] = (
            config if isinstance(config, BaseModel) else None
        )
        if facet_from:
            if isinstance(facet_from, str):
                raise ValueError("facet_from must be a list of config keys.")
            config_dict_for_facet = tracker._coerce_facet_mapping(config, "config")
            missing = [key for key in facet_from if key not in config_dict_for_facet]
            if missing:
                raise KeyError(f"facet_from keys not found in config: {missing}")
            derived = {key: config_dict_for_facet[key] for key in facet_from}
            if facet is not None:
                facet_dict = tracker._coerce_facet_mapping(facet, "facet")
                merged = dict(derived)
                merged.update(facet_dict)
                facet = tracker.identity.normalize_json(merged)
            else:
                facet = tracker.identity.normalize_json(derived)

        year = kwargs.pop("year", None)
        iteration = kwargs.pop("iteration", None)
        cache_epoch = kwargs.pop("cache_epoch", None)
        cache_version = kwargs.pop("cache_version", None)
        parent_run_id = kwargs.pop("parent_run_id", None)
        code_identity_callable = kwargs.pop("_consist_code_identity_callable", None)

        if artifact_dir is not None:
            kwargs["artifact_dir"] = str(artifact_dir)
        if allow_external_paths is not None:
            kwargs["allow_external_paths"] = bool(allow_external_paths)

        if config is None:
            config_dict: Dict[str, Any] = {}
        elif isinstance(config, BaseModel):
            config_dict = config.model_dump()
        else:
            config_dict = config

        normalized_hash_inputs = _normalize_identity_inputs(hash_inputs)
        if normalized_hash_inputs:
            config_dict = dict(config_dict)
            digest_map = tracker.identity.compute_hash_inputs_digests(
                normalized_hash_inputs
            )
            failed_digests = {
                label: digest
                for label, digest in digest_map.items()
                if isinstance(digest, str) and digest.startswith("ERROR:")
            }
            if failed_digests:
                failed_labels = ", ".join(sorted(failed_digests.keys()))
                raise ValueError(
                    format_problem_cause_fix(
                        problem=(
                            "Failed to compute identity input digests for: "
                            f"{failed_labels}."
                        ),
                        cause=(
                            "One or more identity_inputs/hash_inputs paths are missing "
                            "or unreadable."
                        ),
                        fix=(
                            "Use the recommended path and pass existing files/directories "
                            "in identity_inputs=[...], then retry."
                        ),
                    )
                )
            if "__consist_hash_inputs__" in config_dict:
                logging.warning(
                    "[Consist] Overwriting user-provided '__consist_hash_inputs__' in config for run %s.",
                    run_id,
                )
            config_dict["__consist_hash_inputs__"] = digest_map
            kwargs["consist_hash_inputs"] = digest_map

        config_dict = tracker.identity.normalize_json(config_dict)
        if not isinstance(config_dict, MappingABC):
            raise TypeError("config must be a mapping after normalization.")
        validate_config_structure(config_dict)

        config_hash = tracker.identity.compute_run_config_hash(
            config=config_dict,
            model=model,
            year=year,
            iteration=iteration,
            cache_epoch=cache_epoch,
            cache_version=cache_version,
        )
        identity_mode = code_identity or "repo_git"
        try:
            git_hash = tracker.identity.resolve_code_version(
                mode=identity_mode,
                func=code_identity_callable,
                extra_deps=code_identity_extra_deps,
            )
        except Exception as exc:
            logging.warning(
                "[Consist] Failed to resolve code identity mode=%s for run %s: %s. "
                "Falling back to repo git identity.",
                identity_mode,
                run_id,
                exc,
            )
            git_hash = tracker.identity.get_code_version()
        if code_identity is not None:
            kwargs["code_identity"] = identity_mode
        if code_identity_extra_deps:
            kwargs["code_identity_extra_deps"] = list(code_identity_extra_deps)

        kwargs["_physical_run_dir"] = str(tracker.run_dir)
        if cache_epoch is not None:
            kwargs["cache_epoch"] = cache_epoch
        if cache_version is not None:
            kwargs["cache_version"] = cache_version
        if kwargs:
            validate_run_meta(kwargs)

        now = datetime.now(UTC)
        run = Run(
            id=run_id,
            model_name=model,
            description=description,
            year=year,
            iteration=iteration,
            parent_run_id=parent_run_id,
            tags=tags or [],
            status="running",
            config_hash=config_hash,
            git_hash=git_hash,
            meta=kwargs,
            started_at=now,
            created_at=now,
        )
        tracker._runs_by_id[run.id] = run

        if run.meta is None:
            run.meta = {}
        run.meta.setdefault("mounts", dict(tracker.mounts))

        push_tracker(tracker)
        tracker.current_consist = ConsistRecord(run=run, config=config_dict)
        tracker._active_run_cache_options = ActiveRunCacheOptions(
            cache_mode=cache_mode,
            cache_hydration=cache_hydration,
            materialize_cached_output_paths=materialize_cached_output_paths,
            materialize_cached_outputs_dir=materialize_cached_outputs_dir,
            materialize_cached_outputs_source_root=(
                materialize_cached_outputs_source_root
            ),
            validate_cached_outputs=validate_cached_outputs,
        )

        facet_dict = tracker.config_facets.resolve_facet_dict(
            model=model, raw_config_model=raw_config_model, facet=facet, run_id=run.id
        )
        if facet_dict is not None:
            tracker.current_consist.facet = facet_dict
            schema_version = facet_schema_version
            if (
                schema_version is None
                and raw_config_model is not None
                and isinstance(raw_config_model, HasFacetSchemaVersion)
            ):
                schema_version = raw_config_model.facet_schema_version
            tracker.config_facets.persist_facet(
                run=run,
                model=model,
                facet_dict=facet_dict,
                schema_name=tracker.config_facets.infer_schema_name(
                    raw_config_model, facet
                ),
                schema_version=schema_version,
                index_kv=facet_index,
            )

        current_consist = tracker.current_consist
        if current_consist is None:
            raise RuntimeError("Cannot start run: no active consist record.")

        if inputs:
            for item in inputs:
                if isinstance(item, Artifact):
                    tracker.log_artifact(item, direction="input")
                else:
                    key = Path(item).stem
                    tracker.log_artifact(item, key=key, direction="input")

        if not run.parent_run_id:
            parent_candidates = [
                a.run_id for a in current_consist.inputs if a.run_id is not None
            ]
            if parent_candidates:
                run.parent_run_id = parent_candidates[-1]

        try:
            t0 = time.perf_counter()
            tracker._prefetch_run_signatures(current_consist.inputs)
            _log_timing("prefetch_run_signatures", t0)
            t0 = time.perf_counter()
            input_hash = tracker.identity.compute_input_hash(
                current_consist.inputs,
                path_resolver=tracker.resolve_uri,
                signature_lookup=tracker._resolve_run_signature,
            )
            run.input_hash = input_hash
            run.signature = tracker.identity.calculate_run_signature(
                code_hash=git_hash,
                config_hash=config_hash,
                input_hash=input_hash,
            )
            _log_timing("compute_input_hash", t0)
        except Exception as exc:
            logging.warning(
                "[Consist Warning] Failed to compute inputs hash for run %s: %s",
                run_id,
                exc,
            )
            run.input_hash = "error"
            run.signature = "error"

        cached_output_ids: Optional[List[uuid.UUID]] = None

        if cache_mode == "reuse":
            cache_key = (
                (run.config_hash, run.input_hash, run.git_hash)
                if run.config_hash and run.input_hash and run.git_hash
                else None
            )
            if debug_cache:
                logging.info(
                    "[Consist][cache] lookup key config=%s input=%s code=%s",
                    run.config_hash,
                    run.input_hash,
                    run.git_hash,
                )

            cached_run = (
                tracker._local_cache_index.get(cache_key) if cache_key else None
            )
            if cached_run is None:
                t0 = time.perf_counter()
                if cache_key:
                    config_h, input_h, git_h = cache_key
                    cached_run = tracker.find_matching_run(
                        config_hash=config_h,
                        input_hash=input_h,
                        git_hash=git_h,
                    )
                _log_timing("find_matching_run", t0)
            cache_valid = False
            if cached_run:
                t0 = time.perf_counter()
                cache_valid = validate_cached_run_outputs(
                    tracker=cast(CacheValidationContext, tracker),
                    run=cached_run,
                    options=tracker._active_run_cache_options,
                )
                _log_timing("validate_cached_outputs", t0)
            elif debug_cache:
                logging.info("[Consist][cache] miss: no matching completed run.")

            if cached_run and cache_valid:
                t0 = time.perf_counter()
                cached_items = hydrate_cache_hit_outputs(
                    tracker=cast(CacheHydrationContext, tracker),
                    run=run,
                    cached_run=cached_run,
                    options=tracker._active_run_cache_options,
                    link_outputs=False,
                )
                cached_output_ids = [art.id for art in cached_items.outputs.values()]
                _log_timing("hydrate_cache_hit_outputs", t0)
                if debug_cache:
                    logging.info(
                        "[Consist][cache] hit: cached_run=%s outputs=%d hydration=%s",
                        cached_run.id,
                        len(cached_items.outputs),
                        tracker._active_run_cache_options.cache_hydration,
                    )
            else:
                if cached_run and not cache_valid and debug_cache:
                    logging.info(
                        "[Consist][cache] miss: cached run %s failed validation.",
                        cached_run.id,
                    )
                logging.debug("🔄 [Consist] Cache Miss. Running...")
        elif cache_mode == "overwrite":
            logging.debug("⚠️ [Consist] Cache lookup skipped (Mode: Overwrite).")
        elif cache_mode == "readonly":
            logging.debug("👁️ [Consist] Read-only mode.")

        if not tracker.current_consist.cached_run:
            materialize_missing_inputs(
                tracker=cast(CacheMaterializationContext, tracker),
                options=tracker._active_run_cache_options,
            )

        tracker._flush_json()
        if cached_output_ids and tracker.db:
            tracker.persistence.sync_run_with_links(
                run,
                artifact_ids=cached_output_ids,
                direction="output",
            )
        else:
            tracker._sync_run_to_db(run)
        tracker._emit_run_start(run)

        return run

    def end_run(
        self,
        status: str = "completed",
        error: Optional[Exception] = None,
    ) -> Run:
        """Finalize the active run and clear lifecycle state.

        Parameters
        ----------
        status : str, default "completed"
            Final run status to persist.
        error : Exception | None, optional
            Failure exception to persist and emit on failed runs.

        Returns
        -------
        Run
            Finalized run record.

        Notes
        -----
        This method intentionally performs teardown in strict order:
        update run status/meta, persist snapshots, sync DB/cache index, emit
        lifecycle hooks, and finally clear active tracker state.
        """
        tracker = self._tracker
        if not tracker.current_consist:
            raise RuntimeError("No active run to end. Call begin_run() first.")

        run = tracker.current_consist.run
        cache_mode = tracker._active_run_cache_options.cache_mode or "reuse"

        if cache_mode != "readonly":
            run.status = status
        else:
            run.status = "skipped_save" if status == "completed" else status

        if error:
            run.meta["error"] = str(error)

        pop_tracker()

        tracker._last_consist = tracker.current_consist

        end_time = datetime.now(UTC)
        run.ended_at = end_time
        run.updated_at = end_time

        tracker._flush_json()
        if cache_mode != "readonly":
            tracker._sync_run_to_db(run)

        if cache_mode != "readonly" and run.status == "completed":
            cache_key = (
                run.config_hash or "",
                run.input_hash or "",
                run.git_hash or "",
            )
            cache_hit = bool(run.meta.get("cache_hit")) if run.meta else False
            if cache_key in tracker._local_cache_index and cache_mode != "overwrite":
                if not cache_hit:
                    logging.warning(
                        "Cache key collision detected (extremely rare): %s. "
                        "Keeping first cached run (created %s).",
                        cache_key,
                        tracker._local_cache_index[cache_key].created_at,
                    )
            else:
                tracker._local_cache_index[cache_key] = run
            if len(tracker._local_cache_index) > tracker._local_cache_max_entries:
                tracker._local_cache_index.pop(next(iter(tracker._local_cache_index)))

        if error is not None:
            tracker._emit_run_failed(run, error)
        elif run.status == "completed":
            outputs = tracker.current_consist.outputs if tracker.current_consist else []
            tracker._emit_run_complete(run, outputs)

        tracker.current_consist = None
        tracker._active_run_cache_options = ActiveRunCacheOptions()

        return run
