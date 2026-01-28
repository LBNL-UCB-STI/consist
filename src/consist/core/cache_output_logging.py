import logging
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable

from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord
from consist.types import ArtifactRef


@runtime_checkable
class CacheHitOutputLogContext(Protocol):
    run_dir: Path
    current_consist: Optional[ConsistRecord]

    def resolve_uri(self, uri: str) -> str: ...

    @property
    def is_cached(self) -> bool: ...

    def cached_output(self, key: Optional[str] = None) -> Optional[Artifact]: ...


def maybe_return_cached_output_or_demote_cache_hit(
    *,
    tracker: CacheHitOutputLogContext,
    path: ArtifactRef,
    key: Optional[str],
) -> Optional[Artifact]:
    """
    Cache-hit output logging policy for `Tracker.log_artifact(..., direction="output")`.

    On cache hits, Consist hydrates outputs but does not skip user code; many workflows
    therefore call `log_artifact(...)` in both cache-hit and cache-miss cases.

    Behavior:
    - If the output `key` exists in hydrated cached outputs, return it (when the caller
      is "re-logging by key", including placeholder/nonexistent paths).
    - If the caller provides a conflicting, existing path for that key, demote the run
      from cache-hit to executing semantics so provenance remains truthful.
    - If the key does not exist in cached outputs, demote and allow the caller to create it.
    """
    if not tracker.is_cached:
        return None

    record = tracker.current_consist
    if record is None or record.cached_run is None:
        return None

    lookup_key = key
    if lookup_key is None and isinstance(path, Artifact):
        lookup_key = path.key

    if not lookup_key:
        raise ValueError(
            "Cannot log output artifact on a cache hit without a key. "
            "Provide `key=...` (or pass an Artifact with `.key` set)."
        )

    cached = tracker.cached_output(key=lookup_key)
    if cached is None:
        _demote_cache_hit(
            record,
            reason=f"caller attempted to log output key={lookup_key!r} not present in cached outputs",
        )
        return None

    # Artifact inputs: allow "same artifact" relog, otherwise demote.
    if isinstance(path, Artifact):
        if path.id == cached.id or path.container_uri == cached.container_uri:
            return cached
        _demote_cache_hit(
            record,
            reason=f"caller provided an Artifact for key={lookup_key!r} that does not match cached output",
        )
        return None

    # Path-like inputs:
    try:
        candidate_abs = _resolve_candidate_output_path(
            path=str(path),
            run_dir=tracker.run_dir,
            resolve_uri=tracker.resolve_uri,
        )
        cached_abs = Path(
            cached.abs_path or tracker.resolve_uri(cached.container_uri)
        ).resolve()

        # Placeholder path => treat as "re-log by key" (common cache-agnostic pattern).
        if not candidate_abs.exists():
            return cached

        if candidate_abs == cached_abs:
            return cached

        _demote_cache_hit(
            record,
            reason=(
                f"caller provided a different path for key={lookup_key!r} "
                f"(cached={str(cached_abs)!r}, requested={str(candidate_abs)!r})"
            ),
        )
        return None
    except Exception:
        _demote_cache_hit(
            record,
            reason=f"could not compare cached vs requested paths for key={lookup_key!r}",
        )
        return None


def _resolve_candidate_output_path(*, path: str, run_dir: Path, resolve_uri) -> Path:
    if "://" in path:
        return Path(resolve_uri(path)).resolve()
    candidate_path = Path(path)
    if not candidate_path.is_absolute():
        candidate_path = run_dir / candidate_path
    return candidate_path.resolve()


def _demote_cache_hit(record: ConsistRecord, *, reason: str) -> None:
    cached_source = record.cached_run.id if record.cached_run else None
    run_id = record.run.id

    already_warned = bool((record.run.meta or {}).get("_demoted_cache_hit_warned"))
    if not already_warned:
        record.run.meta["_demoted_cache_hit_warned"] = True
        logging.warning(
            "[Consist] Demoting cache hit to cache miss for run_id=%s (cache_source=%s): %s. "
            "This usually means the run signature matched but your code is producing outputs "
            "that differ from the cached run (e.g., output path/key changes, implicit randomness, "
            "or identity-relevant parameters not included in `config`/`hash_inputs`/declared `inputs`). "
            "Best practice: make the signature reflect the true work (add missing inputs/config), "
            "or use cache_mode='overwrite' when you intentionally want fresh outputs.",
            run_id,
            cached_source,
            reason,
        )
    else:
        logging.warning(
            "[Consist] Demoting cache hit to cache miss for run_id=%s (cache_source=%s): %s",
            run_id,
            cached_source,
            reason,
        )

    record.cached_run = None
    record.outputs = []

    try:
        record.run.meta.pop("cache_hit", None)
        record.run.meta.pop("cache_source", None)
        record.run.meta.pop("declared_outputs", None)
        record.run.meta.pop("materialized_outputs", None)
    except Exception:
        pass
