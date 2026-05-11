from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

from consist.models.run import Run


@dataclass(frozen=True)
class RunMatchTarget:
    model: Optional[str] = None
    status: Optional[str] = None
    year: Optional[Union[int, str]] = None
    iteration: Optional[Union[int, str]] = None
    stage: Optional[str] = None
    phase: Optional[str] = None
    cache_epoch: Optional[Union[int, str]] = None
    run_scope: Optional[str] = None
    parent_id: Optional[str] = None
    facet: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    limit: int = 100
    allow_missing_cache_epoch: bool = False


FindRunsCallable = Callable[..., Union[List[Run], Dict[Any, Run]]]


def find_matching_runs_for_tracker(
    find_runs: FindRunsCallable,
    *,
    model: Optional[str] = None,
    status: Optional[str] = None,
    year: Optional[Union[int, str]] = None,
    iteration: Optional[Union[int, str]] = None,
    stage: Optional[str] = None,
    phase: Optional[str] = None,
    cache_epoch: Optional[Union[int, str]] = None,
    run_scope: Optional[str] = None,
    parent_id: Optional[str] = None,
    facet: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    allow_missing_cache_epoch: bool = False,
) -> List[Run]:
    target = RunMatchTarget(
        model=model,
        status=status,
        year=year,
        iteration=iteration,
        stage=stage,
        phase=phase,
        cache_epoch=cache_epoch,
        run_scope=run_scope,
        parent_id=parent_id,
        facet=facet,
        metadata=metadata,
        limit=limit,
        allow_missing_cache_epoch=allow_missing_cache_epoch,
    )
    year_filter = _as_int(year) if year is not None else None
    iteration_filter = _as_int(iteration) if iteration is not None else None
    candidates = find_runs(
        model=model,
        status=status,
        year=year_filter,
        iteration=iteration_filter,
        stage=stage,
        phase=phase,
        parent_id=parent_id,
        facet=facet,
        metadata=metadata,
        limit=limit,
        run_scope=run_scope,
        raise_on_error=True,
    )
    runs = list(candidates.values()) if isinstance(candidates, dict) else candidates
    matches = [run for run in runs if _candidate_matches_target(run, target)]
    return sorted(matches, key=_run_recency_key, reverse=True)


def _candidate_matches_target(run: Run, target: RunMatchTarget) -> bool:
    if not _matches_run_scope(run, target.run_scope):
        return False

    field_names = (
        "model",
        "status",
        "year",
        "iteration",
        "stage",
        "phase",
    )
    for field_name in field_names:
        expected = getattr(target, field_name)
        actual = _candidate_value(run, field_name)
        if not _values_match(actual, expected):
            return False

    cache_epoch = _candidate_value(run, "cache_epoch")
    if target.cache_epoch is not None and cache_epoch is None:
        return target.allow_missing_cache_epoch
    return _values_match(cache_epoch, target.cache_epoch)


def _candidate_value(run: Run, field_name: str) -> Any:
    meta = run.meta if isinstance(run.meta, dict) else {}

    if field_name == "model":
        return _first_present(run.model_name, meta.get("model"), meta.get("model_name"))
    if field_name == "status":
        return _first_present(run.status, meta.get("status"))
    if field_name == "year":
        return _first_present(run.year, meta.get("year"))
    if field_name == "iteration":
        return _first_present(run.iteration, meta.get("iteration"))
    if field_name == "stage":
        return _first_present(run.stage, meta.get("stage"), meta.get("stage_name"))
    if field_name == "phase":
        return _first_present(run.phase, meta.get("phase"))
    if field_name == "cache_epoch":
        return meta.get("cache_epoch")
    raise ValueError(f"Unsupported run match field: {field_name}")


def _matches_run_scope(run: Run, run_scope: Optional[str]) -> bool:
    if run_scope is None or str(run_scope).strip() == "":
        return True

    scope = str(run_scope).strip()
    scope_prefix = f"{scope}__"
    values = [run.id, run.description]
    return any(
        isinstance(value, str) and (value == scope or value.startswith(scope_prefix))
        for value in values
    )


def _values_match(actual: Any, expected: Any) -> bool:
    if expected is None:
        return True
    if actual is None:
        return False

    expected_int = _as_int(expected)
    actual_int = _as_int(actual)
    if expected_int is not None and actual_int is not None:
        return actual_int == expected_int

    return str(actual).strip().casefold() == str(expected).strip().casefold()


def _run_recency_key(run: Run) -> tuple[float, float, float, float, str]:
    return (
        _timestamp_or_floor(run.ended_at),
        _timestamp_or_floor(run.updated_at),
        _timestamp_or_floor(run.created_at),
        _timestamp_or_floor(run.started_at),
        run.id,
    )


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped.lstrip("+-").isdigit():
            return int(stripped)
    return None


def _timestamp_or_floor(value: Optional[datetime]) -> float:
    if value is None:
        return float("-inf")
    return value.timestamp()
