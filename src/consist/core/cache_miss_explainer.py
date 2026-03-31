from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Protocol, Sequence, runtime_checkable

from consist.models.run import Run


@runtime_checkable
class CacheMissExplainerDb(Protocol):
    """Database-facing surface required by the cache-miss explainer.

    The Phase 1 explainer only needs a lightweight historical lookup: recent
    completed runs for the same model. The concrete tracker/database layer can
    provide richer helpers later without changing this protocol.
    """

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]: ...


@runtime_checkable
class CacheMissExplainerContext(Protocol):
    """Tracker-like context required by :class:`CacheMissExplainer`.

    The explainer intentionally depends on a very small interface so it can stay
    decoupled from the full tracker lifecycle implementation and remain easy to
    test in isolation.
    """

    db: Optional[CacheMissExplainerDb]
    current_consist: Any

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]: ...

    def get_config_values(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Sequence[str]] = None,
        limit: int = 10_000,
    ) -> dict[str, Any]: ...

    def get_run_config(
        self, run_id: str, *, allow_missing: bool = False
    ) -> Optional[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class CacheMissExplanation:
    """Structured Phase 1 explanation payload for a cache miss.

    This model captures only top-level miss classification: which identity
    components matched, which differed, which prior run was used for comparison,
    and a coarse confidence value. Rich config/input/code detail is intentionally
    deferred to later phases.
    """

    status: Literal["miss"] = "miss"
    reason: str = "no_similar_prior_run"
    candidate_run_id: str | None = None
    matched_components: list[str] = field(default_factory=list)
    mismatched_components: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    confidence: Literal["low", "medium", "high"] = "medium"

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation for ``run.meta`` storage."""

        return {
            "status": self.status,
            "reason": self.reason,
            "candidate_run_id": self.candidate_run_id,
            "matched_components": list(self.matched_components),
            "mismatched_components": list(self.mismatched_components),
            "details": dict(self.details),
            "confidence": self.confidence,
        }


_COMPONENT_FIELDS: tuple[str, ...] = ("config_hash", "input_hash", "git_hash")


def _component_names(run: Run) -> dict[str, Optional[str]]:
    return {
        "config_hash": run.config_hash,
        "input_hash": run.input_hash,
        "git_hash": run.git_hash,
    }


def _meta_dict(run: Run) -> dict[str, Any]:
    meta = run.meta
    if isinstance(meta, dict):
        return meta
    return {}


def _get_run_meta_value(run: Run, key: str) -> Any:
    return _meta_dict(run).get(key)


def _identity_input_map(run: Run) -> dict[str, Any]:
    value = _get_run_meta_value(run, "consist_hash_inputs")
    if isinstance(value, dict):
        return value
    return {}


def _changed_keys(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, list[str]]:
    left_keys = set(left)
    right_keys = set(right)
    changed = sorted(key for key in left_keys & right_keys if left[key] != right[key])
    # The explainer treats ``left`` as the current run and ``right`` as the
    # comparison candidate, so added/removed are oriented relative to the
    # current run's config.
    added = sorted(left_keys - right_keys)
    removed = sorted(right_keys - left_keys)
    result: dict[str, list[str]] = {}
    if changed:
        result["changed"] = changed
    if added:
        result["added"] = added
    if removed:
        result["removed"] = removed
    return result


def _changed_identity_inputs(run: Run, candidate: Run) -> list[str]:
    diff = _changed_keys(_identity_input_map(run), _identity_input_map(candidate))
    labels = set()
    for value in diff.values():
        labels.update(value)
    return sorted(labels)


def _changed_adapter_identity(run: Run, candidate: Run) -> list[str]:
    fields = ("config_adapter", "config_adapter_version", "config_bundle_hash")
    return [
        field
        for field in fields
        if _get_run_meta_value(run, field) != _get_run_meta_value(candidate, field)
    ]


def _strip_internal_config_keys(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {
            key: _strip_internal_config_keys(value)
            for key, value in payload.items()
            if not str(key).startswith("__consist_")
        }
    if isinstance(payload, list):
        return [_strip_internal_config_keys(value) for value in payload]
    if isinstance(payload, tuple):
        return tuple(_strip_internal_config_keys(value) for value in payload)
    return payload


def _flatten_config_payload(
    payload: Any,
    *,
    prefix: str = "",
    flattened: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if flattened is None:
        flattened = {}

    if not isinstance(payload, Mapping):
        if prefix:
            flattened[prefix] = payload
        return flattened

    for raw_key, value in payload.items():
        key = str(raw_key).replace(".", r"\.")
        flattened_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, Mapping):
            _flatten_config_payload(value, prefix=flattened_key, flattened=flattened)
        else:
            flattened[flattened_key] = value
    return flattened


def _diff_flattened_payloads(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, list[str]]:
    diff = _changed_keys(left, right)
    return {
        key: value
        for key, value in (
            ("config_keys_changed", diff.get("changed", [])),
            ("config_keys_added", diff.get("added", [])),
            ("config_keys_removed", diff.get("removed", [])),
        )
        if value
    }


def _normalize_config_source(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}
    stripped = _strip_internal_config_keys(payload)
    if isinstance(stripped, dict):
        return stripped
    return {}


def _safe_get_config_values(
    tracker: CacheMissExplainerContext, run_id: str
) -> dict[str, Any]:
    getter = getattr(tracker, "get_config_values", None)
    if getter is None:
        return {}
    try:
        values = getter(run_id)
    except Exception:
        return {}
    if isinstance(values, dict):
        return values
    return {}


def _safe_get_run_config(
    tracker: CacheMissExplainerContext, run_id: str
) -> Optional[dict[str, Any]]:
    getter = getattr(tracker, "get_run_config", None)
    if getter is None:
        return None
    try:
        config = getter(run_id, allow_missing=True)
    except Exception:
        return None
    if isinstance(config, dict):
        return config
    return None


def _config_snapshot_diff(
    left: Any, right: Any
) -> tuple[dict[str, list[str]], bool]:
    left_payload = _flatten_config_payload(_normalize_config_source(left))
    right_payload = _flatten_config_payload(_normalize_config_source(right))
    return _diff_flattened_payloads(left_payload, right_payload), bool(
        left_payload or right_payload
    )


def _config_facet_diff(
    tracker: CacheMissExplainerContext,
    run: Run,
    candidate: Run,
) -> tuple[dict[str, list[str]], bool]:
    current_consist = getattr(tracker, "current_consist", None)
    left_facet = {}
    if current_consist is not None:
        facet = getattr(current_consist, "facet", None)
        if isinstance(facet, dict):
            left_facet = facet
    right_facet = _safe_get_config_values(tracker, candidate.id)
    if not left_facet or not right_facet:
        return {}, False
    diff, has_payload = _config_snapshot_diff(left_facet, right_facet)
    return diff, has_payload


def _build_config_details(
    tracker: CacheMissExplainerContext,
    run: Run,
    candidate: Run,
) -> dict[str, Any]:
    details: dict[str, Any] = {}

    changed_identity_inputs = _changed_identity_inputs(run, candidate)
    if changed_identity_inputs:
        details["identity_inputs_changed"] = changed_identity_inputs

    changed_adapter_identity = _changed_adapter_identity(run, candidate)
    if changed_adapter_identity:
        details["adapter_identity_changed"] = changed_adapter_identity

    facet_diff, has_facet_payload = _config_facet_diff(tracker, run, candidate)
    if facet_diff:
        details.update(facet_diff)
        details["fallbacks_used"] = ["config_facet"]
        return details

    current_consist = getattr(tracker, "current_consist", None)
    current_config = getattr(current_consist, "config", None) if current_consist else None
    candidate_config = _safe_get_run_config(tracker, candidate.id)
    snapshot_diff, has_snapshot_payload = _config_snapshot_diff(
        current_config, candidate_config
    )
    if snapshot_diff:
        details.update(snapshot_diff)
        if has_facet_payload:
            details["fallbacks_used"] = ["config_facet", "json_snapshot"]
        elif has_snapshot_payload:
            details["fallbacks_used"] = ["json_snapshot"]

    return details


def _match_components(run: Run, candidate: Run) -> tuple[list[str], list[str]]:
    matched: list[str] = []
    mismatched: list[str] = []
    current = _component_names(run)
    prior = _component_names(candidate)

    for component in _COMPONENT_FIELDS:
        if current[component] == prior[component]:
            matched.append(component)
        else:
            mismatched.append(component)
    return matched, mismatched


def _classify_reason(mismatched_components: Sequence[str]) -> str:
    """Map top-level identity mismatches to a Phase 1 reason code.

    An empty mismatch set means the fallback candidate search found a run whose
    identity matches exactly even though the primary exact-match lookup did not
    produce a reusable cache hit. That case is reported separately so the
    explanation does not incorrectly claim that no similar run exists.
    """

    mismatched = set(mismatched_components)
    if not mismatched:
        return "exact_match_lookup_inconclusive"
    if mismatched == {"config_hash"}:
        return "config_changed"
    if mismatched == {"input_hash"}:
        return "inputs_changed"
    if mismatched == {"git_hash"}:
        return "code_changed"
    if mismatched == {"config_hash", "input_hash"}:
        return "config_and_inputs_changed"
    if mismatched == {"config_hash", "git_hash"}:
        return "config_and_code_changed"
    if mismatched == {"input_hash", "git_hash"}:
        return "inputs_and_code_changed"
    if mismatched == {"config_hash", "input_hash", "git_hash"}:
        return "all_components_changed"
    return "no_similar_prior_run"


def _best_candidate(run: Run, candidates: Sequence[Run]) -> tuple[Run | None, int]:
    """Select the closest prior run using Phase 1 scoring rules.

    Candidates are ranked first by the number of matching top-level identity
    components and then by recency. This keeps the query logic simple while
    producing a useful comparison target for miss explanations.
    """

    best_run: Run | None = None
    best_score = -1
    best_created_at = None

    for candidate in candidates:
        score = sum(
            1
            for field in _COMPONENT_FIELDS
            if getattr(run, field) == getattr(candidate, field)
        )
        created_at = candidate.created_at
        if score > best_score:
            best_run = candidate
            best_score = score
            best_created_at = created_at
            continue
        if score == best_score and best_run is not None:
            if best_created_at is None or created_at > best_created_at:
                best_run = candidate
                best_created_at = created_at

    return best_run, best_score


class CacheMissExplainer:
    """Best-effort classifier for cache misses after lookup or validation failure.

    The explainer does not participate in cache correctness. It runs only after
    the lifecycle has already decided that the current run is not a reusable
    cache hit, and it produces metadata/logging intended for debugging.
    """

    def __init__(self, tracker: CacheMissExplainerContext) -> None:
        """Bind the tracker-like context used for historical candidate lookup."""

        self._tracker = tracker

    def explain(
        self,
        run: Run,
        *,
        cached_run: Run | None = None,
        cache_valid: bool | None = None,
    ) -> CacheMissExplanation:
        """Build a Phase 1 explanation for the current cache miss.

        Parameters
        ----------
        run : Run
            The in-progress run that just missed the cache.
        cached_run : Run | None, optional
            Exact-match candidate returned by cache lookup, if any.
        cache_valid : bool | None, optional
            Output-validation result for ``cached_run``. When this is ``False``,
            the miss is classified as ``candidate_outputs_invalid``.

        Returns
        -------
        CacheMissExplanation
            Structured top-level miss metadata suitable for persistence in
            ``run.meta["cache_miss_explanation"]``.
        """

        if cached_run is not None and cache_valid is False:
            matched, mismatched = _match_components(run, cached_run)
            details = {"candidate_output_validation_failure": True}
            details.update(_build_config_details(self._tracker, run, cached_run))
            return CacheMissExplanation(
                reason="candidate_outputs_invalid",
                candidate_run_id=cached_run.id,
                matched_components=matched,
                mismatched_components=mismatched,
                details=details,
                confidence="high",
            )

        candidate = self._select_candidate(run)
        if candidate is None:
            return CacheMissExplanation(
                reason="no_similar_prior_run",
                confidence="low",
            )

        matched, mismatched = _match_components(run, candidate)
        reason = _classify_reason(mismatched)
        if reason == "exact_match_lookup_inconclusive":
            confidence = "low"
        else:
            confidence = "high" if len(matched) >= 2 else "medium" if matched else "low"
        details = _build_config_details(self._tracker, run, candidate)
        return CacheMissExplanation(
            reason=reason,
            candidate_run_id=candidate.id,
            matched_components=matched,
            mismatched_components=mismatched,
            details=details,
            confidence=confidence,
        )

    def _select_candidate(self, run: Run) -> Run | None:
        """Return the most relevant same-model completed run for comparison."""

        candidates = self._tracker.find_recent_completed_runs_for_model(
            run.model_name, limit=20
        )
        candidate, _score = _best_candidate(run, candidates)
        return candidate
