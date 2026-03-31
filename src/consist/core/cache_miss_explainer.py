from __future__ import annotations

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

    def find_recent_completed_runs_for_model(
        self, model_name: str, *, limit: int = 20
    ) -> list[Run]: ...


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
            return CacheMissExplanation(
                reason="candidate_outputs_invalid",
                candidate_run_id=cached_run.id,
                matched_components=matched,
                mismatched_components=mismatched,
                details={"candidate_output_validation_failure": True},
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
        return CacheMissExplanation(
            reason=reason,
            candidate_run_id=candidate.id,
            matched_components=matched,
            mismatched_components=mismatched,
            confidence=confidence,
        )

    def _select_candidate(self, run: Run) -> Run | None:
        """Return the most relevant same-model completed run for comparison."""

        candidates = self._tracker.find_recent_completed_runs_for_model(
            run.model_name, limit=20
        )
        candidate, _score = _best_candidate(run, candidates)
        return candidate
