from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Protocol, Sequence, runtime_checkable

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifacts


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

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts: ...


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


def _changed_keys(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, list[str]]:
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


def _code_identity_summary(run: Run) -> dict[str, Any]:
    summary = run.identity_summary
    if isinstance(summary, dict):
        code_identity = summary.get("code_identity")
        if isinstance(code_identity, dict):
            return code_identity
    return {}


def _code_identity_inputs(run: Run) -> dict[str, Any]:
    code_identity = _get_run_meta_value(run, "code_identity")
    extra_deps = _get_run_meta_value(run, "code_identity_extra_deps")
    summary = _code_identity_summary(run)
    payload: dict[str, Any] = {}

    if isinstance(code_identity, str) and code_identity:
        payload["mode"] = code_identity
    elif isinstance(summary.get("mode"), str) and summary["mode"]:
        payload["mode"] = summary["mode"]

    if isinstance(extra_deps, list) and all(isinstance(dep, str) for dep in extra_deps):
        payload["extra_deps"] = list(extra_deps)
    elif isinstance(summary.get("extra_deps"), list) and all(
        isinstance(dep, str) for dep in summary["extra_deps"]
    ):
        payload["extra_deps"] = list(summary["extra_deps"])

    return payload


def _changed_code_identity_inputs(run: Run, candidate: Run) -> list[str]:
    current = _code_identity_inputs(run)
    prior = _code_identity_inputs(candidate)
    diff = _changed_keys(current, prior)
    labels = set()
    for value in diff.values():
        labels.update(value)
    return sorted(labels)


def _build_code_details(run: Run, candidate: Run) -> dict[str, Any]:
    details: dict[str, Any] = {}

    changed_code_identity_inputs = _changed_code_identity_inputs(run, candidate)
    if changed_code_identity_inputs:
        details["code_identity_changed"] = changed_code_identity_inputs

    current_code_identity = _code_identity_inputs(run)
    prior_code_identity = _code_identity_inputs(candidate)
    current_mode = current_code_identity.get("mode")
    prior_mode = prior_code_identity.get("mode")
    if (
        current_mode != prior_mode
        and current_mode is not None
        and prior_mode is not None
    ):
        details["code_identity_mode_changed"] = [prior_mode, current_mode]

    current_extra_deps = current_code_identity.get("extra_deps")
    prior_extra_deps = prior_code_identity.get("extra_deps")
    if current_extra_deps != prior_extra_deps and (
        current_extra_deps is not None or prior_extra_deps is not None
    ):
        details["code_identity_extra_deps_changed"] = {
            "current": current_extra_deps,
            "candidate": prior_extra_deps,
        }

    if run.git_hash != candidate.git_hash:
        if current_mode == "repo_git" and prior_mode == "repo_git":
            details["repo_git_identity_changed"] = True
        if (
            "code_identity_changed" not in details
            and "code_identity_mode_changed" not in details
            and "code_identity_extra_deps_changed" not in details
        ):
            details["code_hash_changed"] = True

    return details


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


def _safe_get_run_artifacts(
    tracker: CacheMissExplainerContext, run_id: str
) -> Optional[Any]:
    getter = getattr(tracker, "get_artifacts_for_run", None)
    if getter is None:
        return None
    try:
        artifacts = getter(run_id)
    except Exception:
        return None
    if hasattr(artifacts, "inputs"):
        return artifacts
    return None


def _artifact_map_from_inputs(
    inputs: Any,
) -> dict[str, Artifact]:
    if isinstance(inputs, dict):
        return {
            key: artifact
            for key, artifact in inputs.items()
            if isinstance(key, str) and isinstance(artifact, Artifact)
        }
    if isinstance(inputs, list):
        return {
            artifact.key: artifact
            for artifact in inputs
            if isinstance(artifact, Artifact) and isinstance(artifact.key, str)
        }
    return {}


def _current_input_artifacts(tracker: CacheMissExplainerContext) -> dict[str, Artifact]:
    current_consist = getattr(tracker, "current_consist", None)
    if current_consist is None:
        return {}
    return _artifact_map_from_inputs(getattr(current_consist, "inputs", None))


def _resolve_run_signature(
    tracker: CacheMissExplainerContext, run_id: Optional[str]
) -> Optional[str]:
    if not run_id:
        return None
    resolver = getattr(tracker, "_resolve_run_signature", None)
    if resolver is None:
        return None
    try:
        signature = resolver(run_id)
    except Exception:
        return None
    if isinstance(signature, str):
        return signature
    return None


def _artifact_content_id(artifact: Artifact) -> Optional[str]:
    content_id = getattr(artifact, "content_id", None)
    if content_id is None:
        return None
    return str(content_id)


def _artifact_change_labels(
    tracker: CacheMissExplainerContext,
    current: Artifact,
    candidate: Artifact,
) -> list[str]:
    labels: list[str] = []

    current_run_id = getattr(current, "run_id", None)
    candidate_run_id = getattr(candidate, "run_id", None)
    if current_run_id and candidate_run_id and current_run_id != candidate_run_id:
        current_signature = _resolve_run_signature(tracker, current_run_id)
        candidate_signature = _resolve_run_signature(tracker, candidate_run_id)
        if current_signature is None or candidate_signature is None:
            labels.append("upstream_run_changed")
        elif current_signature != candidate_signature:
            labels.append("upstream_run_changed")

    if getattr(current, "hash", None) != getattr(candidate, "hash", None):
        labels.append("artifact_hash_changed")

    if _artifact_content_id(current) != _artifact_content_id(candidate):
        labels.append("artifact_content_id_changed")

    if getattr(current, "container_uri", None) != getattr(
        candidate, "container_uri", None
    ):
        labels.append("input_location_changed")

    if getattr(current, "table_path", None) != getattr(
        candidate, "table_path", None
    ) or getattr(current, "array_path", None) != getattr(candidate, "array_path", None):
        labels.append("container_member_changed")

    return labels


def _build_input_details(
    tracker: CacheMissExplainerContext,
    run: Run,
    candidate: Run,
) -> dict[str, Any]:
    del run
    details: dict[str, Any] = {}
    current_inputs = _current_input_artifacts(tracker)
    candidate_artifacts = _safe_get_run_artifacts(tracker, candidate.id)
    if candidate_artifacts is None:
        details["fallbacks_used"] = ["candidate_input_artifacts_unavailable"]
        return details
    candidate_inputs = _artifact_map_from_inputs(
        getattr(candidate_artifacts, "inputs", None)
    )

    if not current_inputs and not candidate_inputs:
        return details

    current_keys = set(current_inputs)
    candidate_keys = set(candidate_inputs)

    added_keys = sorted(current_keys - candidate_keys)
    removed_keys = sorted(candidate_keys - current_keys)
    if added_keys:
        details["input_keys_added"] = added_keys
    if removed_keys:
        details["input_keys_removed"] = removed_keys

    shared_keys = sorted(current_keys & candidate_keys)
    input_changes: list[dict[str, Any]] = []
    for key in shared_keys:
        current_artifact = current_inputs[key]
        candidate_artifact = candidate_inputs[key]
        labels = _artifact_change_labels(tracker, current_artifact, candidate_artifact)
        if not labels:
            continue
        change_entry: dict[str, Any] = {"key": key, "changes": labels}
        current_signature = _resolve_run_signature(
            tracker, getattr(current_artifact, "run_id", None)
        )
        candidate_signature = _resolve_run_signature(
            tracker, getattr(candidate_artifact, "run_id", None)
        )
        if "upstream_run_changed" in labels:
            change_entry["current_run_id"] = getattr(current_artifact, "run_id", None)
            change_entry["candidate_run_id"] = getattr(
                candidate_artifact, "run_id", None
            )
            if current_signature is not None:
                change_entry["current_run_signature"] = current_signature
            if candidate_signature is not None:
                change_entry["candidate_run_signature"] = candidate_signature
        if "artifact_hash_changed" in labels:
            change_entry["current_hash"] = getattr(current_artifact, "hash", None)
            change_entry["candidate_hash"] = getattr(candidate_artifact, "hash", None)
        if "artifact_content_id_changed" in labels:
            change_entry["current_content_id"] = _artifact_content_id(current_artifact)
            change_entry["candidate_content_id"] = _artifact_content_id(
                candidate_artifact
            )
        if "input_location_changed" in labels:
            change_entry["current_container_uri"] = getattr(
                current_artifact, "container_uri", None
            )
            change_entry["candidate_container_uri"] = getattr(
                candidate_artifact, "container_uri", None
            )
        if "container_member_changed" in labels:
            change_entry["current_table_path"] = getattr(
                current_artifact, "table_path", None
            )
            change_entry["candidate_table_path"] = getattr(
                candidate_artifact, "table_path", None
            )
            change_entry["current_array_path"] = getattr(
                current_artifact, "array_path", None
            )
            change_entry["candidate_array_path"] = getattr(
                candidate_artifact, "array_path", None
            )
        input_changes.append(change_entry)

    if input_changes:
        details["input_artifact_changes"] = input_changes

    return details


def _has_code_details(details: dict[str, Any]) -> bool:
    return any(
        key in details
        for key in (
            "repo_git_identity_changed",
            "code_identity_changed",
            "code_identity_mode_changed",
            "code_identity_extra_deps_changed",
            "code_hash_changed",
        )
    )


def _has_code_changes(code_details: dict[str, Any]) -> bool:
    return _has_code_details(code_details)


def _classify_reason(
    mismatched_components: Sequence[str],
    *,
    code_changed: bool = False,
) -> str:
    """Map identity mismatches to a best-effort miss reason."""

    mismatched = set(mismatched_components)
    if code_changed:
        if not mismatched:
            return "code_changed"
        if mismatched == {"config_hash"}:
            return "config_and_code_changed"
        if mismatched == {"input_hash"}:
            return "inputs_and_code_changed"
        if mismatched == {"config_hash", "input_hash"}:
            return "all_components_changed"
        if mismatched == {"git_hash"}:
            return "code_changed"
        if mismatched == {"config_hash", "git_hash"}:
            return "config_and_code_changed"
        if mismatched == {"input_hash", "git_hash"}:
            return "inputs_and_code_changed"
        if mismatched == {"config_hash", "input_hash", "git_hash"}:
            return "all_components_changed"
        return "code_changed"

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


def _merge_detail_payloads(*payloads: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    fallbacks: list[str] = []
    for payload in payloads:
        if not payload:
            continue
        for key, value in payload.items():
            if key == "fallbacks_used":
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str) and item not in fallbacks:
                            fallbacks.append(item)
                continue
            merged[key] = value
    if fallbacks:
        merged["fallbacks_used"] = fallbacks
    return merged


def _should_include_input_details(
    *,
    reason: str,
    mismatched_components: Sequence[str],
) -> bool:
    """Return whether input-side detail can plausibly explain this miss.

    Input hints are useful only when inputs actually contributed to the miss. For
    config-only misses and cache-validation failures, including input drift or
    fallback noise would contradict the top-level explanation.
    """

    if reason in {
        "inputs_changed",
        "config_and_inputs_changed",
        "inputs_and_code_changed",
        "all_components_changed",
    }:
        return True
    return "input_hash" in set(mismatched_components)


def _config_snapshot_diff(left: Any, right: Any) -> tuple[dict[str, list[str]], bool]:
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
    current_config = (
        getattr(current_consist, "config", None) if current_consist else None
    )
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
            details = _merge_detail_payloads(
                details,
                _build_config_details(self._tracker, run, cached_run),
            )
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
        code_details = _build_code_details(run, candidate)
        reason = _classify_reason(
            mismatched, code_changed=_has_code_changes(code_details)
        )
        if reason == "exact_match_lookup_inconclusive":
            confidence = "low"
        elif reason == "code_changed" and "code_hash_changed" in code_details:
            confidence = "medium"
        else:
            confidence = "high" if len(matched) >= 2 else "medium" if matched else "low"
        detail_payloads = [_build_config_details(self._tracker, run, candidate)]
        if _should_include_input_details(
            reason=reason,
            mismatched_components=mismatched,
        ):
            detail_payloads.append(_build_input_details(self._tracker, run, candidate))
        if code_details:
            detail_payloads.append(code_details)
        details = _merge_detail_payloads(*detail_payloads)
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
