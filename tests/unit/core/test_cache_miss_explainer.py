from __future__ import annotations

import logging
import uuid
from pathlib import Path
from types import SimpleNamespace

from consist.core.config_canonicalization import CanonicalConfig, ConfigPlan
from consist.core.cache_miss_explainer import CacheMissExplainer
from consist.models.artifact import Artifact
from consist.models.run import Run
from consist.types import CacheOptions
from consist.types import ExecutionOptions


def _artifact(
    *,
    key: str,
    run_id: str | None = None,
    container_uri: str = "inputs://shared.csv",
    driver: str = "csv",
    hash_value: str | None = None,
    content_id: uuid.UUID | None = None,
    table_path: str | None = None,
    array_path: str | None = None,
) -> Artifact:
    """Create a minimal artifact for focused explainer tests.

    The cache-miss explainer only looks at a small subset of artifact identity
    fields, so this helper keeps the tests compact while making the relevant
    inputs explicit at each call site.
    """

    return Artifact(
        key=key,
        run_id=run_id,
        container_uri=container_uri,
        driver=driver,
        hash=hash_value,
        content_id=content_id,
        table_path=table_path,
        array_path=array_path,
    )


def test_cache_miss_explanation_records_changed_components(tracker, caplog):
    """A same-model miss with only config drift should classify as config_changed.

    This is the core Phase 1/2 happy path for top-level classification: the
    explainer should pick the prior completed run as the comparison candidate,
    mark config as the only mismatched component, and emit the explanation log.
    """

    caplog.set_level(logging.INFO)

    with tracker.start_run(
        "run_a",
        model="miss_model",
        config={"value": 1},
    ):
        pass

    with tracker.start_run(
        "run_b",
        model="miss_model",
        config={"value": 2},
    ):
        pass

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["status"] == "miss"
    assert explanation["reason"] == "config_changed"
    assert explanation["candidate_run_id"] == "run_a"
    assert explanation["matched_components"] == ["input_hash", "git_hash"]
    assert explanation["mismatched_components"] == ["config_hash"]
    assert explanation["details"]["config_keys_changed"] == ["value"]
    assert explanation["details"]["fallbacks_used"] == ["json_snapshot"]
    assert any(
        "[Consist][cache] miss explanation: reason=config_changed" in record.message
        for record in caplog.records
    )


def test_cache_miss_explanation_records_identity_input_digest_changes():
    """Digest drift in ``consist_hash_inputs`` should be surfaced by label.

    This isolates the config-side identity-input path without needing a full
    tracker run. When two otherwise comparable runs differ only in a named
    identity-input digest, the explanation should call out that label so a user
    can tell which external config input likely triggered the miss.
    """

    current = Run(
        id="current",
        model_name="identity_model",
        config_hash="config_hash_current",
        input_hash="input_hash",
        git_hash="git_hash",
        meta={"consist_hash_inputs": {"scenario_cfg": "digest_a"}},
    )
    candidate = Run(
        id="candidate",
        model_name="identity_model",
        config_hash="config_hash_candidate",
        input_hash="input_hash",
        git_hash="git_hash",
        status="completed",
        meta={"consist_hash_inputs": {"scenario_cfg": "digest_b"}},
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "identity_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "config_changed"
    assert explanation.details["identity_inputs_changed"] == ["scenario_cfg"]


def test_cache_miss_explanation_records_callable_identity_mode_drift():
    """Callable identity mode changes should be reported as a code-side miss cause.

    This covers the case where the top-level code hash stays the same, but the
    code-identity strategy itself changes between runs. The explainer should say
    the miss is code-related and point to the mode change explicitly.
    """

    current = Run(
        id="current",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="code_hash",
        meta={"code_identity": "callable_module"},
    )
    candidate = Run(
        id="candidate",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="code_hash",
        status="completed",
        meta={"code_identity": "repo_git"},
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "code_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "code_changed"
    assert explanation.details["code_identity_mode_changed"] == [
        "repo_git",
        "callable_module",
    ]
    assert explanation.details["code_identity_changed"] == ["mode"]


def test_cache_miss_explanation_records_callable_extra_deps_drift():
    """Callable extra dependency changes should be reported as code drift.

    The top-level code hash remains the same here, so the explainer needs to use
    the extra dependency metadata to explain why the current run no longer
    matches the prior one.
    """

    current = Run(
        id="current",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="code_hash",
        meta={"code_identity": "callable_module", "code_identity_extra_deps": ["a"]},
    )
    candidate = Run(
        id="candidate",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="code_hash",
        status="completed",
        meta={"code_identity": "callable_module", "code_identity_extra_deps": ["b"]},
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "code_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "code_changed"
    assert explanation.details["code_identity_extra_deps_changed"] == {
        "current": ["a"],
        "candidate": ["b"],
    }
    assert explanation.details["code_identity_changed"] == ["extra_deps"]


def test_cache_miss_explanation_falls_back_to_code_hash_when_metadata_missing():
    """Without richer code metadata, a changed git hash should still be explained.

    This guards the fallback path for older or sparse records: even if the runs
    do not carry explicit code identity metadata, the explainer should still say
    that the repo/code identity changed when the git hashes differ.
    """

    current = Run(
        id="current",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="git_hash_v2",
    )
    candidate = Run(
        id="candidate",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="git_hash_v1",
        status="completed",
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "code_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "code_changed"
    assert explanation.details["repo_git_identity_changed"] is True
    assert explanation.details["code_hash_changed"] is True
    assert "code_identity_changed" not in explanation.details


def test_cache_miss_explanation_uses_generic_code_hash_label_for_callable_modes():
    """Callable-mode code hash changes should avoid repo-specific wording.

    Both runs explicitly use a callable-based identity mode, so a changed code
    hash means the callable-derived identity changed, not necessarily the Git
    repository identity. The explanation should therefore use the generic
    ``code_hash_changed`` hint and avoid the more specific
    ``repo_git_identity_changed`` label.
    """

    current = Run(
        id="current",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="callable_hash_v2",
        meta={"code_identity": "callable_module"},
    )
    candidate = Run(
        id="candidate",
        model_name="code_model",
        config_hash="config",
        input_hash="input",
        git_hash="callable_hash_v1",
        status="completed",
        meta={"code_identity": "callable_module"},
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "code_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "code_changed"
    assert explanation.details["code_hash_changed"] is True
    assert "repo_git_identity_changed" not in explanation.details


def test_cache_miss_explanation_is_quiet_when_code_metadata_is_absent():
    """Missing code metadata should not fabricate code-side drift details.

    The only thing changing here is the top-level config hash. Because neither
    run has explicit code identity metadata and the git hash is unchanged, the
    explainer should stay on the config path and avoid emitting code-side hints.
    """

    current = Run(
        id="current",
        model_name="code_model",
        config_hash="config_current",
        input_hash="input",
        git_hash="code_hash",
    )
    candidate = Run(
        id="candidate",
        model_name="code_model",
        config_hash="config_candidate",
        input_hash="input",
        git_hash="code_hash",
        status="completed",
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "code_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "config_changed"
    assert "code_identity_changed" not in explanation.details
    assert "code_identity_mode_changed" not in explanation.details
    assert "code_identity_extra_deps_changed" not in explanation.details
    assert "repo_git_identity_changed" not in explanation.details


def test_cache_miss_explanation_records_input_key_added():
    """A new current-run input key should be reported as an added input.

    The baseline candidate only has ``base`` while the current run has both
    ``base`` and ``extra``. The desired behavior is that the explanation points
    to ``extra`` as a newly introduced input rather than reporting a generic
    inputs_changed reason with no constituent detail.
    """

    current_input = _artifact(key="base", run_id=None)
    extra_current_input = _artifact(key="extra", run_id=None)
    candidate_input = _artifact(key="base", run_id=None)

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(
            inputs=[current_input, extra_current_input], config={}
        )

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "input_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            assert run_id == "candidate"
            return SimpleNamespace(inputs={"base": candidate_input})

    current = Run(
        id="current",
        model_name="input_model",
        config_hash="config",
        input_hash="current_input",
        git_hash="code",
    )
    candidate = Run(
        id="candidate",
        model_name="input_model",
        config_hash="config",
        input_hash="candidate_input",
        git_hash="code",
        status="completed",
    )

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "inputs_changed"
    assert explanation.details["input_keys_added"] == ["extra"]
    assert "input_keys_removed" not in explanation.details


def test_cache_miss_explanation_records_input_key_removed():
    """A missing current-run input key should be reported as a removed input.

    Here the prior candidate consumed ``base`` and ``extra`` but the current run
    only consumes ``base``. The explanation should describe that as the current
    run having removed ``extra`` from its input set.
    """

    current_input = _artifact(key="base", run_id=None)
    candidate_input = _artifact(key="base", run_id=None)
    extra_candidate_input = _artifact(key="extra", run_id=None)

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(inputs=[current_input], config={})

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "input_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            assert run_id == "candidate"
            return SimpleNamespace(
                inputs={"base": candidate_input, "extra": extra_candidate_input}
            )

    current = Run(
        id="current",
        model_name="input_model",
        config_hash="config",
        input_hash="current_input",
        git_hash="code",
    )
    candidate = Run(
        id="candidate",
        model_name="input_model",
        config_hash="config",
        input_hash="candidate_input",
        git_hash="code",
        status="completed",
    )

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "inputs_changed"
    assert explanation.details["input_keys_removed"] == ["extra"]
    assert "input_keys_added" not in explanation.details


def test_cache_miss_explanation_records_upstream_run_changed():
    """Different producer run signatures should be surfaced as upstream drift.

    The artifacts intentionally keep the same key and raw content-ish markers so
    the only meaningful distinction is the producing run identity. The desired
    behavior is that the explanation says the upstream run changed and includes
    the resolved signatures for both producers.
    """

    current_input = _artifact(
        key="upstream",
        run_id="producer_v2",
        hash_value="same_hash",
        content_id=uuid.uuid4(),
    )
    candidate_input = _artifact(
        key="upstream",
        run_id="producer_v1",
        hash_value="same_hash",
        content_id=uuid.uuid4(),
    )

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(inputs=[current_input], config={})

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "upstream_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            assert run_id == "candidate"
            return SimpleNamespace(inputs={"upstream": candidate_input})

        def _resolve_run_signature(self, run_id: str) -> str | None:
            return {
                "producer_v1": "sig_v1",
                "producer_v2": "sig_v2",
            }.get(run_id)

    current = Run(
        id="current",
        model_name="upstream_model",
        config_hash="config",
        input_hash="current_input",
        git_hash="code",
    )
    candidate = Run(
        id="candidate",
        model_name="upstream_model",
        config_hash="config",
        input_hash="candidate_input",
        git_hash="code",
        status="completed",
    )

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "inputs_changed"
    change = explanation.details["input_artifact_changes"][0]
    assert change["key"] == "upstream"
    assert "upstream_run_changed" in change["changes"]
    assert change["current_run_signature"] == "sig_v2"
    assert change["candidate_run_signature"] == "sig_v1"


def test_cache_miss_explanation_records_content_hash_and_id_drift():
    """Raw file drift should call out hash and content-id changes together.

    This case represents the same logical input key pointing at different raw
    bytes. The explanation should surface the concrete artifact-level reasons
    instead of collapsing them into a generic inputs_changed label.
    """

    current_input = _artifact(
        key="raw",
        run_id=None,
        container_uri="inputs://raw_v2.csv",
        hash_value="hash_v2",
        content_id=uuid.uuid4(),
    )
    candidate_input = _artifact(
        key="raw",
        run_id=None,
        container_uri="inputs://raw_v1.csv",
        hash_value="hash_v1",
        content_id=uuid.uuid4(),
    )

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(inputs=[current_input], config={})

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "raw_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            assert run_id == "candidate"
            return SimpleNamespace(inputs={"raw": candidate_input})

    current = Run(
        id="current",
        model_name="raw_model",
        config_hash="config",
        input_hash="current_input",
        git_hash="code",
    )
    candidate = Run(
        id="candidate",
        model_name="raw_model",
        config_hash="config",
        input_hash="candidate_input",
        git_hash="code",
        status="completed",
    )

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "inputs_changed"
    change = explanation.details["input_artifact_changes"][0]
    assert change["key"] == "raw"
    assert "artifact_hash_changed" in change["changes"]
    assert "artifact_content_id_changed" in change["changes"]
    assert "input_location_changed" in change["changes"]


def test_cache_miss_explanation_gracefully_handles_missing_candidate_inputs():
    """Missing candidate artifact history should not break the explainer.

    The desired behavior is graceful degradation: keep the top-level reason,
    record that candidate input artifacts were unavailable, and avoid crashing or
    inventing input change details.
    """

    current_input = _artifact(key="base", run_id=None)

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(inputs=[current_input], config={})

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "fallback_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            raise RuntimeError("history unavailable")

    current = Run(
        id="current",
        model_name="fallback_model",
        config_hash="config",
        input_hash="current_input",
        git_hash="code",
    )
    candidate = Run(
        id="candidate",
        model_name="fallback_model",
        config_hash="config",
        input_hash="candidate_input",
        git_hash="code",
        status="completed",
    )

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "inputs_changed"
    assert explanation.details["fallbacks_used"] == [
        "candidate_input_artifacts_unavailable"
    ]
    assert "input_artifact_changes" not in explanation.details


def test_cache_miss_explanation_records_adapter_identity_changes(
    tracker, tmp_path: Path, monkeypatch
):
    """Adapter metadata changes should be surfaced as config-side causes.

    The desired behavior is that if a config adapter changes its identity hash
    or reported version between runs, the miss explanation points to those
    adapter-level fields instead of only saying that the top-level config hash
    changed.
    """

    config_root = tmp_path / "adapter_config"
    config_root.mkdir(parents=True, exist_ok=True)

    class DummyAdapter:
        model_name = "dummy_adapter"
        root_dirs = [config_root]

    adapter_obj = DummyAdapter()
    plans = [
        ConfigPlan(
            adapter_name="dummy_adapter",
            adapter_version="1.0",
            canonical=CanonicalConfig(
                root_dirs=[config_root],
                primary_config=None,
                config_files=[],
                external_files=[],
                content_hash="adapter_hash_v1",
            ),
            artifacts=[],
            ingestables=[],
        ),
        ConfigPlan(
            adapter_name="dummy_adapter",
            adapter_version="2.0",
            canonical=CanonicalConfig(
                root_dirs=[config_root],
                primary_config=None,
                config_files=[],
                external_files=[],
                content_hash="adapter_hash_v2",
            ),
            artifacts=[],
            ingestables=[],
        ),
    ]
    calls = {"count": 0}

    def fake_prepare_config(*, adapter, config_dirs, **kwargs):
        del config_dirs, kwargs
        plan = plans[calls["count"]]
        calls["count"] += 1
        assert adapter is adapter_obj
        return plan

    monkeypatch.setattr(tracker, "prepare_config", fake_prepare_config)

    def step() -> None:
        return None

    # Seed a first adapter-backed run, then force a different config plan on the
    # second run so the explainer has adapter metadata drift to report.
    tracker.run(
        fn=step,
        name="adapter_seed",
        model="adapter_model",
        adapter=adapter_obj,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    tracker.run(
        fn=step,
        name="adapter_miss",
        model="adapter_model",
        adapter=adapter_obj,
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["reason"] == "config_changed"
    assert set(explanation["details"]["adapter_identity_changed"]) == {
        "config_adapter",
        "config_adapter_version",
        "config_bundle_hash",
    }


def test_cache_miss_explanation_uses_facet_diff_when_available(tracker):
    """Indexed config facets should be the preferred source for key-level diffs.

    When both runs have facet KV rows, the explainer should report changed keys
    from that structured facet data and label the explanation as coming from the
    config-facet path rather than the noisier JSON snapshot fallback.
    """

    with tracker.start_run(
        "facet_seed",
        model="facet_model",
        config={"sample_rate": 0.1, "mode": "base"},
        facet={"sample_rate": 0.1, "mode": "base"},
        facet_index=True,
    ):
        pass

    with tracker.start_run(
        "facet_miss",
        model="facet_model",
        config={"sample_rate": 0.2, "mode": "base"},
        facet={"sample_rate": 0.2, "mode": "base"},
        facet_index=True,
    ):
        pass

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["reason"] == "config_changed"
    assert explanation["details"]["config_keys_changed"] == ["sample_rate"]
    assert explanation["details"]["fallbacks_used"] == ["config_facet"]


def test_cache_miss_explanation_falls_back_to_snapshot_when_facet_index_missing(
    tracker,
):
    """Missing facet indexing should fall back to the persisted run snapshot.

    This guards the best-effort fallback path: even when the facet itself is too
    sparse or not indexed, the explainer should still recover useful config-key
    detail from the run snapshot instead of returning no constituent config
    hints.
    """

    with tracker.start_run(
        "snapshot_seed",
        model="snapshot_model",
        config={"sample_rate": 0.1, "mode": "base"},
        facet={"mode": "base"},
        facet_index=False,
    ):
        pass

    with tracker.start_run(
        "snapshot_miss",
        model="snapshot_model",
        config={"sample_rate": 0.2, "mode": "base"},
        facet={"mode": "base"},
        facet_index=False,
    ):
        pass

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["reason"] == "config_changed"
    assert explanation["details"]["config_keys_changed"] == ["sample_rate"]
    assert explanation["details"]["fallbacks_used"] == ["json_snapshot"]


def test_cache_miss_explanation_orients_added_removed_keys_to_current_run(tracker):
    """Added/removed keys should be described relative to the current miss run.

    The current run introduces ``sample_rate`` while the comparison candidate
    does not have it. The explanation should therefore say the key was added,
    not removed, because the payload is meant to explain what changed in the
    current run relative to the prior candidate.
    """

    with tracker.start_run(
        "shape_seed",
        model="shape_model",
        config={"mode": "base"},
        facet={"mode": "base"},
        facet_index=True,
    ):
        pass

    with tracker.start_run(
        "shape_miss",
        model="shape_model",
        config={"mode": "base", "sample_rate": 0.2},
        facet={"mode": "base", "sample_rate": 0.2},
        facet_index=True,
    ):
        pass

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["reason"] == "config_changed"
    assert explanation["details"]["config_keys_added"] == ["sample_rate"]
    assert "config_keys_removed" not in explanation["details"]
    assert explanation["details"]["fallbacks_used"] == ["config_facet"]


def test_cache_miss_explanation_records_no_similar_prior_run(tracker):
    """A first run for a model should report that no prior comparison run exists.

    When there is no completed same-model history to compare against, the
    explainer should degrade cleanly to the coarse no_similar_prior_run reason.
    """

    with tracker.start_run(
        "lonely_run",
        model="lonely_model",
        config={"value": 1},
    ):
        pass

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["status"] == "miss"
    assert explanation["reason"] == "no_similar_prior_run"
    assert explanation["candidate_run_id"] is None
    assert explanation["matched_components"] == []
    assert explanation["mismatched_components"] == []
    assert explanation["confidence"] == "low"


def test_cache_miss_explanation_marks_candidate_outputs_invalid(tracker, caplog):
    """A validated exact-match candidate with missing outputs should explain why.

    This protects the cache-validation failure branch: the exact cache identity
    matches, but validation demotes the hit to a miss, so the explanation should
    explicitly say candidate_outputs_invalid rather than a config/input/code
    drift reason.
    """

    caplog.set_level(logging.INFO)

    def step(ctx) -> None:
        output_path = ctx.run_dir / "out.txt"
        output_path.write_text("seed\n", encoding="utf-8")

    seed_result = tracker.run(
        fn=step,
        name="seed_run",
        model="validation_model",
        config={"value": 1},
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert seed_result.outputs
    produced = next(iter(seed_result.outputs.values()))
    produced.path.unlink()

    tracker.run(
        fn=step,
        name="validation_miss_run",
        model="validation_model",
        config={"value": 1},
        output_paths={"out": "out.txt"},
        cache_options=CacheOptions(validate_cached_outputs="eager"),
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    explanation = tracker.last_run.run.meta["cache_miss_explanation"]

    assert explanation["status"] == "miss"
    assert explanation["reason"] == "candidate_outputs_invalid"
    assert explanation["candidate_run_id"].startswith("seed_run")
    assert explanation["matched_components"] == [
        "config_hash",
        "input_hash",
        "git_hash",
    ]
    assert explanation["mismatched_components"] == []
    assert explanation["details"] == {"candidate_output_validation_failure": True}
    assert any(
        "[Consist][cache] miss explanation: reason=candidate_outputs_invalid"
        in record.message
        for record in caplog.records
    )
    assert "input_artifact_changes" not in explanation["details"]
    assert explanation["details"].get("fallbacks_used") is None


def test_cache_miss_explanation_does_not_attach_input_details_to_config_only_miss():
    """Config-only misses should not include unrelated input-side detail payloads.

    The current and candidate runs deliberately differ only in config hash. Even
    if candidate input artifacts are unavailable, the explanation should stay
    focused on config drift and avoid adding input fallback noise.
    """

    current = Run(
        id="current",
        model_name="config_only_model",
        config_hash="config_current",
        input_hash="shared_input_hash",
        git_hash="shared_code_hash",
    )
    candidate = Run(
        id="candidate",
        model_name="config_only_model",
        config_hash="config_candidate",
        input_hash="shared_input_hash",
        git_hash="shared_code_hash",
        status="completed",
    )

    class StubTracker:
        db = object()
        current_consist = SimpleNamespace(inputs=[_artifact(key="base")], config={})

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "config_only_model"
            assert limit == 20
            return [candidate]

        def get_artifacts_for_run(self, run_id: str):
            raise RuntimeError("should not be consulted for config-only misses")

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "config_changed"
    assert "input_artifact_changes" not in explanation.details
    assert "input_keys_added" not in explanation.details
    assert "input_keys_removed" not in explanation.details
    assert explanation.details.get("fallbacks_used") is None


def test_cache_miss_explanation_handles_empty_mismatch_fallback() -> None:
    """An exact-match fallback candidate should not be mislabeled as 'no similar run'.

    This covers the case where the primary exact-match lookup did not yield a
    reusable hit, but the broader same-model candidate search still finds a run
    with identical top-level identity components.
    """

    current = Run(
        id="current",
        model_name="demo_model",
        config_hash="config_hash",
        input_hash="input_hash",
        git_hash="git_hash",
    )
    candidate = Run(
        id="candidate",
        model_name="demo_model",
        config_hash="config_hash",
        input_hash="input_hash",
        git_hash="git_hash",
        status="completed",
    )

    class StubTracker:
        db = object()
        current_consist = None

        def find_recent_completed_runs_for_model(
            self, model_name: str, *, limit: int = 20
        ) -> list[Run]:
            assert model_name == "demo_model"
            assert limit == 20
            return [candidate]

    explanation = CacheMissExplainer(StubTracker()).explain(current)

    assert explanation.reason == "exact_match_lookup_inconclusive"
    assert explanation.candidate_run_id == "candidate"
    assert explanation.matched_components == [
        "config_hash",
        "input_hash",
        "git_hash",
    ]
    assert explanation.mismatched_components == []
    assert explanation.confidence == "low"


def test_cache_hit_does_not_persist_cache_miss_explanation(tracker):
    """Successful cache hits should leave cache-miss metadata unset.

    The explainer is only for miss paths; this guards against accidental
    persistence of stale or misleading miss metadata on ordinary cache hits.
    """

    def step(ctx) -> None:
        output_path = ctx.run_dir / "out.txt"
        output_path.write_text("seed\n", encoding="utf-8")

    tracker.run(
        fn=step,
        name="seed_hit_run",
        model="cache_hit_model",
        config={"value": 1},
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )
    result = tracker.run(
        fn=step,
        name="cache_hit_run",
        model="cache_hit_model",
        config={"value": 1},
        output_paths={"out": "out.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
    )

    assert result.cache_hit is True
    assert "cache_miss_explanation" not in result.run.meta
