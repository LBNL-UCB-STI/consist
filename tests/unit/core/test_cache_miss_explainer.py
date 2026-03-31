from __future__ import annotations

import logging

from consist.core.cache_miss_explainer import CacheMissExplainer
from consist.models.run import Run
from consist.types import CacheOptions
from consist.types import ExecutionOptions


def test_cache_miss_explanation_records_changed_components(tracker, caplog):
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
    assert explanation["details"] == {}
    assert any(
        "[Consist][cache] miss explanation: reason=config_changed" in record.message
        for record in caplog.records
    )


def test_cache_miss_explanation_records_no_similar_prior_run(tracker):
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
    assert explanation["details"] == {
        "candidate_output_validation_failure": True
    }
    assert any(
        "[Consist][cache] miss explanation: reason=candidate_outputs_invalid"
        in record.message
        for record in caplog.records
    )


def test_cache_miss_explanation_handles_empty_mismatch_fallback() -> None:
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
