from __future__ import annotations

import consist
import pytest
from consist import use_tracker
from consist.models.artifact import Artifact
from consist.types import CacheOptions, ExecutionOptions


def test_consist_run_forwards_options_objects(tracker):
    calls: list[str] = []

    def step(ctx) -> None:
        calls.append("called")
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    with use_tracker(tracker):
        with pytest.warns(DeprecationWarning, match="consist.run"):
            first = consist.run(
                fn=step,
                output_paths={"out": "out.txt"},
                cache_options=CacheOptions(cache_mode="reuse"),
                execution_options=ExecutionOptions(inject_context="ctx"),
            )
        with pytest.warns(DeprecationWarning, match="consist.run"):
            second = consist.run(
                fn=step,
                output_paths={"out": "out.txt"},
                cache_options=CacheOptions(cache_mode="reuse"),
                execution_options=ExecutionOptions(inject_context="ctx"),
            )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert calls == ["called"]


def test_consist_run_rejects_legacy_policy_kwargs(tracker):
    def step() -> None:
        return None

    with use_tracker(tracker):
        with pytest.raises(
            TypeError,
            match="consist.run no longer accepts legacy policy kwarg: `cache_mode`",
        ):
            consist.run(fn=step, cache_mode="reuse")


def test_consist_get_run_result_wrapper(tracker):
    def step(ctx) -> None:
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        (ctx.run_dir / "out.txt").write_text("ok")

    with use_tracker(tracker):
        produced = tracker.run(
            fn=step,
            output_paths={"out": "out.txt"},
            execution_options=ExecutionOptions(inject_context="ctx"),
        )
        historical = consist.get_run_result(produced.run.id, keys=["out"])

    assert historical.run.id == produced.run.id
    assert list(historical.outputs.keys()) == ["out"]


def test_consist_trace_forwards_identity_kwargs(tracker, tmp_path):
    identity_dep = tmp_path / "trace_identity_dep.txt"
    identity_dep.write_text("dep=true\n")

    with use_tracker(tracker):
        with pytest.warns(DeprecationWarning, match="consist.trace"):
            with consist.trace(
                "trace_identity_api",
                identity_inputs=[identity_dep],
                cache_version=5,
                cache_epoch=4,
                code_identity="repo_git",
                code_identity_extra_deps=[str(identity_dep)],
            ) as t:
                out_path = t.run_dir / "out.txt"
                out_path.write_text("ok")
                t.log_artifact(out_path, key="out", direction="output")

    run = tracker.last_run.run
    assert run.meta["cache_version"] == 5
    assert run.meta["cache_epoch"] == 4
    assert run.meta["code_identity"] == "repo_git"
    assert run.meta["code_identity_extra_deps"] == [str(identity_dep)]


def test_consist_start_run_warns_and_delegates(tracker) -> None:
    with use_tracker(tracker):
        with pytest.warns(DeprecationWarning, match="consist.start_run"):
            with consist.start_run("deprecated_start", "model") as active:
                assert active is tracker
                assert active.current_consist is not None
                assert active.current_consist.run.id == "deprecated_start"


def test_consist_ingest_delegates_without_warning(
    tracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: dict[str, object] = {}
    artifact = Artifact(
        key="table",
        container_uri="./table.csv",
        driver="csv",
        meta={},
    )

    def _fake_ingest(*, artifact, data=None, schema=None, run=None):
        calls.update(artifact=artifact, data=data, schema=schema, run=run)
        return "ingested"

    monkeypatch.setattr(tracker, "ingest", _fake_ingest)

    with tracker.start_run("ingest_warn", "model"):
        result = consist.ingest(artifact, data=[{"id": 1}])

    assert result == "ingested"
    assert calls == {
        "artifact": artifact,
        "data": [{"id": 1}],
        "schema": None,
        "run": None,
    }
