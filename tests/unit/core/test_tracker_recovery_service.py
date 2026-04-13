from __future__ import annotations

from pathlib import Path
import uuid

from consist.core.materialize import (
    HydratedRunOutput,
    HydratedRunOutputsResult,
)
from consist.models.artifact import Artifact


def test_recovery_service_stage_inputs_normalizes_destination_paths(
    monkeypatch,
    tracker,
) -> None:
    service = tracker._recovery_service
    artifact = Artifact(
        id=uuid.uuid4(),
        key="input",
        container_uri="inputs://input.csv",
        driver="csv",
    )
    captured: dict[str, object] = {}

    def fake_stage_inputs(
        tracker_arg,
        inputs_by_key,
        destinations_by_key,
        **kwargs,
    ):
        captured.update(
            tracker=tracker_arg,
            inputs_by_key=inputs_by_key,
            destinations_by_key=destinations_by_key,
            kwargs=kwargs,
        )
        return "sentinel"

    monkeypatch.setattr(
        "consist.core.tracker_recovery.stage_inputs_core",
        fake_stage_inputs,
    )

    result = service.stage_inputs(
        {"input": artifact},
        destinations_by_key={"input": "relative/staged.csv"},
        overwrite=True,
    )

    assert result == "sentinel"
    assert captured["tracker"] is tracker
    assert captured["inputs_by_key"] == {"input": artifact}
    assert captured["destinations_by_key"] == {"input": Path("relative/staged.csv")}
    assert captured["kwargs"]["overwrite"] is True


def test_recovery_service_materialize_run_outputs_folds_tracker_results(
    monkeypatch,
    tracker,
    tmp_path: Path,
) -> None:
    service = tracker._recovery_service
    artifact = Artifact(
        id=uuid.uuid4(),
        key="result",
        container_uri="outputs://result.csv",
        driver="csv",
    )
    restored_path = tmp_path / "restored" / "result.csv"
    hydrated = HydratedRunOutputsResult(
        outputs={
            "result": HydratedRunOutput(
                key="result",
                artifact=artifact,
                path=restored_path,
                status="materialized_from_filesystem",
                resolvable=True,
            )
        }
    )
    captured: dict[str, object] = {}

    def fake_hydrate_run_outputs(run_id: str, **kwargs):
        captured.update(run_id=run_id, kwargs=kwargs)
        return hydrated

    monkeypatch.setattr(tracker, "hydrate_run_outputs", fake_hydrate_run_outputs)

    result = service.materialize_run_outputs(
        "prior-run",
        target_root=tmp_path / "restored",
        keys=["result"],
        preserve_existing=False,
    )

    assert captured["run_id"] == "prior-run"
    assert captured["kwargs"]["keys"] == ["result"]
    assert captured["kwargs"]["preserve_existing"] is False
    assert result.materialized_from_filesystem == {"result": str(restored_path)}
