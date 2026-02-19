import json
from datetime import datetime, timezone
from pathlib import Path

from consist.core.openlineage import (
    OpenLineageEmitter,
    OpenLineageOptions,
    _dataset_name_from_artifact,
)
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def test_openlineage_dataset_name_suffixes():
    run = Run(
        id="r1",
        model_name="step",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.meta = {"year": 2026, "iteration": 1}

    assert _dataset_name_from_artifact(artifact, run) == "trips_2026_iteration_1"


def test_openlineage_jsonl_written(tmp_path: Path):
    run = Run(
        id="r1",
        model_name="step_a",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
        started_at=datetime.now(timezone.utc),
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.meta = {"year": 2025}

    emitter = OpenLineageEmitter(
        OpenLineageOptions(
            enabled=True, namespace="consist", path=tmp_path / "openlineage.jsonl"
        )
    )
    emitter.emit_start(run, inputs=[artifact], outputs=[])
    emitter.emit_complete(run, inputs=[artifact], outputs=[artifact])

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["eventType"] == "START"
    assert payload["job"]["name"] == "step_a"
    assert payload["job"]["namespace"] == "consist"
    assert payload["run"]["runId"] == "r1"
    assert payload["producer"] == "consist"
    assert "eventTime" in payload
    assert payload["job"]["facets"]["jobType"]["jobType"] == "consist_step"


def test_openlineage_schema_facet_from_meta(tmp_path: Path):
    run = Run(
        id="r1",
        model_name="step_a",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
        started_at=datetime.now(timezone.utc),
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.hash = "hash123"
    artifact.meta = {
        "schema_profile": {"fields": [{"name": "id", "logical_type": "bigint"}]}
    }

    emitter = OpenLineageEmitter(
        OpenLineageOptions(
            enabled=True, namespace="consist", path=tmp_path / "openlineage.jsonl"
        )
    )
    emitter.emit_complete(run, inputs=[], outputs=[artifact])

    payload = json.loads((tmp_path / "openlineage.jsonl").read_text().strip())
    schema = payload["outputs"][0]["facets"]["schema"]
    assert schema["fields"][0]["name"] == "id"
    assert schema["fields"][0]["type"] == "bigint"
    assert payload["outputs"][0]["facets"]["version"]["datasetVersion"] == "hash123"


def test_openlineage_parent_facet_for_scenario(tmp_path: Path):
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    with tracker.scenario("baseline") as sc:
        sc.run(lambda: None, name="step_a")

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    child_events = [
        evt
        for evt in events
        if evt["job"]["name"] == "step_a" and evt["eventType"] == "START"
    ]
    assert child_events
    parent_facet = child_events[0]["run"]["facets"]["parent"]
    assert parent_facet["job"]["name"] == "baseline"
    assert parent_facet["run"]["runId"] == "baseline"
    assert child_events[0]["job"]["facets"]["jobType"]["jobType"] == "consist_step"

    scenario_events = [
        evt
        for evt in events
        if evt["job"]["name"] == "baseline" and evt["eventType"] == "START"
    ]
    assert scenario_events
    assert (
        scenario_events[0]["job"]["facets"]["jobType"]["jobType"] == "consist_scenario"
    )


def test_openlineage_includes_config_facet(tmp_path: Path):
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    tracker.run(
        fn=lambda: None,
        name="step_a",
        config={"region": "north", "year": 2025},
        facet={"region": "north"},
    )

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    start_event = next(evt for evt in events if evt["eventType"] == "START")
    consist_facet = start_event["run"]["facets"]["consist"]
    assert consist_facet["config_facet"]["region"] == "north"
    assert "region" in consist_facet["config_keys"]
